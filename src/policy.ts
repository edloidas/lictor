import { Data, Effect, Schema } from 'effect';
import { LictorConfig } from './config.ts';

const Execution = Schema.Literal('automatic', 'approval', 'denied');
const Clone = Schema.Literal('allowed', 'denied');
const Capabilities = Schema.Struct({
  read: Schema.optional(Schema.Boolean),
  comment: Schema.optional(Schema.Boolean),
  issues: Schema.optional(Schema.Boolean),
  branches: Schema.optional(Schema.Boolean),
  pullRequests: Schema.optional(Schema.Boolean),
  merge: Schema.optional(Schema.Boolean),
  forcePush: Schema.optional(Schema.Boolean),
  deleteBranches: Schema.optional(Schema.Boolean),
  scripts: Schema.optional(Schema.Array(Schema.String)),
});
const Costs = Schema.Struct({
  maxAttempts: Schema.optional(Schema.Number),
  maxDurationMinutes: Schema.optional(Schema.Number),
});
const RepositoryOverride = Schema.Struct({
  execution: Schema.optional(Execution),
  clone: Schema.optional(Clone),
  capabilities: Schema.optional(Capabilities),
  costs: Schema.optional(Costs),
});
const PolicyDocument = Schema.Struct({
  defaults: Schema.optional(
    Schema.Struct({
      execution: Schema.optional(Execution),
      clone: Schema.optional(Clone),
      capabilities: Schema.optional(Capabilities),
    }),
  ),
  repositories: Schema.optional(
    Schema.Struct({
      allow: Schema.optional(Schema.Array(Schema.String)),
      deny: Schema.optional(Schema.Array(Schema.String)),
      overrides: Schema.optional(Schema.Record({ key: Schema.String, value: RepositoryOverride })),
    }),
  ),
  retention: Schema.optional(
    Schema.Struct({
      completedDays: Schema.optional(Schema.Number),
      failedDays: Schema.optional(Schema.Number),
    }),
  ),
  limits: Schema.optional(
    Schema.Struct({
      maxQueueDepth: Schema.optional(Schema.Number),
      maxJobAgeMinutes: Schema.optional(Schema.Number),
      costs: Schema.optional(Costs),
    }),
  ),
});

type PolicyDocument = Schema.Schema.Type<typeof PolicyDocument>;
type Capabilities = Required<Schema.Schema.Type<typeof Capabilities>>;

export type RepositoryPolicy = {
  readonly repository: string;
  readonly accepted: boolean;
  readonly execution: 'automatic' | 'approval' | 'denied';
  readonly clone: 'allowed' | 'denied';
  readonly capabilities: Capabilities;
  readonly maxAttempts: number;
  readonly maxDurationMs: number;
};

export class PolicyError extends Data.TaggedError('PolicyError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const defaultCapabilities: Capabilities = {
  read: true,
  comment: false,
  issues: false,
  branches: false,
  pullRequests: false,
  merge: false,
  forcePush: false,
  deleteBranches: false,
  scripts: [],
};

const canonicalRepository = (repository: string): string => repository.trim().toLowerCase();

// ! Underscores are legal in an owner: an Enterprise Managed User login is
// ! `shortname_enterprise`, and rejecting one refuses that whole namespace.
const ownerPattern = /^[a-z0-9](?:[a-z0-9_-]{0,37}[a-z0-9])?$/;
const namePattern = /^[a-z0-9_.-]{1,100}$/;

/**
 * Whether a name is safe to treat as one repository and to join into a path.
 *
 * Repository names arrive from GitHub payloads, and the daemon computes a
 * filesystem path from them. This is the only check standing between that input
 * and a `join`, which is why dot-only segments are rejected explicitly: the
 * character class permits `..`.
 *
 * A leading dot stays legal — `owner/.github` is a real repository — so nothing
 * the daemon puts beside a clone may be named like one.
 */
export const isSafeRepository = (repository: string): boolean => {
  const segments = canonicalRepository(repository).split('/');
  const [account, project] = segments;
  if (segments.length !== 2 || account === undefined || project === undefined) return false;
  if (project === '.' || project === '..') return false;
  return ownerPattern.test(account) && namePattern.test(project);
};

const validateExact = (repository: string): string => {
  const canonical = canonicalRepository(repository);
  if (!isSafeRepository(canonical)) {
    throw new PolicyError({ message: `Invalid repository name: ${repository}` });
  }
  return canonical;
};

const denyPattern = /^[a-z0-9_.*-]+\/[a-z0-9_.*-]+$/;

const validateDenyPattern = (pattern: string): string => {
  const canonical = canonicalRepository(pattern);
  if (!denyPattern.test(canonical) || canonical.split('/').includes('..')) {
    throw new PolicyError({ message: `Invalid repository pattern: ${pattern}` });
  }
  return canonical;
};

const patternMatches = (pattern: string, repository: string): boolean => {
  const escaped = canonicalRepository(pattern)
    .replace(/[.+?^${}()|[\]\\]/g, '\\$&')
    .replaceAll('**', '\u0000')
    .replaceAll('*', '[^/]*')
    .replaceAll('\u0000', '.*');
  return new RegExp(`^${escaped}$`).test(repository);
};

const positiveDays = (value: number | undefined, fallback: number, name: string): number => {
  const resolved = value ?? fallback;
  if (!Number.isInteger(resolved) || resolved < 1 || resolved > 3650) {
    throw new PolicyError({ message: `${name} must be an integer between 1 and 3650` });
  }
  return resolved;
};

const positiveLimit = (
  value: number | undefined,
  fallback: number,
  maximum: number,
  name: string,
) => {
  const resolved = value ?? fallback;
  if (!Number.isInteger(resolved) || resolved < 1 || resolved > maximum) {
    throw new PolicyError({ message: `${name} must be an integer between 1 and ${maximum}` });
  }
  return resolved;
};

export type AutomationPolicy = {
  readonly completedRetentionDays: number;
  readonly failedRetentionDays: number;
  readonly maxQueueDepth: number;
  readonly maxJobAgeMs: number;
  readonly forRepository: (repository: string) => RepositoryPolicy;
};

const makePolicy = (document: PolicyDocument): AutomationPolicy => {
  const defaults = document.defaults;
  const repositories = document.repositories;
  // ! Exact names only. An owner wildcard would arm every repository the
  // ! account is ever invited to, collapsing reach and permission into one key.
  // ! Deny keeps its patterns: a wildcard there can only ever subtract.
  const allow = new Set((repositories?.allow ?? []).map(validateExact));
  const deny = (repositories?.deny ?? []).map(validateDenyPattern);
  const overrides = new Map<string, Schema.Schema.Type<typeof RepositoryOverride>>();
  const defaultCosts = document.limits?.costs;
  const defaultMaxAttempts = positiveLimit(defaultCosts?.maxAttempts, 3, 100, 'costs.maxAttempts');
  const defaultMaxDurationMinutes = positiveLimit(
    defaultCosts?.maxDurationMinutes,
    30,
    24 * 60,
    'costs.maxDurationMinutes',
  );
  for (const [repository, override] of Object.entries(repositories?.overrides ?? {})) {
    const canonical = validateExact(repository);
    if (overrides.has(canonical)) {
      throw new PolicyError({ message: `Duplicate repository override: ${repository}` });
    }
    positiveLimit(override.costs?.maxAttempts, defaultMaxAttempts, 100, 'costs.maxAttempts');
    positiveLimit(
      override.costs?.maxDurationMinutes,
      defaultMaxDurationMinutes,
      24 * 60,
      'costs.maxDurationMinutes',
    );
    overrides.set(canonical, override);
  }

  const forRepository = (input: string): RepositoryPolicy => {
    const repository = canonicalRepository(input);
    const denied = deny.some((pattern) => patternMatches(pattern, repository));
    const allowed = isSafeRepository(repository) && allow.has(repository);
    const override = overrides.get(repository);
    const capabilities = {
      ...defaultCapabilities,
      ...defaults?.capabilities,
      ...override?.capabilities,
      scripts: override?.capabilities?.scripts ?? defaults?.capabilities?.scripts ?? [],
    };

    return {
      repository,
      accepted: allowed && !denied,
      execution: override?.execution ?? defaults?.execution ?? 'automatic',
      clone: override?.clone ?? defaults?.clone ?? 'denied',
      capabilities,
      maxAttempts: positiveLimit(
        override?.costs?.maxAttempts,
        defaultMaxAttempts,
        100,
        'costs.maxAttempts',
      ),
      maxDurationMs:
        positiveLimit(
          override?.costs?.maxDurationMinutes,
          defaultMaxDurationMinutes,
          24 * 60,
          'costs.maxDurationMinutes',
        ) * 60_000,
    };
  };

  return {
    completedRetentionDays: positiveDays(
      document.retention?.completedDays,
      30,
      'retention.completedDays',
    ),
    failedRetentionDays: positiveDays(document.retention?.failedDays, 90, 'retention.failedDays'),
    maxQueueDepth: positiveLimit(
      document.limits?.maxQueueDepth,
      10_000,
      1_000_000,
      'limits.maxQueueDepth',
    ),
    maxJobAgeMs:
      positiveLimit(
        document.limits?.maxJobAgeMinutes,
        24 * 60,
        30 * 24 * 60,
        'limits.maxJobAgeMinutes',
      ) * 60_000,
    forRepository,
  };
};

export const parsePolicy = (source: string): Effect.Effect<AutomationPolicy, PolicyError> =>
  Effect.try({
    try: () => Bun.TOML.parse(source),
    catch: (cause) => new PolicyError({ message: 'Could not parse policy TOML', cause }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(PolicyDocument, { onExcessProperty: 'error' })),
    Effect.flatMap((document) =>
      Effect.try({
        try: () => makePolicy(document),
        catch: (cause) =>
          cause instanceof PolicyError
            ? cause
            : new PolicyError({ message: 'Could not build policy', cause }),
      }),
    ),
    Effect.mapError((cause) =>
      cause._tag === 'PolicyError'
        ? cause
        : new PolicyError({ message: `Policy schema error: ${String(cause)}`, cause }),
    ),
  );

export class Policy extends Effect.Service<Policy>()('Policy', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const source = yield* Effect.tryPromise({
      try: () => Bun.file(config.policyPath).text(),
      catch: (cause) =>
        new PolicyError({ message: `Could not read policy: ${config.policyPath}`, cause }),
    });
    return yield* parsePolicy(source);
  }),
  dependencies: [LictorConfig.Default],
}) {}
