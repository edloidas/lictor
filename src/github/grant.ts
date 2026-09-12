import { createHash } from 'node:crypto';
import { Schema } from 'effect';
import type { Capabilities, RepositoryPolicy } from '../policy.ts';
import type { WorkItem } from '../work-item.ts';

export type BrokerTool =
  | 'create_branch'
  | 'create_blob'
  | 'create_comment'
  | 'create_commit'
  | 'create_issue'
  | 'create_tree'
  | 'create_pull_request'
  | 'get_issue'
  | 'get_pull_request'
  | 'get_repository'
  | 'list_comments'
  | 'list_review_threads'
  | 'list_review_comments'
  | 'merge_pull_request'
  | 'update_branch'
  | 'update_issue';

/**
 * Narrower than `keyof Capabilities` on purpose: no tool maps to `forcePush`
 * (an argument on `update_branch`, gated per call) or `deleteBranches` (nothing
 * deletes a branch). Widening it lets a comparison against either compile, and
 * it can only ever be false.
 */
export type ToolCapability = 'read' | 'comment' | 'issues' | 'branches' | 'pullRequests' | 'merge';

export const toolCapabilities: Readonly<Record<BrokerTool, ToolCapability>> = {
  get_issue: 'read',
  get_pull_request: 'read',
  get_repository: 'read',
  list_comments: 'read',
  list_review_threads: 'read',
  list_review_comments: 'read',
  create_comment: 'comment',
  create_issue: 'issues',
  update_issue: 'issues',
  create_branch: 'branches',
  create_blob: 'branches',
  create_commit: 'branches',
  create_tree: 'branches',
  create_pull_request: 'pullRequests',
  merge_pull_request: 'merge',
  update_branch: 'branches',
};

/**
 * Policy's capabilities with `undefined` settled to `false` before storage, and
 * without `scripts`, which is workspace authority rather than the broker's.
 */
export type GrantCapabilities = {
  readonly read: boolean;
  readonly comment: boolean;
  readonly issues: boolean;
  readonly branches: boolean;
  readonly pullRequests: boolean;
  readonly merge: boolean;
  readonly forcePush: boolean;
  readonly deleteBranches: boolean;
};

/**
 * What the daemon authorized one job to do, recorded when it decided.
 *
 * ! Effective authority is this record intersected with policy as it stands, so
 * ! a policy widened after the mint cannot raise the ceiling while a tightening
 * ! still bites. Nothing writes the intersection back: a narrowing that is later
 * ! reverted has to recover, and a stored minimum never does.
 */
export type Grant = {
  readonly version: 1;
  readonly repository: string;
  readonly interactionId: string;
  /** Whether policy admitted this automatically, or an operator released the hold. */
  readonly decision: 'automatic' | 'approved';
  readonly continuation: boolean;
  readonly mintedAt: number;
  readonly capabilities: GrantCapabilities;
  readonly maxAttempts: number;
  readonly maxDurationMs: number;
  /**
   * Diagnostic only — the intersection is what enforces. Covers what the grant
   * records and nothing else, so a `trustedSenders` or `scripts` edit does not
   * move it.
   */
  readonly fingerprint: string;
};

const GrantCapabilitiesSchema = Schema.Struct({
  read: Schema.Boolean,
  comment: Schema.Boolean,
  issues: Schema.Boolean,
  branches: Schema.Boolean,
  pullRequests: Schema.Boolean,
  merge: Schema.Boolean,
  forcePush: Schema.Boolean,
  deleteBranches: Schema.Boolean,
});

export const GrantSchema: Schema.Schema<Grant> = Schema.Struct({
  version: Schema.Literal(1),
  repository: Schema.String,
  interactionId: Schema.String,
  decision: Schema.Literal('automatic', 'approved'),
  continuation: Schema.Boolean,
  mintedAt: Schema.Number,
  capabilities: GrantCapabilitiesSchema,
  maxAttempts: Schema.Number,
  maxDurationMs: Schema.Number,
  fingerprint: Schema.String,
});

const capabilityKeys = [
  'read',
  'comment',
  'issues',
  'branches',
  'pullRequests',
  'merge',
  'forcePush',
  'deleteBranches',
] as const satisfies readonly (keyof GrantCapabilities)[];

/**
 * What a policy tightened since the mint took from a job that still holds the
 * wider grant.
 *
 * Diagnostic only, like the fingerprint — the intersection is what enforces.
 * It exists because the partial case is otherwise silent: withdrawing every
 * capability refuses the job with a reason, and lowering the attempt ceiling
 * past what it has spent refuses it through `policyRefusal`, but revoking one
 * capability of several just runs the job narrower with nothing on the row.
 */
export type GrantNarrowing = {
  /** The policy the grant records, as the mint fingerprinted it. */
  readonly grantFingerprint: string;
  /** Policy as it stood when the job ran. */
  readonly policyFingerprint: string;
  readonly withheld: readonly (typeof capabilityKeys)[number][];
  /** Each present only where current policy lowered the recorded budget. */
  readonly maxAttempts?: number;
  readonly maxDurationMs?: number;
};

export const GrantNarrowingSchema: Schema.Schema<GrantNarrowing> = Schema.Struct({
  grantFingerprint: Schema.String,
  policyFingerprint: Schema.String,
  withheld: Schema.Array(Schema.Literal(...capabilityKeys)),
  maxAttempts: Schema.optionalWith(Schema.Number, { exact: true }),
  maxDurationMs: Schema.optionalWith(Schema.Number, { exact: true }),
});

/** Fixed key order, so the fingerprint is stable without a canonicalizer. */
export const grantCapabilities = (capabilities: Capabilities): GrantCapabilities => ({
  read: capabilities.read === true,
  comment: capabilities.comment === true,
  issues: capabilities.issues === true,
  branches: capabilities.branches === true,
  pullRequests: capabilities.pullRequests === true,
  merge: capabilities.merge === true,
  forcePush: capabilities.forcePush === true,
  deleteBranches: capabilities.deleteBranches === true,
});

const fingerprintOf = (policy: RepositoryPolicy): string =>
  createHash('sha256')
    .update(
      JSON.stringify({
        execution: policy.execution,
        clone: policy.clone,
        capabilities: grantCapabilities(policy.capabilities),
        maxAttempts: policy.maxAttempts,
        maxDurationMs: policy.maxDurationMs,
      }),
    )
    .digest('hex')
    .slice(0, 16);

export const mintGrant = (policy: RepositoryPolicy, work: WorkItem, now: number): Grant => ({
  version: 1,
  repository: policy.repository,
  interactionId: work.interactionId,
  // Only `approve` writes the literal `false`; an automatic repository leaves it
  // absent, so this cannot read a job that was never held as one that was.
  decision: work.approvalRequired === false ? 'approved' : 'automatic',
  continuation: work.continuation === true,
  mintedAt: now,
  capabilities: grantCapabilities(policy.capabilities),
  maxAttempts: policy.maxAttempts,
  maxDurationMs: policy.maxDurationMs,
  fingerprint: fingerprintOf(policy),
});

export const intersectCapabilities = (
  stored: GrantCapabilities,
  live: GrantCapabilities,
): GrantCapabilities => ({
  read: stored.read && live.read,
  comment: stored.comment && live.comment,
  issues: stored.issues && live.issues,
  branches: stored.branches && live.branches,
  pullRequests: stored.pullRequests && live.pullRequests,
  merge: stored.merge && live.merge,
  forcePush: stored.forcePush && live.forcePush,
  deleteBranches: stored.deleteBranches && live.deleteBranches,
});

/** Identity stays the stored grant's: what may run now, not a second authorization. */
export const intersectGrant = (stored: Grant, live: Grant): Grant => ({
  ...stored,
  capabilities: intersectCapabilities(stored.capabilities, live.capabilities),
  maxAttempts: Math.min(stored.maxAttempts, live.maxAttempts),
  maxDurationMs: Math.min(stored.maxDurationMs, live.maxDurationMs),
});

/**
 * What the intersection takes, or `undefined` where it takes nothing. A
 * fingerprint that merely differs is not a narrowing: policy may have widened,
 * or moved a field the grant does not record.
 */
export const grantNarrowing = (stored: Grant, live: Grant): GrantNarrowing | undefined => {
  const effective = intersectGrant(stored, live);
  const withheld = capabilityKeys.filter(
    (capability) => stored.capabilities[capability] && !effective.capabilities[capability],
  );
  const attempts = effective.maxAttempts < stored.maxAttempts ? effective.maxAttempts : undefined;
  const duration =
    effective.maxDurationMs < stored.maxDurationMs ? effective.maxDurationMs : undefined;
  if (withheld.length === 0 && attempts === undefined && duration === undefined) return undefined;
  return {
    grantFingerprint: stored.fingerprint,
    policyFingerprint: live.fingerprint,
    withheld,
    ...(attempts === undefined ? {} : { maxAttempts: attempts }),
    ...(duration === undefined ? {} : { maxDurationMs: duration }),
  };
};

// Escalation is `merge`, `forcePush`, `deleteBranches`; only `merge` is a `ToolCapability`.
const withheldOnContinuation = (capability: ToolCapability): boolean => capability === 'merge';

export const grantedTools = (
  capabilities: GrantCapabilities,
  narrowed: boolean,
): readonly BrokerTool[] =>
  (Object.keys(toolCapabilities) as BrokerTool[]).filter((tool) => {
    const capability = toolCapabilities[tool];
    return capabilities[capability] && !(narrowed && withheldOnContinuation(capability));
  });

/**
 * The granted tools as the prompt names them. The force gate is on an argument,
 * not the tool, so a bare `update_branch` under a policy that grants `branches`
 * and denies `forcePush` — the shipped default — would advertise a call that
 * answers `CAPABILITY_DENIED`. A continuation is never allowed force at all.
 */
export const describeGrantedTools = (
  capabilities: GrantCapabilities,
  narrowed: boolean,
): readonly string[] =>
  grantedTools(capabilities, narrowed).map((tool) =>
    tool === 'update_branch' && (!capabilities.forcePush || narrowed)
      ? `${tool} (never with force)`
      : tool,
  );
