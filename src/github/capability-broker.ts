import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Effect } from 'effect';
import { Policy } from '../policy.ts';
import { WorkQueue } from '../queue/work-queue.ts';
import { GitHubClient } from './client.ts';
import { CredentialHealth } from './credential-health.ts';
import { GitHubIdentity } from './identity.ts';
import { DEFAULT_THROTTLE_WAIT_MS, isSecondaryRateLimit, retryAfterMs } from './retry-after.ts';

export type BrokerTool =
  | 'create_branch'
  | 'create_blob'
  | 'create_comment'
  | 'create_commit'
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

export class CapabilityError extends Data.TaggedError('CapabilityError')<{
  readonly code: string;
  readonly message: string;
  /** How long to wait before retrying, when GitHub said so. */
  readonly retryAfterMs?: number;
  readonly cause?: unknown;
}> {}

const capabilities: Readonly<
  Record<BrokerTool, keyof ReturnType<InstanceType<typeof Policy>['forRepository']>['capabilities']>
> = {
  get_issue: 'read',
  get_pull_request: 'read',
  get_repository: 'read',
  list_comments: 'read',
  list_review_threads: 'read',
  list_review_comments: 'read',
  create_comment: 'comment',
  update_issue: 'issues',
  create_branch: 'branches',
  create_blob: 'branches',
  create_commit: 'branches',
  create_tree: 'branches',
  create_pull_request: 'pullRequests',
  merge_pull_request: 'merge',
  update_branch: 'branches',
};

const issueNumber = { type: 'integer', minimum: 1, description: 'Issue number.' } as const;
const commentableNumber = {
  type: 'integer',
  minimum: 1,
  description: 'Issue or pull request number; both are addressed as issues here.',
} as const;
const pullNumber = { type: 'integer', minimum: 1, description: 'Pull request number.' } as const;
const pageNumber = {
  type: 'integer',
  minimum: 1,
  description: 'Page of results, three per page. Defaults to 1.',
} as const;
const repositoryProperty = {
  type: 'string',
  description:
    'Optional `owner/name`. It must match the repository the job was created for; any other value is denied.',
} as const;

/**
 * The whole input object becomes the request body, so a property this table
 * does not name is one the agent has to guess. What it advertises has to agree
 * with what `route` enforces, `ref` patterns included.
 */
const toolSchemas: Readonly<
  Record<
    BrokerTool,
    {
      readonly description: string;
      readonly properties: Readonly<Record<string, unknown>>;
      readonly required?: readonly string[];
    }
  >
> = {
  get_repository: {
    description: 'Read the job repository: default branch, visibility and granted permissions.',
    properties: {},
  },
  get_issue: {
    description: 'Read one issue: title, body, state, labels and assignees.',
    properties: { number: issueNumber },
    required: ['number'],
  },
  get_pull_request: {
    description: 'Read one pull request: title, body, state, head and base refs, mergeability.',
    properties: { number: pullNumber },
    required: ['number'],
  },
  list_comments: {
    description: 'List comments on an issue or pull request, three per page, oldest first.',
    properties: { number: commentableNumber, page: pageNumber },
    required: ['number'],
  },
  list_review_threads: {
    description:
      'List review threads on a pull request with their comments and resolution state, ten per page.',
    properties: {
      number: pullNumber,
      after: {
        type: 'string',
        description:
          'Cursor from the previous page, `pageInfo.endCursor`. Omit for the first page.',
      },
    },
    required: ['number'],
  },
  list_review_comments: {
    description: 'List review comments on a pull request, three per page.',
    properties: { number: pullNumber, page: pageNumber },
    required: ['number'],
  },
  create_comment: {
    description: 'Post a comment on an issue or pull request.',
    properties: {
      number: commentableNumber,
      body: { type: 'string', description: 'Comment text, GitHub-flavored markdown.' },
    },
    required: ['number', 'body'],
  },
  update_issue: {
    description: 'Update an issue. Only the fields sent are changed.',
    properties: {
      number: issueNumber,
      title: { type: 'string' },
      body: { type: 'string' },
      state: { type: 'string', enum: ['open', 'closed'] },
      state_reason: { type: 'string', enum: ['completed', 'not_planned', 'reopened'] },
      labels: {
        type: 'array',
        items: { type: 'string' },
        description: 'Replaces the label set; it is not additive.',
      },
      assignees: {
        type: 'array',
        items: { type: 'string' },
        description: 'Replaces the assignee set; it is not additive.',
      },
    },
    required: ['number'],
  },
  create_branch: {
    description: 'Create a branch pointing at an existing commit.',
    properties: {
      ref: {
        type: 'string',
        pattern: '^refs/heads/(?!.*\\.\\.)[A-Za-z0-9._/-]+$',
        description:
          'Full ref of the new branch, `refs/heads/<name>`. Note the `refs/` prefix — `update_branch` takes the short form instead. The name allows only letters, digits, `.`, `_`, `-` and `/`, and may not contain `..`.',
      },
      sha: { type: 'string', description: 'Commit SHA the new branch points at.' },
    },
    required: ['ref', 'sha'],
  },
  update_branch: {
    description: 'Move an existing branch to another commit.',
    properties: {
      ref: {
        type: 'string',
        pattern: '^heads/(?!.*\\.\\.)[A-Za-z0-9._/-]+$',
        description:
          'Short ref of the branch to move, `heads/<name>`, with no `refs/` prefix — unlike `create_branch`. The name allows only letters, digits, `.`, `_`, `-` and `/`, and may not contain `..`.',
      },
      sha: { type: 'string', description: 'Commit SHA to move the branch to.' },
      force: {
        type: 'boolean',
        description:
          'Move the branch even when the new commit is not a descendant. Denied unless repository policy grants force pushes, and denied outright on a continuation whatever policy says.',
      },
    },
    required: ['ref', 'sha'],
  },
  create_blob: {
    description: 'Store file content as a blob and return its SHA, for use in a tree entry.',
    properties: {
      content: {
        type: 'string',
        description:
          'File content, encoded as `encoding` says. The 256 KiB cap is on the whole serialized call, not on this field, so a large blob may need splitting.',
      },
      encoding: { type: 'string', enum: ['utf-8', 'base64'], description: 'Defaults to `utf-8`.' },
    },
    required: ['content'],
  },
  create_tree: {
    description: 'Build a tree from entries, usually over an existing tree.',
    properties: {
      tree: {
        type: 'array',
        description:
          'Entries to write. The whole call is capped at 256 KiB once serialized, so send bulk content through `create_blob` and reference the SHA here.',
        items: {
          type: 'object',
          properties: {
            path: { type: 'string', description: 'Path relative to the repository root.' },
            mode: {
              type: 'string',
              enum: ['100644', '100755', '040000', '160000', '120000'],
              description: 'File mode: `100644` for a regular file, `100755` for an executable.',
            },
            type: { type: 'string', enum: ['blob', 'tree', 'commit'] },
            sha: {
              type: ['string', 'null'],
              description: 'Blob SHA from `create_blob`. Null deletes the path.',
            },
            content: {
              type: 'string',
              description: 'Inline content, as an alternative to `sha`.',
            },
          },
          required: ['path', 'mode', 'type'],
          anyOf: [{ required: ['sha'] }, { required: ['content'] }],
        },
      },
      base_tree: {
        type: 'string',
        description: 'Tree SHA to layer the entries onto. Omit it and every unlisted path is gone.',
      },
    },
    required: ['tree'],
  },
  create_commit: {
    description:
      'Create a commit object. Authorship is pinned to the account the daemon runs as; `author` and `committer` are dropped.',
    properties: {
      message: { type: 'string', description: 'Commit message.' },
      tree: { type: 'string', description: 'Tree SHA from `create_tree`.' },
      parents: {
        type: 'array',
        items: { type: 'string' },
        description: 'Parent commit SHAs, the branch tip first.',
      },
    },
    required: ['message', 'tree', 'parents'],
  },
  create_pull_request: {
    description: 'Open a pull request between two branches of the job repository.',
    properties: {
      title: { type: 'string' },
      head: { type: 'string', description: 'Branch the changes are on, as a bare name.' },
      base: { type: 'string', description: 'Branch to merge into, as a bare name.' },
      body: { type: 'string', description: 'Description, GitHub-flavored markdown.' },
      draft: { type: 'boolean' },
    },
    required: ['title', 'head', 'base'],
  },
  merge_pull_request: {
    description: 'Merge a pull request.',
    properties: {
      number: pullNumber,
      merge_method: {
        type: 'string',
        enum: ['merge', 'squash', 'rebase'],
        description: 'Defaults to `merge`. The repository may not allow every method.',
      },
      commit_title: { type: 'string' },
      commit_message: { type: 'string' },
      sha: {
        type: 'string',
        description: 'Head SHA the merge expects; the merge fails if the branch has moved past it.',
      },
    },
    required: ['number'],
  },
};

const number = (input: Readonly<Record<string, unknown>>, name: string): number => {
  const value = input[name];
  if (!Number.isInteger(value) || Number(value) < 1)
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: `${name} must be a positive integer`,
    });
  return Number(value);
};

const branchRef = (input: Readonly<Record<string, unknown>>): string => {
  const value = input.ref;
  if (typeof value !== 'string' || !/^heads\/[a-z0-9._/-]+$/i.test(value) || value.includes('..')) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: 'ref must name a branch under heads/',
    });
  }
  return value;
};

const createdBranchRef = (input: Readonly<Record<string, unknown>>): void => {
  const value = input.ref;
  if (
    typeof value !== 'string' ||
    !/^refs\/heads\/[a-z0-9._/-]+$/i.test(value) ||
    value.includes('..')
  ) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: 'ref must name a branch under refs/heads/',
    });
  }
};

const boundedInput = (input: Readonly<Record<string, unknown>>): void => {
  const bytes = Buffer.byteLength(JSON.stringify(input));
  if (bytes > 256 * 1024) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_TOO_LARGE',
      message: 'Capability input exceeds 256 KiB',
    });
  }
};

/**
 * Commit authorship is GitHub's to assign, not the agent's.
 *
 * The tool schema allows additional properties, so `create_commit` would
 * otherwise forward whatever `author` and `committer` the agent supplied. That
 * was cosmetic while every commit was visibly `lictor[bot]`; once commits carry
 * a person's account it becomes a way to attribute work to someone who did not
 * do it. Dropping the fields makes GitHub fall back to the authenticated user.
 */
const pinCommitIdentity = (
  tool: BrokerTool,
  input: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => {
  if (tool !== 'create_commit') return input;
  const { author: _author, committer: _committer, ...rest } = input;
  return rest;
};

const route = (repository: string, tool: BrokerTool, input: Readonly<Record<string, unknown>>) => {
  const base = `/repos/${repository}`;
  switch (tool) {
    case 'get_repository':
      return { method: 'GET', path: base } as const;
    case 'get_issue':
      return { method: 'GET', path: `${base}/issues/${number(input, 'number')}` } as const;
    case 'get_pull_request':
      return { method: 'GET', path: `${base}/pulls/${number(input, 'number')}` } as const;
    case 'list_comments':
      return {
        method: 'GET',
        path: `${base}/issues/${number(input, 'number')}/comments?per_page=3&page=${input.page === undefined ? 1 : number(input, 'page')}`,
      } as const;
    case 'list_review_threads':
      return { method: 'POST', path: '/graphql' } as const;
    case 'list_review_comments':
      return {
        method: 'GET',
        path: `${base}/pulls/${number(input, 'number')}/comments?per_page=3&page=${input.page === undefined ? 1 : number(input, 'page')}`,
      } as const;
    case 'create_comment':
      return {
        method: 'POST',
        path: `${base}/issues/${number(input, 'number')}/comments`,
      } as const;
    case 'update_issue':
      return { method: 'PATCH', path: `${base}/issues/${number(input, 'number')}` } as const;
    case 'create_branch':
      createdBranchRef(input);
      return { method: 'POST', path: `${base}/git/refs` } as const;
    case 'update_branch':
      return { method: 'PATCH', path: `${base}/git/refs/${branchRef(input)}` } as const;
    case 'create_blob':
      return { method: 'POST', path: `${base}/git/blobs` } as const;
    case 'create_tree':
      return { method: 'POST', path: `${base}/git/trees` } as const;
    case 'create_commit':
      return { method: 'POST', path: `${base}/git/commits` } as const;
    case 'create_pull_request':
      return { method: 'POST', path: `${base}/pulls` } as const;
    case 'merge_pull_request':
      return { method: 'PUT', path: `${base}/pulls/${number(input, 'number')}/merge` } as const;
  }
};

/** `remaining quota N` when GitHub reported the bucket, for non-2xx diagnostics. */
const quotaNote = (headers: Record<string, string | undefined>): string | undefined => {
  const remaining = headers['x-ratelimit-remaining'];
  return remaining === undefined ? undefined : `remaining quota ${remaining}`;
};

const sanitized = (input: Readonly<Record<string, unknown>>): string => {
  const clean = Object.fromEntries(
    Object.entries(input).map(([key, value]) => [
      key,
      /token|secret|authorization|private.?key/i.test(key) ? '[REDACTED]' : value,
    ]),
  );
  return Buffer.from(JSON.stringify(clean))
    .subarray(0, 4096)
    .toString('utf8')
    .replace(/\uFFFD$/u, '');
};

const boundedJson = (value: unknown): unknown => {
  const encoded = Buffer.from(
    JSON.stringify(value, (_key, item) =>
      typeof item === 'string' && Buffer.byteLength(item) > 4096
        ? Buffer.from(item)
            .subarray(0, 4096)
            .toString('utf8')
            .replace(/\uFFFD$/u, '')
        : item,
    ),
  );
  if (encoded.byteLength > 64 * 1024) throw new Error('GitHub response exceeds broker limit');
  return JSON.parse(encoded.toString('utf8')) as unknown;
};

export class CapabilityBroker extends Effect.Service<CapabilityBroker>()('CapabilityBroker', {
  effect: Effect.gen(function* () {
    const github = yield* GitHubClient;
    const identity = yield* GitHubIdentity;
    const health = yield* CredentialHealth;
    // Resolved per call, not construction: building the broker costs no network;
    // the first call pays for the probe.
    const actor = identity.verified.pipe(
      Effect.map(({ login }) => login),
      Effect.mapError(
        (cause) =>
          new CapabilityError({
            code: 'CAPABILITY_CREDENTIAL_REJECTED',
            message: cause.message,
            cause,
          }),
      ),
    );
    const policy = yield* Policy;
    const queue = yield* WorkQueue;

    const callTool = (request: {
      readonly jobId: number;
      readonly attemptNumber: number;
      readonly workerId: string;
      /** Ignored legacy field; persisted queue state is authoritative. */
      readonly work?: unknown;
      readonly name: BrokerTool;
      readonly input: Readonly<Record<string, unknown>>;
    }) =>
      Effect.gen(function* () {
        const auditInput = sanitized(request.input);
        const job = yield* queue.job(request.jobId);
        if (job === undefined || job.status !== 'running') {
          return yield* new CapabilityError({
            code: 'CAPABILITY_JOB_INACTIVE',
            message: 'Capability calls require an active persisted job',
          });
        }
        if (job.attempts !== request.attemptNumber || job.workerId !== request.workerId) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_ATTEMPT_STALE',
            message: 'Capability session belongs to a stale job attempt',
          });
        }
        const now = yield* Clock.currentTimeMillis;
        if (job.leaseExpiresAt === undefined || job.leaseExpiresAt <= now) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_LEASE_EXPIRED',
            message: 'Capability session lease has expired',
          });
        }
        const auditIdentity = {
          jobId: request.jobId,
          repository: job.work.repository,
          actor: yield* actor,
          capability: request.name,
          input: auditInput,
        };
        yield* queue.recordAudit({ ...auditIdentity, outcome: 'started' });
        const invoke = Effect.gen(function* () {
          yield* Effect.try({
            try: () => boundedInput(request.input),
            catch: (cause) => cause as CapabilityError,
          });
          const expectedRepository = job.work.repository.toLowerCase();
          const requestedRepository = request.input.repository;
          if (
            requestedRepository !== undefined &&
            String(requestedRepository).toLowerCase() !== expectedRepository
          ) {
            return yield* new CapabilityError({
              code: 'CAPABILITY_REPOSITORY_DENIED',
              message: 'Tool request targets another repository',
            });
          }
          const repositoryPolicy = policy.forRepository(expectedRepository);
          const capability = capabilities[request.name];
          // A continuation inherits its authority from the arming trigger: it
          // never reaches escalation capabilities however generous policy is.
          const narrowed = job.work.continuation === true;
          const forcePushDenied =
            (request.name === 'update_branch' &&
              request.input.force === true &&
              repositoryPolicy.capabilities.forcePush !== true) ||
            (narrowed && request.name === 'update_branch' && request.input.force === true);
          if (
            !repositoryPolicy.accepted ||
            repositoryPolicy.capabilities[capability] !== true ||
            (narrowed &&
              (capability === 'merge' ||
                capability === 'forcePush' ||
                capability === 'deleteBranches')) ||
            forcePushDenied
          ) {
            return yield* new CapabilityError({
              code: 'CAPABILITY_DENIED',
              message: `${request.name} is denied by repository policy`,
            });
          }
          const target = yield* Effect.try({
            try: () => route(expectedRepository, request.name, request.input),
            catch: (cause) =>
              cause instanceof CapabilityError
                ? cause
                : new CapabilityError({
                    code: 'CAPABILITY_INPUT_INVALID',
                    message: 'Capability input is invalid',
                    cause,
                  }),
          });
          const client = yield* github.authenticated;
          let baseRequest = HttpClientRequest.get(target.path);
          if (target.method === 'POST') baseRequest = HttpClientRequest.post(target.path);
          if (target.method === 'PATCH') baseRequest = HttpClientRequest.patch(target.path);
          if (target.method === 'PUT') baseRequest = HttpClientRequest.put(target.path);
          const body =
            request.name === 'list_review_threads'
              ? yield* Effect.try({
                  try: () => ({
                    query:
                      'query($owner:String!,$name:String!,$number:Int!,$after:String){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewThreads(first:10,after:$after){pageInfo{hasNextPage endCursor} nodes{isResolved comments(first:10){pageInfo{hasNextPage endCursor} nodes{id body path line author{login} url}}}}}}}',
                    variables: {
                      owner: expectedRepository.split('/')[0],
                      name: expectedRepository.split('/')[1],
                      number: number(request.input, 'number'),
                      after: typeof request.input.after === 'string' ? request.input.after : null,
                    },
                  }),
                  catch: (cause) =>
                    cause instanceof CapabilityError
                      ? cause
                      : new CapabilityError({
                          code: 'CAPABILITY_INPUT_INVALID',
                          message: 'Capability input is invalid',
                          cause,
                        }),
                })
              : pinCommitIdentity(request.name, request.input);
          const httpRequest =
            target.method === 'GET'
              ? baseRequest
              : HttpClientRequest.bodyUnsafeJson(baseRequest, body);
          const response = yield* client.execute(httpRequest);
          if (response.status < 200 || response.status >= 300) {
            // An installation token heals by re-minting; a revoked PAT never
            // does. Collapsing a 401 into a generic failure spends every attempt,
            // each one a full clone cycle.
            if (response.status === 401) {
              yield* health.suspend;
              return yield* new CapabilityError({
                code: 'CAPABILITY_CREDENTIAL_REJECTED',
                message: 'GitHub rejected the daemon credential',
                ...(quotaNote(response.headers) === undefined
                  ? {}
                  : { cause: quotaNote(response.headers) }),
              });
            }
            // A 429 is definitive alone; 403 needs evidence — headers first,
            // prose second (a secondary limit answers with neither rate header).
            // Retrying either against a closed bucket is a retry storm.
            const hinted =
              response.status === 403 || response.status === 429
                ? retryAfterMs(response.headers, yield* Clock.currentTimeMillis)
                : undefined;
            const secondary =
              response.status === 403 &&
              hinted === undefined &&
              // No reset time still means exhausted; secondary limits say so only
              // in prose.
              (response.headers['x-ratelimit-remaining'] === '0' ||
                isSecondaryRateLimit(yield* Effect.orElseSucceed(response.text, () => '')));
            const wait =
              response.status === 429 || secondary ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : hinted;
            if (wait !== undefined) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_RATE_LIMITED',
                message: `GitHub rate limit reached; retry in ${Math.ceil(wait / 1000)}s${
                  quotaNote(response.headers) === undefined
                    ? ''
                    : `, ${quotaNote(response.headers)}`
                }`,
                retryAfterMs: wait,
              });
            }
            return yield* new CapabilityError({
              code: 'CAPABILITY_GITHUB_FAILED',
              message: `GitHub returned status ${response.status}`,
              ...(quotaNote(response.headers) === undefined
                ? {}
                : { cause: quotaNote(response.headers) }),
            });
          }
          return boundedJson(yield* response.json);
        }).pipe(
          Effect.mapError((cause) =>
            cause instanceof CapabilityError
              ? cause
              : new CapabilityError({
                  code: 'CAPABILITY_FAILED',
                  message: 'Capability call failed',
                  cause,
                }),
          ),
        );

        return yield* Effect.matchEffect(invoke, {
          onFailure: (error) =>
            queue
              .recordAudit({
                ...auditIdentity,
                outcome: error.code,
              })
              .pipe(Effect.zipRight(Effect.fail(error))),
          onSuccess: (result) =>
            // A branch she created outlives the session — durable state the next
            // interaction continues from, never failed after a successful call.
            (request.name === 'create_branch' &&
            typeof request.input.ref === 'string' &&
            request.input.ref.startsWith('refs/heads/')
              ? queue
                  .recordSubjectBranch({
                    repository: job.work.repository,
                    subjectKind: job.work.subject.kind,
                    subjectNumber: job.work.subject.number,
                    branch: request.input.ref.slice('refs/heads/'.length),
                  })
                  .pipe(
                    Effect.catchAll((cause) =>
                      Effect.logError('Could not record created branch').pipe(
                        Effect.annotateLogs({ job: request.jobId, error: cause.message }),
                      ),
                    ),
                  )
              : Effect.void
            ).pipe(
              Effect.zipRight(
                queue
                  .recordAudit({ ...auditIdentity, outcome: 'ok' })
                  .pipe(
                    Effect.catchAll((cause) =>
                      Effect.logError('Could not finalize capability audit', cause),
                    ),
                  ),
              ),
              Effect.as(result),
            ),
        });
      });

    const listTools = (Object.keys(capabilities) as BrokerTool[]).map((name) => {
      const { description, properties, required } = toolSchemas[name];
      return {
        name,
        description,
        inputSchema: {
          type: 'object',
          properties: { repository: repositoryProperty, ...properties },
          ...(required === undefined ? {} : { required }),
          additionalProperties: true,
        },
      };
    });

    // Discovery is scoped to the job's repository policy: offering a tool that
    // could only ever fail invites attempts whose denial is the feature.
    const visibleTools = (repository: string, narrowed: boolean) => {
      const granted = policy.forRepository(repository.toLowerCase()).capabilities;
      // Escalation capabilities stay invisible on continuations: denied means
      // unseen, not merely refused at call time.
      return listTools.filter(
        (tool) =>
          granted[capabilities[tool.name]] === true &&
          !(
            narrowed &&
            (capabilities[tool.name] === 'merge' ||
              capabilities[tool.name] === 'forcePush' ||
              capabilities[tool.name] === 'deleteBranches')
          ),
      );
    };

    const handleMcp = (
      jobId: number,
      attemptOrRequest: number | unknown,
      workerOrRequest?: string | unknown,
      sessionRequest?: {
        readonly jsonrpc: '2.0';
        readonly id: string | number;
        readonly method: string;
        readonly params?: Readonly<Record<string, unknown>>;
      },
    ) => {
      const request = (sessionRequest ?? workerOrRequest ?? attemptOrRequest) as {
        readonly jsonrpc: '2.0';
        readonly id: string | number;
        readonly method: string;
        readonly params?: Readonly<Record<string, unknown>>;
      };
      if (request.method === 'initialize') {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'lictor', version: '1.0.0' },
          },
        });
      }
      if (request.method === 'tools/list') {
        return Effect.flatMap(queue.job(jobId), (job) =>
          Effect.succeed({
            jsonrpc: '2.0' as const,
            id: request.id,
            result: {
              tools:
                job === undefined || job.status !== 'running'
                  ? []
                  : visibleTools(job.work.repository, job.work.continuation === true),
            },
          }),
        );
      }
      if (request.method !== 'tools/call') {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          error: { code: -32601, message: 'Method not found' },
        });
      }
      const name = request.params?.name;
      if (typeof name !== 'string' || !(name in capabilities)) {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          error: { code: -32602, message: 'Unknown tool' },
        });
      }
      const args = request.params?.arguments;
      const input =
        typeof args === 'object' && args !== null
          ? (args as Readonly<Record<string, unknown>>)
          : {};
      return Effect.match(
        callTool({
          jobId,
          attemptNumber: Number(attemptOrRequest),
          workerId: String(workerOrRequest),
          name: name as BrokerTool,
          input,
        }),
        {
          // The code is the whole contract — what the agent branches on. `data`
          // carries only what the code cannot: a closed bucket without a wait
          // leaves the agent guessing, and its guess is a retry storm.
          onFailure: (error) => ({
            jsonrpc: '2.0' as const,
            id: request.id,
            error: {
              code: -32000,
              message: error instanceof CapabilityError ? error.code : 'CAPABILITY_AUDIT_FAILED',
              ...(error instanceof CapabilityError && error.retryAfterMs !== undefined
                ? { data: { detail: error.message, retryAfterMs: error.retryAfterMs } }
                : {}),
            },
          }),
          onSuccess: (result) => ({
            jsonrpc: '2.0' as const,
            id: request.id,
            result: { content: [{ type: 'text', text: JSON.stringify(result) }] },
          }),
        },
      );
    };

    return { callTool, handleMcp, listTools };
  }),
  dependencies: [
    GitHubClient.Default,
    GitHubIdentity.Default,
    Policy.Default,
    WorkQueue.Default,
    CredentialHealth.Default,
  ],
}) {}
