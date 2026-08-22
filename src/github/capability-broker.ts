import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Effect } from 'effect';
import { Policy } from '../policy.ts';
import { WorkQueue } from '../queue/work-queue.ts';
import { GitHubClient } from './client.ts';
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
    // ! Resolved per call rather than at construction, so building the broker
    // ! costs no network. The first call pays for the probe; the rest share it.
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
          ...(job.work.installationId === undefined
            ? {}
            : { installationId: job.work.installationId }),
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
          const forcePushDenied =
            request.name === 'update_branch' &&
            request.input.force === true &&
            repositoryPolicy.capabilities.forcePush !== true;
          if (
            !repositoryPolicy.accepted ||
            repositoryPolicy.capabilities[capability] !== true ||
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
            // ! An installation token healed by re-minting; a revoked PAT never
            // ! does. Collapsing a 401 into a generic failure spends every
            // ! remaining attempt, and each one costs a full clone cycle.
            if (response.status === 401) {
              yield* Effect.logError('GitHub rejected the daemon credential').pipe(
                Effect.annotateLogs({ job: request.jobId, capability: request.name }),
              );
              return yield* new CapabilityError({
                code: 'CAPABILITY_CREDENTIAL_REJECTED',
                message: 'GitHub rejected the daemon credential',
              });
            }
            // ! A 429 is definitive on its own; a 403 also means "forbidden", so
            // ! it needs evidence. Headers are the first source, the body the
            // ! second: a secondary rate limit — the realistic tripwire for an
            // ! agent creating content — is documented to answer 403 with
            // ! neither rate header. Dropping either case into the generic branch
            // ! tells the agent to retry at once against a closed bucket.
            const hinted =
              response.status === 403 || response.status === 429
                ? retryAfterMs(response.headers, yield* Clock.currentTimeMillis)
                : undefined;
            const secondary =
              response.status === 403 &&
              hinted === undefined &&
              // ! An exhausted bucket reported without a reset time is still an
              // ! exhausted bucket, and a secondary limit says so only in prose.
              (response.headers['x-ratelimit-remaining'] === '0' ||
                isSecondaryRateLimit(yield* Effect.orElseSucceed(response.text, () => '')));
            const wait =
              response.status === 429 || secondary ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : hinted;
            if (wait !== undefined) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_RATE_LIMITED',
                message: `GitHub rate limit reached; retry in ${Math.ceil(wait / 1000)}s`,
                retryAfterMs: wait,
              });
            }
            return yield* new CapabilityError({
              code: 'CAPABILITY_GITHUB_FAILED',
              message: `GitHub returned status ${response.status}`,
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
            queue
              .recordAudit({
                ...auditIdentity,
                outcome: 'ok',
              })
              .pipe(
                Effect.catchAll((cause) =>
                  Effect.logError('Could not finalize capability audit', cause),
                ),
                Effect.as(result),
              ),
        });
      });

    const numberedTools = new Set<BrokerTool>([
      'get_issue',
      'get_pull_request',
      'list_comments',
      'list_review_threads',
      'list_review_comments',
      'create_comment',
      'merge_pull_request',
    ]);
    const listTools = (Object.keys(capabilities) as BrokerTool[]).map((name) => ({
      name,
      description: `Job-scoped GitHub capability: ${name}`,
      inputSchema: {
        type: 'object',
        properties: {
          ...(numberedTools.has(name)
            ? { number: { type: 'integer', minimum: 1 }, page: { type: 'integer', minimum: 1 } }
            : {}),
        },
        ...(numberedTools.has(name) ? { required: ['number'] } : {}),
        additionalProperties: true,
      },
    }));

    // ! Discovery is scoped to the job's repository policy, so the agent never
    // ! sees a tool it could only ever fail: offering `merge_pull_request` to
    // ! a read+comment repository invites attempts whose denial is the feature.
    const visibleTools = (repository: string) => {
      const granted = policy.forRepository(repository.toLowerCase()).capabilities;
      return listTools.filter((tool) => granted[capabilities[tool.name]] === true);
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
                  : visibleTools(job.work.repository),
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
          // ! The code stays the whole contract, because that is what the agent
          // ! branches on. `data` appears only when there is something the agent
          // ! can act on that the code cannot carry: telling it the bucket is
          // ! closed without telling it for how long leaves it to guess, and its
          // ! guess is a retry storm.
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
  dependencies: [GitHubClient.Default, GitHubIdentity.Default, Policy.Default, WorkQueue.Default],
}) {}
