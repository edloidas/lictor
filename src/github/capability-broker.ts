import { HttpClientRequest } from '@effect/platform';
import { Data, Effect } from 'effect';
import { Policy } from '../policy.ts';
import { WorkQueue } from '../queue/work-queue.ts';
import type { WorkItem } from '../webhook/qualification.ts';
import { GitHubClient } from './client.ts';

export type BrokerTool =
  | 'create_branch'
  | 'create_comment'
  | 'create_commit'
  | 'create_pull_request'
  | 'get_issue'
  | 'get_pull_request'
  | 'get_repository'
  | 'list_comments'
  | 'list_review_threads'
  | 'merge_pull_request'
  | 'update_issue';

export class CapabilityError extends Data.TaggedError('CapabilityError')<{
  readonly code: string;
  readonly message: string;
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
  create_comment: 'comment',
  update_issue: 'issues',
  create_branch: 'branches',
  create_commit: 'branches',
  create_pull_request: 'pullRequests',
  merge_pull_request: 'merge',
};

const number = (input: Readonly<Record<string, unknown>>, name: string): number => {
  const value = input[name];
  if (!Number.isInteger(value) || Number(value) < 1)
    throw new Error(`${name} must be a positive integer`);
  return Number(value);
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
      return { method: 'GET', path: `${base}/issues/${number(input, 'number')}/comments` } as const;
    case 'list_review_threads':
      return { method: 'GET', path: `${base}/pulls/${number(input, 'number')}/comments` } as const;
    case 'create_comment':
      return {
        method: 'POST',
        path: `${base}/issues/${number(input, 'number')}/comments`,
      } as const;
    case 'update_issue':
      return { method: 'PATCH', path: `${base}/issues/${number(input, 'number')}` } as const;
    case 'create_branch':
      return { method: 'POST', path: `${base}/git/refs` } as const;
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
  const encoded = Buffer.from(JSON.stringify(value));
  if (encoded.byteLength > 64 * 1024) throw new Error('GitHub response exceeds broker limit');
  return JSON.parse(encoded.toString('utf8')) as unknown;
};

export class CapabilityBroker extends Effect.Service<CapabilityBroker>()('CapabilityBroker', {
  effect: Effect.gen(function* () {
    const github = yield* GitHubClient;
    const policy = yield* Policy;
    const queue = yield* WorkQueue;

    const callTool = (request: {
      readonly jobId: number;
      readonly work: WorkItem;
      readonly name: BrokerTool;
      readonly input: Readonly<Record<string, unknown>>;
    }) => {
      const auditInput = sanitized(request.input);
      const invoke = Effect.gen(function* () {
        const expectedRepository = request.work.repository.toLowerCase();
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
        const installationId = request.work.installationId;
        if (installationId === undefined)
          return yield* new CapabilityError({
            code: 'CAPABILITY_INSTALLATION_MISSING',
            message: 'Job has no installation identity',
          });
        const repositoryPolicy = policy.forRepository(expectedRepository);
        const capability = capabilities[request.name];
        if (!repositoryPolicy.accepted || repositoryPolicy.capabilities[capability] !== true) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_DENIED',
            message: `${request.name} is denied by repository policy`,
          });
        }
        const target = route(expectedRepository, request.name, request.input);
        const client = yield* github.forInstallation(installationId);
        let baseRequest = HttpClientRequest.get(target.path);
        if (target.method === 'POST') baseRequest = HttpClientRequest.post(target.path);
        if (target.method === 'PATCH') baseRequest = HttpClientRequest.patch(target.path);
        if (target.method === 'PUT') baseRequest = HttpClientRequest.put(target.path);
        const httpRequest =
          target.method === 'GET'
            ? baseRequest
            : HttpClientRequest.bodyUnsafeJson(baseRequest, request.input);
        const response = yield* client.execute(httpRequest);
        if (response.status < 200 || response.status >= 300) {
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

      return Effect.matchEffect(invoke, {
        onFailure: (error) =>
          queue
            .recordAudit({
              jobId: request.jobId,
              repository: request.work.repository,
              ...(request.work.installationId === undefined
                ? {}
                : { installationId: request.work.installationId }),
              capability: request.name,
              input: auditInput,
              outcome: error.code,
            })
            .pipe(Effect.zipRight(Effect.fail(error))),
        onSuccess: (result) =>
          queue
            .recordAudit({
              jobId: request.jobId,
              repository: request.work.repository,
              ...(request.work.installationId === undefined
                ? {}
                : { installationId: request.work.installationId }),
              capability: request.name,
              input: auditInput,
              outcome: 'ok',
            })
            .pipe(Effect.as(result)),
      });
    };

    const listTools = Object.keys(capabilities).map((name) => ({
      name,
      description: `Job-scoped GitHub capability: ${name}`,
      inputSchema: { type: 'object', additionalProperties: true },
    }));

    const handleMcp = (
      jobId: number,
      work: WorkItem,
      request: {
        readonly jsonrpc: '2.0';
        readonly id: string | number;
        readonly method: string;
        readonly params?: Readonly<Record<string, unknown>>;
      },
    ) => {
      if (request.method === 'tools/list') {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          result: { tools: listTools },
        });
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
      return Effect.match(callTool({ jobId, work, name: name as BrokerTool, input }), {
        onFailure: (error) => ({
          jsonrpc: '2.0' as const,
          id: request.id,
          error: {
            code: -32000,
            message: error instanceof CapabilityError ? error.code : 'CAPABILITY_AUDIT_FAILED',
          },
        }),
        onSuccess: (result) => ({
          jsonrpc: '2.0' as const,
          id: request.id,
          result: { content: [{ type: 'text', text: JSON.stringify(result) }] },
        }),
      });
    };

    return { callTool, handleMcp, listTools };
  }),
  dependencies: [GitHubClient.Default, Policy.Default, WorkQueue.Default],
}) {}
