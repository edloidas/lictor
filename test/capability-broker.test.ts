import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';
import { GitHubClient } from '../src/github/client.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { GitHubIdentity } from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const work: WorkItem = {
  deliveryId: 'delivery-13',
  interactionId: 'interaction-13',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 13,
    title: 'Broker capabilities',
    url: 'https://github.com/edloidas/lictor/issues/13',
  },
};
const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: [],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor.sock',
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  }),
);

const readBody = (request: { readonly body: unknown }): string => {
  const body = request.body as { readonly body?: unknown };
  return body.body instanceof Uint8Array ? new TextDecoder().decode(body.body) : '';
};

const run = <A, E>(
  effect: Effect.Effect<A, E, CapabilityBroker | WorkQueue>,
  source: string,
  reply: {
    readonly status?: number;
    readonly headers?: Record<string, string>;
    readonly body?: unknown;
  } = {},
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const requests = yield* Ref.make<string[]>([]);
        const bodies = yield* Ref.make<string[]>([]);
        const client = HttpClient.make((request) =>
          Ref.update(requests, (items) => [...items, request.url]).pipe(
            Effect.zipRight(Ref.update(bodies, (items) => [...items, readBody(request)])),
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new Response(JSON.stringify(reply.body ?? { ok: true, token: undefined }), {
                  status: reply.status ?? 200,
                  headers: { 'content-type': 'application/json', ...reply.headers },
                }),
              ),
            ),
          ),
        );
        const scopedClient = client.pipe(
          HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test')),
        );
        const GitHubLive = Layer.succeed(
          GitHubClient,
          GitHubClient.make({
            authenticated: Effect.succeed(scopedClient),
            addReaction: () => Effect.succeed(undefined),
          }),
        );
        const PolicyLive = Layer.effect(
          Policy,
          parsePolicy(`${source}\n[repositories]\nallow = ["edloidas/lictor"]`).pipe(
            Effect.map(Policy.make),
          ),
        );
        const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
        const IdentityLive = Layer.succeed(
          GitHubIdentity,
          GitHubIdentity.make({
            verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
          }),
        );
        const BrokerLive = CapabilityBroker.DefaultWithoutDependencies.pipe(
          Layer.provide(
            Layer.mergeAll(
              GitHubLive,
              IdentityLive,
              PolicyLive,
              QueueLive,
              CredentialHealth.Default,
            ),
          ),
        );
        const value = yield* effect.pipe(Effect.provide(Layer.merge(BrokerLive, QueueLive)));
        return { value, requests: yield* Ref.get(requests), bodies: yield* Ref.get(bodies) };
      }),
    ),
  );

describe('CapabilityBroker', () => {
  it('exposes MCP-compatible tool discovery bound to a job session', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        yield* queue.claim;
        return yield* broker.handleMcp(enqueued.jobId, work, {
          jsonrpc: '2.0',
          id: 1,
          method: 'tools/list',
        });
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(JSON.stringify(result.value)).toContain('get_issue');
  });

  it('hides tools a repository policy denies from discovery', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        yield* queue.claim;
        const response = yield* broker.handleMcp(enqueued.jobId, work, {
          jsonrpc: '2.0',
          id: 1,
          method: 'tools/list',
        });
        if (!('result' in response) || !('tools' in response.result))
          throw new Error('tools/list failed');
        return response.result.tools.map((tool: { readonly name: string }) => tool.name);
      }),
      '[defaults.capabilities]\nread = true\ncomment = true',
    );
    expect(result.value).toContain('get_issue');
    expect(result.value).toContain('create_comment');
    expect(result.value).not.toContain('create_branch');
    expect(result.value).not.toContain('merge_pull_request');
  });

  it('exposes no tools to discovery without an active job session', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        return yield* broker.handleMcp(13, work, {
          jsonrpc: '2.0',
          id: 1,
          method: 'tools/list',
        });
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(result.value).toMatchObject({ result: { tools: [] } });
  });

  it('accepts the MCP initialization handshake', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        return yield* broker.handleMcp(13, {
          jsonrpc: '2.0',
          id: 1,
          method: 'initialize',
        });
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(result.value).toMatchObject({ result: { serverInfo: { name: 'lictor' } } });
  });

  it('rejects capability calls after the durable job is canceled', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        yield* queue.cancel(enqueued.jobId);
        return yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_repository',
            input: {},
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(String(result.value)).toContain('CAPABILITY_JOB_INACTIVE');
    expect(result.requests).toHaveLength(0);
  });

  it('rejects a capability session after the job moves to a new attempt', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const stale = yield* queue.claim;
        yield* queue.recoverStale((stale?.leaseExpiresAt ?? 0) + 1);
        yield* queue.claim;
        return yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: stale?.attempts ?? -1,
            workerId: stale?.workerId ?? '',
            name: 'get_repository',
            input: {},
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(String(result.value)).toContain('CAPABILITY_ATTEMPT_STALE');
    expect(result.requests).toHaveLength(0);
  });

  it('executes an allowed read and audits it with the authenticated actor', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        const response = yield* broker.callTool({
          jobId: enqueued.jobId,
          attemptNumber: claimed?.attempts ?? -1,
          workerId: claimed?.workerId ?? '',
          name: 'get_issue',
          input: { number: 13 },
        });
        return { response, audit: yield* queue.auditLog(enqueued.jobId) };
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(result.requests[0]).toContain('/repos/edloidas/lictor/issues/13');
    expect(result.value.audit.at(-1)).toMatchObject({
      repository: 'edloidas/lictor',
      // ! The actor is the only identity an audit row carries: the PAT acts as
      // ! one account, so attribution is the verified login, never a payload
      // ! field.
      actor: 'adiutriel',
      capability: 'get_issue',
      outcome: 'ok',
    });
    expect(JSON.stringify(result.value.response)).not.toContain('ghs_');
  });

  it('remembers a branch a job created for its subject', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        const response = yield* broker.callTool({
          jobId: enqueued.jobId,
          attemptNumber: claimed?.attempts ?? -1,
          workerId: claimed?.workerId ?? '',
          name: 'create_branch',
          input: { ref: 'refs/heads/lictor-issue-13' },
        });
        return {
          response,
          branch: yield* queue.branchForSubject('edloidas/lictor', 'issue', 13),
        };
      }),
      '[defaults.capabilities]\nread = true\nbranches = true',
    );
    expect(result.value.response).toMatchObject({ ok: true });
    expect(result.value.branch).toBe('lictor-issue-13');
  });

  it('leaves the subject branch untouched when a denied call fails', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'create_branch',
            input: { ref: 'refs/heads/lictor-issue-13' },
          }),
        );
        return yield* queue.branchForSubject('edloidas/lictor', 'issue', 13);
      }),
      '[defaults.capabilities]\nread = true\nbranches = false',
    );
    expect(result.value).toBeUndefined();
  });

  it('fails closed for a forbidden mutation and records the denial', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        const exit = yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'create_comment',
            input: { number: 13, body: 'hello', token: 'hidden' },
          }),
        );
        return { exit, audit: yield* queue.auditLog(enqueued.jobId) };
      }),
      '[defaults.capabilities]\nread = true\ncomment = false',
    );
    expect(String(result.value.exit)).toContain('CAPABILITY_DENIED');
    expect(result.requests).toHaveLength(0);
    expect(result.value.audit[0]?.input).toContain('[REDACTED]');
  });

  it('rejects attempts to address another repository', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_repository',
            input: { repository: 'other/repo' },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(String(result.value)).toContain('CAPABILITY_REPOSITORY_DENIED');
    expect(result.requests).toHaveLength(0);
  });
  // ! Harmless while every commit was visibly `lictor[bot]`; once commits carry a
  // ! person's account, a forwarded `author` attributes work to someone who did
  // ! not do it.
  it('strips agent-supplied author and committer from a commit', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* broker.callTool({
          jobId: enqueued.jobId,
          attemptNumber: claimed?.attempts ?? -1,
          workerId: claimed?.workerId ?? '',
          name: 'create_commit',
          input: {
            message: 'chore: something',
            tree: 'abc',
            parents: ['def'],
            author: { name: 'Someone Else', email: 'someone@example.com' },
            committer: { name: 'Someone Else', email: 'someone@example.com' },
          },
        });
      }),
      '[defaults.capabilities]\nread = true\nbranches = true',
    );

    expect(result.bodies[0]).toContain('chore: something');
    expect(result.bodies[0]).not.toContain('author');
    expect(result.bodies[0]).not.toContain('committer');
    expect(result.bodies[0]).not.toContain('someone@example.com');
  });

  it('leaves other capabilities\u2019 input untouched', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* broker.callTool({
          jobId: enqueued.jobId,
          attemptNumber: claimed?.attempts ?? -1,
          workerId: claimed?.workerId ?? '',
          name: 'create_comment',
          input: { number: 13, body: 'mentioning author and committer' },
        });
      }),
      '[defaults.capabilities]\nread = true\ncomment = true',
    );

    expect(result.bodies[0]).toContain('mentioning author and committer');
  });

  // ! An installation token healed by re-minting. A revoked PAT never does, so a
  // ! generic failure code spends every remaining attempt on a dead credential.
  it('reports a rejected credential distinctly from a generic failure', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 401 },
    );

    expect(String(result.value)).toContain('CAPABILITY_CREDENTIAL_REJECTED');
  });

  it('turns a throttled response into a rate-limit code carrying the wait', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.flip(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 429, headers: { 'retry-after': '30' } },
    );

    expect(result.value._tag).toBe('CapabilityError');
    if (result.value._tag !== 'CapabilityError') return;
    expect(result.value.code).toBe('CAPABILITY_RATE_LIMITED');
    expect(result.value.retryAfterMs).toBe(30_000);
  });

  // ! 403 is GitHub's answer for both "forbidden" and "slow down". Only the
  // ! throttled variant is worth retrying.
  it('keeps an unthrottled 403 as a generic failure', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.flip(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 403 },
    );

    expect(result.value._tag).toBe('CapabilityError');
    if (result.value._tag !== 'CapabilityError') return;
    expect(result.value.code).toBe('CAPABILITY_GITHUB_FAILED');
  });

  // ! Unlike 403, a 429 has no second meaning. Falling through to the generic
  // ! code when the header is missing tells the agent to retry at once, against
  // ! a bucket GitHub just said is closed.
  it('treats a 429 with no usable header as rate limited anyway', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.flip(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 429, headers: { 'retry-after': 'soon' } },
    );

    expect(result.value._tag).toBe('CapabilityError');
    if (result.value._tag !== 'CapabilityError') return;
    expect(result.value.code).toBe('CAPABILITY_RATE_LIMITED');
    expect(result.value.retryAfterMs).toBe(60_000);
  });

  // ! A secondary limit is what an agent creating content actually trips, and
  // ! GitHub answers it with 403 and no rate headers at all. Reading it as
  // ! "forbidden" tells the agent to give up on a call that would succeed later.
  it('recognises a secondary rate limit that arrives with no rate headers', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.flip(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      {
        status: 403,
        body: { message: 'You have exceeded a secondary rate limit. Please wait a few minutes.' },
      },
    );

    expect(result.value._tag).toBe('CapabilityError');
    if (result.value._tag !== 'CapabilityError') return;
    expect(result.value.code).toBe('CAPABILITY_RATE_LIMITED');
    expect(result.value.retryAfterMs).toBe(60_000);
  });

  it('still refuses a 403 whose body is an ordinary permission failure', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* Effect.flip(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'get_issue',
            input: { number: 13 },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 403, body: { message: 'Resource not accessible by personal access token' } },
    );

    expect(result.value._tag).toBe('CapabilityError');
    if (result.value._tag !== 'CapabilityError') return;
    expect(result.value.code).toBe('CAPABILITY_GITHUB_FAILED');
  });

  // ! `callTool` is only ever reached through `handleMcp` in production. A wait
  // ! the agent cannot see is a wait that does not exist.
  it('carries the wait and the prose across the MCP boundary', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        return yield* broker.handleMcp(
          enqueued.jobId,
          claimed?.attempts ?? -1,
          claimed?.workerId ?? '',
          {
            jsonrpc: '2.0',
            id: 7,
            method: 'tools/call',
            params: { name: 'get_issue', arguments: { number: 13 } },
          },
        );
      }),
      '[defaults.capabilities]\nread = true',
      { status: 429, headers: { 'retry-after': '30' } },
    );

    expect(result.value).toMatchObject({
      id: 7,
      error: {
        code: -32000,
        message: 'CAPABILITY_RATE_LIMITED',
        data: { retryAfterMs: 30_000 },
      },
    });
    expect(JSON.stringify(result.value)).toContain('retry in 30s');
  });
});
