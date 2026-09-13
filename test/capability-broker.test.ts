import { describe, expect, it } from 'bun:test';
import {
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from '@effect/platform';
import { Effect, Exit, Layer, Redacted, Ref } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';
import { GitHubClient } from '../src/github/client.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import type { Grant, GrantCapabilities } from '../src/github/grant.ts';
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

type AdvertisedTool = {
  readonly name: string;
  readonly description: string;
  readonly annotations?: {
    readonly readOnlyHint?: boolean;
    readonly destructiveHint?: boolean;
    readonly openWorldHint?: boolean;
  };
  readonly inputSchema: {
    readonly properties: Readonly<
      Record<string, { readonly description?: string; readonly pattern?: string }>
    >;
    readonly required?: readonly string[];
  };
};

const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: [],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    stateDir: stateDirOf(':memory:'),
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor.sock',
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    executorResultBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  }),
);

/** Statuses `Response` refuses a body on, and so must the stub — or the broker's own 204 guard is never reached. */
const nullBodyStatus = new Set([204, 205, 304]);

const readBody = (request: { readonly body: unknown }): string => {
  const body = request.body as { readonly body?: unknown };
  return body.body instanceof Uint8Array ? new TextDecoder().decode(body.body) : '';
};

type Reply = {
  readonly status?: number;
  readonly headers?: Record<string, string>;
  readonly body?: unknown;
  /** Answer nothing at all, the way an unreachable GitHub does. */
  readonly transportFails?: boolean;
};

const run = <A, E>(
  effect: Effect.Effect<A, E, CapabilityBroker | WorkQueue>,
  source: string,
  // A flow that makes two requests — a probe and the write it gates — must be
  // able to answer them differently, or neither answer proves which was read.
  reply: Reply | ((body: string) => Reply) = {},
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const requests = yield* Ref.make<string[]>([]);
        const methods = yield* Ref.make<string[]>([]);
        const bodies = yield* Ref.make<string[]>([]);
        const client = HttpClient.make((request) => {
          const sent = readBody(request);
          const answer = typeof reply === 'function' ? reply(sent) : reply;
          return Ref.update(requests, (items) => [...items, request.url]).pipe(
            Effect.zipRight(Ref.update(methods, (items) => [...items, request.method])),
            Effect.zipRight(Ref.update(bodies, (items) => [...items, sent])),
            Effect.zipRight(
              answer.transportFails === true
                ? Effect.fail(
                    new HttpClientError.RequestError({ request, reason: 'Transport', cause: 'no' }),
                  )
                : Effect.succeed(
                    HttpClientResponse.fromWeb(
                      request,
                      new Response(
                        nullBodyStatus.has(answer.status ?? 200)
                          ? null
                          : JSON.stringify(answer.body ?? { ok: true, token: undefined }),
                        {
                          status: answer.status ?? 200,
                          headers: { 'content-type': 'application/json', ...answer.headers },
                        },
                      ),
                    ),
                  ),
            ),
          );
        });
        const scopedClient = client.pipe(
          HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test')),
        );
        const GitHubLive = Layer.succeed(
          GitHubClient,
          GitHubClient.make({
            reconcileReaction: () => Effect.void,
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
        return {
          value,
          requests: yield* Ref.get(requests),
          methods: yield* Ref.get(methods),
          bodies: yield* Ref.get(bodies),
        };
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

  /** A grant over the work fixture's repository, opened up per case. */
  const storedGrant = (capabilities: Partial<GrantCapabilities>): Grant => ({
    version: 1,
    repository: work.repository,
    interactionId: work.interactionId,
    decision: 'automatic',
    continuation: false,
    mintedAt: 1_700_000_000_000,
    capabilities: {
      read: true,
      comment: false,
      issues: false,
      branches: false,
      pullRequests: false,
      review: false,
      merge: false,
      forcePush: false,
      deleteBranches: false,
      ...capabilities,
    },
    maxAttempts: 3,
    maxDurationMs: 30 * 60 * 1000,
    fingerprint: 'minted',
  });

  /** Claims one job and stores `grant` against that attempt. */
  const claimedWithGrant = (grant: Grant) =>
    Effect.gen(function* () {
      const queue = yield* WorkQueue;
      const enqueued = yield* queue.enqueue(work);
      const claimed = yield* queue.claim;
      const attemptNumber = claimed?.attempts ?? 1;
      const workerId = claimed?.workerId ?? '';
      // Asserted here rather than left to the caller: a claim that did not take,
      // or a record that did not write, otherwise surfaces two tests later as a
      // missing tool — which reads as the rule under test having held.
      expect(yield* queue.recordGrant(enqueued.jobId, attemptNumber, workerId, grant)).toBe(true);
      return { jobId: enqueued.jobId, attemptNumber, workerId };
    });

  const toolNamesFor = (jobId: number) =>
    Effect.gen(function* () {
      const broker = yield* CapabilityBroker;
      const response = yield* broker.handleMcp(jobId, work, {
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/list',
      });
      if (!('result' in response) || !('tools' in response.result))
        throw new Error('tools/list failed');
      return response.result.tools.map((tool: { readonly name: string }) => tool.name);
    });

  // ! The rule the record exists for. Policy edited after the job was admitted
  // ! may take authority away and may never add any, so a job that outlived a
  // ! restart under a loosened policy still runs at the width it was accepted at.
  it('does not let policy widened after the mint raise the stored ceiling', async () => {
    const result = await run(
      Effect.flatMap(claimedWithGrant(storedGrant({})), ({ jobId }) => toolNamesFor(jobId)),
      '[defaults.capabilities]\nread = true\ncomment = true\nissues = true',
    );

    expect(result.value).toContain('get_issue');
    expect(result.value).not.toContain('create_comment');
    expect(result.value).not.toContain('update_issue');
  });

  it('applies a policy tightened after the mint straight away', async () => {
    const result = await run(
      Effect.flatMap(claimedWithGrant(storedGrant({ comment: true })), ({ jobId }) =>
        toolNamesFor(jobId),
      ),
      '[defaults.capabilities]\nread = true',
    );

    expect(result.value).toContain('get_issue');
    expect(result.value).not.toContain('create_comment');
  });

  // Discovery and enforcement read one function over one record, so a tool the
  // prompt and `tools/list` withhold cannot be reachable by naming it anyway.
  it('denies a call the stored grant withholds even where policy allows it', async () => {
    const result = await run(
      Effect.flatMap(claimedWithGrant(storedGrant({})), (session) =>
        Effect.flatMap(CapabilityBroker, (broker) =>
          Effect.flip(
            broker.callTool({
              ...session,
              name: 'create_comment',
              input: { number: 13, body: 'hi' },
            }),
          ),
        ),
      ),
      '[defaults.capabilities]\nread = true\ncomment = true',
    );

    expect(result.value).toMatchObject({ _tag: 'CapabilityError', code: 'CAPABILITY_DENIED' });
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

  const advertisedTools = async () => {
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
        return response.result.tools as readonly AdvertisedTool[];
      }),
      '[defaults.capabilities]\nread = true\ncomment = true\nissues = true\nbranches = true\npullRequests = true\nreview = true\nmerge = true',
    );
    return result.value;
  };

  // ! The failure this guards is silent: the tool stays advertised, the agent
  // ! cannot reach it, and the job reports a summary having done nothing.
  it('annotates every advertised tool so the executor policy cannot refuse it', async () => {
    const tools = await advertisedTools();
    // Spelled out, never derived from the `capabilities` map the code reads —
    // that would pass whatever the map said.
    const reads = [
      'get_issue',
      'get_pull_request',
      'get_repository',
      'list_comments',
      'list_reviews',
      'list_review_threads',
      'list_review_comments',
    ];

    // A length floor would let a truncated discovery vacate the loop below.
    expect(tools.map((tool) => tool.name).sort()).toEqual(
      [
        ...reads,
        'create_comment',
        'create_issue',
        'update_issue',
        'create_branch',
        'create_blob',
        'create_commit',
        'create_tree',
        'create_pull_request',
        'create_review',
        'submit_review',
        'delete_pending_review',
        'reply_review_comment',
        'resolve_review_thread',
        'unresolve_review_thread',
        'merge_pull_request',
        'update_branch',
      ].sort(),
    );
    for (const tool of tools) {
      expect(tool.annotations, `${tool.name} carries no annotations`).toBeDefined();
      expect(tool.annotations?.destructiveHint, tool.name).toBe(false);
      expect(tool.annotations?.openWorldHint, tool.name).toBe(false);
      expect(tool.annotations?.readOnlyHint, tool.name).toBe(reads.includes(tool.name));
    }
  });

  it('declares the payload every write tool sends', async () => {
    const tools = await advertisedTools();
    // `required` is a set in JSON Schema, so compare it as one.
    const required = Object.fromEntries(
      tools.map((tool) => [tool.name, [...(tool.inputSchema.required ?? [])].sort()]),
    );
    expect(required.create_comment).toEqual(['body', 'number']);
    // No `number` — the issue it opens does not exist yet.
    expect(required.create_issue).toEqual(['title']);
    expect(required.update_issue).toEqual(['number']);
    expect(required.create_branch).toEqual(['ref', 'sha']);
    expect(required.update_branch).toEqual(['ref', 'sha']);
    expect(required.create_blob).toEqual(['content']);
    expect(required.create_tree).toEqual(['tree']);
    expect(required.create_commit).toEqual(['message', 'parents', 'tree']);
    expect(required.create_pull_request).toEqual(['base', 'head', 'title']);
    expect(required.merge_pull_request).toEqual(['number']);
    // No `event` — omitting it is what leaves the review pending.
    expect(required.create_review).toEqual(['number']);
    expect(required.submit_review).toEqual(['event', 'number', 'review_id']);
    expect(required.delete_pending_review).toEqual(['number', 'review_id']);
    expect(required.reply_review_comment).toEqual(['body', 'comment_id', 'number']);
    // No `number` — a thread node id addresses the thread on its own.
    expect(required.resolve_review_thread).toEqual(['thread_id']);
    expect(required.unresolve_review_thread).toEqual(['thread_id']);
  });

  it('describes every property it makes required', async () => {
    const tools = await advertisedTools();
    for (const tool of tools) {
      const declared = Object.keys(tool.inputSchema.properties);
      for (const name of tool.inputSchema.required ?? []) expect(declared).toContain(name);
    }
  });

  // Policy gates `force`; the repository may refuse a merge method.
  it('declares the optional fields the broker and GitHub act on', async () => {
    const tools = await advertisedTools();
    const propertiesOf = (name: string) =>
      Object.keys(tools.find((tool) => tool.name === name)?.inputSchema.properties ?? {});
    expect(propertiesOf('update_branch')).toContain('force');
    expect(propertiesOf('merge_pull_request')).toContain('merge_method');
    expect(propertiesOf('create_issue')).toContain('body');
    expect(propertiesOf('create_blob')).toContain('encoding');
    expect(propertiesOf('create_tree')).toContain('base_tree');
    expect(propertiesOf('list_review_threads')).toContain('after');
  });

  // Two shapes one prefix apart, under the same property name.
  it('states the two ref shapes apart', async () => {
    const tools = await advertisedTools();
    const refOf = (name: string) =>
      tools.find((tool) => tool.name === name)?.inputSchema.properties.ref?.description ?? '';
    expect(refOf('create_branch')).toContain('refs/heads/<name>');
    expect(refOf('update_branch')).toContain('heads/<name>');
    expect(refOf('update_branch')).not.toContain('refs/heads/<name>');
  });

  // The advertised pattern may be stricter than `route` — it rejects the
  // uppercase `REFS/HEADS/` the `/i` validators tolerate — but never looser, or
  // it hands the agent a name the broker then refuses.
  it('advertises ref patterns no looser than the refs the broker accepts', async () => {
    const tools = await advertisedTools();
    const patternOf = (name: string) =>
      new RegExp(
        tools.find((tool) => tool.name === name)?.inputSchema.properties.ref?.pattern ?? '',
      );
    const brokerCreate = (v: string) =>
      /^refs\/heads\/[a-z0-9._/-]+$/i.test(v) && !v.includes('..');
    const brokerUpdate = (v: string) => /^heads\/[a-z0-9._/-]+$/i.test(v) && !v.includes('..');
    const create = patternOf('create_branch');
    const update = patternOf('update_branch');
    for (const ref of [
      'refs/heads/issue-118',
      'heads/issue-118',
      'refs/heads/CamelCase',
      'refs/heads/feat/x_y.z-1',
      'REFS/HEADS/shouty',
      'refs/heads/fix#118',
      'refs/heads/a..b',
      'heads/a..b',
      'refs/heads/has space',
      'refs/heads/',
    ]) {
      if (create.test(ref)) expect(brokerCreate(ref)).toBe(true);
      if (update.test(ref)) expect(brokerUpdate(ref)).toBe(true);
    }
    // Still pin the shapes each one is for.
    expect(create.test('refs/heads/issue-118')).toBe(true);
    expect(create.test('heads/issue-118')).toBe(false);
    expect(update.test('heads/issue-118')).toBe(true);
    expect(update.test('refs/heads/issue-118')).toBe(false);
  });

  it('advertises the page parameter only on the routes that read it', async () => {
    const tools = await advertisedTools();
    const paged = tools
      .filter((tool) => 'page' in tool.inputSchema.properties)
      .map((tool) => tool.name)
      .sort();
    expect(paged).toEqual(['list_comments', 'list_review_comments', 'list_reviews']);
  });

  it('gives every tool an input shape and a description of its own', async () => {
    const tools = await advertisedTools();
    expect(tools).toHaveLength(23);
    // The guard reads `input.repository` by that name and skips silently when it
    // is absent, so a renamed key turns a refusal into an unaudited redirect.
    for (const tool of tools)
      expect(Object.keys(tool.inputSchema.properties)).toContain('repository');
    const shapeless = tools
      .filter((tool) => Object.keys(tool.inputSchema.properties).length <= 1)
      .map((tool) => tool.name);
    expect(shapeless).toEqual(['get_repository']);
    for (const tool of tools) {
      expect(tool.description.length).toBeGreaterThan(20);
      expect(tool.description).not.toContain(tool.name);
    }
    expect(new Set(tools.map((tool) => tool.description)).size).toBe(tools.length);
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
      // The actor is the only identity an audit row carries: the PAT acts as
      // one account, so attribution is the verified login, never a payload
      // field.
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

  // A continuation turn inherits its authority from the trigger that armed
  // liveness, so it never reaches the escalation capabilities even where
  // repository policy grants them to the operator.
  it('strips escalation capabilities from continuation turns', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue({ ...work, continuation: true });
        const claimed = yield* queue.claim;
        const exit = yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name: 'merge_pull_request',
            input: { number: 13 },
          }),
        );
        return { exit };
      }),
      '[defaults.capabilities]\nread = true\ncomment = true\nmerge = true',
    );
    expect(String(result.value.exit)).toContain('CAPABILITY_DENIED');
    expect(result.requests).toHaveLength(0);
  });

  /** Claims a job under a policy granting `review`, then makes one call. */
  const reviewCall = (
    name:
      | 'create_review'
      | 'submit_review'
      | 'delete_pending_review'
      | 'reply_review_comment'
      | 'resolve_review_thread'
      | 'unresolve_review_thread'
      | 'list_reviews'
      | 'list_review_threads',
    input: Readonly<Record<string, unknown>>,
    options: {
      readonly continuation?: boolean;
      readonly reply?: Parameters<typeof run>[2];
    } = {},
  ) =>
    run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const enqueued = yield* queue.enqueue(
          options.continuation === true ? { ...work, continuation: true } : work,
        );
        const claimed = yield* queue.claim;
        const exit = yield* Effect.exit(
          broker.callTool({
            jobId: enqueued.jobId,
            attemptNumber: claimed?.attempts ?? -1,
            workerId: claimed?.workerId ?? '',
            name,
            input,
          }),
        );
        return { exit, audit: yield* queue.auditLog(enqueued.jobId) };
      }),
      '[defaults.capabilities]\nread = true\nreview = true',
      options.reply ?? {},
    );

  it('routes each review write to the endpoint that performs it', async () => {
    const created = await reviewCall('create_review', { number: 13, body: 'looks fine' });
    expect(created.methods).toEqual(['POST']);
    expect(created.requests[0]).toContain('/repos/edloidas/lictor/pulls/13/reviews');

    const submitted = await reviewCall('submit_review', {
      number: 13,
      review_id: 99,
      event: 'COMMENT',
    });
    expect(submitted.methods).toEqual(['POST']);
    expect(submitted.requests[0]).toContain('/repos/edloidas/lictor/pulls/13/reviews/99/events');

    const replied = await reviewCall('reply_review_comment', {
      number: 13,
      comment_id: 42,
      body: 'done',
    });
    expect(replied.methods).toEqual(['POST']);
    expect(replied.requests[0]).toContain('/repos/edloidas/lictor/pulls/13/comments/42/replies');

    const listed = await reviewCall('list_reviews', { number: 13 });
    expect(listed.methods).toEqual(['GET']);
    expect(listed.requests[0]).toContain(
      '/repos/edloidas/lictor/pulls/13/reviews?per_page=3&page=1',
    );
  });

  // GitHub answers 422 to a DELETE carrying JSON, so this body must stay empty.
  it('discards a pending review with a bodiless DELETE', async () => {
    const result = await reviewCall('delete_pending_review', { number: 13, review_id: 99 });

    expect(result.methods).toEqual(['DELETE']);
    expect(result.requests[0]).toContain('/repos/edloidas/lictor/pulls/13/reviews/99');
    expect(result.bodies[0]).toBe('');
    expect(result.value.audit.at(-1)).toMatchObject({
      capability: 'delete_pending_review',
      outcome: 'ok',
    });
  });

  it('reads a bodiless 204 as a result rather than a malformed body', async () => {
    const result = await reviewCall(
      'delete_pending_review',
      { number: 13, review_id: 99 },
      { reply: { status: 204 } },
    );

    expect(result.value.exit).toStrictEqual(Exit.succeed({}));
    expect(result.value.audit.at(-1)).toMatchObject({ outcome: 'ok' });
  });

  it('asks the thread query for the ids and flags a decision needs', async () => {
    const result = await reviewCall('list_review_threads', { number: 13 });

    for (const field of [
      'databaseId',
      'originalLine',
      'viewerCanUpdate',
      'isOutdated',
      'subjectType',
      'diffSide',
      'authorAssociation',
      'viewerDidAuthor',
      'diffHunk',
      '__typename',
    ]) {
      expect(result.bodies[0]).toContain(field);
    }
  });

  /**
   * Answers the ownership probe with `owner` and everything else with `mutation`,
   * told apart by the query each carries so neither can stand in for the other.
   */
  const ownedBy =
    (owner: string | undefined, mutation: Reply = {}) =>
    (sent: string) =>
      sent.includes('PullRequestReviewThread')
        ? {
            body: {
              data: {
                node:
                  owner === undefined
                    ? null
                    : { pullRequest: { repository: { nameWithOwner: owner } } },
              },
            },
          }
        : mutation;

  it('resolves and unresolves a thread by node id through GraphQL', async () => {
    const resolved = await reviewCall(
      'resolve_review_thread',
      { thread_id: 'PRRT_node' },
      { reply: ownedBy('edloidas/lictor') },
    );
    expect(resolved.requests[1]).toContain('/graphql');
    expect(resolved.bodies[1]).toContain('resolveReviewThread');
    expect(resolved.bodies[1]).toContain('PRRT_node');

    const reopened = await reviewCall(
      'unresolve_review_thread',
      { thread_id: 'PRRT_node' },
      { reply: ownedBy('edloidas/lictor') },
    );
    expect(reopened.bodies[1]).toContain('unresolveReviewThread');
  });

  it('refuses a thread belonging to another repository', async () => {
    for (const owner of ['other/repository', undefined]) {
      const result = await reviewCall(
        'resolve_review_thread',
        { thread_id: 'PRRT_elsewhere' },
        { reply: ownedBy(owner) },
      );

      expect(String(result.value.exit)).toContain('CAPABILITY_REPOSITORY_DENIED');
      // The probe went out; the mutation did not.
      expect(result.bodies).toHaveLength(1);
      expect(result.bodies[0]).toContain('PullRequestReviewThread');
    }
  });

  // Only an owner GitHub named and that differs is a denial.
  it('reports a failed ownership probe as the failure it was', async () => {
    const refused = await reviewCall(
      'resolve_review_thread',
      { thread_id: 'PRRT_node' },
      { reply: { status: 401 } },
    );
    expect(String(refused.value.exit)).toContain('CAPABILITY_CREDENTIAL_REJECTED');

    const throttled = await reviewCall(
      'resolve_review_thread',
      { thread_id: 'PRRT_node' },
      { reply: { status: 429, headers: { 'retry-after': '30' } } },
    );
    expect(String(throttled.value.exit)).toContain('CAPABILITY_RATE_LIMITED');
    expect(String(throttled.value.exit)).toContain('30');

    const unreachable = await reviewCall(
      'resolve_review_thread',
      { thread_id: 'PRRT_node' },
      { reply: { transportFails: true } },
    );
    expect(String(unreachable.value.exit)).not.toContain('CAPABILITY_REPOSITORY_DENIED');

    // None of the three reached the mutation.
    for (const result of [refused, throttled, unreachable]) expect(result.bodies).toHaveLength(1);
  });

  it('refuses a thread id that names nothing', async () => {
    const result = await reviewCall('resolve_review_thread', { thread_id: '' });

    expect(String(result.value.exit)).toContain('CAPABILITY_INPUT_INVALID');
    expect(result.requests).toHaveLength(0);
  });

  // Passing that through would audit `ok` for a thread that is still open.
  it('fails a GraphQL mutation GitHub answered 200 and refused', async () => {
    const result = await reviewCall(
      'resolve_review_thread',
      { thread_id: 'PRRT_node' },
      {
        reply: ownedBy('edloidas/lictor', {
          body: {
            data: { resolveReviewThread: null },
            errors: [{ message: 'Resource not accessible' }],
          },
        }),
      },
    );

    expect(String(result.value.exit)).toContain('CAPABILITY_GITHUB_FAILED');
    expect(result.value.audit.at(-1)).toMatchObject({ outcome: 'CAPABILITY_GITHUB_FAILED' });
  });

  // The read keeps its behaviour: a partial GraphQL read still carries data.
  it('leaves a thread listing carrying errors as a result', async () => {
    const result = await reviewCall(
      'list_review_threads',
      { number: 13 },
      { reply: { body: { data: { repository: null }, errors: [{ message: 'nope' }] } } },
    );

    expect(String(result.value.exit)).not.toContain('CAPABILITY');
  });

  it('withholds a verdict from a continuation while leaving COMMENT', async () => {
    const approved = await reviewCall(
      'create_review',
      { number: 13, event: 'APPROVE' },
      { continuation: true },
    );
    expect(String(approved.value.exit)).toContain('CAPABILITY_DENIED');
    expect(approved.requests).toHaveLength(0);

    const commented = await reviewCall(
      'create_review',
      { number: 13, event: 'COMMENT' },
      { continuation: true },
    );
    expect(commented.methods).toEqual(['POST']);
  });

  it('withholds a verdict on a pull request this account opened', async () => {
    const result = await reviewCall(
      'submit_review',
      { number: 13, review_id: 99, event: 'REQUEST_CHANGES' },
      { reply: { body: { user: { login: 'Adiutriel' } } } },
    );

    expect(String(result.value.exit)).toContain('CAPABILITY_DENIED');
    // The author probe ran; the review never did.
    expect(result.methods).toEqual(['GET']);
  });

  it('lets a verdict through on a pull request someone else opened', async () => {
    const result = await reviewCall(
      'create_review',
      { number: 13, event: 'APPROVE' },
      { reply: { body: { user: { login: 'edloidas' } } } },
    );

    expect(result.methods).toEqual(['GET', 'POST']);
    expect(result.value.audit.at(-1)).toMatchObject({ capability: 'create_review', outcome: 'ok' });
  });

  it('proceeds with a verdict when the author probe never answers', async () => {
    const result = await reviewCall(
      'create_review',
      { number: 13, event: 'APPROVE' },
      { reply: { transportFails: true } },
    );

    expect(result.methods).toEqual(['GET', 'POST']);
  });

  it('reads no author out of a failed probe response', async () => {
    const result = await reviewCall(
      'create_review',
      { number: 13, event: 'APPROVE' },
      { reply: { status: 500, body: { user: { login: 'adiutriel' } } } },
    );

    expect(result.methods).toEqual(['GET', 'POST']);
    expect(String(result.value.exit)).not.toContain('CAPABILITY_DENIED');
  });

  it('hides the review tools where policy withholds the capability', async () => {
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
      '[defaults.capabilities]\nread = true\npullRequests = true',
    );

    expect(result.value).toContain('list_reviews');
    expect(result.value).toContain('create_pull_request');
    expect(result.value).not.toContain('create_review');
    expect(result.value).not.toContain('resolve_review_thread');
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
  // Harmless while every commit was visibly `lictor[bot]`; once commits carry a
  // person's account, a forwarded `author` attributes work to someone who did
  // not do it.
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

  it('opens an issue against the repository collection', async () => {
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
          name: 'create_issue',
          input: { title: 'feat: a test issue', body: 'opened by the broker' },
        });
      }),
      '[defaults.capabilities]\nread = true\nissues = true',
    );
    expect(result.methods[0]).toBe('POST');
    expect(result.requests[0]).toEndWith('/repos/edloidas/lictor/issues');
    expect(result.bodies[0]).toContain('feat: a test issue');
    expect(result.bodies[0]).toContain('opened by the broker');
  });

  it('denies opening an issue without the issues capability', async () => {
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
            name: 'create_issue',
            input: { title: 'feat: a test issue' },
          }),
        );
      }),
      '[defaults.capabilities]\nread = true\nissues = false',
    );
    expect(String(result.value)).toContain('CAPABILITY_DENIED');
    expect(result.requests).toHaveLength(0);
  });

  // An installation token healed by re-minting. A revoked PAT never does, so a
  // generic failure code spends every remaining attempt on a dead credential.
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

  // 403 is GitHub's answer for both "forbidden" and "slow down". Only the
  // throttled variant is worth retrying.
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

  // Unlike 403, a 429 has no second meaning. Falling through to the generic
  // code when the header is missing tells the agent to retry at once, against
  // a bucket GitHub just said is closed.
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

  // A secondary limit is what an agent creating content actually trips, and
  // GitHub answers it with 403 and no rate headers at all. Reading it as
  // "forbidden" tells the agent to give up on a call that would succeed later.
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

  // `callTool` is only ever reached through `handleMcp` in production. A wait
  // the agent cannot see is a wait that does not exist.
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
