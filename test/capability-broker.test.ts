import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';
import { GitHubClient } from '../src/github/client.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/webhook/qualification.ts';

const work: WorkItem = {
  deliveryId: 'delivery-13',
  interactionId: 'interaction-13',
  event: 'issues',
  action: 'assigned',
  repository: 'edloidas/lictor',
  installationId: 42,
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
    appId: '1',
    privateKey: Redacted.make('private'),
    webhookSecret: Redacted.make('secret'),
    trustedSenders: [],
    targetUsers: [],
    databasePath: ':memory:',
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor.sock',
    webhookMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
  }),
);

const run = <A, E>(effect: Effect.Effect<A, E, CapabilityBroker | WorkQueue>, source: string) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const requests = yield* Ref.make<string[]>([]);
        const client = HttpClient.make((request) =>
          Ref.update(requests, (items) => [...items, request.url]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new Response(JSON.stringify({ ok: true, token: undefined }), {
                  headers: { 'content-type': 'application/json' },
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
          GitHubClient.make({ forInstallation: () => Effect.succeed(scopedClient) }),
        );
        const PolicyLive = Layer.effect(Policy, parsePolicy(source).pipe(Effect.map(Policy.make)));
        const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
        const BrokerLive = CapabilityBroker.DefaultWithoutDependencies.pipe(
          Layer.provide(Layer.mergeAll(GitHubLive, PolicyLive, QueueLive)),
        );
        const value = yield* effect.pipe(Effect.provide(Layer.merge(BrokerLive, QueueLive)));
        return { value, requests: yield* Ref.get(requests) };
      }),
    ),
  );

describe('CapabilityBroker', () => {
  it('exposes MCP-compatible tool discovery bound to a job session', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        return yield* broker.handleMcp(13, work, { jsonrpc: '2.0', id: 1, method: 'tools/list' });
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(JSON.stringify(result.value)).toContain('get_issue');
  });

  it('executes an allowed read with the job installation and audits it', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const response = yield* broker.callTool({
          jobId: 13,
          work,
          name: 'get_issue',
          input: { number: 13 },
        });
        return { response, audit: yield* queue.auditLog(13) };
      }),
      '[defaults.capabilities]\nread = true',
    );
    expect(result.requests[0]).toContain('/repos/edloidas/lictor/issues/13');
    expect(result.value.audit[0]).toMatchObject({
      repository: 'edloidas/lictor',
      installationId: 42,
      capability: 'get_issue',
      outcome: 'ok',
    });
    expect(JSON.stringify(result.value.response)).not.toContain('ghs_');
  });

  it('fails closed for a forbidden mutation and records the denial', async () => {
    const result = await run(
      Effect.gen(function* () {
        const broker = yield* CapabilityBroker;
        const queue = yield* WorkQueue;
        const exit = yield* Effect.exit(
          broker.callTool({
            jobId: 13,
            work,
            name: 'create_comment',
            input: { number: 13, body: 'hello', token: 'hidden' },
          }),
        );
        return { exit, audit: yield* queue.auditLog(13) };
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
        return yield* Effect.exit(
          broker.callTool({
            jobId: 13,
            work,
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
});
