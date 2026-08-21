import { describe, expect, test } from 'bun:test';
import { HttpClient, HttpClientRequest } from '@effect/platform';
import { BunHttpServer } from '@effect/platform-bun';
import { Deferred, Effect, Fiber, Layer, Logger, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { QueueError, WorkQueue } from '../src/queue/work-queue.ts';
import { Server, WEBHOOK_PATH } from '../src/server.ts';
import { sign } from '../src/webhook/signature.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('test-token'),
  expectedLogin: 'adiutriel',
  webhookSecret: Redacted.make('storage-secret'),
  trustedSenders: [],
  targetUsers: [],
  databasePath: ':memory:',
  policyPath: 'policy.toml',
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
});

type QueueService = Parameters<typeof WorkQueue.make>[0];
const queue = (receiveDelivery: QueueService['receiveDelivery']) =>
  WorkQueue.make({
    receiveDelivery,
    claimDelivery: Effect.die('unused'),
    finishDelivery: () => Effect.die('unused'),
    retryDelivery: () => Effect.die('unused'),
    deliveryStatus: () => Effect.die('unused'),
    enqueue: () => Effect.die('unused'),
    claim: Effect.die('unused'),
    claimFor: () => Effect.die('unused'),
    heartbeat: () => Effect.die('unused'),
    heartbeatDaemon: Effect.die('unused'),
    complete: () => Effect.die('unused'),
    fail: () => Effect.die('unused'),
    recoverStale: () => Effect.die('unused'),
    counts: Effect.die('unused'),
    maintenance: () => Effect.die('unused'),
    recordAudit: () => Effect.die('unused'),
    auditLog: () => Effect.die('unused'),
    listJobs: () => Effect.die('unused'),
    job: () => Effect.die('unused'),
    approve: () => Effect.die('unused'),
    retry: () => Effect.die('unused'),
    cancel: () => Effect.die('unused'),
    diagnostics: Effect.die('unused'),
    backup: () => Effect.die('unused'),
    ownerId: '00000000-0000-0000-0000-000000000000',
  });

const request = Effect.gen(function* () {
  const client = yield* HttpClient.HttpClient;
  const body = '{}';
  return yield* client.execute(
    HttpClientRequest.post(WEBHOOK_PATH).pipe(
      HttpClientRequest.setHeaders({
        'x-github-event': 'ping',
        'x-github-delivery': 'storage-test',
        'x-hub-signature-256': sign(body, 'storage-secret'),
      }),
      HttpClientRequest.bodyText(body, 'application/json'),
    ),
  );
});

const serve = (workQueue: WorkQueue) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.provide(
        request,
        Layer.merge(
          Server.pipe(
            Layer.provide(Layer.succeed(LictorConfig, config)),
            Layer.provide(Layer.succeed(WorkQueue, workQueue)),
            Layer.provide(
              Layer.succeed(
                GitHubClient,
                GitHubClient.make({ authenticated: Effect.die('unused') }),
              ),
            ),
            Layer.provide(Layer.effect(Policy, parsePolicy('').pipe(Effect.map(Policy.make)))),
            Layer.provide(BunHttpServer.layerTest),
            Layer.provide(Logger.remove(Logger.defaultLogger)),
          ),
          BunHttpServer.layerTest,
        ),
      ),
    ),
  );

describe('webhook durable receipt', () => {
  test('returns 503 when durable storage fails', async () => {
    const response = await serve(
      queue(() => Effect.fail(new QueueError({ operation: 'receive', cause: 'disk full' }))),
    );
    expect(response.status).toBe(503);
  });

  test('does not acknowledge before durable storage completes', async () => {
    const gate = await Effect.runPromise(Deferred.make<void>());
    const fiber = Effect.runFork(
      Effect.promise(() =>
        serve(queue(() => Deferred.await(gate).pipe(Effect.as({ inserted: true })))),
      ),
    );
    await Bun.sleep(10);
    expect((await Effect.runPromise(Fiber.poll(fiber)))._tag).toBe('None');
    await Effect.runPromise(Deferred.succeed(gate, undefined));
    expect((await Effect.runPromise(Fiber.join(fiber))).status).toBe(202);
  });
});
