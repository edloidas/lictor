import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest } from '@effect/platform';
import { BunHttpServer } from '@effect/platform-bun';
import { Effect, Layer, Logger, Redacted, Schedule } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { DeliveryWorker } from '../src/delivery-worker.ts';
import { AgentExecutor } from '../src/executor/agent-executor.ts';
import { GitHubClient } from '../src/github/client.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import { Server, WEBHOOK_PATH } from '../src/server.ts';
import { sign } from '../src/webhook/signature.ts';
import { Worker } from '../src/worker.ts';
import { RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const secret = 'pipeline-secret';
const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    appId: '1',
    privateKey: Redacted.make('unused'),
    webhookSecret: Redacted.make(secret),
    trustedSenders: ['edloidas'],
    targetUsers: ['adiutriel'],
    databasePath: ':memory:',
    policyPath: 'policy.toml',
    controlSocketPath: '/tmp/lictor.sock',
    webhookMaxBytes: 1024 * 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 1,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 10,
  }),
);
const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
let executionCount = 0;
const ExecutorLive = Layer.succeed(
  AgentExecutor,
  AgentExecutor.make({
    enabled: true,
    execute: () =>
      Effect.sync(() => ++executionCount).pipe(
        Effect.as({ status: 'completed' as const, summary: 'agent completed' }),
      ),
  }),
);
const GitHubLive = Layer.succeed(
  GitHubClient,
  GitHubClient.make({ forInstallation: () => Effect.die('pipeline must not call GitHub') }),
);
const PolicyLive = Layer.effect(
  Policy,
  parsePolicy(
    '[defaults]\nexecution = "automatic"\n[repositories]\nallow = ["edloidas/lictor"]',
  ).pipe(Effect.map(Policy.make)),
);
const WorkspaceLive = Layer.succeed(
  RepositoryWorkspace,
  RepositoryWorkspace.make({
    create: (_jobId, work) =>
      Effect.succeed({
        repository: work.repository,
        clonePath: process.cwd(),
        worktreePath: process.cwd(),
      }),
    cleanup: () => Effect.void,
    withRepositoryLock: <A, E, R>(_repository: string, effect: Effect.Effect<A, E, R>) => effect,
  }),
);
const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, QueueLive, ExecutorLive, PolicyLive, WorkspaceLive)),
);
const DeliveryWorkerLive = DeliveryWorker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, QueueLive, GitHubLive, PolicyLive)),
);
const Services = Layer.mergeAll(
  ConfigLive,
  QueueLive,
  WorkerLive,
  DeliveryWorkerLive,
  GitHubLive,
  PolicyLive,
  WorkspaceLive,
);
const Application = Layer.mergeAll(
  Server,
  Layer.scopedDiscard(Effect.flatMap(Worker, (worker) => Effect.forkScoped(worker.run))),
  Layer.scopedDiscard(Effect.flatMap(DeliveryWorker, (worker) => Effect.forkScoped(worker.run))),
).pipe(Layer.provide(Services));
const TestApp = Layer.merge(Application, QueueLive).pipe(
  Layer.provide(BunHttpServer.layerTest),
  Layer.provide(Logger.remove(Logger.defaultLogger)),
);

describe('webhook-to-agent pipeline', () => {
  it('acknowledges, persists once, executes, and completes a signed interaction', async () => {
    executionCount = 0;
    const body = JSON.stringify({
      action: 'assigned',
      sender: { login: 'edloidas' },
      repository: { name: 'lictor', full_name: 'edloidas/lictor' },
      issue: {
        number: 17,
        title: 'Exercise the pipeline',
        html_url: 'https://github.com/edloidas/lictor/issues/17',
        updated_at: '2026-08-12T12:00:00Z',
      },
      assignee: { login: 'adiutriel' },
    });

    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* HttpClient.HttpClient;
          const queue = yield* WorkQueue;
          const response = yield* client.execute(
            HttpClientRequest.post(WEBHOOK_PATH).pipe(
              HttpClientRequest.setHeaders({
                'content-type': 'application/json',
                'x-github-event': 'issues',
                'x-github-delivery': 'pipeline-delivery',
                'x-hub-signature-256': sign(body, secret),
              }),
              HttpClientRequest.bodyText(body, 'application/json'),
            ),
          );
          const redelivery = yield* client.execute(
            HttpClientRequest.post(WEBHOOK_PATH).pipe(
              HttpClientRequest.setHeaders({
                'content-type': 'application/json',
                'x-github-event': 'issues',
                'x-github-delivery': 'pipeline-redelivery',
                'x-hub-signature-256': sign(body, secret),
              }),
              HttpClientRequest.bodyText(body, 'application/json'),
            ),
          );
          const pingBody = JSON.stringify({ zen: 'durable ping', repository: null });
          yield* client.execute(
            HttpClientRequest.post(WEBHOOK_PATH).pipe(
              HttpClientRequest.setHeaders({
                'content-type': 'application/json',
                'x-github-event': 'ping',
                'x-github-delivery': 'pipeline-ping',
                'x-hub-signature-256': sign(pingBody, secret),
              }),
              HttpClientRequest.bodyText(pingBody, 'application/json'),
            ),
          );
          const counts = yield* queue.counts.pipe(
            Effect.filterOrFail(
              (current) => current.completed === 1,
              () => new Error('work not completed yet'),
            ),
            Effect.retry(
              Schedule.spaced('5 millis').pipe(Schedule.intersect(Schedule.recurs(100))),
            ),
          );
          return { status: response.status, redeliveryStatus: redelivery.status, counts };
        }).pipe(Effect.provide(Layer.merge(TestApp, BunHttpServer.layerTest))),
      ),
    );

    expect(result.status).toBe(202);
    expect(result.redeliveryStatus).toBe(202);
    expect(result.counts.completed).toBe(1);
    expect(result.counts.pending).toBe(0);
    expect(executionCount).toBe(1);
  });
});
