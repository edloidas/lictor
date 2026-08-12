import { describe, expect, it } from 'bun:test';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { AgentExecutor, ExecutorError } from '../src/executor/agent-executor.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/webhook/qualification.ts';
import { Worker } from '../src/worker.ts';

const work: WorkItem = {
  deliveryId: 'delivery-1',
  interactionId: 'interaction-1',
  event: 'issues',
  action: 'assigned',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 17,
    title: 'Run the worker',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
};

const config = (maxAttempts = 3) =>
  LictorConfig.make({
    appId: '1',
    privateKey: Redacted.make('unused'),
    webhookSecret: Redacted.make('unused'),
    trustedSenders: ['edloidas'],
    targetUsers: ['adiutriel'],
    databasePath: ':memory:',
    policyPath: 'policy.toml',
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 10,
    workerMaxAttempts: maxAttempts,
    workerRetryBaseMs: 100,
  });

const run = <A, E>(
  effect: Effect.Effect<A, E, Worker | WorkQueue>,
  execute: InstanceType<typeof AgentExecutor>['execute'],
  maxAttempts = 3,
  enabled = true,
) => {
  const ConfigLive = Layer.succeed(LictorConfig, config(maxAttempts));
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const ExecutorLive = Layer.succeed(AgentExecutor, AgentExecutor.make({ enabled, execute }));
  const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.mergeAll(ConfigLive, QueueLive, ExecutorLive)),
  );

  return Effect.runPromise(
    Effect.scoped(
      effect.pipe(Effect.provide(Layer.merge(QueueLive, WorkerLive)), Effect.provide(ConfigLive)),
    ),
  );
};

describe('Worker.runOnce', () => {
  it('completes a queued job after successful execution', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        const worked = yield* worker.runOnce;
        return { worked, counts: yield* queue.counts };
      }),
      () => Effect.succeed('done'),
    );

    expect(result.worked).toBe(true);
    expect(result.counts.completed).toBe(1);
  });

  it('schedules retryable failures without immediately reclaiming them', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { secondRun: yield* worker.runOnce, counts: yield* queue.counts };
      }),
      () => Effect.fail(new ExecutorError({ message: 'temporary', retryable: true })),
    );

    expect(result.secondRun).toBe(false);
    expect(result.counts.retry).toBe(1);
  });

  it('marks a failure final when the attempt limit is reached', async () => {
    const counts = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.counts;
      }),
      () => Effect.fail(new ExecutorError({ message: 'temporary', retryable: true })),
      1,
    );

    expect(counts.failed).toBe(1);
    expect(counts.retry).toBe(0);
  });

  it('leaves queued work pending while execution is disabled', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        return { worked: yield* worker.runOnce, counts: yield* queue.counts };
      }),
      () => Effect.die('must not execute'),
      3,
      false,
    );

    expect(result.worked).toBe(false);
    expect(result.counts.pending).toBe(1);
  });
});
