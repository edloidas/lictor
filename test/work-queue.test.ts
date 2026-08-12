import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Clock, Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/webhook/qualification.ts';

const config = (databasePath: string) =>
  LictorConfig.make({
    appId: '1',
    privateKey: Redacted.make('unused'),
    webhookSecret: Redacted.make('unused'),
    trustedSenders: ['edloidas'],
    targetUsers: ['adiutriel'],
    databasePath,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
  });

const queueLayer = (databasePath = ':memory:') =>
  WorkQueue.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(LictorConfig, config(databasePath))),
  );

const run = <A, E>(effect: Effect.Effect<A, E, WorkQueue>) =>
  Effect.runPromise(Effect.scoped(Effect.provide(effect, queueLayer())));

const work = (deliveryId: string): WorkItem => ({
  deliveryId,
  interactionId: `interaction-${deliveryId}`,
  event: 'issues',
  action: 'assigned',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 17,
    title: 'Queue this',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
});

describe('WorkQueue', () => {
  it('enqueues a delivery once and returns its existing job for duplicates', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const first = yield* queue.enqueue(work('delivery-1'));
        const duplicate = yield* queue.enqueue(work('delivery-1'));
        return { first, duplicate, counts: yield* queue.counts };
      }),
    );

    expect(result.first).toEqual({ jobId: 1, inserted: true });
    expect(result.duplicate).toEqual({ jobId: 1, inserted: false });
    expect(result.counts.pending).toBe(1);
  });

  it('deduplicates the same interaction across different delivery ids', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const first = work('delivery-1');
        const redelivery = { ...work('delivery-2'), interactionId: first.interactionId };
        yield* queue.enqueue(first);
        return yield* queue.enqueue(redelivery);
      }),
    );

    expect(result).toEqual({ jobId: 1, inserted: false });
  });

  it('claims pending jobs in insertion order exactly once', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('delivery-1'));
        yield* queue.enqueue(work('delivery-2'));
        const first = yield* queue.claim;
        const second = yield* queue.claim;
        const empty = yield* queue.claim;
        return { first, second, empty };
      }),
    );

    expect(result.first?.work.deliveryId).toBe('delivery-1');
    expect(result.first?.attempts).toBe(1);
    expect(result.second?.work.deliveryId).toBe('delivery-2');
    expect(result.empty).toBeUndefined();
  });

  it('completes a claimed job', async () => {
    const counts = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('delivery-1'));
        const job = yield* queue.claim;
        yield* queue.complete(job?.id ?? -1, job?.attempts ?? -1, 'done');
        return yield* queue.counts;
      }),
    );

    expect(counts.completed).toBe(1);
    expect(counts.running).toBe(0);
  });

  it('does not claim a retry before its availability time', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('delivery-1'));
        const job = yield* queue.claim;
        const now = yield* Clock.currentTimeMillis;
        yield* queue.fail(job?.id ?? -1, job?.attempts ?? -1, 'temporary', now + 60_000);
        return { next: yield* queue.claim, counts: yield* queue.counts };
      }),
    );

    expect(result.next).toBeUndefined();
    expect(result.counts.retry).toBe(1);
  });

  it('recovers stale running work as retryable', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('delivery-1'));
        yield* queue.claim;
        const now = yield* Clock.currentTimeMillis;
        const recovered = yield* queue.recoverStale(now + 1);
        const retried = yield* queue.claim;
        return { recovered, retried, counts: yield* queue.counts };
      }),
    );

    expect(result.recovered).toBe(1);
    expect(result.retried?.work.deliveryId).toBe('delivery-1');
    expect(result.retried?.attempts).toBe(2);
    expect(result.counts.running).toBe(1);
  });

  it('rejects completion from a stale attempt without changing the newer claim', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('delivery-1'));
        const stale = yield* queue.claim;
        const now = yield* Clock.currentTimeMillis;
        yield* queue.recoverStale(now + 1);
        const current = yield* queue.claim;
        const staleResult = yield* Effect.either(
          queue.complete(stale?.id ?? -1, stale?.attempts ?? -1, 'stale'),
        );
        yield* queue.complete(current?.id ?? -1, current?.attempts ?? -1, 'current');
        return { staleResult, counts: yield* queue.counts };
      }),
    );

    expect(result.staleResult._tag).toBe('Left');
    expect(result.counts.completed).toBe(1);
  });

  it('recovers a prior process claim when reopening a file database', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.enqueue(work('delivery-1'));
            yield* queue.claim;
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      const recovered = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            const beforeClaim = yield* queue.counts;
            const claimed = yield* queue.claim;
            return { beforeClaim, claimed };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(recovered.beforeClaim.interrupted).toBe(1);
      expect(recovered.claimed?.attempts).toBe(2);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('refuses a database created by a newer queue schema', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    const database = new Database(path, { create: true });
    database.exec('PRAGMA user_version = 2');
    database.close();

    try {
      const exit = await Effect.runPromise(
        Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const queue = yield* WorkQueue;
              return yield* queue.counts;
            }).pipe(Effect.provide(queueLayer(path))),
          ),
        ),
      );

      expect(exit._tag).toBe('Failure');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
