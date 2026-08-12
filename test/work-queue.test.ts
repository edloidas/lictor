import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
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
  it('persists a delivery once and claims it exactly once', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const delivery = { id: 'delivery-1', event: 'issues', body: '{"action":"opened"}' };
        const first = yield* queue.receiveDelivery(delivery);
        const duplicate = yield* queue.receiveDelivery(delivery);
        const claimed = yield* queue.claimDelivery;
        const empty = yield* queue.claimDelivery;
        return { first, duplicate, claimed, empty };
      }),
    );

    expect(result.first.inserted).toBe(true);
    expect(result.duplicate.inserted).toBe(false);
    expect(result.claimed).toMatchObject({ id: 'delivery-1', status: 'processing', attempts: 1 });
    expect(result.empty).toBeUndefined();
  });

  it('records completed and failed delivery processing', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({ id: 'completed', event: 'ping', body: '{}' });
        yield* queue.claimDelivery;
        yield* queue.finishDelivery('completed', 'completed');
        yield* queue.receiveDelivery({ id: 'failed', event: 'issues', body: '{}' });
        yield* queue.claimDelivery;
        yield* queue.finishDelivery('failed', 'failed', 'invalid payload');
        return {
          completed: yield* queue.deliveryStatus('completed'),
          failed: yield* queue.deliveryStatus('failed'),
        };
      }),
    );

    expect(result).toEqual({ completed: 'completed', failed: 'failed' });
  });

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

  it('keeps approval-required jobs pending and unclaimable', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue({ ...work('approval'), approvalRequired: true });
        return { claimed: yield* queue.claim, counts: yield* queue.counts };
      }),
    );
    expect(result.claimed).toBeUndefined();
    expect(result.counts.pending).toBe(1);
  });

  it('rejects enqueue when the configured queue depth is exhausted', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('depth-1'), 1);
        return yield* Effect.flip(queue.enqueue(work('depth-2'), 1));
      }),
    );
    expect(result.operation).toBe('enqueue');
    expect((result.cause as Error).message).toBe('QUEUE_DEPTH_LIMIT');
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
        const recovered = yield* queue.recoverStale(now + 60_001);
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
        yield* queue.recoverStale(now + 60_001);
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

  it('recovers a processing delivery when reopening a file database', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-inbox-'));
    const path = join(directory, 'queue.sqlite');

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.receiveDelivery({ id: 'delivery-1', event: 'ping', body: '{}' });
            yield* queue.claimDelivery;
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      const recovered = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            return yield* queue.claimDelivery;
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(recovered).toMatchObject({ id: 'delivery-1', attempts: 2, status: 'processing' });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('refuses a database created by a newer queue schema', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    const database = new Database(path, { create: true });
    database.exec('PRAGMA user_version = 5');
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

  it('rejects a second live daemon owner for one database', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-owner-'));
    const path = join(directory, 'queue.sqlite');
    try {
      const exit = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* WorkQueue;
            return yield* Effect.exit(Effect.scoped(Effect.provide(WorkQueue, queueLayer(path))));
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      expect(String(exit)).toContain('claim daemon ownership');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('dead-letters an expired claim at the attempt limit', async () => {
    const counts = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.enqueue(work('delivery-dead'));
          const claimed = yield* queue.claim;
          yield* queue.recoverStale((claimed?.leaseExpiresAt ?? 0) + 1);
          return yield* queue.counts;
        }).pipe(
          Effect.provide(
            WorkQueue.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.succeed(LictorConfig, { ...config(':memory:'), workerMaxAttempts: 1 }),
              ),
            ),
          ),
        ),
      ),
    );
    expect(counts.dead_letter).toBe(1);
  });

  it('dead-letters a corrupt stored payload without wedging the queue', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-poison-'));
    const path = join(directory, 'queue.sqlite');
    try {
      const counts = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* Effect.sync(() => {
              const writer = new Database(path);
              writer
                .query(
                  `INSERT INTO jobs (delivery_id, interaction_id, payload, status, available_at, created_at, updated_at)
                   VALUES ('poison', 'poison', '{', 'pending', 0, 0, 0)`,
                )
                .run();
              writer.close();
            });
            expect(yield* queue.claim).toBeUndefined();
            return yield* queue.counts;
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      expect(counts.dead_letter).toBe(1);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('prunes completed jobs at the retention boundary and reports size', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-maintenance-'));
    const path = join(directory, 'queue.sqlite');
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.enqueue(work('delivery-prune'));
            const job = yield* queue.claim;
            yield* queue.complete(job?.id ?? -1, job?.attempts ?? -1, 'done');
            const report = yield* queue.maintenance(Date.now() + 1, Date.now() + 1);
            return { report, counts: yield* queue.counts };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      expect(result.report.completed).toBeGreaterThanOrEqual(1);
      expect(result.report.sizeBytes).toBeGreaterThan(0);
      expect(result.counts.completed).toBe(0);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('creates an owner-only WAL-consistent backup that SQLite can restore', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-backup-'));
    const path = join(directory, 'queue.sqlite');
    const backupPath = join(directory, 'backup', 'lictor.sqlite');
    try {
      const report = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.enqueue(work('delivery-backup'));
            return yield* queue.backup(backupPath);
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      const restored = new Database(backupPath, { readonly: true });
      const jobs = restored.query('SELECT COUNT(*) AS count FROM jobs').get() as { count: number };
      restored.close();
      expect(report.sizeBytes).toBeGreaterThan(0);
      expect(jobs.count).toBe(1);
      expect(statSync(path).mode & 0o777).toBe(0o600);
      expect(statSync(backupPath).mode & 0o777).toBe(0o600);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
