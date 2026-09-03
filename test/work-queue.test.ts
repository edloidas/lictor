import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Clock, Effect, Layer, Logger, Redacted, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { QueueFull, WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const config = (databasePath: string) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    autoAcceptInviters: [],
    databasePath,
    stateDir: stateDirOf(databasePath),
    policyPath: 'policy.toml',
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
        const delivery = {
          id: 'delivery-1',
          event: 'notification',
          body: '{"action":"opened"}',
          source: 'notification',
        } as const;
        const first = yield* queue.receiveDelivery(delivery);
        const duplicate = yield* queue.receiveDelivery(delivery);
        const claimed = yield* queue.claimDelivery;
        const empty = yield* queue.claimDelivery;
        return { first, duplicate, claimed, empty };
      }),
    );

    expect(result.first.inserted).toBe(true);
    expect(result.duplicate.inserted).toBe(false);
    expect(result.claimed).toMatchObject({
      id: 'delivery-1',
      source: 'notification',
      status: 'processing',
      attempts: 1,
    });
    expect(result.empty).toBeUndefined();
  });

  it('records completed and failed delivery processing', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'completed',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        yield* queue.claimDelivery;
        yield* queue.finishDelivery('completed', 1, 'completed');
        yield* queue.receiveDelivery({
          id: 'failed',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        yield* queue.claimDelivery;
        yield* queue.finishDelivery('failed', 1, 'failed', 'invalid payload');
        return {
          completed: yield* queue.deliveryStatus('completed'),
          failed: yield* queue.deliveryStatus('failed'),
        };
      }),
    );

    expect(result).toEqual({ completed: 'completed', failed: 'failed' });
  });

  it('makes a failed delivery pending when GitHub redelivers it', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const delivery = {
          id: 'redelivery',
          event: 'notification',
          body: '{"version":1}',
          source: 'notification',
        } as const;
        yield* queue.receiveDelivery(delivery);
        yield* queue.claimDelivery;
        yield* queue.finishDelivery(delivery.id, 1, 'failed', 'old decoder');
        const received = yield* queue.receiveDelivery({ ...delivery, body: '{"version":2}' });
        return { received, claimed: yield* queue.claimDelivery };
      }),
    );
    expect(result.received.inserted).toBe(true);
    expect(result.claimed).toMatchObject({ id: 'redelivery', body: '{"version":2}' });
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
    expect(result.cause).toBeInstanceOf(QueueFull);
    expect(result.cause).toMatchObject({ limit: 1 });
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

  it('resets exhausted attempts when an operator retries a dead-letter job', async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.enqueue(work('operator-retry'));
          const first = yield* queue.claim;
          yield* queue.recoverStale((first?.leaseExpiresAt ?? 0) + 1);
          yield* queue.retry(first?.id ?? -1);
          return yield* queue.claim;
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
    expect(result?.attempts).toBe(1);
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

  // The cutoff is exclusive and `daemonTick` passes the current tick as it, so a
  // lease expiring exactly on one belongs to the pass after. Both arms run off the
  // lease the claim reported; a `Clock` read taken beside it would fall on either
  // side by accident.
  it('holds a job whose lease expires exactly on the reclaim cutoff', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-job-cutoff-'));
    const path = join(directory, 'queue.sqlite');
    const attemptStatus = (side: Database) =>
      (side.query('SELECT status FROM attempts WHERE job_id = ?').get(1) as { status: string })
        .status;
    let side: Database | undefined;

    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.enqueue(work('delivery-1'));
            const lease = (yield* queue.claim)?.leaseExpiresAt ?? 0;
            side = new Database(path);
            const onCutoff = yield* queue.recoverStale(lease);
            const held = yield* queue.counts;
            const heldAttempt = attemptStatus(side);
            const pastCutoff = yield* queue.recoverStale(lease + 1);
            const sweptAttempt = attemptStatus(side);
            return { onCutoff, held, heldAttempt, pastCutoff, sweptAttempt };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(result.onCutoff).toBe(0);
      expect(result.held.running).toBe(1);
      expect(result.heldAttempt).toBe('running');
      expect(result.pastCutoff).toBe(1);
      expect(result.sweptAttempt).toBe('interrupted');
    } finally {
      side?.close();
      rmSync(directory, { recursive: true, force: true });
    }
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

  // Startup reads its cutoff from `Clock` while the layer is still being built,
  // so landing it exactly on a lease means stopping the clock first. The first
  // boot's own claim cannot serve as the stale row: releasing ownership
  // interrupts it on the way out, so the row is rewritten by hand.
  it('holds a job whose lease expires exactly on the startup cutoff', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-startup-'));
    const path = join(directory, 'queue.sqlite');
    const boot = <A, E>(effect: Effect.Effect<A, E, WorkQueue>) =>
      Effect.scoped(Effect.provide(effect, queueLayer(path)));
    const countsAtBoot = boot(Effect.flatMap(WorkQueue, (queue) => queue.counts));
    const attemptStatus = (side: Database) =>
      (side.query('SELECT status FROM attempts WHERE job_id = ?').get(1) as { status: string })
        .status;
    let side: Database | undefined;

    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          yield* boot(
            Effect.gen(function* () {
              const queue = yield* WorkQueue;
              yield* queue.enqueue(work('delivery-1'));
              yield* queue.claim;
            }),
          );
          side = new Database(path);
          side
            .query(
              `UPDATE jobs SET status = 'running', worker_id = 'dead-worker',
                 lease_expires_at = ? WHERE delivery_id = ?`,
            )
            .run(60_000, 'delivery-1');
          side.query("UPDATE attempts SET status = 'running', finished_at = NULL").run();
          yield* TestClock.adjust('60 seconds');
          const onCutoff = yield* countsAtBoot;
          const heldAttempt = attemptStatus(side);
          yield* TestClock.adjust('1 millis');
          const pastCutoff = yield* countsAtBoot;
          const sweptAttempt = attemptStatus(side);
          return { onCutoff, heldAttempt, pastCutoff, sweptAttempt };
        }).pipe(Effect.provide(TestContext.TestContext)),
      );

      expect(result.onCutoff.running).toBe(1);
      expect(result.onCutoff.interrupted).toBe(0);
      expect(result.heldAttempt).toBe('running');
      expect(result.pastCutoff.running).toBe(0);
      expect(result.pastCutoff.interrupted).toBe(1);
      expect(result.sweptAttempt).toBe('interrupted');
    } finally {
      side?.close();
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
            yield* queue.receiveDelivery({
              id: 'delivery-1',
              event: 'notification',
              body: '{}',
              source: 'notification',
            });
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

  it('reclaims a delivery whose lease expired', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'stuck',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        const claimed = yield* queue.claimDelivery;
        const now = yield* Clock.currentTimeMillis;
        const reclaimed = yield* queue.recoverStaleDeliveries(now + 60_001);
        return {
          claimed,
          reclaimed,
          status: yield* queue.deliveryStatus('stuck'),
          next: yield* queue.claimDelivery,
        };
      }),
    );

    expect(result.claimed).toMatchObject({ id: 'stuck', attempts: 1 });
    expect(result.reclaimed).toBe(1);
    expect(result.status).toBe('pending');
    expect(result.next).toMatchObject({ id: 'stuck', attempts: 2 });
  });

  it('leaves a live delivery lease alone', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'live',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        yield* queue.claimDelivery;
        const now = yield* Clock.currentTimeMillis;
        const reclaimed = yield* queue.recoverStaleDeliveries(now);
        return { reclaimed, status: yield* queue.deliveryStatus('live') };
      }),
    );

    expect(result.reclaimed).toBe(0);
    expect(result.status).toBe('processing');
  });

  // Same exclusive cutoff as `recoverStale`, read through a side handle because
  // a delivery claim does not report its lease.
  it('holds a delivery whose lease expires exactly on the reclaim cutoff', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-cutoff-'));
    const path = join(directory, 'queue.sqlite');
    let side: Database | undefined;

    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.receiveDelivery({
              id: 'edge',
              event: 'notification',
              body: '{}',
              source: 'notification',
            });
            yield* queue.claimDelivery;
            side = new Database(path);
            const { leaseExpiresAt: lease } = side
              .query('SELECT lease_expires_at AS leaseExpiresAt FROM deliveries WHERE id = ?')
              .get('edge') as { leaseExpiresAt: number };
            const onCutoff = yield* queue.recoverStaleDeliveries(lease);
            const held = yield* queue.deliveryStatus('edge');
            const pastCutoff = yield* queue.recoverStaleDeliveries(lease + 1);
            return { onCutoff, held, pastCutoff, status: yield* queue.deliveryStatus('edge') };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(result.onCutoff).toBe(0);
      expect(result.held).toBe('processing');
      expect(result.pastCutoff).toBe(1);
      expect(result.status).toBe('pending');
    } finally {
      side?.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // The budget, not the expiry, is what condemns the row: a delivery whose
  // processing reliably kills the fiber stops after `workerMaxAttempts` instead
  // of cycling through the queue forever.
  it('condemns a delivery whose lease expires past the attempt budget', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'doomed',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        const statuses: (string | undefined)[] = [];
        for (let pass = 0; pass < 3; pass += 1) {
          yield* queue.claimDelivery;
          const now = yield* Clock.currentTimeMillis;
          yield* queue.recoverStaleDeliveries(now + 60_001);
          statuses.push(yield* queue.deliveryStatus('doomed'));
        }
        return { statuses, next: yield* queue.claimDelivery };
      }),
    );

    expect(result.statuses).toEqual(['pending', 'pending', 'failed']);
    expect(result.next).toBeUndefined();
  });

  // The startup reset returns a `processing` row to `pending` without reading the
  // budget, so the claim is the only thing between a restart and one extra run.
  it('refuses a delivery a restart returned to pending past the attempt budget', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-budget-'));
    const path = join(directory, 'queue.sqlite');

    try {
      const spent = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.receiveDelivery({
              id: 'ceiling',
              event: 'notification',
              body: '{}',
              source: 'notification',
            });
            // Two expiries, then a claim that spends the last attempt the budget allows.
            for (let pass = 0; pass < 2; pass += 1) {
              yield* queue.claimDelivery;
              const now = yield* Clock.currentTimeMillis;
              yield* queue.recoverStaleDeliveries(now + 60_001);
            }
            return yield* queue.claimDelivery;
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      const logged: string[] = [];
      const afterRestart = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            return {
              claimed: yield* queue.claimDelivery,
              status: yield* queue.deliveryStatus('ceiling'),
            };
          }).pipe(
            Effect.provide(queueLayer(path)),
            Effect.provide(
              Logger.replace(
                Logger.defaultLogger,
                Logger.make<unknown, void>(({ message }) => {
                  logged.push(String(message));
                }),
              ),
            ),
          ),
        ),
      );

      const side = new Database(path);
      const condemned = side
        .query('SELECT attempts, last_error AS lastError FROM deliveries WHERE id = ?')
        .get('ceiling') as { attempts: number; lastError: string | null };
      side.close();

      expect(spent).toMatchObject({ id: 'ceiling', attempts: 3, status: 'processing' });
      expect(afterRestart.claimed).toBeUndefined();
      expect(afterRestart.status).toBe('failed');
      // Refused before the increment, so the row records the three attempts it ran.
      expect(condemned).toEqual({ attempts: 3, lastError: 'attempt limit exhausted' });
      // The log is the drop's only trace; the caller just sees an empty claim.
      expect(logged).toContain('Refused a delivery past its attempt budget');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('gives a redelivered failed delivery its whole budget back', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const delivery = {
          id: 'exhausted',
          event: 'notification',
          body: '{}',
          source: 'notification',
        } as const;
        yield* queue.receiveDelivery(delivery);
        const spent: (number | undefined)[] = [];
        for (let pass = 0; pass < 3; pass += 1) {
          const claimed = yield* queue.claimDelivery;
          spent.push(claimed?.attempts);
          if (claimed === undefined) break;
          yield* queue.retryDelivery(claimed.id, claimed.attempts, 'transport failed');
        }
        const exhausted = yield* queue.deliveryStatus('exhausted');
        yield* queue.receiveDelivery(delivery);
        return { spent, exhausted, revived: yield* queue.claimDelivery };
      }),
    );

    expect(result.spent).toEqual([1, 2, 3]);
    expect(result.exhausted).toBe('failed');
    expect(result.revived).toMatchObject({ id: 'exhausted', attempts: 1, status: 'processing' });
  });

  // Driven to the ceiling first: from a virgin row a refund and a reset to zero look
  // identical, and the raised ceiling is never exercised.
  it('spends no attempt on a retry that opts out of the budget', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'dead-credential',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        const spent: (number | undefined)[] = [];
        const claim = (terminalAfterAttempts: boolean) =>
          Effect.gen(function* () {
            const claimed = yield* queue.claimDelivery;
            spent.push(claimed?.attempts);
            if (claimed === undefined) return;
            yield* queue.retryDelivery(
              claimed.id,
              claimed.attempts,
              'identity rejected',
              terminalAfterAttempts,
            );
          });
        // Two budgeted failures leave the row one short of the budget.
        yield* claim(true);
        yield* claim(true);
        for (let cycle = 0; cycle < 3; cycle += 1) yield* claim(false);
        return { spent, status: yield* queue.deliveryStatus('dead-credential') };
      }),
    );

    // The row idles at the ceiling instead of crossing it: zeroing the count would
    // restart the sequence at 1, and a real ceiling would flip the status to `failed`.
    expect(result.spent).toEqual([1, 2, 3, 3, 3]);
    expect(result.status).toBe('pending');
  });

  // Aged and read through a side handle rather than timed: the renewal has to
  // be observed as a value, and racing two `Clock` reads milliseconds apart
  // proves nothing about which one moved.
  it('renews a live delivery lease and clears it when the delivery finishes', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-lease-'));
    const path = join(directory, 'queue.sqlite');
    const lease = (side: Database) =>
      (
        side
          .query('SELECT lease_expires_at AS leaseExpiresAt FROM deliveries WHERE id = ?')
          .get('renewed') as { leaseExpiresAt: number | null }
      ).leaseExpiresAt;

    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.receiveDelivery({
              id: 'renewed',
              event: 'notification',
              body: '{}',
              source: 'notification',
            });
            yield* queue.claimDelivery;
            const now = yield* Clock.currentTimeMillis;
            const side = new Database(path);
            side
              .query('UPDATE deliveries SET lease_expires_at = ? WHERE id = ?')
              .run(now + 5_000, 'renewed');
            yield* queue.heartbeatDelivery('renewed', 1);
            const renewed = lease(side);
            yield* queue.finishDelivery('renewed', 1, 'completed');
            const finished = lease(side);
            side.close();
            return { now, renewed, finished };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      // A band, not a floor: the renewal must land a full lease out, so
      // shortening `DELIVERY_LEASE_MS` fails here rather than passing on any
      // value that merely beats the aged one.
      expect(result.renewed).toBeGreaterThanOrEqual(result.now + 59_000);
      expect(result.renewed).toBeLessThanOrEqual(result.now + 61_000);
      expect(result.finished).toBeNull();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // Each case is a state the row can reach while a worker still believes it
  // holds the claim. The third is the one the `attempts` fence exists for: the
  // row is `processing` again with a live lease, owned by a newer claim.
  it.each(['expired', 'reclaimed', 'reclaimed and taken by a newer attempt'])(
    'refuses to renew a delivery lease that is %s',
    async (state) => {
      const directory = mkdtempSync(join(tmpdir(), 'lictor-lease-'));
      const path = join(directory, 'queue.sqlite');

      try {
        const exit = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const queue = yield* WorkQueue;
              yield* queue.receiveDelivery({
                id: 'lost',
                event: 'notification',
                body: '{}',
                source: 'notification',
              });
              yield* queue.claimDelivery;
              const now = yield* Clock.currentTimeMillis;
              if (state === 'expired') {
                const side = new Database(path);
                side
                  .query('UPDATE deliveries SET lease_expires_at = ? WHERE id = ?')
                  .run(now - 1, 'lost');
                side.close();
              } else {
                yield* queue.recoverStaleDeliveries(now + 120_000);
                if (state !== 'reclaimed') yield* queue.claimDelivery;
              }
              return yield* Effect.exit(queue.heartbeatDelivery('lost', 1));
            }).pipe(Effect.provide(queueLayer(path))),
          ),
        );

        expect(exit._tag).toBe('Failure');
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );

  // The heartbeat fence alone is not enough: a worker that lost its lease still
  // reaches the writes that end the delivery. Both are fenced on `attempts`, so
  // a stale one cannot complete, requeue, or condemn the claim that replaced it.
  it.each(['finish', 'retry'])(
    'refuses a stale %s against a delivery a newer claim owns',
    async (write) => {
      const result = await run(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.receiveDelivery({
            id: 'stolen',
            event: 'notification',
            body: '{}',
            source: 'notification',
          });
          yield* queue.claimDelivery;
          const now = yield* Clock.currentTimeMillis;
          yield* queue.recoverStaleDeliveries(now + 120_000);
          const current = yield* queue.claimDelivery;
          const stale =
            write === 'finish'
              ? yield* Effect.exit(queue.finishDelivery('stolen', 1, 'completed'))
              : yield* Effect.exit(queue.retryDelivery('stolen', 1, 'stale worker', true));
          return { current, stale, status: yield* queue.deliveryStatus('stolen') };
        }),
      );

      expect(result.current).toMatchObject({ id: 'stolen', attempts: 2 });
      expect(result.stale._tag).toBe('Failure');
      // Untouched: the newer claim still holds it.
      expect(result.status).toBe('processing');
    },
  );

  // Both stamps rewind to the same pre-v10 shape: 9 exercises the version path,
  // 10 the presence guard, which is the only thing that heals a database
  // stamped current by a build that never ran the ALTERs. The two v10 cases
  // rewind one artifact each: breaking both at once passes with either guard
  // clause deleted, so neither would be pinned.
  it.each([
    [9, 'both'],
    [10, 'lease'],
    [10, 'installation'],
  ])('upgrades a version-%i database missing %s', async (version, artifact) => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    await Effect.runPromise(
      Effect.scoped(
        Effect.flatMap(WorkQueue, (queue) => queue.counts).pipe(Effect.provide(queueLayer(path))),
      ),
    );
    const database = new Database(path);
    if (artifact !== 'installation') {
      database.exec('ALTER TABLE deliveries DROP COLUMN lease_expires_at');
    }
    if (artifact !== 'lease') {
      database.exec('ALTER TABLE capability_audit ADD COLUMN installation_id INTEGER');
    }
    // Written before the migration, so the drop is shown to carry existing rows
    // rather than merely leaving the table writable afterwards.
    database
      .query(
        `INSERT INTO capability_audit
           (job_id, repository, actor, capability, input, outcome, created_at)
         VALUES (1, 'edloidas/lictor', 'daemon', 'legacy', '{}', 'ok', 1)`,
      )
      .run();
    database.exec(`PRAGMA user_version = ${version}`);
    database.close();

    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            yield* queue.receiveDelivery({
              id: 'delivery-lease',
              event: 'notification',
              body: '{}',
              source: 'notification',
            });
            yield* queue.claimDelivery;
            const now = yield* Clock.currentTimeMillis;
            const reclaimed = yield* queue.recoverStaleDeliveries(now + 60_001);
            const enqueued = yield* queue.enqueue(work('delivery-audit'), 10);
            yield* queue.recordAudit({
              jobId: enqueued.jobId,
              repository: 'edloidas/lictor',
              actor: 'daemon',
              capability: 'get_issue',
              input: '{}',
              outcome: 'ok',
            });
            return {
              reclaimed,
              status: yield* queue.deliveryStatus('delivery-lease'),
              audit: yield* queue.auditLog(enqueued.jobId),
              legacy: yield* queue.auditLog(1),
            };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(result.reclaimed).toBe(1);
      expect(result.status).toBe('pending');
      expect(result.audit.at(-1)).toMatchObject({ actor: 'daemon', capability: 'get_issue' });
      expect(result.legacy.at(0)).toMatchObject({ actor: 'daemon', capability: 'legacy' });

      const after = new Database(path);
      const columns = (
        after.query('PRAGMA table_info(capability_audit)').all() as { name: string }[]
      ).map((column) => column.name);
      const indexes = (
        after.query('PRAGMA index_list(capability_audit)').all() as { name: string }[]
      ).map((index) => index.name);
      after.close();

      expect(columns).not.toContain('installation_id');
      expect(indexes).toContain('capability_audit_job');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // `capability_audit` is created with `IF NOT EXISTS`, so a database that
  // already has the table skips it entirely and needs the column added by
  // hand. Without the ALTER, every audit write on an existing install fails.
  it('adds the audit actor column to a database created before it existed', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    // Build a faithful pre-actor database: let the current schema create every
    // table, then take the column and the version back off.
    await Effect.runPromise(
      Effect.scoped(
        Effect.flatMap(WorkQueue, (queue) => queue.counts).pipe(Effect.provide(queueLayer(path))),
      ),
    );
    const database = new Database(path);
    database.exec('ALTER TABLE capability_audit DROP COLUMN actor');
    database.exec('PRAGMA user_version = 4');
    database.close();

    try {
      const audit = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            const enqueued = yield* queue.enqueue(work('delivery-actor'), 10);
            yield* queue.recordAudit({
              jobId: enqueued.jobId,
              repository: 'edloidas/lictor',
              actor: 'adiutriel',
              capability: 'get_issue',
              input: '{}',
              outcome: 'ok',
            });
            return yield* queue.auditLog(enqueued.jobId);
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(audit.at(-1)).toMatchObject({ actor: 'adiutriel', capability: 'get_issue' });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // The migration guard checks artifacts, not the version stamp, so a database
  // stamped current but missing one of them must still be repaired. Every
  // `recordAudit` names `actor`, so an unrepaired column fails every
  // acknowledgement — and the guard is the only thing standing between the two.
  it.each(['notification_cursors', 'poller_state', 'capability_audit.actor'])(
    'repairs a database stamped current but missing %s',
    async (artifact) => {
      const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
      const path = join(directory, 'queue.sqlite');
      await Effect.runPromise(
        Effect.scoped(
          Effect.flatMap(WorkQueue, (queue) => queue.counts).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      const database = new Database(path);
      if (artifact === 'capability_audit.actor') {
        database.exec('ALTER TABLE capability_audit DROP COLUMN actor');
      } else {
        database.exec(`DROP TABLE ${artifact}`);
      }
      database.close();

      try {
        const repaired = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const queue = yield* WorkQueue;
              const enqueued = yield* queue.enqueue(work('delivery-repair'), 10);
              yield* queue.recordAudit({
                jobId: enqueued.jobId,
                repository: 'edloidas/lictor',
                actor: 'daemon',
                capability: 'react',
                input: '{}',
                outcome: 'ok',
              });
              yield* queue.advanceNotificationCursor('14567', 1);
              yield* queue.setPollerCursor('Thu, 21 Aug 2026 10:00:00 GMT');
              return {
                audit: yield* queue.auditLog(enqueued.jobId),
                cursor: yield* queue.notificationCursor('14567'),
                poller: yield* queue.pollerCursor,
              };
            }).pipe(Effect.provide(queueLayer(path))),
          ),
        );

        expect(repaired.audit.at(-1)).toMatchObject({ actor: 'daemon', capability: 'react' });
        expect(repaired.cursor).toBe(1);
        expect(repaired.poller).toBe('Thu, 21 Aug 2026 10:00:00 GMT');
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );

  // `DeliverySource` no longer has a `webhook` member, so a leftover
  // claimable row would make the delivery worker look up a decoder that is
  // not there and die on a defect every cycle. The upgrade condemns them
  // instead, and says why in `last_error`.
  it('condemns undrained webhook deliveries when the transport is removed', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    await Effect.runPromise(
      Effect.scoped(
        Effect.flatMap(WorkQueue, (queue) => queue.counts).pipe(Effect.provide(queueLayer(path))),
      ),
    );
    const database = new Database(path);
    database.exec(
      `INSERT INTO deliveries (id, event, body, source, status, received_at)
       VALUES ('legacy-pending', 'issues', '{}', 'webhook', 'pending', 1),
              ('legacy-processing', 'issues', '{}', 'webhook', 'processing', 2),
              ('legacy-done', 'issues', '{}', 'webhook', 'completed', 3)`,
    );
    database.exec('PRAGMA user_version = 6');
    database.close();

    try {
      const statuses = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            return {
              pending: yield* queue.deliveryStatus('legacy-pending'),
              processing: yield* queue.deliveryStatus('legacy-processing'),
              done: yield* queue.deliveryStatus('legacy-done'),
              claimed: yield* queue.claimDelivery,
            };
          }).pipe(Effect.provide(queueLayer(path))),
        ),
      );

      expect(statuses.pending).toBe('failed');
      expect(statuses.processing).toBe('failed');
      // A row already drained is history, not a hazard — leave it alone so the
      // retention window prunes it on schedule.
      expect(statuses.done).toBe('completed');
      expect(statuses.claimed).toBeUndefined();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // Every delivery stored before sources existed came from the webhook, so
  // the migration must backfill the column rather than leave old rows
  // unreadable by a consumer that switches over it. The seeds rewind to
  // faithful pre-source shapes — version 2 predates the table entirely, and
  // the audit table is dropped or stripped to match what each later version
  // actually held, since its own migration is equality-gated on version 4.
  it.each([2, 3, 4, 5])(
    'upgrades a version-%i database so deliveries carry a source',
    async (version) => {
      const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
      const path = join(directory, 'queue.sqlite');
      // Build the current schema first, then rewind it to the shape `version`
      // would have held.
      await Effect.runPromise(
        Effect.scoped(
          Effect.flatMap(WorkQueue, (queue) => queue.counts).pipe(Effect.provide(queueLayer(path))),
        ),
      );
      const database = new Database(path);
      if (version === 2) database.exec('DROP TABLE deliveries');
      else database.exec('ALTER TABLE deliveries DROP COLUMN source');
      if (version === 3) database.exec('DROP TABLE capability_audit');
      if (version === 4) database.exec('ALTER TABLE capability_audit DROP COLUMN actor');
      database.exec(`PRAGMA user_version = ${version}`);
      database.close();

      try {
        const claimed = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const queue = yield* WorkQueue;
              yield* queue.receiveDelivery({
                id: 'delivery-source',
                event: 'notification',
                body: '{}',
                source: 'notification',
              });
              return yield* queue.claimDelivery;
            }).pipe(Effect.provide(queueLayer(path))),
          ),
        );

        expect(claimed).toMatchObject({ id: 'delivery-source', source: 'notification' });
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );

  it('refuses a database created by a newer queue schema', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-queue-'));
    const path = join(directory, 'queue.sqlite');
    const database = new Database(path, { create: true });
    database.exec('PRAGMA user_version = 11');
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
                   VALUES ('poison', 'poison', '{}', 'pending', 0, 0, 0)`,
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

  // Liveness feeds the sweep predicate, where absence means deletable. The
  // result must therefore be exhaustive over every non-terminal status — a
  // page cap here would name live sessions dead — while every terminal
  // status is absent. Live ids span both ends of the table so a
  // newest-first limit of any small size would miss one. `recoverStale`
  // recovers every stale running job, so the interrupted fixture lives in a
  // scope of its own rather than beside the others.
  it('lists live job ids exhaustively across non-terminal statuses', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const now = yield* Clock.currentTimeMillis;

        // Dead-letter first: its recovery passes would flip any other
        // running job, so nothing else may be running yet.
        yield* queue.enqueue(work('j-dead'));
        for (let cycle = 0; cycle < 3; cycle++) {
          const claimed = yield* queue.claim;
          yield* queue.recoverStale((claimed?.leaseExpiresAt ?? 0) + 1);
        }

        yield* queue.enqueue(work('j-running'));
        const running = yield* queue.claim;

        yield* queue.enqueue(work('j-completed'));
        const completed = yield* queue.claim;
        yield* queue.complete(completed?.id ?? -1, completed?.attempts ?? -1, 'done');

        yield* queue.enqueue(work('j-failed'));
        const failed = yield* queue.claim;
        yield* queue.fail(failed?.id ?? -1, failed?.attempts ?? -1, 'final');

        yield* queue.enqueue(work('j-retry'));
        const retried = yield* queue.claim;
        yield* queue.fail(retried?.id ?? -1, retried?.attempts ?? -1, 'temporary', now + 60_000);

        const pending = yield* queue.enqueue(work('j-pending'));

        return {
          live: yield* queue.liveJobIds,
          counts: yield* queue.counts,
          liveIds: [pending.jobId, running?.id, retried?.id].filter(
            (id): id is number => id !== undefined,
          ),
          deadIds: [completed?.id, failed?.id].filter((id): id is number => id !== undefined),
        };
      }),
    );

    expect(result.counts).toMatchObject({
      pending: 1,
      running: 1,
      retry: 1,
      completed: 1,
      failed: 1,
      dead_letter: 1,
    });
    expect(result.live).toEqual(new Set(result.liveIds));
    for (const id of result.deadIds) {
      expect(result.live.has(id)).toBe(false);
    }

    const interruptedResult = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work('j-interrupted'));
        const claimed = yield* queue.claim;
        yield* queue.recoverStale((claimed?.leaseExpiresAt ?? 0) + 1);
        return { live: yield* queue.liveJobIds, counts: yield* queue.counts };
      }),
    );
    expect(interruptedResult.counts).toMatchObject({ interrupted: 1 });
    expect(interruptedResult.live.size).toBe(1);
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

  it('arms liveness per subject and reports it only inside the window', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.markLive({
          repository: 'edloidas/lictor',
          subjectKind: 'issue',
          subjectNumber: 17,
          expiresAt: (yield* Clock.currentTimeMillis) + 60_000,
        });
        const live = yield* queue.livenessFor('edloidas/lictor', 'issue', 17);
        // Case-insensitive on the repository, like every other name compare.
        const liveCanonical = yield* queue.livenessFor('EDLOIDAS/LICTOR', 'issue', 17);
        const expired = yield* queue.livenessFor('edloidas/lictor', 'issue', 18);
        return { live, liveCanonical, expired };
      }),
    );

    expect(result).toEqual({ live: true, liveCanonical: true, expired: false });
  });
});
