import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Clock, Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const config = (databasePath: string) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    autoAcceptInviters: [],
    databasePath,
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
        yield* queue.finishDelivery('completed', 'completed');
        yield* queue.receiveDelivery({
          id: 'failed',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
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
        yield* queue.finishDelivery(delivery.id, 'failed', 'old decoder');
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

  // ! `capability_audit` is created with `IF NOT EXISTS`, so a database that
  // ! already has the table skips it entirely and needs the column added by
  // ! hand. Without the ALTER, every audit write on an existing install fails.
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

  // ! The migration guard checks artifacts, not the version stamp, so a database
  // ! stamped current but missing one of them must still be repaired. Every
  // ! `recordAudit` names `actor`, so an unrepaired column fails every
  // ! acknowledgement — and the guard is the only thing standing between the two.
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

  // ! `DeliverySource` no longer has a `webhook` member, so a leftover
  // ! claimable row would make the delivery worker look up a decoder that is
  // ! not there and die on a defect every cycle. The upgrade condemns them
  // ! instead, and says why in `last_error`.
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
      // ! A row already drained is history, not a hazard — leave it alone so the
      // ! retention window prunes it on schedule.
      expect(statuses.done).toBe('completed');
      expect(statuses.claimed).toBeUndefined();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // ! Every delivery stored before sources existed came from the webhook, so
  // ! the migration must backfill the column rather than leave old rows
  // ! unreadable by a consumer that switches over it. The seeds rewind to
  // ! faithful pre-source shapes — version 2 predates the table entirely, and
  // ! the audit table is dropped or stripped to match what each later version
  // ! actually held, since its own migration is equality-gated on version 4.
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
    database.exec('PRAGMA user_version = 7');
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

  // ! Liveness feeds the sweep predicate, where absence means deletable. The
  // ! result must therefore be exhaustive over every non-terminal status — a
  // ! page cap here would name live sessions dead — while every terminal
  // ! status is absent. Live ids span both ends of the table so a
  // ! newest-first limit of any small size would miss one. `recoverStale`
  // ! recovers every stale running job, so the interrupted fixture lives in a
  // ! scope of its own rather than beside the others.
  it('lists live job ids exhaustively across non-terminal statuses', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const now = yield* Clock.currentTimeMillis;

        // ! Dead-letter first: its recovery passes would flip any other
        // ! running job, so nothing else may be running yet.
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
});
