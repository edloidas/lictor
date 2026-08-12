import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect } from 'effect';
import { LictorConfig } from '../config.ts';
import type { WorkItem } from '../webhook/qualification.ts';

export type InboxStatus = 'pending' | 'processing' | 'completed' | 'failed';

export type ReceivedDelivery = {
  readonly id: string;
  readonly event: string;
  readonly body: string;
};

export type InboxDelivery = ReceivedDelivery & {
  readonly status: InboxStatus;
  readonly attempts: number;
};

export type JobStatus = 'completed' | 'failed' | 'interrupted' | 'pending' | 'retry' | 'running';

export type QueuedJob = {
  readonly id: number;
  readonly work: WorkItem;
  readonly status: JobStatus;
  readonly attempts: number;
};

export type QueueCounts = Readonly<Record<JobStatus, number>>;

export class QueueError extends Data.TaggedError('QueueError')<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

type JobRow = {
  readonly id: number;
  readonly payload: string;
  readonly status: JobStatus;
  readonly attempts: number;
};

const migrate = (database: Database) => {
  database.exec('PRAGMA journal_mode = WAL');
  database.exec('PRAGMA foreign_keys = ON');
  const version = database.query('PRAGMA user_version').get() as { user_version: number };
  if (version.user_version === 2) return;
  if (version.user_version > 2) {
    throw new Error(`Unsupported queue schema version ${version.user_version}`);
  }

  database.transaction(() => {
    if (version.user_version === 0)
      database.exec(`
      CREATE TABLE jobs (
        id INTEGER PRIMARY KEY,
        delivery_id TEXT NOT NULL UNIQUE,
        interaction_id TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'retry', 'interrupted', 'completed', 'failed')),
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL,
        claimed_at INTEGER,
        completed_at INTEGER,
        failed_at INTEGER,
        retry_at INTEGER,
        interrupted_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX jobs_claimable ON jobs(status, available_at, id);
      CREATE TABLE attempts (
        id INTEGER PRIMARY KEY,
        job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        number INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'interrupted')),
        started_at INTEGER NOT NULL,
        finished_at INTEGER,
        error TEXT,
        output TEXT,
        UNIQUE(job_id, number)
      );
    `);
    database.exec(`
      CREATE TABLE IF NOT EXISTS deliveries (
        id TEXT PRIMARY KEY,
        event TEXT NOT NULL,
        body TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
        attempts INTEGER NOT NULL DEFAULT 0,
        received_at INTEGER NOT NULL,
        claimed_at INTEGER,
        processed_at INTEGER,
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS deliveries_claimable ON deliveries(status, received_at);
      PRAGMA user_version = 2;
    `);
  })();
};

const openDatabase = (path: string) =>
  Effect.try({
    try: () => {
      if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
      const database = new Database(path, { create: true, strict: true });
      migrate(database);
      return database;
    },
    catch: (cause) => new QueueError({ operation: 'open', cause }),
  });

const attempt = <A>(operation: string, body: () => A) =>
  Effect.try({
    try: body,
    catch: (cause) => new QueueError({ operation, cause }),
  });

const decodeJob = (row: JobRow): QueuedJob => ({
  id: row.id,
  work: JSON.parse(row.payload) as WorkItem,
  status: row.status,
  attempts: row.attempts,
});

export class WorkQueue extends Effect.Service<WorkQueue>()('WorkQueue', {
  scoped: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const database = yield* Effect.acquireRelease(openDatabase(config.databasePath), (connection) =>
      Effect.sync(() => connection.close()),
    );
    const startupTime = yield* Clock.currentTimeMillis;
    const startupRecovered = yield* attempt('recover startup jobs', () =>
      database.transaction(() => {
        database
          .query(
            `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'process restarted'
             WHERE status = 'running'`,
          )
          .run(startupTime);
        return database
          .query(
            `UPDATE jobs
             SET status = 'interrupted', available_at = ?, claimed_at = NULL,
                 interrupted_at = ?, last_error = 'process restarted', updated_at = ?
             WHERE status = 'running'`,
          )
          .run(startupTime, startupTime, startupTime).changes;
      })(),
    );
    if (Number(startupRecovered) > 0) {
      yield* Effect.logWarning('Recovered interrupted work').pipe(
        Effect.annotateLogs({ interrupted: Number(startupRecovered) }),
      );
    }

    yield* attempt('recover startup deliveries', () =>
      database
        .query(
          `UPDATE deliveries SET status = 'pending', claimed_at = NULL,
             last_error = 'process restarted' WHERE status = 'processing'`,
        )
        .run(),
    );

    const receiveDelivery = (delivery: ReceivedDelivery) =>
      Effect.gen(function* () {
        if (new TextEncoder().encode(delivery.body).byteLength > config.webhookMaxBytes) {
          return yield* new QueueError({
            operation: 'receive delivery',
            cause: new Error('Delivery body exceeds configured maximum'),
          });
        }
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('receive delivery', () => {
          const result = database
            .query(
              `INSERT INTO deliveries (id, event, body, status, received_at)
               VALUES (?, ?, ?, 'pending', ?)
               ON CONFLICT(id) DO NOTHING`,
            )
            .run(delivery.id, delivery.event, delivery.body, now);
          return { inserted: result.changes === 1 } as const;
        });
      });

    const claimDelivery = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return yield* attempt('claim delivery', () =>
        database
          .transaction(() => {
            const row = database
              .query(
                `SELECT id, event, body, status, attempts FROM deliveries
                 WHERE status = 'pending' ORDER BY received_at, id LIMIT 1`,
              )
              .get() as InboxDelivery | null;
            if (row === null) return undefined;
            const attempts = row.attempts + 1;
            database
              .query(
                `UPDATE deliveries SET status = 'processing', attempts = ?, claimed_at = ?
                 WHERE id = ? AND status = 'pending'`,
              )
              .run(attempts, now, row.id);
            return { ...row, status: 'processing' as const, attempts };
          })
          .immediate(),
      );
    });

    const finishDelivery = (id: string, status: 'completed' | 'failed', error?: string) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('finish delivery', () => {
          const result = database
            .query(
              `UPDATE deliveries SET status = ?, processed_at = ?, last_error = ?
               WHERE id = ? AND status = 'processing'`,
            )
            .run(status, now, error ?? null, id);
          if (result.changes !== 1) throw new Error(`Delivery ${id} is not processing`);
        });
      });

    const retryDelivery = (id: string, error: string) =>
      attempt('retry delivery', () => {
        const result = database
          .query(
            `UPDATE deliveries SET status = 'pending', claimed_at = NULL, last_error = ?
             WHERE id = ? AND status = 'processing'`,
          )
          .run(error, id);
        if (result.changes !== 1) throw new Error(`Delivery ${id} is not processing`);
      });

    const deliveryStatus = (id: string) =>
      attempt(
        'inspect delivery',
        () =>
          database.query('SELECT status FROM deliveries WHERE id = ?').get(id) as {
            readonly status: InboxStatus;
          } | null,
      ).pipe(Effect.map((row) => row?.status));

    const enqueue = (work: WorkItem) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('enqueue', () => {
          const result = database
            .query(
              `INSERT INTO jobs
                (delivery_id, interaction_id, payload, status, available_at, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?)
               ON CONFLICT DO NOTHING`,
            )
            .run(work.deliveryId, work.interactionId, JSON.stringify(work), now, now, now);
          const row = database
            .query('SELECT id FROM jobs WHERE delivery_id = ? OR interaction_id = ?')
            .get(work.deliveryId, work.interactionId) as { id: number };
          return { jobId: row.id, inserted: result.changes === 1 } as const;
        });
      });

    const claim = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return yield* attempt('claim', () =>
        database
          .transaction(() => {
            const row = database
              .query(
                `SELECT id, payload, status, attempts
               FROM jobs
               WHERE status IN ('pending', 'retry', 'interrupted') AND available_at <= ?
               ORDER BY available_at, id
               LIMIT 1`,
              )
              .get(now) as JobRow | null;
            if (row === null) return undefined;

            const attemptNumber = row.attempts + 1;
            database
              .query(
                `UPDATE jobs
               SET status = 'running', attempts = ?, claimed_at = ?, updated_at = ?
               WHERE id = ?`,
              )
              .run(attemptNumber, now, now, row.id);
            database
              .query(
                `INSERT INTO attempts (job_id, number, status, started_at)
               VALUES (?, ?, 'running', ?)`,
              )
              .run(row.id, attemptNumber, now);

            return decodeJob({ ...row, status: 'running', attempts: attemptNumber });
          })
          .immediate(),
      );
    });

    const complete = (jobId: number, attemptNumber: number, output: string) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('complete', () =>
          database.transaction(() => {
            const result = database
              .query(
                `UPDATE jobs SET status = 'completed', completed_at = ?, updated_at = ?
                 WHERE id = ? AND status = 'running' AND attempts = ?`,
              )
              .run(now, now, jobId, attemptNumber);
            if (result.changes !== 1) {
              throw new Error(`Job ${jobId} attempt ${attemptNumber} is stale`);
            }
            database
              .query(
                `UPDATE attempts SET status = 'completed', finished_at = ?, output = ?
                 WHERE job_id = ? AND number = ? AND status = 'running'`,
              )
              .run(now, output, jobId, attemptNumber);
          })(),
        );
      });

    const fail = (jobId: number, attemptNumber: number, error: string, retryAt?: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const status: JobStatus = retryAt === undefined ? 'failed' : 'retry';
        yield* attempt('fail', () =>
          database.transaction(() => {
            const result = database
              .query(
                `UPDATE jobs
                 SET status = ?, available_at = ?, claimed_at = NULL, last_error = ?,
                     failed_at = ?, retry_at = ?, updated_at = ?
                 WHERE id = ? AND status = 'running' AND attempts = ?`,
              )
              .run(
                status,
                retryAt ?? now,
                error,
                retryAt === undefined ? now : null,
                retryAt ?? null,
                now,
                jobId,
                attemptNumber,
              );
            if (result.changes !== 1) {
              throw new Error(`Job ${jobId} attempt ${attemptNumber} is stale`);
            }
            database
              .query(
                `UPDATE attempts SET status = 'failed', finished_at = ?, error = ?
                 WHERE job_id = ? AND number = ? AND status = 'running'`,
              )
              .run(now, error, jobId, attemptNumber);
          })(),
        );
      });

    const recoverStale = (olderThan: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('recover stale jobs', () =>
          database.transaction(() => {
            database
              .query(
                `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'worker interrupted'
                 WHERE status = 'running' AND job_id IN
                   (SELECT id FROM jobs WHERE status = 'running' AND claimed_at < ?)`,
              )
              .run(now, olderThan);
            return database
              .query(
                `UPDATE jobs
                 SET status = 'interrupted', available_at = ?, claimed_at = NULL,
                     interrupted_at = ?, last_error = 'worker interrupted', updated_at = ?
                 WHERE status = 'running' AND claimed_at < ?`,
              )
              .run(now, now, now, olderThan).changes;
          })(),
        );
      });

    const counts = attempt('count jobs', () => {
      const result: Record<JobStatus, number> = {
        pending: 0,
        running: 0,
        retry: 0,
        interrupted: 0,
        completed: 0,
        failed: 0,
      };
      const rows = database
        .query('SELECT status, COUNT(*) AS count FROM jobs GROUP BY status')
        .all() as { status: JobStatus; count: number }[];
      for (const row of rows) result[row.status] = row.count;
      return result;
    });

    return {
      receiveDelivery,
      claimDelivery,
      finishDelivery,
      retryDelivery,
      deliveryStatus,
      enqueue,
      claim,
      complete,
      fail,
      recoverStale,
      counts,
    };
  }),
  dependencies: [LictorConfig.Default],
}) {}
