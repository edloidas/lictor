import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { type WorkItem, WorkItemSchema } from '../work-item.ts';

const WORKER_LEASE_MS = 60_000;
const DAEMON_LEASE_MS = 30_000;

export type InboxStatus = 'pending' | 'processing' | 'completed' | 'failed';

/**
 * The producer that stored a delivery. One per transport; a consumer that
 * decodes stored bodies switches over this instead of assuming an envelope.
 * Grows when a second producer lands.
 *
 * ! `webhook` is gone rather than retained, so no decoder has to exist for a
 * ! transport with no producer. The v7 migration condemns the rows it left
 * ! behind — see `migrate` — because a `claimDelivery` returning a source
 * ! nothing can decode is a defect, not a failure.
 */
export type DeliverySource = 'notification';

export type ReceivedDelivery = {
  readonly id: string;
  readonly event: string;
  readonly body: string;
  readonly source: DeliverySource;
};

export type InboxDelivery = ReceivedDelivery & {
  readonly source: DeliverySource;
  readonly status: InboxStatus;
  readonly attempts: number;
};

export type JobStatus =
  | 'completed'
  | 'dead_letter'
  | 'failed'
  | 'interrupted'
  | 'pending'
  | 'retry'
  | 'running';

export type QueuedJob = {
  readonly id: number;
  readonly work: WorkItem;
  readonly status: JobStatus;
  readonly attempts: number;
  readonly workerId?: string;
  readonly leaseExpiresAt?: number;
  readonly createdAt: number;
};

export type QueueCounts = Readonly<Record<JobStatus, number>>;
export type JobDetails = QueuedJob & {
  readonly lastError?: string;
  readonly createdAt: number;
  readonly updatedAt: number;
};

export class QueueError extends Data.TaggedError('QueueError')<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

type JobRow = {
  readonly id: number;
  readonly payload: string;
  readonly status: JobStatus;
  readonly attempts: number;
  readonly createdAt: number;
  readonly workerId?: string | null;
  readonly leaseExpiresAt?: number | null;
};

const migrate = (database: Database) => {
  database.exec('PRAGMA journal_mode = WAL');
  database.exec('PRAGMA foreign_keys = ON');
  const version = database.query('PRAGMA user_version').get() as { user_version: number };
  // ! Column presence, not the version stamp alone, decides whether migration
  // ! runs. The deliveries table predates its source column by several schema
  // ! versions, and a database can carry the current stamp while its table
  // ! lacks the column — an equality guard once stamped v6 onto v2–v4 tables
  // ! it never altered, leaving every claim dying on a missing column while
  // ! the server kept acknowledging deliveries into a queue nothing could
  // ! drain. Checking presence makes both that state and any like it heal.
  const deliveriesHaveSource = () =>
    (database.query('PRAGMA table_info(deliveries)').all() as { name: string }[]).some(
      (column) => column.name === 'source',
    );
  const hasTable = (name: string) =>
    database.query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name) !==
    null;
  const hasColumn = (table: string, column: string) =>
    (database.query(`PRAGMA table_info(${table})`).all() as { name: string }[]).some(
      (existing) => existing.name === column,
    );
  // ! Every v7 artifact, not a sample of them. A database stamped 7 that is
  // ! missing one table or one column returns early here and then fails on every
  // ! use of it — which is exactly the failure the v6 equality guard used to
  // ! cause, one version later.
  if (
    version.user_version === 7 &&
    deliveriesHaveSource() &&
    hasColumn('capability_audit', 'actor') &&
    hasTable('notification_cursors') &&
    hasTable('poller_state')
  )
    return;
  if (version.user_version > 7) {
    throw new Error(`Unsupported queue schema version ${version.user_version}`);
  }

  database.transaction(() => {
    if (version.user_version === 0)
      database.exec(`
      CREATE TABLE jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id TEXT NOT NULL UNIQUE,
        interaction_id TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'retry', 'interrupted', 'completed', 'failed', 'dead_letter')),
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL,
        claimed_at INTEGER,
        completed_at INTEGER,
        failed_at INTEGER,
        retry_at INTEGER,
        interrupted_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        worker_id TEXT,
        lease_expires_at INTEGER
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
        source TEXT NOT NULL DEFAULT 'webhook',
        status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
        attempts INTEGER NOT NULL DEFAULT 0,
        received_at INTEGER NOT NULL,
        claimed_at INTEGER,
        processed_at INTEGER,
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS deliveries_claimable ON deliveries(status, received_at);
    `);
    if (version.user_version > 0 && version.user_version < 3) {
      database.exec(`
        ALTER TABLE attempts RENAME TO attempts_v2;
        ALTER TABLE jobs RENAME TO jobs_v2;
        DROP INDEX IF EXISTS jobs_claimable;
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          delivery_id TEXT NOT NULL UNIQUE,
          interaction_id TEXT NOT NULL UNIQUE,
          payload TEXT NOT NULL,
          status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'retry', 'interrupted', 'completed', 'failed', 'dead_letter')),
          attempts INTEGER NOT NULL DEFAULT 0,
          available_at INTEGER NOT NULL,
          claimed_at INTEGER,
          completed_at INTEGER,
          failed_at INTEGER,
          retry_at INTEGER,
          interrupted_at INTEGER,
          last_error TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          worker_id TEXT,
          lease_expires_at INTEGER
        );
        INSERT INTO jobs (id, delivery_id, interaction_id, payload, status, attempts, available_at,
          claimed_at, completed_at, failed_at, retry_at, interrupted_at, last_error, created_at, updated_at)
          SELECT id, delivery_id, interaction_id, payload, status, attempts, available_at,
          claimed_at, completed_at, failed_at, retry_at, interrupted_at, last_error, created_at, updated_at FROM jobs_v2;
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
        INSERT INTO attempts SELECT * FROM attempts_v2;
        DROP TABLE attempts_v2;
        DROP TABLE jobs_v2;
        UPDATE attempts SET status = 'interrupted', finished_at = COALESCE(finished_at, started_at),
          error = COALESCE(error, 'migrated without a lease') WHERE status = 'running';
        UPDATE jobs SET status = 'interrupted', available_at = updated_at,
          interrupted_at = updated_at, last_error = 'migrated without a lease'
          WHERE status = 'running' AND lease_expires_at IS NULL;
      `);
    }
    // ! Column presence, not the version stamp, for the same reason the guard
    // ! above checks artifacts rather than trusting `user_version`: a database
    // ! stamped past 4 whose `capability_audit` lacks `actor` would leave this
    // ! migration unrepaired, and every `recordAudit` — which names the column —
    // ! then fails. The block below creates the table for every path that
    // ! predates it, so only a schema that already has it can need the column.
    if (hasTable('capability_audit') && !hasColumn('capability_audit', 'actor')) {
      database.exec('ALTER TABLE capability_audit ADD COLUMN actor TEXT');
    }
    // ! The check sits after the `CREATE TABLE IF NOT EXISTS` above, which is
    // ! its only ordering constraint: a table that never existed is created
    // ! complete, and one that predates the column is altered here.
    if (!deliveriesHaveSource()) {
      database.exec("ALTER TABLE deliveries ADD COLUMN source TEXT NOT NULL DEFAULT 'webhook'");
    }
    database.exec(`
      CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs(status, available_at, id);
      CREATE TABLE IF NOT EXISTS daemon_owner (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        owner_id TEXT NOT NULL,
        heartbeat_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS capability_audit (
        id INTEGER PRIMARY KEY,
        job_id INTEGER NOT NULL,
        repository TEXT NOT NULL,
        installation_id INTEGER,
        actor TEXT,
        capability TEXT NOT NULL,
        input TEXT NOT NULL,
        outcome TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS capability_audit_job ON capability_audit(job_id, id);
      CREATE TABLE IF NOT EXISTS notification_cursors (
        thread_id TEXT PRIMARY KEY,
        last_activity_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS notification_cursors_stale ON notification_cursors(updated_at);
      CREATE TABLE IF NOT EXISTS poller_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        last_modified TEXT
      );
      PRAGMA user_version = 7;
    `);
    // ! Condemned, not drained. `DeliverySource` no longer has a `webhook`
    // ! member, so nothing can decode these bodies — leaving one claimable
    // ! means the delivery worker looks up a decoder that is not there and
    // ! dies on a defect every cycle. `failed` is terminal and `last_error`
    // ! says why, which is the most an upgrade can honestly offer.
    database.exec(
      `UPDATE deliveries
       SET status = 'failed', processed_at = unixepoch('subsec') * 1000,
           last_error = 'webhook transport removed'
       WHERE source = 'webhook' AND status IN ('pending', 'processing')`,
    );
  })();
};

const openDatabase = (path: string) =>
  Effect.try({
    try: () => {
      if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      const database = new Database(path, { create: true, strict: true });
      migrate(database);
      if (path !== ':memory:') {
        chmodSync(path, 0o600);
        for (const suffix of ['-wal', '-shm']) {
          if (existsSync(`${path}${suffix}`)) chmodSync(`${path}${suffix}`, 0o600);
        }
      }
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
  work: Schema.decodeUnknownSync(WorkItemSchema)(JSON.parse(row.payload)),
  status: row.status,
  attempts: row.attempts,
  createdAt: row.createdAt,
  ...(row.workerId == null ? {} : { workerId: row.workerId }),
  ...(row.leaseExpiresAt == null ? {} : { leaseExpiresAt: row.leaseExpiresAt }),
});

export class WorkQueue extends Effect.Service<WorkQueue>()('WorkQueue', {
  scoped: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const database = yield* Effect.acquireRelease(openDatabase(config.databasePath), (connection) =>
      Effect.sync(() => connection.close()),
    );
    const startupTime = yield* Clock.currentTimeMillis;
    const ownerId = randomUUID();
    yield* Effect.acquireRelease(
      attempt('claim daemon ownership', () => {
        const result = database
          .query(
            `INSERT INTO daemon_owner (singleton, owner_id, heartbeat_at, expires_at)
             VALUES (1, ?, ?, ?)
             ON CONFLICT(singleton) DO UPDATE SET owner_id = excluded.owner_id,
               heartbeat_at = excluded.heartbeat_at, expires_at = excluded.expires_at
             WHERE daemon_owner.expires_at < ?`,
          )
          .run(ownerId, startupTime, startupTime + DAEMON_LEASE_MS, startupTime);
        if (result.changes !== 1) throw new Error('Another Lictor daemon owns this database');
      }),
      () =>
        attempt('release daemon ownership', () => {
          const now = Date.now();
          database.transaction(() => {
            database
              .query(
                `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'daemon stopped'
                 WHERE status = 'running' AND job_id IN
                   (SELECT id FROM jobs WHERE status = 'running' AND worker_id = ?)`,
              )
              .run(now, ownerId);
            database
              .query(
                `UPDATE jobs SET status = 'interrupted', available_at = ?, claimed_at = NULL,
                   worker_id = NULL, lease_expires_at = NULL, interrupted_at = ?,
                   last_error = 'daemon stopped', updated_at = ?
                 WHERE status = 'running' AND worker_id = ?`,
              )
              .run(now, now, now, ownerId);
            database.query('DELETE FROM daemon_owner WHERE owner_id = ?').run(ownerId);
          })();
        }).pipe(Effect.orElseSucceed(() => undefined)),
    );
    const startupRecovered = yield* attempt('recover startup jobs', () =>
      database.transaction(() => {
        database
          .query(
            `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'process restarted'
             WHERE status = 'running' AND job_id IN
               (SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at < ?)`,
          )
          .run(startupTime, startupTime);
        return database
          .query(
            `UPDATE jobs
             SET status = 'interrupted', available_at = ?, claimed_at = NULL,
                 interrupted_at = ?, last_error = 'process restarted', updated_at = ?
             WHERE status = 'running' AND lease_expires_at < ?`,
          )
          .run(startupTime, startupTime, startupTime, startupTime).changes;
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
        if (new TextEncoder().encode(delivery.body).byteLength > config.deliveryMaxBytes) {
          return yield* new QueueError({
            operation: 'receive delivery',
            cause: new Error('Delivery body exceeds configured maximum'),
          });
        }
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('receive delivery', () => {
          const result = database
            .query(
              `INSERT INTO deliveries (id, event, body, source, status, received_at)
               VALUES (?, ?, ?, ?, 'pending', ?)
              ON CONFLICT(id) DO UPDATE SET event = excluded.event, body = excluded.body,
                source = excluded.source, status = 'pending', claimed_at = NULL,
                processed_at = NULL, last_error = NULL
              WHERE deliveries.status = 'failed'`,
            )
            .run(delivery.id, delivery.event, delivery.body, delivery.source, now);
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
                `SELECT id, event, body, source, status, attempts FROM deliveries
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

    const retryDelivery = (id: string, error: string, terminalAfterAttempts = true) =>
      attempt('retry delivery', () => {
        const result = database
          .query(
            `UPDATE deliveries SET
               status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
               claimed_at = NULL,
               processed_at = CASE WHEN attempts >= ? THEN unixepoch('subsec') * 1000 ELSE NULL END,
               last_error = ?
             WHERE id = ? AND status = 'processing'`,
          )
          .run(
            terminalAfterAttempts ? config.workerMaxAttempts : Number.MAX_SAFE_INTEGER,
            terminalAfterAttempts ? config.workerMaxAttempts : Number.MAX_SAFE_INTEGER,
            error,
            id,
          );
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

    /**
     * Newest activity already turned into work for one notification thread.
     *
     * ! Not an optimisation. A notification says a thread changed, never which
     * ! comment changed it, so the qualifier scans comments newer than this to
     * ! find the one that mentioned her. Without it the only available anchor is
     * ! `latest_comment_url`, which points at the newest comment rather than the
     * ! triggering one — two comments inside one poll window would then be
     * ! attributed to the wrong author, and the sender check would run against a
     * ! sender who never mentioned anybody.
     */
    const notificationCursor = (threadId: string) =>
      attempt(
        'read notification cursor',
        () =>
          database
            .query(
              'SELECT last_activity_at AS lastActivityAt FROM notification_cursors WHERE thread_id = ?',
            )
            .get(threadId) as { readonly lastActivityAt: number } | null,
      ).pipe(Effect.map((row) => row?.lastActivityAt));

    const advanceNotificationCursor = (threadId: string, lastActivityAt: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('advance notification cursor', () => {
          database
            .query(
              `INSERT INTO notification_cursors (thread_id, last_activity_at, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(thread_id) DO UPDATE SET
                 last_activity_at = MAX(notification_cursors.last_activity_at, excluded.last_activity_at),
                 updated_at = excluded.updated_at`,
            )
            .run(threadId, lastActivityAt, now);
        });
      });

    /**
     * `Last-Modified` from the most recent successful poll.
     *
     * Replayed back as `If-Modified-Since`, whose 304 costs nothing against the
     * rate limit. Durable rather than in-memory so a restart does not re-read
     * the whole notification list against the budget.
     */
    const pollerCursor = attempt(
      'read poller cursor',
      () =>
        database
          .query('SELECT last_modified AS lastModified FROM poller_state WHERE singleton = 1')
          .get() as {
          readonly lastModified: string | null;
        } | null,
    ).pipe(Effect.map((row) => row?.lastModified ?? undefined));

    const setPollerCursor = (lastModified: string) =>
      attempt('write poller cursor', () => {
        database
          .query(
            `INSERT INTO poller_state (singleton, last_modified) VALUES (1, ?)
             ON CONFLICT(singleton) DO UPDATE SET last_modified = excluded.last_modified`,
          )
          .run(lastModified);
      });

    const enqueue = (work: WorkItem, maxDepth = 10_000) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('enqueue', () => {
          const existing = database
            .query('SELECT id FROM jobs WHERE delivery_id = ? OR interaction_id = ?')
            .get(work.deliveryId, work.interactionId) as { id: number } | null;
          if (existing !== null) return { jobId: existing.id, inserted: false } as const;
          const active = database
            .query(
              "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('pending', 'retry', 'interrupted', 'running')",
            )
            .get() as { count: number };
          if (active.count >= maxDepth) throw new Error('QUEUE_DEPTH_LIMIT');
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

    const claimFor = (workerId: string, maxJobAgeMs = Number.POSITIVE_INFINITY) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('claim', () =>
          database
            .transaction(() => {
              const row = database
                .query(
                  `SELECT id, payload, status, attempts, created_at AS createdAt
               FROM jobs
               WHERE status IN ('pending', 'retry', 'interrupted') AND available_at <= ?
                 AND (
                   CASE WHEN json_valid(payload)
                     THEN COALESCE(json_extract(payload, '$.approvalRequired'), 0)
                     ELSE 0 END = 0
                   OR created_at <= ?
                   OR json_type(payload, '$.repository') IS NULL
                   OR json_type(payload, '$.subject') IS NULL
                 )
               ORDER BY available_at, id
               LIMIT 1`,
                )
                .get(now, Number.isFinite(maxJobAgeMs) ? now - maxJobAgeMs : -1) as JobRow | null;
              if (row === null) return undefined;

              const attemptNumber = row.attempts + 1;
              if (attemptNumber > config.workerMaxAttempts) {
                database
                  .query(
                    `UPDATE jobs SET status = 'dead_letter', failed_at = ?, updated_at = ?,
                     last_error = 'attempt limit exhausted' WHERE id = ?`,
                  )
                  .run(now, now, row.id);
                return undefined;
              }
              let decoded: QueuedJob;
              try {
                decoded = decodeJob({ ...row, status: 'running', attempts: attemptNumber });
              } catch {
                database
                  .query(
                    `UPDATE jobs SET status = 'dead_letter', failed_at = ?, updated_at = ?,
                     last_error = 'invalid stored payload' WHERE id = ?`,
                  )
                  .run(now, now, row.id);
                return undefined;
              }
              database
                .query(
                  `UPDATE jobs
               SET status = 'running', attempts = ?, claimed_at = ?, updated_at = ?,
                   worker_id = ?, lease_expires_at = ?
               WHERE id = ?`,
                )
                .run(attemptNumber, now, now, workerId, now + WORKER_LEASE_MS, row.id);
              database
                .query(
                  `INSERT INTO attempts (job_id, number, status, started_at)
               VALUES (?, ?, 'running', ?)`,
                )
                .run(row.id, attemptNumber, now);

              return {
                ...decoded,
                workerId,
                leaseExpiresAt: now + WORKER_LEASE_MS,
              };
            })
            .immediate(),
        );
      });
    const claim = claimFor(ownerId);

    const heartbeat = (jobId: number, attemptNumber: number, workerId: string) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('renew job lease', () => {
          const result = database
            .query(
              `UPDATE jobs SET lease_expires_at = ?, updated_at = ?
               WHERE id = ? AND status = 'running' AND attempts = ? AND worker_id = ?
                 AND lease_expires_at > ?`,
            )
            .run(now + WORKER_LEASE_MS, now, jobId, attemptNumber, workerId, now);
          if (result.changes !== 1)
            throw new Error(`Job ${jobId} attempt ${attemptNumber} is stale`);
        });
      });

    const heartbeatDaemon = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      yield* attempt('renew daemon ownership', () => {
        const result = database
          .query('UPDATE daemon_owner SET heartbeat_at = ?, expires_at = ? WHERE owner_id = ?')
          .run(now, now + DAEMON_LEASE_MS, ownerId);
        if (result.changes !== 1) throw new Error('Daemon ownership was lost');
      });
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
                   (SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at < ?)`,
              )
              .run(now, olderThan);
            return database
              .query(
                `UPDATE jobs
                 SET status = CASE WHEN attempts >= ? THEN 'dead_letter' ELSE 'interrupted' END,
                     available_at = ?, claimed_at = NULL, worker_id = NULL, lease_expires_at = NULL,
                     interrupted_at = ?, failed_at = CASE WHEN attempts >= ? THEN ? ELSE failed_at END,
                     last_error = 'worker interrupted', updated_at = ?
                 WHERE status = 'running' AND lease_expires_at < ?`,
              )
              .run(
                config.workerMaxAttempts,
                now,
                now,
                config.workerMaxAttempts,
                now,
                now,
                olderThan,
              ).changes;
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
        dead_letter: 0,
      };
      const rows = database
        .query('SELECT status, COUNT(*) AS count FROM jobs GROUP BY status')
        .all() as { status: JobStatus; count: number }[];
      for (const row of rows) result[row.status] = row.count;
      return result;
    });

    const maintenance = (completedBefore: number, failedBefore: number) =>
      attempt('maintain queue', () => {
        database
          .query(
            `DELETE FROM capability_audit WHERE job_id IN
             (SELECT id FROM jobs WHERE
               (status = 'completed' AND completed_at < ?) OR
               (status IN ('failed', 'dead_letter') AND failed_at < ?))`,
          )
          .run(completedBefore, failedBefore);
        const completed = database
          .query("DELETE FROM jobs WHERE status = 'completed' AND completed_at < ?")
          .run(completedBefore).changes;
        const failed = database
          .query("DELETE FROM jobs WHERE status IN ('failed', 'dead_letter') AND failed_at < ?")
          .run(failedBefore).changes;
        database
          .query(
            `DELETE FROM deliveries WHERE
             (status = 'completed' AND processed_at < ?) OR
             (status = 'failed' AND processed_at < ?)`,
          )
          .run(completedBefore, failedBefore);
        // ! Pruned on the failed window, the longer of the two. A cursor is the
        // ! only record of what a thread already produced, so dropping one early
        // ! makes the qualifier rescan from the beginning of the thread and
        // ! re-attribute an old comment as fresh activity.
        database.query('DELETE FROM notification_cursors WHERE updated_at < ?').run(failedBefore);
        database.exec('PRAGMA wal_checkpoint(PASSIVE)');
        const sizeBytes =
          config.databasePath === ':memory:' ? 0 : statSync(config.databasePath).size;
        return { completed: Number(completed), failed: Number(failed), sizeBytes };
      });

    const recordAudit = (entry: {
      readonly jobId: number;
      readonly repository: string;
      readonly installationId?: number;
      readonly actor?: string;
      readonly capability: string;
      readonly input: string;
      readonly outcome: string;
    }) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('record capability audit', () => {
          database
            .query(
              `INSERT INTO capability_audit
                (job_id, repository, installation_id, actor, capability, input, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            )
            .run(
              entry.jobId,
              entry.repository,
              entry.installationId ?? null,
              entry.actor ?? null,
              entry.capability,
              entry.input,
              entry.outcome,
              now,
            );
        });
      });

    const auditLog = (jobId: number) =>
      attempt(
        'list capability audit',
        () =>
          database
            .query(
              `SELECT repository, installation_id AS installationId, actor, capability, input, outcome,
                 created_at AS createdAt FROM capability_audit WHERE job_id = ? ORDER BY id`,
            )
            .all(jobId) as readonly {
            readonly repository: string;
            readonly installationId: number | null;
            readonly actor: string | null;
            readonly capability: string;
            readonly input: string;
            readonly outcome: string;
            readonly createdAt: number;
          }[],
      );

    const listJobs = (limit = 100) =>
      attempt('list jobs', () => {
        const rows = database
          .query(
            `SELECT id, payload, status, attempts, last_error AS lastError,
               created_at AS createdAt, updated_at AS updatedAt
             FROM jobs ORDER BY id DESC LIMIT ?`,
          )
          .all(Math.max(1, Math.min(1000, limit))) as readonly (JobRow & {
          readonly lastError: string | null;
          readonly createdAt: number;
          readonly updatedAt: number;
        })[];
        return rows.flatMap((row) => {
          try {
            return [
              {
                ...decodeJob(row),
                ...(row.lastError === null ? {} : { lastError: row.lastError }),
                createdAt: row.createdAt,
                updatedAt: row.updatedAt,
              } satisfies JobDetails,
            ];
          } catch {
            return [];
          }
        });
      });

    const job = (id: number) =>
      attempt('inspect job', () => {
        const row = database
          .query(
            `SELECT id, payload, status, attempts, last_error AS lastError,
               created_at AS createdAt, updated_at AS updatedAt,
               worker_id AS workerId, lease_expires_at AS leaseExpiresAt
             FROM jobs WHERE id = ?`,
          )
          .get(id) as
          | (JobRow & {
              readonly lastError: string | null;
              readonly createdAt: number;
              readonly updatedAt: number;
            })
          | null;
        if (row === null) return undefined;
        const decoded = decodeJob(row);
        return {
          ...decoded,
          ...(row.lastError === null ? {} : { lastError: row.lastError }),
          createdAt: row.createdAt,
          updatedAt: row.updatedAt,
        } satisfies JobDetails;
      });

    const mutateJob = (id: number, action: 'approve' | 'cancel' | 'retry') =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt(`${action} job`, () =>
          database.transaction(() => {
            const row = database.query('SELECT payload, status FROM jobs WHERE id = ?').get(id) as {
              payload: string;
              status: JobStatus;
            } | null;
            if (row === null) return false;
            if (action === 'approve') {
              const payload = JSON.parse(row.payload) as WorkItem;
              if (payload.approvalRequired !== true || row.status !== 'pending') return false;
              return (
                database
                  .query(
                    "UPDATE jobs SET payload = ?, updated_at = ? WHERE id = ? AND status = 'pending'",
                  )
                  .run(JSON.stringify({ ...payload, approvalRequired: false }), now, id).changes ===
                1
              );
            }
            if (action === 'retry') {
              if (row.status !== 'failed' && row.status !== 'dead_letter') return false;
              database.query('DELETE FROM attempts WHERE job_id = ?').run(id);
              return (
                database
                  .query(
                    `UPDATE jobs SET status = 'retry', attempts = 0, available_at = ?, failed_at = NULL,
                   retry_at = ?, last_error = NULL, updated_at = ? WHERE id = ?`,
                  )
                  .run(now, now, now, id).changes === 1
              );
            }
            if (!['pending', 'retry', 'interrupted', 'running'].includes(row.status)) return false;
            database
              .query(
                `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'canceled by operator'
               WHERE job_id = ? AND status = 'running'`,
              )
              .run(now, id);
            return (
              database
                .query(
                  `UPDATE jobs SET status = 'failed', failed_at = ?, worker_id = NULL,
                 lease_expires_at = NULL, last_error = 'canceled by operator', updated_at = ? WHERE id = ?`,
                )
                .run(now, now, id).changes === 1
            );
          })(),
        );
      });

    /**
     * Work already accepted and not yet finished: active jobs plus deliveries
     * the worker has not drained.
     *
     * ! The poller checks this before storing anything, because the depth limit
     * ! itself lives in `enqueue` — which the poller never calls. By the time
     * ! `enqueue` refuses, the notification is already committed and the thread
     * ! already marked read, so GitHub has forgotten it and the overflow has
     * ! nowhere left to sit. Measured here instead, an over-depth sweep simply
     * ! leaves the threads unread and GitHub holds them until the queue drains.
     * !
     * ! A delivery being handed off to a job is counted twice for that moment.
     * ! Deliberate: erring toward a smaller sweep costs a poll interval, erring
     * ! the other way costs whatever the queue could not hold.
     */
    const backlog = attempt('measure backlog', () => {
      const jobs = database
        .query(
          "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('pending', 'retry', 'interrupted', 'running')",
        )
        .get() as { count: number };
      const deliveries = database
        .query("SELECT COUNT(*) AS count FROM deliveries WHERE status IN ('pending', 'processing')")
        .get() as { count: number };
      return jobs.count + deliveries.count;
    });

    const diagnostics = Effect.gen(function* () {
      const jobCounts = yield* counts;
      return yield* attempt('queue diagnostics', () => {
        const oldest = database
          .query(
            `SELECT MIN(created_at) AS createdAt FROM jobs
           WHERE status IN ('pending', 'retry', 'interrupted', 'running')`,
          )
          .get() as { createdAt: number | null };
        const daemon = database
          .query(
            'SELECT owner_id AS ownerId, heartbeat_at AS heartbeatAt FROM daemon_owner WHERE singleton = 1',
          )
          .get() as { ownerId: string; heartbeatAt: number } | null;
        return {
          counts: jobCounts,
          oldestJobAt: oldest.createdAt,
          workerHeartbeatAt: daemon?.heartbeatAt,
          databaseSizeBytes:
            config.databasePath === ':memory:' ? 0 : statSync(config.databasePath).size,
        };
      });
    });

    const backup = (destination: string) =>
      attempt('back up queue', () => {
        if (existsSync(destination)) throw new Error('Backup destination already exists');
        mkdirSync(dirname(destination), { recursive: true, mode: 0o700 });
        database.exec('PRAGMA wal_checkpoint(FULL)');
        database.query('VACUUM INTO ?').run(destination);
        chmodSync(destination, 0o600);
        return { path: destination, sizeBytes: statSync(destination).size };
      });

    return {
      receiveDelivery,
      claimDelivery,
      finishDelivery,
      retryDelivery,
      deliveryStatus,
      notificationCursor,
      advanceNotificationCursor,
      pollerCursor,
      setPollerCursor,
      backlog,
      enqueue,
      claim,
      claimFor,
      heartbeat,
      heartbeatDaemon,
      complete,
      fail,
      recoverStale,
      counts,
      maintenance,
      recordAudit,
      auditLog,
      listJobs,
      job,
      approve: (id: number) => mutateJob(id, 'approve'),
      retry: (id: number) => mutateJob(id, 'retry'),
      cancel: (id: number) => mutateJob(id, 'cancel'),
      diagnostics,
      backup,
      ownerId,
    };
  }),
  dependencies: [LictorConfig.Default],
}) {}
