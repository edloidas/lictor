import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { type WorkItem, WorkItemSchema } from '../work-item.ts';

const WORKER_LEASE_MS = 60_000;
const DAEMON_LEASE_MS = 30_000;
const DELIVERY_LEASE_MS = 60_000;

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

/**
 * Statuses past which a job never runs again. Only `liveJobIds` reads this
 * list; `maintenance` inlines the same literals in its SQL. The values agree
 * today — keep them that way when editing either.
 */
const TERMINAL_JOB_STATUSES: readonly JobStatus[] = ['completed', 'failed', 'dead_letter'];

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

/**
 * `QueueError.operation` for every delivery write fenced on `attempts`, the way
 * the job writes are fenced on `attemptNumber`. Status alone is not enough: the
 * sweep returns a reclaimed row to `pending` and the next claim increments it,
 * so a row a newer claim has taken is `processing` again.
 *
 * ! A failure carrying one of these means a newer claim owns the row. The
 * ! caller must abandon it — retrying writes over whoever holds it now.
 *
 * Named so the consumer matches on a value, not a copied string literal.
 */
export const CLAIM_FENCED_OPERATIONS = {
  finish: 'finish delivery',
  retry: 'retry delivery',
  renew: 'renew delivery lease',
} as const;

const migrate = (database: Database) => {
  database.exec('PRAGMA journal_mode = WAL');
  database.exec('PRAGMA foreign_keys = ON');
  const version = database.query('PRAGMA user_version').get() as { user_version: number };
  // ! Column presence, not the version stamp, decides whether migration runs:
  // ! a database can carry the current stamp while its tables lack the columns
  // ! (an equality guard once stamped v6 onto v2–v4 it never altered), and only
  // ! presence checks make that state heal.
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
  // Every v10 artifact checked, not a sample: one missing piece would fail on
  // every use — the v6 equality-guard failure, one version later. The
  // `installation_id` check is negative so a database predating its drop heals.
  if (
    version.user_version === 10 &&
    deliveriesHaveSource() &&
    hasColumn('deliveries', 'lease_expires_at') &&
    hasColumn('capability_audit', 'actor') &&
    !hasColumn('capability_audit', 'installation_id') &&
    hasTable('notification_cursors') &&
    hasTable('poller_state') &&
    hasTable('subject_branches') &&
    hasTable('thread_liveness')
  )
    return;
  if (version.user_version > 10) {
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
        last_error TEXT,
        lease_expires_at INTEGER
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
    // Column presence, not the stamp, for the same reason as the guard above:
    // `recordAudit` names this column, so an unrepaired table fails forever.
    if (hasTable('capability_audit') && !hasColumn('capability_audit', 'actor')) {
      database.exec('ALTER TABLE capability_audit ADD COLUMN actor TEXT');
    }
    // App-era vocabulary no code sets any more. The drop keeps the
    // `capability_audit_job` index and every row.
    if (hasTable('capability_audit') && hasColumn('capability_audit', 'installation_id')) {
      database.exec('ALTER TABLE capability_audit DROP COLUMN installation_id');
    }
    // Ordered after the CREATE TABLE IF NOT EXISTS above: a fresh table is
    // created complete; only one predating the column reaches this ALTER.
    if (!deliveriesHaveSource()) {
      database.exec("ALTER TABLE deliveries ADD COLUMN source TEXT NOT NULL DEFAULT 'webhook'");
    }
    if (!hasColumn('deliveries', 'lease_expires_at')) {
      database.exec('ALTER TABLE deliveries ADD COLUMN lease_expires_at INTEGER');
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
        actor TEXT,
        capability TEXT NOT NULL,
        input TEXT NOT NULL,
        outcome TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS capability_audit_job ON capability_audit(job_id, id);
      -- Branch a job created for its subject, so a later interaction continues
      -- on that branch instead of restarting from the default one.
      CREATE TABLE IF NOT EXISTS subject_branches (
        repository TEXT NOT NULL,
        subject_kind TEXT NOT NULL,
        subject_number INTEGER NOT NULL,
        branch TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (repository, subject_kind, subject_number)
      );
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
      -- How long a subject stays open to replies from untrusted participants.
      -- Armed when a trusted sender's interaction produces a job; expired rows
      -- are swept by maintenance.
      CREATE TABLE IF NOT EXISTS thread_liveness (
        repository TEXT NOT NULL,
        subject_kind TEXT NOT NULL,
        subject_number INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        PRIMARY KEY (repository, subject_kind, subject_number)
      );
      PRAGMA user_version = 10;
    `);
    // ! Condemned, not drained: no decoder exists for webhook bodies anymore,
    // ! so leaving one claimable kills the delivery worker on a defect per cycle.
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
             lease_expires_at = NULL, last_error = 'process restarted'
           WHERE status = 'processing'`,
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
          // A redelivery is a fresh arrival GitHub still owns, so `attempts` resets:
          // keeping the count condemns the row on its next claim without running it,
          // and the thread was marked read at receipt — the notification is gone.
          // Safe only because the branch requires `failed`: no live claim holds an
          // attempt number the reset would move out from under.
          const result = database
            .query(
              `INSERT INTO deliveries (id, event, body, source, status, received_at)
               VALUES (?, ?, ?, ?, 'pending', ?)
              ON CONFLICT(id) DO UPDATE SET event = excluded.event, body = excluded.body,
                source = excluded.source, status = 'pending', attempts = 0, claimed_at = NULL,
                processed_at = NULL, last_error = NULL
              WHERE deliveries.status = 'failed'`,
            )
            .run(delivery.id, delivery.event, delivery.body, delivery.source, now);
          return { inserted: result.changes === 1 } as const;
        });
      });

    const claimDelivery = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const outcome = yield* attempt('claim delivery', () =>
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
            // `retryDelivery` and `recoverStaleDeliveries` read the budget only after
            // an attempt is spent, so a row returned to `pending` above the ceiling
            // runs once more unless the claim refuses it first.
            if (attempts > config.workerMaxAttempts) {
              database
                .query(
                  `UPDATE deliveries SET status = 'failed', claimed_at = NULL,
                     lease_expires_at = NULL, processed_at = ?,
                     last_error = 'attempt limit exhausted'
                   WHERE id = ? AND status = 'pending'`,
                )
                .run(now, row.id);
              return { condemned: row } as const;
            }
            database
              .query(
                `UPDATE deliveries SET status = 'processing', attempts = ?, claimed_at = ?,
                   lease_expires_at = ?
                 WHERE id = ? AND status = 'pending'`,
              )
              .run(attempts, now, now + DELIVERY_LEASE_MS, row.id);
            return { ...row, status: 'processing' as const, attempts };
          })
          .immediate(),
      );
      if (outcome !== undefined && 'condemned' in outcome) {
        // The caller only sees an empty claim and a delivery has no dead-letter
        // count, so this log is the sole trace of the drop.
        yield* Effect.logError('Refused a delivery past its attempt budget').pipe(
          Effect.annotateLogs({
            delivery: outcome.condemned.id,
            event: outcome.condemned.event,
            attempts: outcome.condemned.attempts,
          }),
        );
        return undefined;
      }
      return outcome;
    });

    const finishDelivery = (
      id: string,
      attempts: number,
      status: 'completed' | 'failed',
      error?: string,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt(CLAIM_FENCED_OPERATIONS.finish, () => {
          const result = database
            .query(
              `UPDATE deliveries SET status = ?, processed_at = ?, last_error = ?,
                 lease_expires_at = NULL
               WHERE id = ? AND status = 'processing' AND attempts = ?`,
            )
            .run(status, now, error ?? null, id, attempts);
          if (result.changes !== 1) throw new Error(`Delivery ${id} attempt ${attempts} is stale`);
        });
      });

    const retryDelivery = (
      id: string,
      attempts: number,
      error: string,
      terminalAfterAttempts = true,
    ) =>
      attempt(CLAIM_FENCED_OPERATIONS.retry, () => {
        const result = database
          .query(
            `UPDATE deliveries SET
               status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
               attempts = ?,
               claimed_at = NULL,
               lease_expires_at = NULL,
               processed_at = CASE WHEN attempts >= ? THEN unixepoch('subsec') * 1000 ELSE NULL END,
               last_error = ?
             WHERE id = ? AND status = 'processing' AND attempts = ?`,
          )
          .run(
            terminalAfterAttempts ? config.workerMaxAttempts : Number.MAX_SAFE_INTEGER,
            // ! Opting out of the budget refunds the attempt, so `attempts` counts only
            // ! what this delivery's own processing spent. Without it a dead credential
            // ! burns one per cycle and `claimDelivery` condemns the whole inbox.
            terminalAfterAttempts ? attempts : attempts - 1,
            terminalAfterAttempts ? config.workerMaxAttempts : Number.MAX_SAFE_INTEGER,
            error,
            id,
            attempts,
          );
        if (result.changes !== 1) throw new Error(`Delivery ${id} attempt ${attempts} is stale`);
      });

    const heartbeatDelivery = (id: string, attempts: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt(CLAIM_FENCED_OPERATIONS.renew, () => {
          const result = database
            .query(
              `UPDATE deliveries SET lease_expires_at = ?
               WHERE id = ? AND status = 'processing' AND attempts = ?
                 AND lease_expires_at > ?`,
            )
            .run(now + DELIVERY_LEASE_MS, id, attempts, now);
          if (result.changes !== 1) throw new Error(`Delivery ${id} attempt ${attempts} is stale`);
        });
      });

    /**
     * Returns a delivery whose worker died mid-processing, on the same tick as
     * `recoverStale` does for jobs. The attempt budget, not the expiry, is what
     * condemns a row, so the terminal branch mirrors `retryDelivery`.
     *
     * A `NULL` lease never matches this, which is what the startup reset is
     * still for: it covers rows claimed before the column existed.
     */
    const recoverStaleDeliveries = (olderThan: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt(
          'recover stale deliveries',
          () =>
            database
              .query(
                `UPDATE deliveries SET
                 status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
                 claimed_at = NULL,
                 lease_expires_at = NULL,
                 processed_at = CASE WHEN attempts >= ? THEN ? ELSE NULL END,
                 last_error = 'delivery lease expired'
               WHERE status = 'processing' AND lease_expires_at < ?`,
              )
              .run(config.workerMaxAttempts, config.workerMaxAttempts, now, olderThan).changes,
        );
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
        const now = Date.now();
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
        // Pruned on the failed window, the longer of the two: a cursor dropped
        // early makes the qualifier rescan and re-attribute an old comment as
        // fresh activity.
        database.query('DELETE FROM notification_cursors WHERE updated_at < ?').run(failedBefore);
        database.query('DELETE FROM thread_liveness WHERE expires_at < ?').run(now);
        database.exec('PRAGMA wal_checkpoint(PASSIVE)');
        const sizeBytes =
          config.databasePath === ':memory:' ? 0 : statSync(config.databasePath).size;
        return { completed: Number(completed), failed: Number(failed), sizeBytes };
      });

    const recordAudit = (entry: {
      readonly jobId: number;
      readonly repository: string;
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
                (job_id, repository, actor, capability, input, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)`,
            )
            .run(
              entry.jobId,
              entry.repository,
              entry.actor ?? null,
              entry.capability,
              entry.input,
              entry.outcome,
              now,
            );
        });
      });

    /**
     * Arms one subject's live window: replies from untrusted participants may
     * continue the work until it expires. Called when a trusted sender's
     * interaction produces a job; a later trusted trigger extends the window.
     */
    const markLive = (input: {
      readonly repository: string;
      readonly subjectKind: 'issue' | 'pull_request';
      readonly subjectNumber: number;
      /** Epoch ms until which the thread accepts untrusted replies. */
      readonly expiresAt: number;
    }) =>
      Effect.gen(function* () {
        yield* attempt('arm thread liveness', () => {
          database
            .query(
              `INSERT INTO thread_liveness (repository, subject_kind, subject_number, expires_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(repository, subject_kind, subject_number)
                 DO UPDATE SET expires_at = MAX(thread_liveness.expires_at, excluded.expires_at)`,
            )
            .run(
              input.repository.toLowerCase(),
              input.subjectKind,
              input.subjectNumber,
              input.expiresAt,
            );
        });
      });

    const livenessFor = (
      repository: string,
      subjectKind: 'issue' | 'pull_request',
      subjectNumber: number,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('read thread liveness', () => {
          // bun:sqlite answers null, not undefined, for a missing row.
          const row = database
            .query(
              `SELECT expires_at AS expiresAt FROM thread_liveness
               WHERE repository = ? AND subject_kind = ? AND subject_number = ?`,
            )
            .get(repository.toLowerCase(), subjectKind, subjectNumber) as
            | { expiresAt: number }
            | null
            | undefined;
          return row != null && row.expiresAt > now;
        });
      });

    /**
     * Remembers the branch a job created for its subject. The broker records on
     * every successful `create_branch`; the worker reads it back as the clone
     * ref of the next interaction with the same subject, so a follow-up builds
     * on the branch the first one started rather than the default HEAD.
     */
    const recordSubjectBranch = (input: {
      readonly repository: string;
      readonly subjectKind: 'issue' | 'pull_request';
      readonly subjectNumber: number;
      readonly branch: string;
    }) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('record subject branch', () => {
          database
            .query(
              `INSERT INTO subject_branches (repository, subject_kind, subject_number, branch, created_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(repository, subject_kind, subject_number)
                 DO UPDATE SET branch = excluded.branch, created_at = excluded.created_at`,
            )
            .run(
              input.repository.toLowerCase(),
              input.subjectKind,
              input.subjectNumber,
              input.branch,
              now,
            );
        });
      });

    const branchForSubject = (
      repository: string,
      subjectKind: 'issue' | 'pull_request',
      subjectNumber: number,
    ) =>
      attempt('look up subject branch', () => {
        const row = database
          .query(
            `SELECT branch FROM subject_branches
             WHERE repository = ? AND subject_kind = ? AND subject_number = ?`,
          )
          .get(repository.toLowerCase(), subjectKind, subjectNumber) as
          | { branch: string }
          | undefined;
        return row?.branch;
      });

    const auditLog = (jobId: number) =>
      attempt(
        'list capability audit',
        () =>
          database
            .query(
              `SELECT repository, actor, capability, input, outcome,
                 created_at AS createdAt FROM capability_audit WHERE job_id = ? ORDER BY id`,
            )
            .all(jobId) as readonly {
            readonly repository: string;
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

    /**
     * Every non-terminal job id, unbounded.
     *
     * ! Ids only and no `LIMIT`, deliberately. The consumer treats absence as
     * ! dead, so a truncated page would name live sessions deletable —
     * ! over-reporting liveness merely delays a sweep by an hour, while
     * ! under-reporting deletes one mid-execution. The result stays small by
     * ! construction: terminal rows, however many, are never selected.
     */
    const liveJobIds = attempt('list live job ids', () => {
      const rows = database
        .query(
          `SELECT id FROM jobs WHERE status NOT IN (${TERMINAL_JOB_STATUSES.map(() => '?').join(', ')})`,
        )
        .all(...TERMINAL_JOB_STATUSES) as readonly { id: number }[];
      return new Set<number>(rows.map((row) => row.id));
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
      heartbeatDelivery,
      recoverStaleDeliveries,
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
      recordSubjectBranch,
      branchForSubject,
      markLive,
      livenessFor,
      listJobs,
      job,
      liveJobIds,
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
