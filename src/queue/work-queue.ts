import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { processAlive } from '../process-liveness.ts';
import { type WorkItem, WorkItemSchema } from '../work-item.ts';

const WORKER_LEASE_MS = 60_000;
const DAEMON_LEASE_MS = 30_000;
/**
 * How long past a lapsed lease an owner is believed alive on its pid alone.
 *
 * The escape from pid reuse, and the reason a takeover cannot deadlock: a
 * crashed daemon's number can be handed to an unrelated process, which reads
 * as alive and would otherwise refuse every start forever. Past this the
 * lease decides again, as it did before liveness was consulted — so the floor
 * is the old behaviour, never worse. Sized well above the 10s heartbeat: an
 * owner this far behind has missed ~60 of them and is not serving work.
 */
const DAEMON_TAKEOVER_GRACE_MS = 10 * 60_000;
const DELIVERY_LEASE_MS = 60_000;
const OUTBOX_LEASE_MS = 60_000;
/**
 * Its own budget rather than `workerMaxAttempts`, which sizes an attempt that
 * clones a repository and runs an agent. A message is one POST, and exhausting
 * the budget is the one way a committed outcome stays unseen.
 */
const OUTBOX_MAX_ATTEMPTS = 10;

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
 * Why a job stopped, kept apart from `status`, which says only what the
 * scheduler may still do with the row. The executor reports four of these and
 * the queue adds three; `failed` covers both an execution failure and one the
 * worker never got to.
 *
 * ! `rejected` is stored under `status = 'failed'`, and so is a `needs_input`
 * ! the queue could not park. Neither is a completion — an agent that refused
 * ! or asked a question did not do the work — and `failed` is also the only
 * ! terminal status `job.retry` accepts, so it is what leaves the operator a
 * ! lever. A question that *was* parked carries no outcome until it ends:
 * ! `unanswered` if its window closes, or whatever the resumed run reports.
 */
export type JobOutcome =
  | 'canceled'
  | 'clipped'
  | 'completed'
  | 'expired'
  | 'failed'
  | 'needs_input'
  | 'rejected'
  | 'unanswered';

export type OutboxStatus = 'pending' | 'sending' | 'delivered' | 'blocked' | 'failed' | 'canceled';

/**
 * The public message a terminal outcome owes its GitHub thread, handed to the
 * write that records the outcome so the two commit together.
 *
 * ! `note` is agent-authored prose and nothing else. Every other string the
 * ! worker holds at a terminal write — an `ExecutorError` message, a policy
 * ! refusal code — is a diagnostic, and this field is published verbatim.
 */
export type OutcomeDelivery = {
  readonly repository: string;
  readonly subjectNumber: number;
  readonly outcome: JobOutcome;
  readonly note?: string;
};

export type OutboxMessage = {
  readonly id: number;
  readonly messageId: string;
  readonly jobId: number;
  readonly attempt: number;
  readonly repository: string;
  readonly subjectNumber: number;
  readonly outcome: JobOutcome;
  readonly note?: string;
  readonly status: OutboxStatus;
  readonly attempts: number;
  readonly createdAt: number;
  /** When delivery may next be attempted; a backoff moves it forward. */
  readonly availableAt: number;
  readonly deliveredAt?: number;
  readonly commentUrl?: string;
  readonly lastError?: string;
};

/**
 * Statuses past which a job never runs again. Only `liveJobIds` reads this
 * list; `maintenance` inlines the same literals in its SQL. The values agree
 * today — keep them that way when editing either.
 */
const TERMINAL_JOB_STATUSES: readonly JobStatus[] = ['completed', 'failed', 'dead_letter'];

/**
 * Not held for an operator approval, with the malformed arms as the exception:
 * a payload carrying no `repository` or no `subject` is claimable despite its
 * hold, because dead-lettering it is the only way it ever finishes. Shared with
 * `claimFor` so the count that gates intake and the claim cannot disagree about
 * which rows are held.
 *
 * Goes in a `WHERE` and nowhere else. `json_type` raises on a malformed
 * payload, and what keeps it unreached is the `json_valid` arm resolving to
 * `0 = 0` first — which relies on SQLite short-circuiting `OR`, and it does
 * that in a predicate but not in a projected column.
 */
const UNHELD_JOBS_WHERE = `(
                   CASE WHEN json_valid(payload)
                     THEN COALESCE(json_extract(payload, '$.approvalRequired'), 0)
                     ELSE 0 END = 0
                   OR json_type(payload, '$.repository') IS NULL
                   OR json_type(payload, '$.subject') IS NULL
                 )`;

/**
 * Work the depth budget holds back: accepted, unfinished, and runnable. Shared
 * by the two counts that gate intake — `backlog`, which the poller reads before
 * it fetches, and `enqueue`'s own refusal a stage later — because they are one
 * rule and drift silently when written twice.
 *
 * ! Both exclusions keep a row the claim skips from deferring the sweep. A job
 * ! parked on its question deferred the sweep its own answer arrives through,
 * ! and the answer expiry then failed it as `unanswered` on a thread that had
 * ! answered; approval holds do that to every parked row at once. So no number
 * ! bounds either population — parked rows are bounded by what one worker can
 * ! park within `limits.answerExpiryHours`, held rows by
 * ! `limits.approvalExpiryHours` against the rate that arms them.
 */
const COUNTED_JOBS_WHERE = `status IN ('pending', 'retry', 'interrupted', 'running')
                 AND question_id IS NULL AND ${UNHELD_JOBS_WHERE}`;

export type QueuedJob = {
  readonly id: number;
  readonly work: WorkItem;
  readonly status: JobStatus;
  readonly attempts: number;
  readonly workerId?: string;
  readonly leaseExpiresAt?: number;
  readonly createdAt: number;
  /**
   * When the job last became runnable, not when it was created. Time spent
   * held for approval doesn't count against the age gate; approving resets
   * this so a long-held job isn't immediately refused by it.
   */
  readonly readyAt: number;
  readonly outcome?: JobOutcome;
  readonly holdExpiresAt?: number;
  /**
   * The outbox `message_id` of a question this job is waiting on, and the only
   * thing that distinguishes a job parked for an answer from one that has
   * simply not run yet — both are `pending`. Set means the claim skips the row
   * and only an answer or the expiry sweep moves it.
   */
  readonly questionId?: string;
  /** Logins allowed to answer that question, fixed when it was asked. */
  readonly questionAnswerers?: readonly string[];
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
  readonly readyAt?: number | null;
  readonly outcome?: JobOutcome | null;
  readonly holdExpiresAt?: number | null;
  readonly questionId?: string | null;
  readonly questionAnswerers?: string | null;
  readonly workerId?: string | null;
  readonly leaseExpiresAt?: number | null;
};

type OutboxRow = {
  readonly id: number;
  readonly messageId: string;
  readonly jobId: number;
  readonly attempt: number;
  readonly repository: string;
  readonly subjectNumber: number;
  readonly outcome: JobOutcome;
  readonly note: string | null;
  readonly status: OutboxStatus;
  readonly attempts: number;
  readonly createdAt: number;
  readonly availableAt: number;
  readonly deliveredAt: number | null;
  readonly commentUrl: string | null;
  readonly lastError: string | null;
};

const decodeOutbox = (row: OutboxRow): OutboxMessage => ({
  id: row.id,
  messageId: row.messageId,
  jobId: row.jobId,
  attempt: row.attempt,
  repository: row.repository,
  subjectNumber: row.subjectNumber,
  outcome: row.outcome,
  status: row.status,
  attempts: row.attempts,
  createdAt: row.createdAt,
  availableAt: row.availableAt,
  ...(row.note === null ? {} : { note: row.note }),
  ...(row.deliveredAt === null ? {} : { deliveredAt: row.deliveredAt }),
  ...(row.commentUrl === null ? {} : { commentUrl: row.commentUrl }),
  ...(row.lastError === null ? {} : { lastError: row.lastError }),
});

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

/** The same fencing, for the outbox rows the delivery of an outcome holds. */
export const OUTBOX_FENCED_OPERATIONS = {
  finish: 'finish outbox message',
  retry: 'retry outbox message',
} as const;

/**
 * Carried as the `cause` of the `QueueError` from an `enqueue` that found the
 * queue at `limit`.
 */
export class QueueFull extends Data.TaggedError('QueueFull')<{
  readonly limit: number;
}> {}

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
  // Every v14 artifact checked, not a sample: one missing piece would fail on
  // every use — the v6 equality-guard failure, one version later. The
  // `installation_id` check is negative so a database predating its drop heals.
  if (
    version.user_version === 14 &&
    hasColumn('daemon_owner', 'pid') &&
    deliveriesHaveSource() &&
    hasColumn('deliveries', 'lease_expires_at') &&
    hasColumn('capability_audit', 'actor') &&
    !hasColumn('capability_audit', 'installation_id') &&
    hasColumn('jobs', 'outcome') &&
    hasColumn('jobs', 'ready_at') &&
    hasColumn('jobs', 'hold_expires_at') &&
    hasColumn('jobs', 'question_id') &&
    hasColumn('jobs', 'question_answerers') &&
    hasTable('notification_cursors') &&
    hasTable('poller_state') &&
    hasTable('subject_branches') &&
    hasTable('thread_liveness') &&
    hasTable('outbox') &&
    hasColumn('outbox', 'message_id') &&
    hasColumn('outbox', 'lease_expires_at')
  )
    return;
  if (version.user_version > 14) {
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
    // Added, not folded into the status CHECK: widening that constraint means
    // rebuilding `jobs`, and renaming it repoints the `attempts` foreign key at
    // the old table, so the rebuild drags a second table with it.
    if (!hasColumn('jobs', 'outcome')) {
      database.exec('ALTER TABLE jobs ADD COLUMN outcome TEXT');
      database.exec(
        `UPDATE jobs SET outcome = CASE
           WHEN status = 'completed' THEN 'completed'
           WHEN status IN ('failed', 'dead_letter') THEN 'failed'
           ELSE NULL END`,
      );
    }
    // Backfilled from `created_at`, which is what the age gate read before
    // this column existed.
    if (!hasColumn('jobs', 'ready_at')) {
      database.exec('ALTER TABLE jobs ADD COLUMN ready_at INTEGER');
      database.exec('UPDATE jobs SET ready_at = created_at WHERE ready_at IS NULL');
    }
    if (!hasColumn('jobs', 'hold_expires_at')) {
      database.exec('ALTER TABLE jobs ADD COLUMN hold_expires_at INTEGER');
    }
    // Left NULL on every existing row, which leaves each of them exactly where
    // it is. A row already terminal at `outcome = 'needs_input'` was asked
    // before a question had an identity anything could answer, so parking it
    // now would only produce waiting work with no way out.
    if (!hasColumn('jobs', 'question_id')) {
      database.exec('ALTER TABLE jobs ADD COLUMN question_id TEXT');
    }
    if (!hasColumn('jobs', 'question_answerers')) {
      database.exec('ALTER TABLE jobs ADD COLUMN question_answerers TEXT');
    }
    database.exec(`
      CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs(status, available_at, id);
      CREATE TABLE IF NOT EXISTS daemon_owner (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        owner_id TEXT NOT NULL,
        heartbeat_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        pid INTEGER
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
      -- One message per terminal job outcome, inserted in the transaction that
      -- writes the outcome. No foreign key: retention prunes jobs on their own
      -- clock, and a cascade would drop an obligation nobody has delivered yet.
      -- The row therefore carries everything delivery needs.
      CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL UNIQUE,
        job_id INTEGER NOT NULL,
        attempt INTEGER NOT NULL,
        repository TEXT NOT NULL,
        subject_number INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        note TEXT,
        status TEXT NOT NULL CHECK (status IN ('pending', 'sending', 'delivered', 'blocked', 'failed', 'canceled')),
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL,
        lease_expires_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        delivered_at INTEGER,
        comment_url TEXT,
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS outbox_claimable ON outbox(status, available_at, id);
      PRAGMA user_version = 14;
    `);
    // Ordered after the CREATE TABLE above, like the `deliveries` and `jobs`
    // ALTERs: a fresh table is created complete, and only one predating the
    // column reaches this.
    if (!hasColumn('daemon_owner', 'pid')) {
      database.exec('ALTER TABLE daemon_owner ADD COLUMN pid INTEGER');
    }
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

/**
 * Never throws. A throw here would dead-letter the row as an undecodable
 * payload, which this column is not part of; an unreadable answer policy means
 * nobody may answer, so the question expires instead of being lost.
 */
const decodeAnswerers = (stored: string | null | undefined): readonly string[] => {
  if (stored == null) return [];
  try {
    const parsed: unknown = JSON.parse(stored);
    return Array.isArray(parsed) ? parsed.filter((login) => typeof login === 'string') : [];
  } catch {
    return [];
  }
};

const decodeJob = (row: JobRow): QueuedJob => ({
  id: row.id,
  work: Schema.decodeUnknownSync(WorkItemSchema)(JSON.parse(row.payload)),
  status: row.status,
  attempts: row.attempts,
  createdAt: row.createdAt,
  // The migration backfills every existing row, so this coalesce is a floor
  // for a row inserted without one, not a path the upgrade leaves behind.
  readyAt: row.readyAt ?? row.createdAt,
  ...(row.outcome == null ? {} : { outcome: row.outcome }),
  ...(row.holdExpiresAt == null ? {} : { holdExpiresAt: row.holdExpiresAt }),
  ...(row.questionId == null
    ? {}
    : {
        questionId: row.questionId,
        questionAnswerers: decodeAnswerers(row.questionAnswerers),
      }),
  ...(row.workerId == null ? {} : { workerId: row.workerId }),
  ...(row.leaseExpiresAt == null ? {} : { leaseExpiresAt: row.leaseExpiresAt }),
});

type OwnerRow = {
  readonly ownerId: string;
  readonly pid: number | null;
  readonly heartbeatAt: number;
  readonly expiresAt: number;
};

/**
 * Whether an owner whose lease has lapsed may be displaced.
 *
 * ! The lease answers "has it stopped reporting", and a suspended laptop or a
 * ! stalled event loop answers yes while the daemon is still running jobs.
 * ! Taking the database from one there is what deletes its live run
 * ! directories and fails a job whose side effects have already landed.
 */
const ownerIsGone = (owner: OwnerRow, now: number): boolean => {
  // ! Recorded before the column existed — and a live owner's row gains a null
  // ! pid the moment an upgraded daemon migrates the table, so this is not
  // ! proof of absence. Only the grace may settle it, or the one daemon still
  // ! running the old code is displaced mid-job during the upgrade.
  if (owner.pid === null) return now - owner.expiresAt > DAEMON_TAKEOVER_GRACE_MS;
  if (!processAlive(owner.pid)) return true;
  return now - owner.expiresAt > DAEMON_TAKEOVER_GRACE_MS;
};

/**
 * Whether an existing row denies this process the database.
 *
 * A row naming our own pid cannot belong to another live process — either this
 * process wrote it, or a dead daemon's number was reassigned here. That test
 * sits ahead of the lease rather than inside `ownerIsGone`: a `bun --watch`
 * reload keeps the pid and runs no finalizer, so behind the lease it refuses
 * against its own row.
 */
const ownerDeniesClaim = (owner: OwnerRow, now: number): boolean => {
  if (owner.pid === process.pid) return false;
  return owner.expiresAt >= now || !ownerIsGone(owner, now);
};

const ownershipRefusal = (owner: OwnerRow, now: number): string =>
  `Another Lictor daemon owns this database: ${owner.pid === null ? 'pid unrecorded' : `pid ${owner.pid}`}, last heartbeat ${Math.round((now - owner.heartbeatAt) / 1000)}s ago. Stop it, or point LICTOR_DATABASE_PATH at a different state directory.`;

export class WorkQueue extends Effect.Service<WorkQueue>()('WorkQueue', {
  scoped: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const database = yield* Effect.acquireRelease(openDatabase(config.databasePath), (connection) =>
      Effect.sync(() => connection.close()),
    );
    const startupTime = yield* Clock.currentTimeMillis;
    const ownerId = randomUUID();
    yield* Effect.acquireRelease(
      Effect.gen(function* () {
        const owner = yield* attempt(
          'read daemon ownership',
          () =>
            database
              .query(
                `SELECT owner_id AS ownerId, pid, heartbeat_at AS heartbeatAt, expires_at AS expiresAt
                 FROM daemon_owner WHERE singleton = 1`,
              )
              .get() as OwnerRow | null,
        );
        if (owner !== null && ownerDeniesClaim(owner, startupTime)) {
          // Logged as well as failed: `QueueError` carries no message, so a
          // described cause names the statement and never the daemon holding
          // the directory — the one thing the operator has to act on.
          yield* Effect.logFatal(ownershipRefusal(owner, startupTime));
          return yield* new QueueError({
            operation: 'claim daemon ownership',
            cause: undefined,
          });
        }
        yield* attempt('claim daemon ownership', () => {
          const result = database
            .query(
              `INSERT INTO daemon_owner (singleton, owner_id, heartbeat_at, expires_at, pid)
               VALUES (1, ?, ?, ?, ?)
               ON CONFLICT(singleton) DO UPDATE SET owner_id = excluded.owner_id,
                 heartbeat_at = excluded.heartbeat_at, expires_at = excluded.expires_at,
                 pid = excluded.pid
               WHERE daemon_owner.expires_at < ? OR daemon_owner.pid = ?`,
            )
            .run(
              ownerId,
              startupTime,
              startupTime + DAEMON_LEASE_MS,
              process.pid,
              startupTime,
              process.pid,
            );
          // The predicate decides, not the read above: two starters that both
          // find the owner gone reach here, and only one matches a lapsed lease.
          // The pid arm does not widen that — two starters are two processes,
          // so only one of them can match its own number.
          if (result.changes !== 1)
            throw new Error('Another Lictor daemon claimed this database first');
        });
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

    /**
     * ! Runs inside the caller's transaction, after its fence has thrown or
     * ! passed, and stores raw fields rather than rendered text. Rendering here
     * ! would put the renderer in the transaction that finishes the job: one
     * ! throw and the outcome rolls back, the lease lapses, and the agent runs
     * ! again — which is what a durable outbox exists to prevent.
     */
    const insertOutbox = (
      delivery: OutcomeDelivery,
      jobId: number,
      attemptNumber: number,
      now: number,
    ) => {
      const messageId = randomUUID();
      database
        .query(
          `INSERT INTO outbox (message_id, job_id, attempt, repository, subject_number,
             outcome, note, status, available_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
        )
        .run(
          messageId,
          jobId,
          attemptNumber,
          delivery.repository,
          delivery.subjectNumber,
          delivery.outcome,
          delivery.note ?? null,
          now,
          now,
          now,
        );
      // Returned because a question is identified by the message that asks it:
      // `outcomeMarker` stamps this id into the posted comment, so it is what a
      // later answer names.
      return messageId;
    };

    /**
     * Outbox rows for outcomes written in bulk over `jobs`, where the message
     * has to come out of the stored payload. A payload that cannot supply a
     * repository and a subject number is skipped rather than inserted: there is
     * nowhere to post it, and the claim admits exactly such rows so it can
     * dead-letter them.
     */
    const insertOutboxFromJobs = (
      outcome: JobOutcome,
      where: string,
      parameters: readonly (string | number)[],
      now: number,
    ) =>
      database
        .query(
          `INSERT INTO outbox (message_id, job_id, attempt, repository, subject_number,
             outcome, status, available_at, created_at, updated_at)
           SELECT lower(hex(randomblob(16))), id, attempts,
                  json_extract(payload, '$.repository'),
                  json_extract(payload, '$.subject.number'),
                  ?, 'pending', ?, ?, ?
           FROM jobs
           WHERE (${where})
             AND json_valid(payload)
             AND json_type(payload, '$.repository') = 'text'
             AND json_type(payload, '$.subject.number') = 'integer'`,
        )
        .run(outcome, now, now, now, ...parameters);

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

    const OUTBOX_COLUMNS = `id, message_id AS messageId, job_id AS jobId, attempt, repository,
        subject_number AS subjectNumber, outcome, note, status, attempts,
        created_at AS createdAt, available_at AS availableAt,
        delivered_at AS deliveredAt, comment_url AS commentUrl,
        last_error AS lastError`;

    const claimOutbox = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return yield* attempt('claim outbox message', () =>
        database
          .transaction(() => {
            const row = database
              .query(
                `SELECT ${OUTBOX_COLUMNS} FROM outbox
                 WHERE status = 'pending' AND available_at <= ?
                 ORDER BY available_at, id LIMIT 1`,
              )
              .get(now) as OutboxRow | null;
            if (row === null) return undefined;
            const attempts = row.attempts + 1;
            database
              .query(
                `UPDATE outbox SET status = 'sending', attempts = ?, lease_expires_at = ?,
                   updated_at = ?
                 WHERE id = ? AND status = 'pending'`,
              )
              .run(attempts, now + OUTBOX_LEASE_MS, now, row.id);
            return decodeOutbox({ ...row, status: 'sending', attempts });
          })
          .immediate(),
      );
    });

    const deliverOutbox = (id: number, attempts: number, commentUrl?: string) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt(OUTBOX_FENCED_OPERATIONS.finish, () => {
          const result = database
            .query(
              `UPDATE outbox SET status = 'delivered', delivered_at = ?, updated_at = ?,
                 lease_expires_at = NULL, comment_url = ?, last_error = NULL
               WHERE id = ? AND status = 'sending' AND attempts = ?`,
            )
            .run(now, now, commentUrl ?? null, id, attempts);
          if (result.changes !== 1)
            throw new Error(`Outbox message ${id} attempt ${attempts} is stale`);
        });
      });

    /** Terminal without delivery: policy forbids the comment, or the budget ran out. */
    const finishOutbox = (
      id: number,
      attempts: number,
      status: 'blocked' | 'failed',
      error: string,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt(OUTBOX_FENCED_OPERATIONS.finish, () => {
          const result = database
            .query(
              `UPDATE outbox SET status = ?, updated_at = ?, lease_expires_at = NULL,
                 last_error = ?
               WHERE id = ? AND status = 'sending' AND attempts = ?`,
            )
            .run(status, now, error, id, attempts);
          if (result.changes !== 1)
            throw new Error(`Outbox message ${id} attempt ${attempts} is stale`);
        });
      });

    const retryOutbox = (
      id: number,
      attempts: number,
      error: string,
      availableAt: number,
      countsAttempt = true,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt(OUTBOX_FENCED_OPERATIONS.retry, () => {
          const result = database
            .query(
              `UPDATE outbox SET
                 status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
                 attempts = ?, available_at = ?, lease_expires_at = NULL, updated_at = ?,
                 last_error = ?
               WHERE id = ? AND status = 'sending' AND attempts = ?`,
            )
            .run(
              // Refunding the attempt keeps a dead credential from spending the
              // whole budget while the daemon-wide breaker is latched.
              countsAttempt ? OUTBOX_MAX_ATTEMPTS : Number.MAX_SAFE_INTEGER,
              countsAttempt ? attempts : attempts - 1,
              availableAt,
              now,
              error,
              id,
              attempts,
            );
          if (result.changes !== 1)
            throw new Error(`Outbox message ${id} attempt ${attempts} is stale`);
        });
      });

    /**
     * ! Returned to `pending`, never re-sent blindly: the POST that lost its
     * ! lease may have reached GitHub. The sender reconciles by marker before
     * ! posting any row it claims with a spent attempt behind it.
     */
    const recoverStaleOutbox = (olderThan: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt(
          'recover stale outbox messages',
          () =>
            database
              .query(
                `UPDATE outbox SET
                 status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
                 lease_expires_at = NULL, available_at = ?, updated_at = ?,
                 last_error = 'sender interrupted'
               WHERE status = 'sending' AND lease_expires_at < ?`,
              )
              .run(OUTBOX_MAX_ATTEMPTS, now, now, olderThan).changes,
        );
      });

    /**
     * The row's state as it stands now, for a sender about to make a request it
     * cannot take back. The fenced write afterwards catches a lost claim, but
     * only once the comment is already on the thread.
     *
     * ! Reports which case it is, never a bare boolean. `canceled` means the
     * ! operator replaced this outcome and it must never post; anything else
     * ! means another pass owns the row and still owes it. Collapsed into one
     * ! answer, a lease this sender keeps losing reads as an operator action
     * ! and the message quietly stops being delivered.
     */
    const outboxHeld = (id: number, attempts: number) =>
      attempt(
        'inspect outbox message',
        () =>
          (database.query('SELECT status, attempts FROM outbox WHERE id = ?').get(id) as {
            status: OutboxStatus;
            attempts: number;
          } | null) ?? undefined,
      ).pipe(
        Effect.map((row) => ({
          held: row?.status === 'sending' && row.attempts === attempts,
          superseded: row?.status === 'canceled',
          status: row?.status,
        })),
      );

    const outboxFor = (jobId: number) =>
      attempt('list outbox messages', () =>
        (
          database
            .query(`SELECT ${OUTBOX_COLUMNS} FROM outbox WHERE job_id = ? ORDER BY id`)
            .all(jobId) as OutboxRow[]
        ).map(decodeOutbox),
      );

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

    const enqueue = (work: WorkItem, maxDepth = 10_000, approvalExpiryMs?: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('enqueue', () => {
          const existing = database
            .query('SELECT id FROM jobs WHERE delivery_id = ? OR interaction_id = ?')
            .get(work.deliveryId, work.interactionId) as { id: number } | null;
          if (existing !== null) return { jobId: existing.id, inserted: false } as const;
          const active = database
            .query(`SELECT COUNT(*) AS count FROM jobs WHERE ${COUNTED_JOBS_WHERE}`)
            .get() as { count: number };
          if (active.count >= maxDepth) throw new QueueFull({ limit: maxDepth });
          const holdExpiresAt =
            work.approvalRequired === true && approvalExpiryMs !== undefined
              ? now + approvalExpiryMs
              : null;
          const result = database
            .query(
              `INSERT INTO jobs
                (delivery_id, interaction_id, payload, status, available_at, created_at, updated_at,
                 ready_at, hold_expires_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?)
               ON CONFLICT DO NOTHING`,
            )
            .run(
              work.deliveryId,
              work.interactionId,
              JSON.stringify(work),
              now,
              now,
              now,
              now,
              holdExpiresAt,
            );
          const row = database
            .query('SELECT id FROM jobs WHERE delivery_id = ? OR interaction_id = ?')
            .get(work.deliveryId, work.interactionId) as { id: number };
          return { jobId: row.id, inserted: result.changes === 1 } as const;
        });
      });

    const claimFor = (workerId: string) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt('claim', () =>
          database
            .transaction(() => {
              // ! An approval-required job is never claimable on age alone. It
              // ! used to be, and the claim then walked it into the worker's
              // ! policy gate, which failed it into a status `job.approve` will
              // ! not accept — so ageing out an unapproved job destroyed the
              // ! operator's ability to approve it. Expiry belongs to
              // ! `maintenance`, which finishes the row without claiming it.
              const row = database
                .query(
                  `SELECT id, payload, status, attempts, created_at AS createdAt,
                      ready_at AS readyAt
               FROM jobs
               WHERE status IN ('pending', 'retry', 'interrupted') AND available_at <= ?
                 AND question_id IS NULL
                 AND ${UNHELD_JOBS_WHERE}
               ORDER BY available_at, id
               LIMIT 1`,
                )
                .get(now) as JobRow | null;
              if (row === null) return undefined;

              const attemptNumber = row.attempts + 1;
              if (attemptNumber > config.workerMaxAttempts) {
                insertOutboxFromJobs('failed', 'id = ?', [row.id], now);
                database
                  .query(
                    `UPDATE jobs SET status = 'dead_letter', outcome = 'failed', failed_at = ?,
                     updated_at = ?, last_error = 'attempt limit exhausted' WHERE id = ?`,
                  )
                  .run(now, now, row.id);
                return undefined;
              }
              let decoded: QueuedJob;
              try {
                decoded = decodeJob({ ...row, status: 'running', attempts: attemptNumber });
              } catch {
                // A payload that fails the schema may still carry a repository
                // and a subject number, and that is all a message needs. The
                // insert's own guards skip the rows where it does not.
                insertOutboxFromJobs('failed', 'id = ?', [row.id], now);
                database
                  .query(
                    `UPDATE jobs SET status = 'dead_letter', outcome = 'failed', failed_at = ?,
                     updated_at = ?, last_error = 'invalid stored payload' WHERE id = ?`,
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

    const complete = (
      jobId: number,
      attemptNumber: number,
      output: string,
      delivery?: OutcomeDelivery,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* attempt('complete', () =>
          database.transaction(() => {
            const result = database
              .query(
                `UPDATE jobs SET status = 'completed', outcome = 'completed',
                   completed_at = ?, updated_at = ?
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
            if (delivery !== undefined) insertOutbox(delivery, jobId, attemptNumber, now);
          })(),
        );
      });

    const fail = (
      jobId: number,
      attemptNumber: number,
      error: string,
      retryAt?: number,
      outcome: JobOutcome = 'failed',
      delivery?: OutcomeDelivery,
    ) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const status: JobStatus = retryAt === undefined ? 'failed' : 'retry';
        yield* attempt('fail', () =>
          database.transaction(() => {
            const result = database
              .query(
                `UPDATE jobs
                 SET status = ?, available_at = ?, claimed_at = NULL, last_error = ?,
                     failed_at = ?, retry_at = ?, updated_at = ?, outcome = ?
                 WHERE id = ? AND status = 'running' AND attempts = ?`,
              )
              .run(
                status,
                retryAt ?? now,
                error,
                retryAt === undefined ? now : null,
                retryAt ?? null,
                now,
                // A row going back to `retry` has not finished, so it carries no
                // outcome yet — the next attempt decides it.
                retryAt === undefined ? outcome : null,
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
            // A row going back to `retry` has not finished, so it owes the
            // thread nothing yet — inserting here posts once per attempt.
            if (delivery !== undefined && retryAt === undefined)
              insertOutbox(delivery, jobId, attemptNumber, now);
          })(),
        );
      });

    /**
     * Parks a job on the question it asked instead of finishing it. The row
     * stays `pending`, the status the approval hold already uses; `question_id`
     * is the whole difference between a job waiting for an answer and one that
     * has simply not run yet, and it is what the claim skips on.
     *
     * ! The attempt that asked is spent and stays spent. Waiting costs no
     * ! further attempt, but the agent ran to produce the question — refunding
     * ! it would let one job ask without bound.
     */
    const park = (input: {
      readonly jobId: number;
      readonly attemptNumber: number;
      readonly repository: string;
      readonly subjectNumber: number;
      /** The question. Recorded either way; published only where it is the agent's. */
      readonly question: string;
      /** Logins allowed to answer, fixed here rather than resolved on reply. */
      readonly answerers: readonly string[];
      readonly expiresAt: number;
      /** What kind of wait this is. Anything but the default is the daemon's own. */
      readonly outcome?: JobOutcome;
    }) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const outcome = input.outcome ?? 'needs_input';
        return yield* attempt('park', () =>
          database.transaction(() => {
            const questionId = insertOutbox(
              {
                repository: input.repository,
                subjectNumber: input.subjectNumber,
                outcome,
                // ! `note` stays agent-authored prose and nothing else. A
                // ! daemon question carries its wording in the outcome's own
                // ! headline, so it publishes without one — otherwise the
                // ! renderer would append "the agent's own summary" to words
                // ! the agent never wrote.
                ...(outcome === 'needs_input' ? { note: input.question } : {}),
              },
              input.jobId,
              input.attemptNumber,
              now,
            );
            const result = database
              .query(
                `UPDATE jobs
                 SET status = 'pending', outcome = NULL, question_id = ?, question_answerers = ?,
                     hold_expires_at = ?, available_at = ?, claimed_at = NULL, worker_id = NULL,
                     lease_expires_at = NULL, retry_at = NULL, failed_at = NULL,
                     last_error = ?, updated_at = ?
                 WHERE id = ? AND status = 'running' AND attempts = ?`,
              )
              .run(
                questionId,
                JSON.stringify(input.answerers),
                input.expiresAt,
                now,
                input.question,
                now,
                input.jobId,
                input.attemptNumber,
              );
            // Throwing rolls the message back with the park. A question posted
            // to a thread nobody is waiting on can never be answered.
            if (result.changes !== 1) {
              throw new Error(`Job ${input.jobId} attempt ${input.attemptNumber} is stale`);
            }
            database
              .query(
                `UPDATE attempts SET status = 'failed', finished_at = ?, error = ?
                 WHERE job_id = ? AND number = ? AND status = 'running'`,
              )
              .run(now, input.question, input.jobId, input.attemptNumber);
            return questionId;
          })(),
        );
      });

    /**
     * Resumes a parked job on an answer to the question it is waiting on.
     *
     * ! Fenced on `question_id`, which is what stops a redelivered answer
     * ! starting a second run: the first clears it and every later one matches
     * ! nothing. `ready_at` resets for the reason `approve` resets it — time
     * ! spent waiting on a person must not age the job out on its next claim.
     *
     * Only `answerUrl` enters the payload. Leaving `sender`, `reasons` and
     * `continuation` alone is what keeps an answer from widening the authority
     * of the turn that asked.
     */
    const answerQuestion = (input: {
      readonly jobId: number;
      readonly questionId: string;
      readonly answerUrl: string;
    }) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        return yield* attempt(
          'answer question',
          () =>
            database
              .query(
                `UPDATE jobs
                 SET question_id = NULL, question_answerers = NULL, hold_expires_at = NULL,
                     ready_at = ?, available_at = ?, last_error = NULL, updated_at = ?,
                     payload = json_set(payload, '$.answerUrl', ?)
                 WHERE id = ? AND status = 'pending' AND question_id = ? AND json_valid(payload)`,
              )
              .run(now, now, now, input.answerUrl, input.jobId, input.questionId).changes === 1,
        );
      });

    /**
     * The question a subject is waiting on, if any. Read before qualification
     * so an answer is recognised in the pass that would otherwise advance the
     * notification cursor past it.
     *
     * ! Gated on the question having been *sent*, not on the daemon having
     * ! recorded it. Consuming a comment as an answer destroys it as work, so a
     * ! question nobody could have seen — never claimed, or refused by a
     * ! repository that withholds `comment` — must match nothing. A POST whose
     * ! response was lost did reach the thread, though, and keying on
     * ! `delivered_at` would hide it for a whole backoff and then stamp a time
     * ! after its answer, losing that answer for good. `created_at` is the park
     * ! instant, always at or before the post, so it can reject no real answer;
     * ! the reverse cost is one comment written before the first send.
     */
    const pendingQuestion = (
      repository: string,
      subjectKind: 'issue' | 'pull_request',
      subjectNumber: number,
    ) =>
      attempt('read pending question', () => {
        const row = database
          .query(
            `SELECT j.id AS jobId, j.question_id AS questionId,
               j.question_answerers AS answerers, o.created_at AS askedAt
             FROM jobs j JOIN outbox o ON o.message_id = j.question_id
             WHERE j.status = 'pending' AND j.question_id IS NOT NULL
               AND o.attempts > 0 AND o.status <> 'blocked'
               AND json_valid(j.payload)
               AND lower(json_extract(j.payload, '$.repository')) = ?
               AND json_extract(j.payload, '$.subject.kind') = ?
               AND json_extract(j.payload, '$.subject.number') = ?
             ORDER BY j.id DESC LIMIT 1`,
          )
          .get(repository.toLowerCase(), subjectKind, subjectNumber) as
          | {
              readonly jobId: number;
              readonly questionId: string;
              readonly answerers: string | null;
              readonly askedAt: number;
            }
          | null
          | undefined;
        return row == null
          ? undefined
          : {
              jobId: row.jobId,
              questionId: row.questionId,
              answerers: decodeAnswerers(row.answerers),
              askedAt: row.askedAt,
            };
      });

    /**
     * Answers already taken on this subject, by the comment each one was.
     *
     * ! Resuming and marking the delivery done are two writes, and a crash
     * ! between them replays the delivery. By then `question_id` is cleared, so
     * ! the comment no longer reads as an answer — it reads as an ordinary
     * ! mention with an identity no job has ever carried, and `enqueue` would
     * ! start a second job doing what the resumed one is already doing.
     */
    const answersTaken = (
      repository: string,
      subjectKind: 'issue' | 'pull_request',
      subjectNumber: number,
    ) =>
      attempt('read answers taken', () => {
        const rows = database
          .query(
            `SELECT DISTINCT json_extract(payload, '$.answerUrl') AS url FROM jobs
             WHERE json_valid(payload)
               AND json_type(payload, '$.answerUrl') = 'text'
               AND lower(json_extract(payload, '$.repository')) = ?
               AND json_extract(payload, '$.subject.kind') = ?
               AND json_extract(payload, '$.subject.number') = ?`,
          )
          .all(repository.toLowerCase(), subjectKind, subjectNumber) as readonly {
          readonly url: string;
        }[];
        return rows.map((row) => row.url);
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
            // Selected before the update, while the rows still match: past it
            // they are `dead_letter` and the predicate no longer finds them.
            insertOutboxFromJobs(
              'failed',
              "status = 'running' AND lease_expires_at < ? AND attempts >= ?",
              [olderThan, config.workerMaxAttempts],
              now,
            );
            return database
              .query(
                `UPDATE jobs
                 SET status = CASE WHEN attempts >= ? THEN 'dead_letter' ELSE 'interrupted' END,
                     outcome = CASE WHEN attempts >= ? THEN 'failed' ELSE outcome END,
                     available_at = ?, claimed_at = NULL, worker_id = NULL, lease_expires_at = NULL,
                     interrupted_at = ?, failed_at = CASE WHEN attempts >= ? THEN ? ELSE failed_at END,
                     last_error = 'worker interrupted', updated_at = ?
                 WHERE status = 'running' AND lease_expires_at < ?`,
              )
              .run(
                config.workerMaxAttempts,
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

    const maintenance = (
      completedBefore: number,
      failedBefore: number,
      heldBefore = Date.now(),
      approvalExpiryMs?: number,
    ) =>
      attempt('maintain queue', () => {
        const now = Date.now();
        // `expired` keeps this apart from a policy refusal and from an
        // execution failure — expiry is decided here, not by the claim (see
        // `claimFor`).
        //
        // A hold with no deadline of its own is due one window after it became
        // ready. That covers rows held before the column existed — which the
        // claim no longer ages out — and dates a re-parked retry from the retry
        // rather than from creation, which would expire it again immediately.
        //
        // `question_id IS NULL` separates this from the answer sweep below by
        // the column rather than by argument: both park a job at `pending`, and
        // both read `hold_expires_at` as their deadline.
        const expiredWhere = `status = 'pending'
               AND question_id IS NULL
               AND CASE WHEN json_valid(payload)
                     THEN COALESCE(json_extract(payload, '$.approvalRequired'), 0)
                     ELSE 0 END = 1
               AND COALESCE(hold_expires_at, ready_at + ?) < ?`;
        const expiryWindow = approvalExpiryMs ?? Number.MAX_SAFE_INTEGER;
        // ! Held work is the one outcome nobody acts on: the requester saw the
        // ! eyes reaction, the operator never approved, and expiry is where the
        // ! thread would go silent forever. The insert and the update share a
        // ! transaction the rest of this sweep does not need — past the update
        // ! the rows no longer match the predicate the insert selects on.
        const expired = database.transaction(() => {
          insertOutboxFromJobs('expired', expiredWhere, [expiryWindow, heldBefore], now);
          return database
            .query(
              `UPDATE jobs
             SET status = 'failed', outcome = 'expired', failed_at = ?, updated_at = ?,
                 hold_expires_at = NULL, last_error = 'approval expired'
             WHERE ${expiredWhere}`,
            )
            .run(now, now, expiryWindow, heldBefore).changes;
        })();
        // The same shape one step later in the lifecycle: a question nobody
        // answered. No window argument and no fallback, unlike the hold above:
        // `park` is the only writer of `question_id` and always stamps a
        // deadline with it, so there is no undated row here to date.
        const unansweredWhere = `status = 'pending'
               AND question_id IS NOT NULL
               AND hold_expires_at < ?`;
        // Its own outcome, not `expired`, because the thread renders it:
        // `expired` is worded for an approval that never came, and this row was
        // waiting on the person it is about to tell that.
        const unanswered = database.transaction(() => {
          insertOutboxFromJobs('unanswered', unansweredWhere, [heldBefore], now);
          return database
            .query(
              `UPDATE jobs
             SET status = 'failed', outcome = 'unanswered', failed_at = ?, updated_at = ?,
                 hold_expires_at = NULL, question_id = NULL, question_answerers = NULL,
                 last_error = 'answer expired'
             WHERE ${unansweredWhere}`,
            )
            .run(now, now, heldBefore).changes;
        })();
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
        // Pruned on the failed window like the jobs they describe, and only once
        // terminal: an undelivered message is an obligation, not history.
        // ! Never the row a parked job is still waiting on, however old. It is
        // ! that question's only record of having been asked, and `retention`
        // ! can be set shorter than the answer window — dropping it strands the
        // ! job at `pending` with a `question_id` nothing can look up, so every
        // ! later reply becomes new work instead of its answer.
        database
          .query(
            `DELETE FROM outbox
             WHERE status IN ('delivered', 'blocked', 'failed', 'canceled') AND updated_at < ?
               AND message_id NOT IN
                 (SELECT question_id FROM jobs WHERE question_id IS NOT NULL)`,
          )
          .run(failedBefore);
        // Pruned on the failed window, the longer of the two: a cursor dropped
        // early makes the qualifier rescan and re-attribute an old comment as
        // fresh activity.
        database.query('DELETE FROM notification_cursors WHERE updated_at < ?').run(failedBefore);
        database.query('DELETE FROM thread_liveness WHERE expires_at < ?').run(now);
        database.exec('PRAGMA wal_checkpoint(PASSIVE)');
        const sizeBytes =
          config.databasePath === ':memory:' ? 0 : statSync(config.databasePath).size;
        return {
          completed: Number(completed),
          failed: Number(failed),
          expired: Number(expired) + Number(unanswered),
          sizeBytes,
        };
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
               created_at AS createdAt, updated_at AS updatedAt,
               ready_at AS readyAt, outcome, hold_expires_at AS holdExpiresAt,
               question_id AS questionId, question_answerers AS questionAnswerers
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
               ready_at AS readyAt, outcome, hold_expires_at AS holdExpiresAt,
               question_id AS questionId, question_answerers AS questionAnswerers,
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

    const mutateJob = (
      id: number,
      action: 'approve' | 'cancel' | 'retry',
      approvalExpiryMs?: number,
    ) =>
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
              // Resets `ready_at` so a long-held job isn't immediately refused
              // by the age gate once approved.
              return (
                database
                  .query(
                    `UPDATE jobs SET payload = ?, updated_at = ?, ready_at = ?,
                       available_at = ?, hold_expires_at = NULL
                     WHERE id = ? AND status = 'pending'`,
                  )
                  .run(JSON.stringify({ ...payload, approvalRequired: false }), now, now, now, id)
                  .changes === 1
              );
            }
            if (action === 'retry') {
              if (row.status !== 'failed' && row.status !== 'dead_letter') return false;
              database.query('DELETE FROM attempts WHERE job_id = ?').run(id);
              // The retried job writes its own outcome. A message describing the
              // one it replaces must not post after it.
              database
                .query(
                  `UPDATE outbox SET status = 'canceled', updated_at = ?,
                     lease_expires_at = NULL, last_error = 'superseded by operator retry'
                   WHERE job_id = ? AND status IN ('pending', 'sending')`,
                )
                .run(now, id);
              // ! A job still carrying `approvalRequired` goes back to `pending`
              // ! with a fresh deadline, never to `retry`. The claim skips
              // ! approval-required rows, and `approve` and the expiry sweep
              // ! both read `pending` only — so a held job left in `retry` can
              // ! never run, be approved, or expire.
              const held = (JSON.parse(row.payload) as WorkItem).approvalRequired === true;
              return (
                database
                  .query(
                    `UPDATE jobs SET status = ?, attempts = 0, available_at = ?, failed_at = NULL,
                   retry_at = ?, last_error = NULL, updated_at = ?, ready_at = ?, outcome = NULL,
                   hold_expires_at = ?
                   WHERE id = ?`,
                  )
                  .run(
                    held ? 'pending' : 'retry',
                    now,
                    now,
                    now,
                    now,
                    held && approvalExpiryMs !== undefined ? now + approvalExpiryMs : null,
                    id,
                  ).changes === 1
              );
            }
            if (!['pending', 'retry', 'interrupted', 'running'].includes(row.status)) return false;
            // The thread saw the eyes reaction and would otherwise never hear
            // again. A `running` job is the sharper case: the agent may already
            // have pushed a branch, and its own terminal write loses the fence.
            insertOutboxFromJobs('canceled', 'id = ?', [id], now);
            database
              .query(
                `UPDATE attempts SET status = 'interrupted', finished_at = ?, error = 'canceled by operator'
               WHERE job_id = ? AND status = 'running'`,
              )
              .run(now, id);
            return (
              database
                .query(
                  `UPDATE jobs SET status = 'failed', outcome = 'canceled', failed_at = ?,
                 worker_id = NULL, lease_expires_at = NULL, hold_expires_at = NULL,
                 question_id = NULL, question_answerers = NULL,
                 last_error = 'canceled by operator', updated_at = ? WHERE id = ?`,
                )
                .run(now, now, id).changes === 1
            );
          })(),
        );
      });

    /**
     * Work already accepted and not yet finished: jobs the depth budget counts
     * (`COUNTED_JOBS_WHERE`) plus deliveries the worker has not drained.
     *
     * ! The poller checks this before storing anything, because the depth limit
     * ! itself lives in `enqueue` — which the poller never calls. By the time
     * ! `enqueue` refuses, the notification is already committed and the thread
     * ! already marked read, so GitHub has forgotten it and the overflow has
     * ! nowhere left to sit. Measured here instead, an over-depth sweep simply
     * ! leaves the threads unread and GitHub holds them until the queue drains.
     * !
     * ! This is the wider of the two counts by the deliveries term, and must
     * ! stay that way: `enqueue` refusing what the sweep admitted is the state
     * ! above.
     * !
     * ! A delivery being handed off to a job is counted twice for that moment.
     * ! Deliberate: erring toward a smaller sweep costs a poll interval, erring
     * ! the other way costs whatever the queue could not hold.
     *
     * `answerQuestion` and `approve` both return a row to this count without
     * consulting it, so a run of either can leave it briefly over the limit.
     * The sweep defers until the worker drains the excess, and a delivery
     * already admitted that then meets a full `enqueue` is refused, refunded,
     * and retried rather than dropped.
     */
    const backlog = attempt('measure backlog', () => {
      const jobs = database
        .query(`SELECT COUNT(*) AS count FROM jobs WHERE ${COUNTED_JOBS_WHERE}`)
        .get() as { count: number };
      const deliveries = database
        .query("SELECT COUNT(*) AS count FROM deliveries WHERE status IN ('pending', 'processing')")
        .get() as { count: number };
      return jobs.count + deliveries.count;
    });

    const diagnostics = Effect.gen(function* () {
      const jobCounts = yield* counts;
      return yield* attempt('queue diagnostics', () => {
        // Deliberately not `COUNTED_JOBS_WHERE`: this answers what the operator
        // is still waiting on, and a job parked for two days is the row they
        // most want to see. That count gates intake; this one only reports.
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
      claimOutbox,
      deliverOutbox,
      finishOutbox,
      retryOutbox,
      recoverStaleOutbox,
      outboxHeld,
      outboxFor,
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
      park,
      answerQuestion,
      pendingQuestion,
      answersTaken,
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
      retry: (id: number, approvalExpiryMs?: number) => mutateJob(id, 'retry', approvalExpiryMs),
      cancel: (id: number) => mutateJob(id, 'cancel'),
      diagnostics,
      backup,
      ownerId,
    };
  }),
  dependencies: [LictorConfig.Default],
}) {}
