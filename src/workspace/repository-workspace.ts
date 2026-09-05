import { existsSync, mkdirSync, readdirSync, renameSync, statfsSync, statSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { Data, Effect, Option, Redacted, Ref } from 'effect';
import { LictorConfig } from '../config.ts';
import { ProcessRunner } from '../executor/process-runner.ts';
import { GitHubCredential } from '../github/credential.ts';
import { canonicalRepository, isSafeRepository, type RepositoryPolicy } from '../policy.ts';
import { WorkQueue } from '../queue/work-queue.ts';

export class WorkspaceError extends Data.TaggedError('WorkspaceError')<{
  readonly code: string;
  readonly message: string;
  /**
   * Whether another attempt could plausibly succeed. Absent means yes — most
   * workspace failures are transient. A refused credential is not.
   */
  readonly retryable?: boolean;
  /** How long to wait before retrying, when GitHub asked for a delay. */
  readonly retryAfterMs?: number;
  readonly cause?: unknown;
}> {}

/**
 * `git` reports a refused credential only in prose on stderr, so the text is
 * the only signal available. Retrying either of these costs a full clone cycle
 * and cannot succeed: the token is wrong, or the account lacks write access.
 */
const CREDENTIAL_REJECTED =
  /invalid credentials|authentication failed|could not read username|terminal prompts disabled/i;
const ACCESS_DENIED = /write access to repository not granted/i;
// GitHub answers "not found" for a private repository the credential cannot
// see — indistinguishable from a truly absent one, and one of the two heals.
const REPOSITORY_UNAVAILABLE = /repository not found/i;
// Git reports throttling two ways: prose on the smart-HTTP channel, and a bare
// status when there is no prose. Only matching the first misses the common case.
const RATE_LIMITED = /exceeded a secondary rate limit|rate limit exceeded|returned error: 429/i;

/** Conservative wait when git reports throttling, which carries no reset header. */
const GIT_RATE_LIMIT_WAIT_MS = 60_000;

/** Room for an operator to notice a dead credential and rotate it. */
const CREDENTIAL_ROTATION_WAIT_MS = 5 * 60 * 1000;

/**
 * Spaces out retries under disk pressure, where each attempt pays for a
 * prune-and-probe cycle. It only delays them: attempts are still spent, so
 * three disk failures 15 minutes apart terminally fail the job anyway.
 * Holding the job without spending attempts on a machine-wide condition
 * needs queue support and belongs with the credential breaker in #28.
 */
const DISK_PRESSURE_WAIT_MS = 15 * 60 * 1000;

// 8 KiB truncates clone/fetch stderr right where TLS and credential
// diagnostics live — exactly the prose `classifyGitFailure` reads.
const OUTPUT_LIMIT_BYTES = 65_536;

/**
 * How many `.failed-*` sessions survive for forensics. Without a cap, one
 * repeatedly failing repository fills the disk one full clone at a time.
 */
const RETAINED_SESSION_LIMIT = 8;

/**
 * Floor under the free space a fresh clone may start with. Every session is a
 * full clone now, so a nearly-full disk fails jobs one by one instead of
 * failing once, loudly — this turns that into the loud failure.
 *
 * Not sized to the repository: it passes with 1.01 GiB free and a larger clone
 * still dies mid-tree (that recovers via quarantine + `pruneRetained(0)`).
 * The floor buys an early refusal for small clones, not a guarantee.
 */
const MIN_FREE_BYTES = 1024 ** 3;

/**
 * The one filesystem probe for free space, reading both fields off a single
 * `statfs` result. A dependency rather than a direct call so the suite can pin
 * the disk-pressure paths without faking whole filesystems.
 */
export class DiskStat extends Effect.Service<DiskStat>()('DiskStat', {
  succeed: {
    statfs: (path: string): StatfsResult => {
      const stats = statfsSync(path);
      return { bavail: stats.bavail, bsize: stats.bsize };
    },
  },
}) {}

/**
 * Structurally typed because `statfsSync`'s return is a number/bigint union.
 */
type StatfsResult = { readonly bavail: number | bigint; readonly bsize: number | bigint };

const classifyGitFailure = (
  stderr: string,
  fallback: {
    readonly code: string;
    readonly message: string;
    /** Passed through untouched when nothing in the prose reclassifies it. */
    readonly retryable?: boolean;
  },
): WorkspaceError => {
  if (CREDENTIAL_REJECTED.test(stderr)) {
    // Retryable, but slowly: the credential heals on rotation, so failing the
    // job discards work over a daemon-side problem; the long wait keeps the
    // attempt budget from burning a clone per minute.
    return new WorkspaceError({
      code: 'WORKSPACE_CREDENTIAL_REJECTED',
      message: 'GitHub rejected the daemon credential',
      retryAfterMs: CREDENTIAL_ROTATION_WAIT_MS,
    });
  }
  if (ACCESS_DENIED.test(stderr)) {
    return new WorkspaceError({
      code: 'WORKSPACE_ACCESS_DENIED',
      message: 'The daemon account cannot write to this repository',
      retryable: false,
    });
  }
  if (REPOSITORY_UNAVAILABLE.test(stderr)) {
    return new WorkspaceError({
      code: 'WORKSPACE_REPOSITORY_UNAVAILABLE',
      message: 'The repository does not exist or is invisible to the daemon account',
      retryAfterMs: CREDENTIAL_ROTATION_WAIT_MS,
    });
  }
  if (RATE_LIMITED.test(stderr)) {
    return new WorkspaceError({
      code: 'WORKSPACE_RATE_LIMITED',
      message: 'GitHub throttled the repository operation',
      retryAfterMs: GIT_RATE_LIMIT_WAIT_MS,
    });
  }
  return new WorkspaceError(fallback);
};

export type JobWorkspace = {
  readonly path: string;
};

const REF_FORBIDDEN = /[\s~^:?*[\\]/;
const CONTROL_CHARACTER = /\p{Cc}/u;

/**
 * A ref reaches a command line, so it must not be able to become an option, a
 * range, or a path. This rejects what git itself refuses in refnames, plus a
 * leading dash that would parse as a flag.
 */
const isSafeRef = (ref: string): boolean =>
  ref.length > 0 &&
  !ref.startsWith('-') &&
  !ref.includes('..') &&
  !REF_FORBIDDEN.test(ref) &&
  !CONTROL_CHARACTER.test(ref);

/** Parses `job-<id>` and `job-<id>.failed-<n>`, the two names the service writes. */
const parseSessionName = (
  name: string,
): { readonly id: number; readonly retained: boolean } | undefined => {
  const match = /^job-(\d+)(?:\.failed-\d+)?$/.exec(name);
  if (match === null) return undefined;
  return { id: Number(match[1]), retained: name.includes('.failed-') };
};

export class RepositoryWorkspace extends Effect.Service<RepositoryWorkspace>()(
  'RepositoryWorkspace',
  {
    effect: Effect.gen(function* () {
      const config = yield* LictorConfig;
      const processes = yield* ProcessRunner;
      const disk = yield* DiskStat;
      const credential = yield* GitHubCredential;
      const queue = yield* WorkQueue;
      /**
       * Job ids this daemon has handed out and not yet taken back.
       *
       * ! The queue answers liveness for other processes and past runs; this
       * ! set answers authoritatively for *this* one, where the queue cannot:
       * ! between the sweep's liveness snapshot and its deletions, an operator
       * ! can retry a failed job and a fresh clone can land on the very
       * ! directory the sweep is about to remove. Membership is live regardless
       * ! of what the queue says.
       */
      const owned = yield* Ref.make<ReadonlySet<number>>(new Set<number>());
      const markOwned = (id: number) => Ref.update(owned, (ids) => new Set(ids).add(id));
      const markReleased = (id: number) =>
        Ref.update(owned, (ids) => {
          const next = new Set(ids);
          next.delete(id);
          return next;
        });
      /**
       * Every session lives here, and nothing else decides where. Beside the
       * database, like the Codex home, so relocating state relocates all of it.
       * Keyed by job id alone: a session belongs to exactly one attempt of one
       * job, never to a repository, so nothing outlives the id that named it.
       */
      const root = join(config.stateDir, 'sessions');

      const command = (
        args: readonly string[],
        cwd: string,
        timeoutMs: number,
        env?: Readonly<Record<string, string>>,
      ) =>
        processes.run({
          command: args,
          cwd,
          input: '',
          timeoutMs,
          outputLimitBytes: OUTPUT_LIMIT_BYTES,
          // git fails fast and its prose never approaches the budget, so the
          // head is both what it has always kept and where its refusal is.
          stderrRetention: 'head',
          env: env ?? {
            PATH: process.env.PATH ?? '/usr/bin:/bin',
            HOME: '/var/empty',
            LANG: process.env.LANG ?? 'C.UTF-8',
            GIT_CONFIG_GLOBAL: '/dev/null',
            GIT_CONFIG_SYSTEM: '/dev/null',
            GIT_CONFIG_COUNT: '1',
            GIT_CONFIG_KEY_0: 'core.hooksPath',
            GIT_CONFIG_VALUE_0: '/dev/null',
          },
        });

      // ! The token reaches git only as config-env, never the URL or a
      // ! credential store: it exists solely in this process and one short-lived
      // ! git command's environment.
      const authEnv = (authorization: Redacted.Redacted<string>) => ({
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        HOME: '/var/empty',
        LANG: process.env.LANG ?? 'C.UTF-8',
        GIT_TERMINAL_PROMPT: '0',
        GIT_CONFIG_GLOBAL: '/dev/null',
        GIT_CONFIG_SYSTEM: '/dev/null',
        GIT_CONFIG_COUNT: '2',
        GIT_CONFIG_KEY_0: 'http.https://github.com/.extraHeader',
        GIT_CONFIG_VALUE_0: `Authorization: ${Redacted.value(authorization)}`,
        GIT_CONFIG_KEY_1: 'core.hooksPath',
        GIT_CONFIG_VALUE_1: '/dev/null',
      });

      /**
       * Move a spent session aside for forensics, or delete it when even the
       * rename refuses. Never fails the job over a directory that was about to
       * be replaced anyway. An Effect because the fallback deletes a whole tree
       * asynchronously — unlinking gigabytes must yield the single thread or
       * the heartbeats stall with it.
       */
      const quarantine = (sessionPath: string): Effect.Effect<void> =>
        Effect.gen(function* () {
          if (!existsSync(sessionPath)) return;
          let n = 1;
          while (existsSync(`${sessionPath}.failed-${n}`)) n += 1;
          const renamed = yield* Effect.try(() =>
            renameSync(sessionPath, `${sessionPath}.failed-${n}`),
          ).pipe(Effect.option);
          if (Option.isSome(renamed)) return;
          yield* Effect.tryPromise({
            try: () => rm(sessionPath, { recursive: true, force: true }),
            catch: () =>
              new WorkspaceError({
                code: 'WORKSPACE_QUARANTINE_FAILED',
                message: 'Could not quarantine or delete the spent session',
              }),
          }).pipe(
            Effect.catchAll((cause) =>
              Effect.logWarning('Could not quarantine a spent session').pipe(
                Effect.annotateLogs({ error: cause.message }),
              ),
            ),
          );
        });

      /**
       * Deletes retained sessions oldest-mtime-first until `limit` remain, and
       * returns how many went away. Under genuine disk pressure the caller may
       * pass zero: forensics are worth less than a daemon that runs. Per-entry
       * recovery like the sweep: one wedged tree must not fail an unrelated job
       * whose own session was retained perfectly.
       */
      const pruneRetained = (limit: number): Effect.Effect<number, WorkspaceError> =>
        Effect.gen(function* () {
          const retained = yield* Effect.try({
            try: (): readonly { readonly name: string; readonly mtimeMs: number }[] => {
              if (!existsSync(root)) return [];
              return readdirSync(root)
                .filter((name) => /^job-\d+\.failed-\d+$/.test(name))
                .map((name) => {
                  let mtimeMs = 0;
                  try {
                    mtimeMs = statSync(join(root, name)).mtimeMs;
                  } catch {
                    // Unreadable mtime sorts oldest, so it is deleted first.
                  }
                  return { name, mtimeMs };
                })
                .sort((a, b) => a.mtimeMs - b.mtimeMs);
            },
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_RETENTION_FAILED',
                message: 'Could not enforce the retained-session cap',
                cause,
              }),
          });
          // slice reads a negative end from the tail; clamp so an under-cap
          // count cannot delete forensics instead.
          const doomed = retained.slice(0, Math.max(0, retained.length - limit));
          let pruned = 0;
          for (const { name } of doomed) {
            const removed = yield* Effect.tryPromise({
              try: () => rm(join(root, name), { recursive: true, force: true }),
              catch: (cause) =>
                new WorkspaceError({
                  code: 'WORKSPACE_RETENTION_FAILED',
                  message: 'Could not enforce the retained-session cap',
                  cause,
                }),
            }).pipe(
              Effect.as(true),
              Effect.catchAll((cause) =>
                Effect.logWarning('Could not delete a retained session').pipe(
                  Effect.annotateLogs({ entry: name, error: cause.message }),
                  Effect.as(false),
                ),
              ),
            );
            if (removed) pruned += 1;
          }
          return pruned;
        });

      const prepare = (
        job: { readonly id: number; readonly repository: string; readonly ref?: string },
        policy: RepositoryPolicy,
      ) =>
        Effect.gen(function* () {
          // The one check between a GitHub payload and a `join`.
          if (!isSafeRepository(job.repository)) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_REPOSITORY_INVALID',
              message: 'Invalid repository name',
              retryable: false,
            });
          }
          const repository = canonicalRepository(job.repository);
          // Terminal checks before any filesystem or network work: neither the
          // policy, the name, nor a bad ref heals on retry.
          if (policy.clone !== 'allowed') {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_CLONE_DENIED',
              message: 'Repository cloning is denied by policy',
              retryable: false,
            });
          }
          if (job.ref !== undefined && !isSafeRef(job.ref)) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_REF_INVALID',
              message: 'Invalid ref name',
              retryable: false,
            });
          }

          // Root exists before anything probes it: `statfs` on a missing path
          // would misreport an absent directory as a full disk.
          yield* Effect.try({
            try: () => mkdirSync(root, { recursive: true, mode: 0o700 }),
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_CREATE_FAILED',
                message: 'Could not prepare the session directory',
                cause,
              }),
          });

          const sessionPath = join(root, `job-${job.id}`);
          // Reclaim the job's own leftover before any disk accounting: it is
          // unreachable by every other reclaim path (pruneRetained matches only
          // `.failed-*`, the sweep skips live jobs), so probing first could fail
          // the job on its own dead weight. Sessions are never reused.
          yield* quarantine(sessionPath);
          // Quarantining adds to the retained pool, so the cap applies here too.
          yield* pruneRetained(RETAINED_SESSION_LIMIT).pipe(
            Effect.catchAll((cause) =>
              Effect.logWarning('Could not enforce retention after quarantining').pipe(
                Effect.annotateLogs({ error: cause.message }),
              ),
            ),
          );

          // ! One syscall: two probes can straddle a deletion and disagree with
          // ! each other about the very space this decides on.
          const freeBytes = () => {
            const stats = disk.statfs(root);
            return Number(stats.bavail) * Number(stats.bsize);
          };
          let availableBytes = yield* Effect.try({
            try: freeBytes,
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_DISK_PROBE_FAILED',
                message: 'Could not measure free disk space',
                cause,
              }),
          });
          if (availableBytes < MIN_FREE_BYTES) {
            // Free space binds first, forensics second: reclaim past the cap,
            // down to zero retained, then re-probe once. Otherwise "8 clones
            // held" is stable and every job dies until an operator intervenes.
            const reclaimed = yield* pruneRetained(0).pipe(
              Effect.catchAll((cause) =>
                Effect.logWarning('Could not reclaim retained sessions').pipe(
                  Effect.annotateLogs({ error: cause.message }),
                  Effect.as(0),
                ),
              ),
            );
            availableBytes = yield* Effect.try({
              try: freeBytes,
              catch: (cause) =>
                new WorkspaceError({
                  code: 'WORKSPACE_DISK_PROBE_FAILED',
                  message: 'Could not measure free disk space',
                  cause,
                }),
            });
            if (availableBytes < MIN_FREE_BYTES) {
              return yield* new WorkspaceError({
                code: 'WORKSPACE_DISK_EXHAUSTED',
                message: 'Not enough free disk space to clone',
                retryAfterMs: DISK_PRESSURE_WAIT_MS,
              });
            }
            yield* Effect.logInfo('Reclaimed retained sessions under disk pressure').pipe(
              Effect.annotateLogs({ reclaimed }),
            );
          }

          const authorization = yield* credential.gitAuthHeader;
          // Credential reaches git as env only — argv stays clean for the audit.
          const audited = (
            capability: string,
            argv: readonly string[],
            cwd: string,
            env?: Record<string, string>,
          ) =>
            Effect.gen(function* () {
              const result = yield* command(
                [...argv],
                cwd,
                config.gitTimeoutMs,
                ...(env === undefined ? [] : [env]),
              );
              yield* queue
                .recordAudit({
                  jobId: job.id,
                  repository,
                  actor: config.expectedLogin,
                  capability,
                  input: JSON.stringify(argv),
                  outcome: result.exitCode === 0 ? 'ok' : `exit_${result.exitCode}`,
                })
                .pipe(
                  Effect.catchAll((cause) =>
                    Effect.logWarning('Could not audit a git invocation').pipe(
                      Effect.annotateLogs({ job: job.id, capability, error: cause.message }),
                    ),
                  ),
                );
              return result;
            });
          const populate = Effect.gen(function* () {
            const result = yield* audited(
              'git_clone',
              ['git', 'clone', `https://github.com/${repository}.git`, sessionPath],
              root,
              authEnv(authorization),
            );
            if (result.exitCode !== 0) {
              return yield* classifyGitFailure(result.stderr, {
                code: 'WORKSPACE_CLONE_FAILED',
                message: 'Could not clone repository',
              });
            }

            if (job.ref !== undefined) {
              // Fetch exactly what was asked and detach onto it: silently
              // landing on the default branch would review the wrong tree.
              const fetch = yield* audited(
                'git_fetch',
                ['git', 'fetch', 'origin', job.ref],
                sessionPath,
                authEnv(authorization),
              );
              if (fetch.exitCode !== 0) {
                // Classify first: stderr "not found" may be a refused credential
                // or a rate limit, not a bad ref — only the first heals.
                return yield* classifyGitFailure(fetch.stderr, {
                  code: 'WORKSPACE_REF_UNAVAILABLE',
                  message: 'Could not fetch the requested ref',
                  retryable: false,
                });
              }
              const checkout = yield* audited(
                'git_checkout',
                ['git', 'checkout', '--detach', 'FETCH_HEAD'],
                sessionPath,
              );
              if (checkout.exitCode !== 0) {
                return yield* new WorkspaceError({
                  code: 'WORKSPACE_REF_UNAVAILABLE',
                  message: 'Could not check out the requested ref',
                  retryable: false,
                });
              }
            }
            // No ref given: the clone already checked out remote default HEAD;
            // pinning to `origin/HEAD` would override it.

            return { path: sessionPath } satisfies JobWorkspace;
          });
          // ! Once the directory exists, every non-success exit quarantines and
          // ! prunes before the job leaves the owned set — unmarking first would
          // ! leave the quarantine rename unprotected against a concurrent sweep,
          // ! which deletes evidence outright (acquireUseRelease runs no
          // ! finalizer on a failed acquire).
          return yield* populate.pipe(
            Effect.onError(() =>
              quarantine(sessionPath).pipe(
                Effect.zipRight(
                  pruneRetained(RETAINED_SESSION_LIMIT).pipe(
                    Effect.catchAll((cause) =>
                      Effect.logWarning('Could not enforce retention after a failed acquire').pipe(
                        Effect.annotateLogs({ error: cause.message }),
                      ),
                    ),
                  ),
                ),
                Effect.zipRight(markReleased(job.id)),
              ),
            ),
          );
        });

      const acquire = (
        job: { readonly id: number; readonly repository: string; readonly ref?: string },
        policy: RepositoryPolicy,
      ) =>
        // Ownership claimed before any filesystem work: a session mid-clone is
        // protected even in the queue's stale-snapshot window. Idempotent with
        // prepare's own late-failure release.
        markOwned(job.id).pipe(
          Effect.flatMap(() => prepare(job, policy)),
          Effect.onError(() => markReleased(job.id)),
        );

      const release = (
        jobId: number,
        options: { readonly retain: boolean },
      ): Effect.Effect<void, WorkspaceError> =>
        Effect.gen(function* () {
          const sessionPath = join(root, `job-${jobId}`);
          const spent = options.retain
            ? quarantine(sessionPath).pipe(Effect.zipRight(pruneRetained(RETAINED_SESSION_LIMIT)))
            : Effect.tryPromise({
                try: () => rm(sessionPath, { recursive: true, force: true }),
                catch: (cause) =>
                  new WorkspaceError({
                    code: 'WORKSPACE_RELEASE_FAILED',
                    message: 'Could not delete the job session',
                    cause,
                  }),
              });
          // Released only after the directory work concludes: unmarking first
          // exposes the rename or rm to a concurrent sweep; `ensuring` also
          // keeps a failed rm from leaking the claim.
          return yield* spent.pipe(Effect.ensuring(markReleased(jobId)));
        }).pipe(Effect.asVoid);

      /**
       * Deletes sessions whose jobs are gone, unrecognized debris, and retained
       * overage.
       *
       * Liveness arrives as an effect, resolved strictly *after* this listing:
       * a ready-made predicate would let a job enqueued between snapshot and
       * readdir be deleted out from under a clone that just started. The owned
       * set closes the narrower remaining window — read per entry, immediately
       * before each delete, because the deletions span real seconds.
       */
      const sweep = <E>(
        liveJobIds: Effect.Effect<ReadonlySet<number>, E>,
      ): Effect.Effect<void, WorkspaceError | E> =>
        Effect.gen(function* () {
          const entries = yield* Effect.try({
            try: (): readonly string[] => (existsSync(root) ? readdirSync(root) : []),
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_SWEEP_FAILED',
                message: 'Could not read the sessions directory',
                cause,
              }),
          });
          const live = yield* liveJobIds;
          const removeEntry = (name: string) =>
            Effect.tryPromise({
              try: () => rm(join(root, name), { recursive: true, force: true }),
              catch: (cause) =>
                new WorkspaceError({
                  code: 'WORKSPACE_SWEEP_FAILED',
                  message: 'Could not delete a session directory',
                  cause,
                }),
            }).pipe(
              // One wedged entry must not abandon the rest of the sweep.
              Effect.catchAll((cause) =>
                Effect.logWarning('Session sweep could not delete an entry').pipe(
                  Effect.annotateLogs({ entry: name, error: cause.message }),
                ),
              ),
            );
          for (const entry of entries) {
            const session = parseSessionName(entry);
            if (session === undefined) {
              // Neither daemon nor git wrote this name; it is debris.
              yield* removeEntry(entry);
            } else if (!session.retained && !live.has(session.id)) {
              // Re-read per entry: authoritative at the moment of deletion.
              const held = yield* Ref.get(owned);
              if (!held.has(session.id)) yield* removeEntry(entry);
            }
          }
          yield* pruneRetained(RETAINED_SESSION_LIMIT).pipe(
            Effect.catchAll((cause) =>
              Effect.logWarning('Session sweep could not enforce retention').pipe(
                Effect.annotateLogs({ error: cause.message }),
              ),
            ),
          );
        }).pipe(Effect.asVoid);

      return { acquire, release, sweep };
    }),
    dependencies: [
      LictorConfig.Default,
      ProcessRunner.Default,
      DiskStat.Default,
      GitHubCredential.Default,
      WorkQueue.Default,
    ],
  },
) {}
