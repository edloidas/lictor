import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { Data, Effect, PartitionedSemaphore, Redacted, Ref } from 'effect';
import { LictorConfig } from '../config.ts';
import { ProcessRunner } from '../executor/process-runner.ts';
import { GitHubCredential } from '../github/credential.ts';
import { isSafeRepository, type RepositoryPolicy } from '../policy.ts';
import type { WorkItem } from '../work-item.ts';

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
// ! Separate from access denial, because GitHub answers "not found" for a private
// ! repository the credential cannot see. That is indistinguishable from a
// ! repository that truly does not exist, and one of the two heals — on a token
// ! rotation, an SSO authorization, or an invitation being accepted.
const REPOSITORY_UNAVAILABLE = /repository not found/i;
// ! Git reports API throttling two ways: the prose GitHub writes on the
// ! smart-HTTP channel, and the bare status the transport surfaces when there is
// ! no prose at all. Only matching the first left the common case falling
// ! through to a generic failure that retries on the short exponential base.
const RATE_LIMITED = /exceeded a secondary rate limit|rate limit exceeded|returned error: 429/i;

/** Conservative wait when git reports throttling, which carries no reset header. */
const GIT_RATE_LIMIT_WAIT_MS = 60_000;

/** Room for an operator to notice a dead credential and rotate it. */
const CREDENTIAL_ROTATION_WAIT_MS = 5 * 60 * 1000;

const classifyGitFailure = (
  stderr: string,
  fallback: { readonly code: string; readonly message: string },
): WorkspaceError => {
  if (CREDENTIAL_REJECTED.test(stderr)) {
    // ! Retryable, but slowly. A refused credential heals the moment an operator
    // ! rotates the token, so failing the job outright discards work for a
    // ! daemon-side problem that says nothing about the job — the same reasoning
    // ! that stops a delivery being condemned for it. The long wait is what
    // ! stops the attempt budget burning a clone cycle per minute in the
    // ! meantime. Retaining the job without spending attempts at all needs queue
    // ! support and belongs with the credential breaker in #28.
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
  readonly repository: string;
  readonly clonePath: string;
  readonly worktreePath: string;
};

export class RepositoryWorkspace extends Effect.Service<RepositoryWorkspace>()(
  'RepositoryWorkspace',
  {
    effect: Effect.gen(function* () {
      const config = yield* LictorConfig;
      const processes = yield* ProcessRunner;
      const credential = yield* GitHubCredential;
      const locks = yield* PartitionedSemaphore.make<string>({ permits: 1 });
      /** Worktrees this daemon created and has not cleaned up. */
      const owned = yield* Ref.make<ReadonlySet<string>>(new Set());
      /**
       * Every clone lives here, and nothing else decides where. Beside the
       * database, like the Codex home, so relocating state relocates all of it.
       */
      const root = join(dirname(resolve(config.databasePath)), 'workspaces');

      const command = (
        args: readonly string[],
        cwd: string,
        env?: Readonly<Record<string, string>>,
      ) =>
        processes.run({
          command: args,
          cwd,
          input: '',
          timeoutMs: 120_000,
          outputLimitBytes: 8192,
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

      const resolveClone = (work: WorkItem, policy: RepositoryPolicy) =>
        Effect.gen(function* () {
          // ! The one check between a GitHub payload and a `join`. Terminal:
          // ! nothing about a name changes on a retry or a token rotation.
          if (!isSafeRepository(work.repository)) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_REPOSITORY_INVALID',
              message: 'Invalid repository name',
              retryable: false,
            });
          }
          const repository = work.repository.trim().toLowerCase();
          const candidate = join(root, repository);

          // ! Asking git, not the filesystem. `git clone` writes `.git` first
          // ! and cleans up after its own failures only, so a hard kill leaves
          // ! a directory that looks like a clone and has no resolvable `HEAD`.
          const healthy =
            existsSync(candidate) &&
            (yield* command(
              ['git', '-C', candidate, 'rev-parse', '--verify', '--quiet', 'HEAD'],
              candidate,
            )).exitCode === 0;

          if (!healthy) {
            // ! Terminal: neither the policy nor the unusable directory
            // ! changes between attempts.
            if (policy.clone !== 'allowed') {
              return yield* new WorkspaceError({
                code: 'WORKSPACE_CLONE_DENIED',
                message: 'Repository cloning is denied by policy',
                retryable: false,
              });
            }
            yield* Effect.try({
              try: () => {
                rmSync(candidate, { recursive: true, force: true });
                mkdirSync(dirname(candidate), { recursive: true, mode: 0o700 });
              },
              catch: (cause) =>
                new WorkspaceError({
                  code: 'WORKSPACE_CREATE_FAILED',
                  message: 'Could not create workspace parent',
                  cause,
                }),
            });
            const authorization = yield* credential.gitAuthHeader;
            const result = yield* command(
              ['git', 'clone', `https://github.com/${repository}.git`, candidate],
              dirname(candidate),
              {
                PATH: process.env.PATH ?? '/usr/bin:/bin',
                HOME: '/var/empty',
                GIT_CONFIG_GLOBAL: '/dev/null',
                GIT_CONFIG_SYSTEM: '/dev/null',
                GIT_TERMINAL_PROMPT: '0',
                GIT_CONFIG_COUNT: '2',
                GIT_CONFIG_KEY_0: 'http.https://github.com/.extraHeader',
                GIT_CONFIG_VALUE_0: `Authorization: ${Redacted.value(authorization)}`,
                GIT_CONFIG_KEY_1: 'core.hooksPath',
                GIT_CONFIG_VALUE_1: '/dev/null',
              },
            );
            if (result.exitCode !== 0) {
              return yield* classifyGitFailure(result.stderr, {
                code: 'WORKSPACE_CLONE_FAILED',
                message: 'Could not clone repository',
              });
            }
          }

          const authorization = yield* credential.gitAuthHeader;
          const fetch = yield* command(
            ['git', '-C', candidate, 'fetch', '--prune', 'origin'],
            candidate,
            {
              PATH: process.env.PATH ?? '/usr/bin:/bin',
              HOME: '/var/empty',
              GIT_CONFIG_GLOBAL: '/dev/null',
              GIT_CONFIG_SYSTEM: '/dev/null',
              GIT_TERMINAL_PROMPT: '0',
              GIT_CONFIG_COUNT: '2',
              GIT_CONFIG_KEY_0: 'http.https://github.com/.extraHeader',
              GIT_CONFIG_VALUE_0: `Authorization: ${Redacted.value(authorization)}`,
              GIT_CONFIG_KEY_1: 'core.hooksPath',
              GIT_CONFIG_VALUE_1: '/dev/null',
            },
          );
          if (fetch.exitCode !== 0) {
            return yield* classifyGitFailure(fetch.stderr, {
              code: 'WORKSPACE_FETCH_FAILED',
              message: 'Could not refresh repository state',
            });
          }
          return { path: candidate, repository, fresh: !healthy };
        });

      const create = (jobId: number, work: WorkItem, policy: RepositoryPolicy) =>
        Effect.gen(function* () {
          const clone = yield* resolveClone(work, policy);
          const clonePath = clone.path;
          // ! Beside the clones, not among them: an owner may not begin with a
          // ! dot, while `owner/.lictor-worktrees` is a name GitHub allows.
          // ! Still keyed by repository, because a job id alone is not unique
          // ! across the databases these directories outlive.
          const worktreeRoot = join(root, '.worktrees', clone.repository);
          yield* Effect.try({
            try: () => mkdirSync(worktreeRoot, { recursive: true, mode: 0o700 }),
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_CREATE_FAILED',
                message: 'Could not create worktree root',
                cause,
              }),
          });
          const worktreePath = join(worktreeRoot, `job-${jobId}`);
          // ! Only a worktree this daemon made, over a clone it did not just
          // ! replace, may be handed back. Anything else at that path belongs
          // ! to another epoch or to a `worktree add` that was killed halfway,
          // ! and running the agent in it is running it in the wrong tree.
          const reusable = (yield* Ref.get(owned)).has(worktreePath) && !clone.fresh;
          if (existsSync(worktreePath) && !reusable) {
            yield* Effect.try({
              try: () => rmSync(worktreePath, { recursive: true, force: true }),
              catch: (cause) =>
                new WorkspaceError({
                  code: 'WORKSPACE_CREATE_FAILED',
                  message: 'Could not discard a stale worktree',
                  cause,
                }),
            });
            // ! `worktree add` refuses a path git still has a record of, and
            // ! removing the directory does not remove the record.
            yield* command(['git', '-C', clonePath, 'worktree', 'prune'], clonePath);
          } else if (reusable) {
            return {
              repository: policy.repository,
              clonePath,
              worktreePath,
            } satisfies JobWorkspace;
          }
          const head = yield* command(
            ['git', '-C', clonePath, 'rev-parse', '--verify', 'origin/HEAD'],
            clonePath,
          );
          const result = yield* command(
            head.exitCode === 0
              ? ['git', '-C', clonePath, 'worktree', 'add', '--detach', worktreePath, 'origin/HEAD']
              : [
                  'git',
                  '-C',
                  clonePath,
                  'worktree',
                  'add',
                  '--orphan',
                  `lictor/job-${jobId}`,
                  worktreePath,
                ],
            clonePath,
          );
          if (result.exitCode !== 0)
            return yield* new WorkspaceError({
              code: 'WORKTREE_CREATE_FAILED',
              message: 'Could not create isolated worktree',
            });
          yield* Ref.update(owned, (paths) => new Set(paths).add(worktreePath));
          return { repository: policy.repository, clonePath, worktreePath } satisfies JobWorkspace;
        });

      const cleanup = (workspace: JobWorkspace, retain: boolean) =>
        retain
          ? Effect.void
          : command(
              [
                'git',
                '-C',
                workspace.clonePath,
                'worktree',
                'remove',
                '--force',
                workspace.worktreePath,
              ],
              workspace.clonePath,
            ).pipe(
              Effect.tap(() =>
                Effect.sync(() => rmSync(workspace.worktreePath, { recursive: true, force: true })),
              ),
              Effect.tap(() =>
                Ref.update(owned, (paths) => {
                  const remaining = new Set(paths);
                  remaining.delete(workspace.worktreePath);
                  return remaining;
                }),
              ),
              Effect.asVoid,
            );

      const withRepositoryLock = <A, E, R>(repository: string, effect: Effect.Effect<A, E, R>) =>
        locks.withPermits(repository.toLowerCase(), 1)(effect);

      return { create, cleanup, withRepositoryLock };
    }),
    dependencies: [LictorConfig.Default, ProcessRunner.Default, GitHubCredential.Default],
  },
) {}
