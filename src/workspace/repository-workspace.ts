import { existsSync, lstatSync, mkdirSync, realpathSync, rmSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { Data, Effect, PartitionedSemaphore, Redacted } from 'effect';
import { ProcessRunner } from '../executor/process-runner.ts';
import { GitHubCredential } from '../github/credential.ts';
import type { RepositoryPolicy } from '../policy.ts';
import type { WorkItem } from '../webhook/qualification.ts';

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

const within = (root: string, candidate: string): boolean => {
  const path = relative(root, candidate);
  return path !== '..' && !path.startsWith(`..${sep}`) && !isAbsolute(path);
};

const canonicalExisting = (path: string) => realpathSync(path);

const nearestExistingParent = (path: string): string => {
  let current = path;
  while (!existsSync(current)) current = dirname(current);
  return current;
};

const expectedRemote = (repository: string): RegExp => {
  const escaped = repository.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^https://github\\.com/${escaped}(?:\\.git)?/?$`, 'i');
};

export class RepositoryWorkspace extends Effect.Service<RepositoryWorkspace>()(
  'RepositoryWorkspace',
  {
    effect: Effect.gen(function* () {
      const processes = yield* ProcessRunner;
      const credential = yield* GitHubCredential;
      const locks = yield* PartitionedSemaphore.make<string>({ permits: 1 });

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

      const resolveClone = (work: WorkItem, policy: RepositoryPolicy, roots: readonly string[]) =>
        Effect.gen(function* () {
          const configured = policy.workspace;
          const [owner, name] = work.repository.toLowerCase().split('/');
          if (owner === undefined || name === undefined) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_REPOSITORY_INVALID',
              message: 'Invalid repository name',
            });
          }
          const candidate =
            configured ?? (roots[0] === undefined ? undefined : join(roots[0], owner, name));
          if (candidate === undefined) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_ROOT_MISSING',
              message: 'No workspace root is configured',
            });
          }
          const allowedRoot = roots.find((root) => within(resolve(root), resolve(candidate)));
          if (allowedRoot === undefined) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_PATH_DENIED',
              message: 'Repository path is outside configured roots',
            });
          }

          if (!existsSync(candidate)) {
            if (policy.clone !== 'allowed') {
              return yield* new WorkspaceError({
                code: 'WORKSPACE_CLONE_DENIED',
                message: 'Repository cloning is denied by policy',
              });
            }
            yield* Effect.try({
              try: () => {
                const canonicalRoot = canonicalExisting(allowedRoot);
                const parent = nearestExistingParent(dirname(candidate));
                if (
                  lstatSync(parent).isSymbolicLink() ||
                  !within(canonicalRoot, canonicalExisting(parent))
                ) {
                  throw new Error('Clone parent escapes its configured root');
                }
                mkdirSync(dirname(candidate), { recursive: true, mode: 0o700 });
                if (!within(canonicalRoot, canonicalExisting(dirname(candidate)))) {
                  throw new Error('Clone parent escapes its configured root');
                }
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
              ['git', 'clone', `https://github.com/${work.repository}.git`, candidate],
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

          const canonicalRoot = canonicalExisting(allowedRoot);
          const canonical = canonicalExisting(candidate);
          if (!within(canonicalRoot, canonical)) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_SYMLINK_ESCAPE',
              message: 'Canonical repository path escapes its root',
            });
          }
          const remote = yield* command(
            ['git', '-C', canonical, 'remote', 'get-url', 'origin'],
            canonical,
          );
          if (
            remote.exitCode !== 0 ||
            !expectedRemote(work.repository).test(remote.stdout.trim())
          ) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_REMOTE_MISMATCH',
              message: 'Existing clone does not match the delivery repository',
            });
          }
          const authorization = yield* credential.gitAuthHeader;
          const fetch = yield* command(
            ['git', '-C', canonical, 'fetch', '--prune', 'origin'],
            canonical,
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
          return canonical;
        });

      const create = (
        jobId: number,
        work: WorkItem,
        policy: RepositoryPolicy,
        roots: readonly string[],
      ) =>
        Effect.gen(function* () {
          const clonePath = yield* resolveClone(work, policy, roots);
          const worktreeRoot = join(dirname(clonePath), '.lictor-worktrees');
          yield* Effect.try({
            try: () => {
              mkdirSync(worktreeRoot, { recursive: true, mode: 0o700 });
              const canonicalRoot = canonicalExisting(
                roots.find((root) => within(resolve(root), resolve(clonePath))) ?? '',
              );
              if (
                lstatSync(worktreeRoot).isSymbolicLink() ||
                !within(canonicalRoot, canonicalExisting(worktreeRoot))
              ) {
                throw new Error('Worktree root escapes its configured root');
              }
            },
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_CREATE_FAILED',
                message: 'Could not create worktree root',
                cause,
              }),
          });
          const worktreePath = join(worktreeRoot, `${basename(clonePath)}-${jobId}`);
          if (existsSync(worktreePath)) {
            if (
              lstatSync(worktreePath).isSymbolicLink() ||
              !within(canonicalExisting(worktreeRoot), canonicalExisting(worktreePath))
            ) {
              return yield* new WorkspaceError({
                code: 'WORKSPACE_SYMLINK_ESCAPE',
                message: 'Retained worktree escapes its root',
              });
            }
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
              Effect.asVoid,
            );

      const withRepositoryLock = <A, E, R>(repository: string, effect: Effect.Effect<A, E, R>) =>
        locks.withPermits(repository.toLowerCase(), 1)(effect);

      return { create, cleanup, withRepositoryLock };
    }),
    dependencies: [ProcessRunner.Default, GitHubCredential.Default],
  },
) {}
