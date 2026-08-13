import { existsSync, lstatSync, mkdirSync, realpathSync, rmSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { Data, Effect, PartitionedSemaphore, Redacted } from 'effect';
import { ProcessRunner } from '../executor/process-runner.ts';
import { GitHubApp } from '../github/app.ts';
import type { RepositoryPolicy } from '../policy.ts';
import type { WorkItem } from '../webhook/qualification.ts';

export class WorkspaceError extends Data.TaggedError('WorkspaceError')<{
  readonly code: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

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
      const app = yield* GitHubApp;
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
            if (work.installationId === undefined) {
              return yield* new WorkspaceError({
                code: 'WORKSPACE_INSTALLATION_MISSING',
                message: 'Installation identity is required to clone',
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
            const token = yield* app.token(work.installationId);
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
                GIT_CONFIG_VALUE_0: `Authorization: Bearer ${Redacted.value(token)}`,
                GIT_CONFIG_KEY_1: 'core.hooksPath',
                GIT_CONFIG_VALUE_1: '/dev/null',
              },
            );
            if (result.exitCode !== 0) {
              return yield* new WorkspaceError({
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
          if (work.installationId === undefined) {
            return yield* new WorkspaceError({
              code: 'WORKSPACE_INSTALLATION_MISSING',
              message: 'Installation identity is required to refresh the repository',
            });
          }
          const token = yield* app.token(work.installationId);
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
              GIT_CONFIG_VALUE_0: `Authorization: Bearer ${Redacted.value(token)}`,
              GIT_CONFIG_KEY_1: 'core.hooksPath',
              GIT_CONFIG_VALUE_1: '/dev/null',
            },
          );
          if (fetch.exitCode !== 0) {
            return yield* new WorkspaceError({
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
    dependencies: [ProcessRunner.Default, GitHubApp.Default],
  },
) {}
