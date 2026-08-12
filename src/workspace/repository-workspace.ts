import { existsSync, mkdirSync, realpathSync, rmSync } from 'node:fs';
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

const expectedRemote = (repository: string): RegExp => {
  const escaped = repository.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(
    `^(?:https://github\\.com/|git@github\\.com:|ssh://git@github\\.com/)${escaped}(?:\\.git)?/?$`,
    'i',
  );
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
          ...(env === undefined ? {} : { env }),
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
              try: () => mkdirSync(dirname(candidate), { recursive: true, mode: 0o700 }),
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
                GIT_TERMINAL_PROMPT: '0',
                GIT_CONFIG_COUNT: '1',
                GIT_CONFIG_KEY_0: 'http.https://github.com/.extraHeader',
                GIT_CONFIG_VALUE_0: `Authorization: Bearer ${Redacted.value(token)}`,
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
            try: () => mkdirSync(worktreeRoot, { recursive: true, mode: 0o700 }),
            catch: (cause) =>
              new WorkspaceError({
                code: 'WORKSPACE_CREATE_FAILED',
                message: 'Could not create worktree root',
                cause,
              }),
          });
          const worktreePath = join(worktreeRoot, `${basename(clonePath)}-${jobId}`);
          const result = yield* command(
            ['git', '-C', clonePath, 'worktree', 'add', '--detach', worktreePath, 'HEAD'],
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
