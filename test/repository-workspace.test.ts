import { describe, expect, it } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Cause, Effect, Layer, Option, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { type ProcessRequest, ProcessRunner } from '../src/executor/process-runner.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import type { RepositoryPolicy } from '../src/policy.ts';
import type { WorkItem } from '../src/work-item.ts';
import { RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const work: WorkItem = {
  deliveryId: 'delivery-1',
  interactionId: 'interaction-1',
  repository: 'edloidas/lictor',
  installationId: 42,
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 10,
    title: 'Workspace',
    url: 'https://github.com/edloidas/lictor/issues/10',
  },
};

const policy = (clone: 'allowed' | 'denied' = 'denied'): RepositoryPolicy => ({
  repository: 'edloidas/lictor',
  accepted: true,
  execution: 'automatic',
  clone,
  maxAttempts: 3,
  maxDurationMs: 30 * 60 * 1000,
  capabilities: {
    read: true,
    comment: false,
    issues: false,
    branches: false,
    pullRequests: false,
    merge: false,
    forcePush: false,
    deleteBranches: false,
    scripts: [],
  },
});

const config = (home: string) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    databasePath: join(home, 'lictor.sqlite'),
    policyPath: join(home, 'policy.toml'),
    controlSocketPath: join(home, 'lictor.sock'),
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  });

/** Where the daemon puts a clone, given its state directory. */
const clonePath = (home: string) => join(home, 'workspaces', 'edloidas', 'lictor');

/** A clone the daemon already made, as `git` would leave it. */
const materialize = (home: string) => mkdirSync(join(clonePath(home), '.git'), { recursive: true });

const service = (home: string, run: InstanceType<typeof ProcessRunner>['run']) =>
  RepositoryWorkspace.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(LictorConfig, config(home))),
    Layer.provide(Layer.succeed(ProcessRunner, ProcessRunner.make({ run }))),
    Layer.provide(
      Layer.succeed(
        GitHubCredential,
        GitHubCredential.make({
          token: Effect.succeed(Redacted.make('secret-token')),
          gitAuthHeader: Effect.succeed(Redacted.make('Basic c2VjcmV0')),
        }),
      ),
    ),
  );

const withHome = async <A>(body: (home: string) => Promise<A>): Promise<A> => {
  const home = mkdtempSync(join(tmpdir(), 'lictor-home-'));
  try {
    return await body(home);
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
};

const ok = () => ({ exitCode: 0, stdout: '', stderr: '', outputTruncated: false });

/**
 * Answers the health probe the way git would: `rev-parse HEAD` resolves only in
 * a directory that actually holds a repository.
 */
const gitLike = (home: string, request: ProcessRequest) =>
  request.command.includes('rev-parse') && request.command.includes('HEAD')
    ? { ...ok(), exitCode: existsSync(join(clonePath(home), '.git')) ? 0 : 1 }
    : ok();

describe('RepositoryWorkspace', () => {
  it('refreshes the clone it owns and creates an isolated worktree', async () => {
    await withHome(async (home) => {
      materialize(home);
      const calls: ProcessRequest[] = [];
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(result.clonePath).toBe(clonePath(home));
      expect(result.worktreePath).toBe(
        join(home, 'workspaces', '.worktrees', 'edloidas', 'lictor', 'job-10'),
      );
      expect(calls.some((call) => call.command.includes('worktree'))).toBe(true);
      // ! Nothing interrogates the remote any more — the path is the daemon's,
      // ! so there is no foreign clone left to mistake for this repository.
      expect(calls.some((call) => call.command.includes('get-url'))).toBe(false);
      // ! The fetch carries whatever scheme the credential produced, untouched.
      // ! A Bearer value here authenticates against the API but not against git.
      const fetch = calls.find((call) => call.command.includes('fetch'));
      expect(fetch?.env?.GIT_CONFIG_VALUE_0).toBe('Authorization: Basic c2VjcmV0');
    });
  });

  it('clones into the daemon state directory when nothing is there yet', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy('allowed'));
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  if (request.command.includes('clone')) materialize(home);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      const clone = calls.find((call) => call.command.includes('clone'));
      expect(clone?.command).toEqual([
        'git',
        'clone',
        'https://github.com/edloidas/lictor.git',
        clonePath(home),
      ]);
    });
  });

  // ! git cleans up after its own failures only, so a hard kill mid-clone
  // ! leaves a directory no later job can fetch in and no retry can repair.
  it('replaces a directory left behind by an interrupted clone', async () => {
    await withHome(async (home) => {
      const debris = join(clonePath(home), 'partial-object');
      mkdirSync(debris, { recursive: true });
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy('allowed'));
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  if (request.command.includes('clone')) materialize(home);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(calls.some((call) => call.command.includes('clone'))).toBe(true);
      expect(existsSync(debris)).toBe(false);
    });
  });

  // ! A retained worktree links into the clone by path, so a replaced clone
  // ! leaves it dangling.
  it('discards a retained worktree whose clone was replaced', async () => {
    await withHome(async (home) => {
      const worktree = join(home, 'workspaces', '.worktrees', 'edloidas', 'lictor', 'job-10');
      mkdirSync(join(worktree, 'stale'), { recursive: true });
      mkdirSync(clonePath(home), { recursive: true });
      const calls: ProcessRequest[] = [];
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy('allowed'));
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  if (request.command.includes('clone')) materialize(home);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(result.worktreePath).toBe(worktree);
      expect(existsSync(join(worktree, 'stale'))).toBe(false);
      expect(calls.some((call) => call.command.includes('worktree'))).toBe(true);
    });
  });

  // ! Job ids restart with a new database while these directories outlive it,
  // ! so a `job-10` on disk may be another epoch's checkout entirely.
  it('discards a worktree it did not create, even over a healthy clone', async () => {
    await withHome(async (home) => {
      materialize(home);
      const worktree = join(home, 'workspaces', '.worktrees', 'edloidas', 'lictor', 'job-10');
      mkdirSync(join(worktree, 'someone-elses-work'), { recursive: true });
      const calls: ProcessRequest[] = [];
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(result.worktreePath).toBe(worktree);
      expect(existsSync(join(worktree, 'someone-elses-work'))).toBe(false);
      // ! git keeps its own record of a worktree path, and removing the
      // ! directory does not remove it — `worktree add` would refuse the path.
      expect(calls.some((call) => call.command.includes('prune'))).toBe(true);
      expect(calls.some((call) => call.command.includes('add'))).toBe(true);
    });
  });

  // ! The point of retention: a second attempt on the same job continues in the
  // ! checkout the first one left.
  it('reuses the worktree it created for an earlier attempt', async () => {
    await withHome(async (home) => {
      materialize(home);
      const calls: ProcessRequest[] = [];
      const [first, second] = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            const one = yield* manager.create(10, work, policy());
            mkdirSync(one.worktreePath, { recursive: true });
            const two = yield* manager.create(10, work, policy());
            return [one, two] as const;
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(second.worktreePath).toBe(first.worktreePath);
      expect(calls.filter((call) => call.command.includes('add')).length).toBe(1);
      expect(calls.some((call) => call.command.includes('prune'))).toBe(false);
    });
  });

  it('does not re-materialize debris when policy denies cloning', async () => {
    await withHome(async (home) => {
      mkdirSync(clonePath(home), { recursive: true });
      const calls: ProcessRequest[] = [];
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.sync(() => {
                  calls.push(request);
                  return gitLike(home, request);
                }),
              ),
            ),
          ),
        ),
      );

      expect(String(exit)).toContain('WORKSPACE_CLONE_DENIED');
      expect(calls.some((call) => call.command.includes('clone'))).toBe(false);
      expect(existsSync(clonePath(home))).toBe(true);
      // ! Terminal, or every attempt pays a clone cycle for a policy decision
      // ! that cannot change between them.
      const failure = Option.getOrUndefined(
        Cause.failureOption(exit._tag === 'Failure' ? exit.cause : Cause.empty),
      );
      expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).toBe(false);
    });
  });

  // ! Repository names come from a GitHub payload and are joined into a path.
  // ! A traversal segment must never reach `join`, let alone `git`.
  it.each([['edloidas/..'], ['../../etc'], ['edloidas/lictor/extra']])(
    'refuses the repository name %p before git runs',
    async (repository) => {
      await withHome(async (home) => {
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const manager = yield* RepositoryWorkspace;
              return yield* manager.create(10, { ...work, repository }, policy('allowed'));
            }).pipe(Effect.provide(service(home, () => Effect.die('git must not run')))),
          ),
        );

        expect(String(exit)).toContain('WORKSPACE_REPOSITORY_INVALID');
      });
    },
  );

  it('does not clone when policy denies it', async () => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(Effect.provide(service(home, () => Effect.die('git must not run')))),
        ),
      );

      expect(String(exit)).toContain('WORKSPACE_CLONE_DENIED');
    });
  });

  it('serializes work for one repository', async () => {
    await withHome(async (home) => {
      const active = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            const current = yield* Ref.make(0);
            const maximum = yield* Ref.make(0);
            const task = manager.withRepositoryLock(
              'edloidas/lictor',
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(current, (value) => value + 1);
                yield* Ref.update(maximum, (value) => Math.max(value, count));
                yield* Effect.sleep('10 millis');
                yield* Ref.update(current, (value) => value - 1);
              }),
            );
            yield* Effect.all([task, task], { concurrency: 'unbounded' });
            return yield* Ref.get(maximum);
          }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
        ),
      );
      expect(active).toBe(1);
    });
  });

  // ! A refused credential never heals, and each retry pays for another clone.
  // ! git reports it only in prose on stderr, so the text is the only signal.
  it.each([
    ['remote: invalid credentials', 'WORKSPACE_CREDENTIAL_REJECTED'],
    [
      'fatal: Authentication failed for https://github.com/edloidas/lictor',
      'WORKSPACE_CREDENTIAL_REJECTED',
    ],
    ['remote: Write access to repository not granted.', 'WORKSPACE_ACCESS_DENIED'],
    // ! GitHub says "not found" for a private repository the credential cannot
    // ! see, so this must not be the terminal code that a real absence deserves.
    ['remote: Repository not found.', 'WORKSPACE_REPOSITORY_UNAVAILABLE'],
    ['You have exceeded a secondary rate limit', 'WORKSPACE_RATE_LIMITED'],
    [
      'fatal: unable to access https://github.com/edloidas/lictor: The requested URL returned error: 429',
      'WORKSPACE_RATE_LIMITED',
    ],
    ['error: something else entirely', 'WORKSPACE_FETCH_FAILED'],
  ])('classifies a failed fetch reporting %p', async (stderr, code) => {
    await withHome(async (home) => {
      materialize(home);
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.succeed({
                  exitCode: request.command.includes('fetch') ? 1 : 0,
                  stdout: '',
                  stderr: request.command.includes('fetch') ? stderr : '',
                  outputTruncated: false,
                }),
              ),
            ),
          ),
        ),
      );

      expect(String(exit)).toContain(code);
    });
  });

  // ! A refused credential heals when an operator rotates the token, so the job
  // ! must survive it — but not by retrying every 30 seconds and paying for a
  // ! clone each time.
  it('keeps a refused credential retryable but far out', async () => {
    await withHome(async (home) => {
      materialize(home);
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy());
          }).pipe(
            Effect.provide(
              service(home, (request) =>
                Effect.succeed({
                  exitCode: request.command.includes('fetch') ? 1 : 0,
                  stdout: '',
                  stderr: request.command.includes('fetch') ? 'remote: invalid credentials' : '',
                  outputTruncated: false,
                }),
              ),
            ),
          ),
        ),
      );

      expect(exit._tag).toBe('Failure');
      if (exit._tag !== 'Failure') return;
      const failure = Option.getOrUndefined(Cause.failureOption(exit.cause));
      expect(failure?._tag).toBe('WorkspaceError');
      expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).not.toBe(false);
      expect(failure?._tag === 'WorkspaceError' ? failure.retryAfterMs : undefined).toBe(
        5 * 60 * 1000,
      );
    });
  });
});
