import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Cause, Effect, Layer, Option, Redacted, Ref } from 'effect';
import { type ProcessRequest, ProcessRunner } from '../src/executor/process-runner.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import type { RepositoryPolicy } from '../src/policy.ts';
import type { WorkItem } from '../src/webhook/qualification.ts';
import { RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const work: WorkItem = {
  deliveryId: 'delivery-1',
  interactionId: 'interaction-1',
  event: 'issues',
  action: 'assigned',
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

const policy = (workspace: string, clone: 'allowed' | 'denied' = 'denied'): RepositoryPolicy => ({
  repository: 'edloidas/lictor',
  accepted: true,
  execution: 'automatic',
  clone,
  workspace,
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

const service = (run: InstanceType<typeof ProcessRunner>['run']) =>
  RepositoryWorkspace.DefaultWithoutDependencies.pipe(
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

describe('RepositoryWorkspace', () => {
  it('verifies an existing clone and creates an isolated worktree', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const clone = join(root, 'edloidas', 'lictor');
    mkdirSync(clone, { recursive: true });
    const calls: ProcessRequest[] = [];
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(clone), [root]);
          }).pipe(
            Effect.provide(
              service((request) =>
                Effect.sync(() => {
                  calls.push(request);
                  return {
                    exitCode: 0,
                    stdout: request.command.includes('get-url')
                      ? 'https://github.com/edloidas/lictor.git\n'
                      : '',
                    stderr: '',
                    outputTruncated: false,
                  };
                }),
              ),
            ),
          ),
        ),
      );
      expect(result.worktreePath).toContain('.lictor-worktrees/lictor-10');
      expect(calls.some((call) => call.command.includes('worktree'))).toBe(true);
      // ! The fetch carries whatever scheme the credential produced, untouched.
      // ! A Bearer value here authenticates against the API but not against git.
      const fetch = calls.find((call) => call.command.includes('fetch'));
      expect(fetch?.env?.GIT_CONFIG_VALUE_0).toBe('Authorization: Basic c2VjcmV0');
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('rejects a clone whose origin belongs to another repository', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const clone = join(root, 'edloidas', 'lictor');
    mkdirSync(clone, { recursive: true });
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(clone), [root]);
          }).pipe(
            Effect.provide(
              service(() =>
                Effect.succeed({
                  exitCode: 0,
                  stdout: 'https://github.com/other/repo.git',
                  stderr: '',
                  outputTruncated: false,
                }),
              ),
            ),
          ),
        ),
      );
      expect(String(exit)).toContain('WORKSPACE_REMOTE_MISMATCH');
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('rejects canonical paths that escape through a symlink', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const outside = mkdtempSync(join(tmpdir(), 'lictor-outside-'));
    const owner = join(root, 'edloidas');
    mkdirSync(owner);
    symlinkSync(outside, join(owner, 'lictor'));
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(join(owner, 'lictor')), [root]);
          }).pipe(Effect.provide(service(() => Effect.die('git must not run')))),
        ),
      );
      expect(String(exit)).toContain('WORKSPACE_SYMLINK_ESCAPE');
    } finally {
      rmSync(root, { recursive: true, force: true });
      rmSync(outside, { recursive: true, force: true });
    }
  });

  it('rejects a symlinked worktree root before Git writes through it', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const outside = mkdtempSync(join(tmpdir(), 'lictor-outside-'));
    const owner = join(root, 'edloidas');
    const clone = join(owner, 'lictor');
    mkdirSync(clone, { recursive: true });
    symlinkSync(outside, join(owner, '.lictor-worktrees'));
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(clone), [root]);
          }).pipe(
            Effect.provide(
              service((request) =>
                Effect.succeed({
                  exitCode: 0,
                  stdout: request.command.includes('get-url')
                    ? 'https://github.com/edloidas/lictor.git\n'
                    : '',
                  stderr: '',
                  outputTruncated: false,
                }),
              ),
            ),
          ),
        ),
      );
      expect(String(exit)).toContain('WORKSPACE_CREATE_FAILED');
    } finally {
      rmSync(root, { recursive: true, force: true });
      rmSync(outside, { recursive: true, force: true });
    }
  });

  it('does not clone when policy denies it', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(join(root, 'edloidas', 'lictor')), [
              root,
            ]);
          }).pipe(Effect.provide(service(() => Effect.die('git must not run')))),
        ),
      );
      expect(String(exit)).toContain('WORKSPACE_CLONE_DENIED');
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('serializes work for one repository', async () => {
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
        }).pipe(Effect.provide(service(() => Effect.die('unused')))),
      ),
    );
    expect(active).toBe(1);
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
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const clone = join(root, 'edloidas', 'lictor');
    mkdirSync(clone, { recursive: true });
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(clone), [root]);
          }).pipe(
            Effect.provide(
              service((request) =>
                Effect.succeed({
                  exitCode: request.command.includes('fetch') ? 1 : 0,
                  stdout: request.command.includes('get-url')
                    ? 'https://github.com/edloidas/lictor.git\n'
                    : '',
                  stderr: request.command.includes('fetch') ? stderr : '',
                  outputTruncated: false,
                }),
              ),
            ),
          ),
        ),
      );

      expect(String(exit)).toContain(code);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  // ! A refused credential heals when an operator rotates the token, so the job
  // ! must survive it — but not by retrying every 30 seconds and paying for a
  // ! clone each time.
  it('keeps a refused credential retryable but far out', async () => {
    const root = mkdtempSync(join(tmpdir(), 'lictor-workspace-'));
    const clone = join(root, 'edloidas', 'lictor');
    mkdirSync(clone, { recursive: true });
    try {
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.create(10, work, policy(clone), [root]);
          }).pipe(
            Effect.provide(
              service((request) =>
                Effect.succeed({
                  exitCode: request.command.includes('fetch') ? 1 : 0,
                  stdout: request.command.includes('get-url')
                    ? 'https://github.com/edloidas/lictor.git\n'
                    : '',
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
      const error = Cause.failureOption(exit.cause);
      const failure = Option.getOrUndefined(error);
      expect(failure?._tag).toBe('WorkspaceError');
      expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).not.toBe(false);
      expect(failure?._tag === 'WorkspaceError' ? failure.retryAfterMs : undefined).toBe(
        5 * 60 * 1000,
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
