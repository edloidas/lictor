import { describe, expect, it } from 'bun:test';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { AgentExecutor, ExecutorError } from '../src/executor/agent-executor.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { Policy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';
import { Worker } from '../src/worker.ts';
import { RepositoryWorkspace, WorkspaceError } from '../src/workspace/repository-workspace.ts';

const work: WorkItem = {
  deliveryId: 'delivery-1',
  interactionId: 'interaction-1',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 17,
    title: 'Run the worker',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
};

const prWork: WorkItem = {
  ...work,
  deliveryId: 'delivery-2',
  interactionId: 'interaction-2',
  subject: {
    kind: 'pull_request',
    number: 42,
    title: 'Run the worker on a pull request',
    url: 'https://github.com/edloidas/lictor/pull/42',
  },
};

const config = (maxAttempts = 3) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    stateDir: stateDirOf(':memory:'),
    policyPath: 'policy.toml',
    controlSocketPath: '/tmp/lictor.sock',
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: maxAttempts,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  });

const run = <A, E>(
  effect: Effect.Effect<A, E, Worker | WorkQueue | CredentialHealth>,
  execute: InstanceType<typeof AgentExecutor>['execute'],
  maxAttempts = 3,
  enabled = true,
  createWorkspace?: InstanceType<typeof RepositoryWorkspace>['acquire'],
) => {
  const ConfigLive = Layer.succeed(LictorConfig, config(maxAttempts));
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const ExecutorLive = Layer.succeed(AgentExecutor, AgentExecutor.make({ enabled, execute }));
  const PolicyLive = Layer.succeed(
    Policy,
    Policy.make({
      completedRetentionDays: 30,
      failedRetentionDays: 90,
      maxQueueDepth: 1000,
      maxJobAgeMs: 86_400_000,
      livenessMs: 24 * 60 * 60 * 1000,
      forRepository: (repository) => ({
        repository,
        accepted: true,
        execution: 'automatic',
        clone: 'denied',
        maxAttempts: 3,
        maxDurationMs: 30 * 60 * 1000,
        trustedSenders: ['edloidas'],
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
      }),
    }),
  );
  const WorkspaceLive = Layer.succeed(
    RepositoryWorkspace,
    RepositoryWorkspace.make({
      acquire: createWorkspace ?? (() => Effect.succeed({ path: '/tmp/lictor-job' })),
      release: () => Effect.void,
      sweep: () => Effect.void,
    }),
  );
  const HealthLive = CredentialHealth.Default;
  const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
    Layer.provide(
      Layer.mergeAll(ConfigLive, QueueLive, ExecutorLive, PolicyLive, WorkspaceLive, HealthLive),
    ),
  );

  return Effect.runPromise(
    Effect.scoped(
      effect.pipe(
        // Health merged outside too, so the test body and the worker share
        // one latch instance — suspending in the test must be visible to the
        // loop under test.
        Effect.provide(Layer.mergeAll(QueueLive, WorkerLive, HealthLive)),
        Effect.provide(ConfigLive),
      ),
    ),
  );
};

describe('Worker.runOnce', () => {
  it('completes a queued job after successful execution', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        const worked = yield* worker.runOnce;
        return { worked, counts: yield* queue.counts };
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
    );

    expect(result.worked).toBe(true);
    expect(result.counts.completed).toBe(1);
  });

  it('schedules retryable failures without immediately reclaiming them', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { secondRun: yield* worker.runOnce, counts: yield* queue.counts };
      }),
      () => Effect.fail(new ExecutorError({ message: 'temporary', retryable: true })),
    );

    expect(result.secondRun).toBe(false);
    expect(result.counts.retry).toBe(1);
  });

  it('marks a failure final when the attempt limit is reached', async () => {
    const counts = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.counts;
      }),
      () => Effect.fail(new ExecutorError({ message: 'temporary', retryable: true })),
      1,
    );

    expect(counts.failed).toBe(1);
    expect(counts.retry).toBe(0);
  });

  // The whole point of the daemon-wide latch: while the credential is dead,
  // claims stop *before* an attempt is spent, so queued work survives a token
  // rotation instead of draining into failures.
  it('stops claiming while the credential is rejected', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const health = yield* CredentialHealth;
        const worker = yield* Worker;
        yield* queue.enqueue(work);
        yield* health.suspend;
        const worked = yield* worker.runOnce;
        return { worked, counts: yield* queue.counts };
      }),
      () => Effect.die('must not execute'),
    );

    expect(result.worked).toBe(false);
    expect(result.counts.pending).toBe(1);
  });

  it('latches the breaker when git reports a refused credential', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const health = yield* CredentialHealth;
        const worker = yield* Worker;
        yield* queue.enqueue(work);
        const first = yield* worker.runOnce;
        const latched = yield* health.isRejected;
        // No second claim: the retained job must not spend another attempt.
        const second = yield* worker.runOnce;
        return { first, latched, second, counts: yield* queue.counts };
      }),
      () => Effect.die('must not execute'),
      3,
      true,
      () =>
        Effect.fail(
          new WorkspaceError({
            code: 'WORKSPACE_CREDENTIAL_REJECTED',
            message: 'GitHub rejected the daemon credential',
            retryAfterMs: 300_000,
          }),
        ),
    );

    expect(result.first).toBe(true);
    expect(result.latched).toBe(true);
    expect(result.second).toBe(false);
    expect(result.counts.retry).toBe(1);
  });

  it('leaves queued work pending while execution is disabled', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        return { worked: yield* worker.runOnce, counts: yield* queue.counts };
      }),
      () => Effect.die('must not execute'),
      3,
      false,
    );

    expect(result.worked).toBe(false);
    expect(result.counts.pending).toBe(1);
  });
  // Access is a fact about the repository, not the daemon, and repeating the
  // attempt cannot change it — unlike a refused credential, which an operator
  // rotates. So this one stays terminal and the credential one does not.
  it('does not retry a workspace failure that can never succeed', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { counts: yield* queue.counts, reclaimed: yield* queue.claim };
      }),
      () => Effect.die('the executor must not run'),
      3,
      true,
      () =>
        new WorkspaceError({
          code: 'WORKSPACE_ACCESS_DENIED',
          message: 'The daemon account cannot write to this repository',
          retryable: false,
        }),
    );

    expect(result.counts.failed).toBe(1);
    expect(result.reclaimed).toBeUndefined();
  });

  it('still retries a workspace failure that might succeed', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.counts;
      }),
      () => Effect.die('the executor must not run'),
      3,
      true,
      () =>
        new WorkspaceError({
          code: 'WORKSPACE_FETCH_FAILED',
          message: 'Could not refresh repository state',
        }),
    );

    expect(result.retry).toBe(1);
  });

  // GitHub publishes when the bucket refills. Guessing with exponential
  // backoff either wastes the wait or retries into the same wall. Both halves
  // are load-bearing: `retry` proves the job is still scheduled rather than
  // abandoned, and the unavailability after 250ms proves it was scheduled from
  // the hint and not from the 100ms configured base. Either one alone is also
  // satisfied by a permanently failed job.
  it('schedules a throttled workspace failure from the wait GitHub asked for', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        yield* Effect.sleep('250 millis');
        return { reclaimed: yield* queue.claim, counts: yield* queue.counts };
      }),
      () => Effect.die('the executor must not run'),
      3,
      true,
      () =>
        new WorkspaceError({
          code: 'WORKSPACE_RATE_LIMITED',
          message: 'GitHub throttled the repository operation',
          retryAfterMs: 60_000,
        }),
    );

    expect(result.counts.retry).toBe(1);
    expect(result.reclaimed).toBeUndefined();
  });

  it('reclaims an ordinary retryable failure once the short backoff elapses', async () => {
    const reclaimed = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        yield* Effect.sleep('250 millis');
        return yield* queue.claim;
      }),
      () => Effect.die('the executor must not run'),
      3,
      true,
      () =>
        new WorkspaceError({
          code: 'WORKSPACE_FETCH_FAILED',
          message: 'Could not refresh repository state',
        }),
    );

    expect(reclaimed?.work.deliveryId).toBe(work.deliveryId);
  });

  // A pull-request job must clone at its head, not the default branch —
  // reviewing a PR requires reviewing its tree. `refs/pull/<n>/head` resolves
  // from the base repository, so it works for fork PRs too.
  it('clones a pull-request job at refs/pull/<n>/head', async () => {
    const acquireCalls: Parameters<InstanceType<typeof RepositoryWorkspace>['acquire']>[0][] = [];
    await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(prWork);
        const worker = yield* Worker;
        return yield* worker.runOnce;
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
      3,
      true,
      (request) => {
        acquireCalls.push(request);
        return Effect.succeed({ path: '/tmp/lictor-job' });
      },
    );

    expect(acquireCalls.length).toBe(1);
    expect(acquireCalls[0]?.ref).toBe('refs/pull/42/head');
  });

  it('passes no ref for an issue job', async () => {
    const acquireCalls: Parameters<InstanceType<typeof RepositoryWorkspace>['acquire']>[0][] = [];
    await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        const worker = yield* Worker;
        return yield* worker.runOnce;
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
      3,
      true,
      (request) => {
        acquireCalls.push(request);
        return Effect.succeed({ path: '/tmp/lictor-job' });
      },
    );

    expect(acquireCalls.length).toBe(1);
    expect(acquireCalls[0]?.ref).toBeUndefined();
  });

  // A branch a previous interaction created wins over the PR head: continuing
  // her own work beats re-reading a head that may have moved.
  it('clones at the branch a prior interaction on this subject created', async () => {
    const acquireCalls: Parameters<InstanceType<typeof RepositoryWorkspace>['acquire']>[0][] = [];
    await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(prWork);
        yield* queue.recordSubjectBranch({
          repository: prWork.repository,
          subjectKind: 'pull_request',
          subjectNumber: 42,
          branch: 'lictor-issue-42',
        });
        const worker = yield* Worker;
        return yield* worker.runOnce;
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
      3,
      true,
      (request) => {
        acquireCalls.push(request);
        return Effect.succeed({ path: '/tmp/lictor-job' });
      },
    );

    expect(acquireCalls[0]?.ref).toBe('refs/heads/lictor-issue-42');
  });
});
