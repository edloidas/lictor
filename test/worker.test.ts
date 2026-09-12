import { describe, expect, it } from 'bun:test';
import { Effect, Layer, Logger, type LogLevel, Redacted, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { AgentExecutor, ExecutorError } from '../src/executor/agent-executor.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import type { Grant } from '../src/github/grant.ts';
import { type Capabilities, Policy, type RepositoryPolicy } from '../src/policy.ts';
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

/** The harness policy below denies this repository and accepts every other. */
const deniedWork: WorkItem = {
  ...work,
  deliveryId: 'delivery-3',
  interactionId: 'interaction-3',
  repository: 'edloidas/denied',
};

const approvedWork: WorkItem = {
  ...work,
  deliveryId: 'delivery-4',
  interactionId: 'interaction-4',
  approvalRequired: false,
};

type LogLine = {
  readonly level: LogLevel.LogLevel['label'];
  readonly message: string;
  readonly annotations: Record<string, unknown>;
};

const capture = (lines: LogLine[]) =>
  Logger.replace(
    Logger.defaultLogger,
    Logger.make<unknown, void>(({ annotations, logLevel, message }) => {
      lines.push({
        level: logLevel.label,
        message: String(message),
        annotations: Object.fromEntries(annotations),
      });
    }),
  );

const annotationsOf = (lines: readonly LogLine[], message: string) =>
  lines.find((line) => line.message === message)?.annotations;

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
    executorResultBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: maxAttempts,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  });

/** Varies one clause of the worker's policy gate without a second harness. */
type PolicyOverrides = {
  readonly maxJobAgeMs?: number;
  readonly repository?: Partial<RepositoryPolicy>;
};

const run = <A, E>(
  effect: Effect.Effect<A, E, Worker | WorkQueue | CredentialHealth>,
  execute: InstanceType<typeof AgentExecutor>['execute'],
  maxAttempts = 3,
  enabled = true,
  createWorkspace?: InstanceType<typeof RepositoryWorkspace>['acquire'],
  policyOverrides: PolicyOverrides = {},
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
      maxJobAgeMs: policyOverrides.maxJobAgeMs ?? 86_400_000,
      livenessMs: 24 * 60 * 60 * 1000,
      approvalExpiryMs: 72 * 60 * 60 * 1000,
      answerExpiryMs: 72 * 60 * 60 * 1000,
      forRepository: (repository) => ({
        repository,
        accepted: true,
        execution: repository === deniedWork.repository ? 'denied' : 'automatic',
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
        ...policyOverrides.repository,
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

const readAndComment: Capabilities = {
  read: true,
  comment: true,
  issues: false,
  branches: false,
  pullRequests: false,
  merge: false,
  forcePush: false,
  deleteBranches: false,
  scripts: [],
};

const readOnly: Capabilities = { ...readAndComment, comment: false };

describe('Worker.runOnce grants', () => {
  /**
   * Runs `attempts` claims over one job, recording the grant each one carried.
   * Each attempt ends `failed` so an operator `retry` can re-queue the same row —
   * the only way one job reaches the worker more than once.
   */
  const grantsOver = (attempts: number, policyOverrides: PolicyOverrides = {}) => {
    const seen: (Grant | undefined)[] = [];
    return run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        for (let index = 0; index < attempts; index += 1) {
          yield* worker.runOnce;
          if (index + 1 < attempts) yield* queue.retry(jobId, 0);
        }
        return { seen, stored: (yield* queue.job(jobId))?.grant };
      }),
      (_work, _dir, _timeout, _job, _attempt, _worker, grant) => {
        seen.push(grant);
        return Effect.succeed({ status: 'failed', summary: 'nope' });
      },
      5,
      true,
      undefined,
      policyOverrides,
    );
  };

  /**
   * Runs two claims over one job, swapping the repository's capabilities in
   * between. `forRepository` spreads the override object per call, so mutating
   * it stands in for the policy file being edited across a daemon restart.
   */
  const acrossPolicyChange = (before: Capabilities, after: Capabilities) => {
    const seen: (Grant | undefined)[] = [];
    const repository: { capabilities: Capabilities } = { capabilities: before };
    return run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        repository.capabilities = after;
        yield* queue.retry(jobId, 0);
        yield* worker.runOnce;
        return { seen, stored: (yield* queue.job(jobId))?.grant };
      }),
      (_work, _dir, _timeout, _job, _attempt, _worker, grant) => {
        seen.push(grant);
        return Effect.succeed({ status: 'failed', summary: 'nope' });
      },
      5,
      true,
      undefined,
      { repository },
    );
  };

  // The tightening half: the stored grant handed over unchanged would advertise
  // a tool the broker now denies.
  it('hands the executor a grant narrowed by a policy tightened since the mint', async () => {
    const result = await acrossPolicyChange(readAndComment, readOnly);

    expect(result.seen[0]?.capabilities.comment).toBe(true);
    expect(result.seen[1]?.capabilities.comment).toBe(false);
    // The record still says what was authorized; only what may run now narrowed.
    expect(result.stored?.capabilities.comment).toBe(true);
  });

  it('does not let a policy widened since the mint raise what the executor gets', async () => {
    const result = await acrossPolicyChange(readOnly, readAndComment);

    expect(result.seen[0]?.capabilities.comment).toBe(false);
    expect(result.seen[1]?.capabilities.comment).toBe(false);
    expect(result.stored?.capabilities.comment).toBe(false);
  });

  // The grant records budgets, so it has to bound them.
  it('runs under the recorded duration budget when policy has since raised it', async () => {
    const timeouts: number[] = [];
    const repository: { maxDurationMs: number } = { maxDurationMs: 60_000 };
    await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        repository.maxDurationMs = 600_000;
        yield* queue.retry(jobId, 0);
        yield* worker.runOnce;
      }),
      (_work, _dir, timeoutMs) => {
        timeouts.push(timeoutMs ?? -1);
        return Effect.succeed({ status: 'failed', summary: 'nope' });
      },
      5,
      true,
      undefined,
      { repository },
    );

    expect(timeouts).toEqual([60_000, 60_000]);
  });

  // Running the agent would spend a Codex run to reach a refusal the daemon
  // already knows.
  it('denies a job a tightening left with no capability at all', async () => {
    let ran = false;
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        yield* (yield* Worker).runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => {
        ran = true;
        return Effect.succeed({ status: 'completed', summary: 'done' });
      },
      5,
      true,
      undefined,
      { repository: { capabilities: { ...readOnly, read: false } } },
    );

    expect(ran).toBe(false);
    expect(result.job?.outcome).toBe('rejected');
    // ! The reason is the daemon's, and `note` publishes as a quotation
    // ! attributed to the agent — which never ran.
    expect(result.messages[0]?.note).toBeUndefined();
    // Nothing recorded: `recordGrant` never overwrites, so an empty grant stored
    // here could never be released by correcting the policy.
    expect(result.job?.grant).toBeUndefined();
  });

  // The recovery path the gate must not destroy.
  it('runs a denied job once the policy that left it nothing is corrected', async () => {
    let ran = false;
    const repository: { capabilities: Capabilities } = {
      capabilities: { ...readOnly, read: false },
    };
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        repository.capabilities = readAndComment;
        yield* queue.retry(jobId, 0);
        yield* worker.runOnce;
        return yield* queue.job(jobId);
      }),
      () => {
        ran = true;
        return Effect.succeed({ status: 'completed', summary: 'done' });
      },
      5,
      true,
      undefined,
      { repository },
    );

    expect(ran).toBe(true);
    expect(result?.outcome).toBe('completed');
    expect(result?.grant?.capabilities.comment).toBe(true);
  });

  // ! Fail closed. A recorded ceiling nobody can read is not an absent one, and
  // ! running on live policy instead widens every such row silently — which is
  // ! what a rollback past a future grant version would do to all of them.
  it('refuses a job whose recorded grant cannot be read rather than running it', async () => {
    let ran = false;
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const claimed = yield* queue.claim;
        yield* queue.recordGrant(jobId, claimed?.attempts ?? 1, claimed?.workerId ?? '', {
          version: 1,
          nonsense: true,
        } as unknown as Grant);
        yield* queue.fail(jobId, claimed?.attempts ?? 1, 'reset', undefined, 'failed', {
          repository: work.repository,
          subjectNumber: work.subject.number,
          outcome: 'failed',
        });
        yield* queue.retry(jobId, 0);
        yield* (yield* Worker).runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => {
        ran = true;
        return Effect.succeed({ status: 'completed', summary: 'done' });
      },
      5,
    );

    expect(ran).toBe(false);
    expect(result.job?.outcome).toBe('rejected');
    expect(result.messages.at(-1)?.note).toBeUndefined();
  });

  it('records what the job was admitted under on the first claim', async () => {
    const result = await grantsOver(1, { repository: { capabilities: readAndComment } });

    expect(result.seen[0]?.decision).toBe('automatic');
    expect(result.seen[0]?.capabilities.comment).toBe(true);
    expect(result.seen[0]?.capabilities.merge).toBe(false);
    expect(result.stored?.fingerprint).toBe(result.seen[0]?.fingerprint);
  });

  // ! Once per job, not per attempt: re-minting on an operator `retry` would take
  // ! whatever policy says by then — a widening arriving through "run it again".
  it('reuses the stored grant across an operator retry instead of minting again', async () => {
    const result = await grantsOver(3);

    expect(result.seen).toHaveLength(3);
    expect(result.seen[1]?.mintedAt).toBe(result.seen[0]?.mintedAt as number);
    expect(result.seen[2]?.mintedAt).toBe(result.seen[0]?.mintedAt as number);
    expect(result.stored?.mintedAt).toBe(result.seen[0]?.mintedAt as number);
  });
});

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

  it("owes the thread the agent's own words on a completion", async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.outboxFor(jobId);
      }),
      () => Effect.succeed({ status: 'completed', summary: 'Opened the pull request.' }),
    );

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      outcome: 'completed',
      note: 'Opened the pull request.',
      subjectNumber: 17,
    });
  });

  it("owes the thread the agent's own words on a question", async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.outboxFor(jobId);
      }),
      () => Effect.succeed({ status: 'needs_input', summary: 'which branch?' }),
      3,
    );

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      outcome: 'needs_input',
      note: 'which branch?',
      repository: 'edloidas/lictor',
      subjectNumber: 17,
    });
  });

  it('owes the thread an outcome with no note when the executor itself failed', async () => {
    // ! The message the worker holds here is Codex's own stderr diagnosis, or
    // ! git's. Publishing it is how a repository gets its prose onto a thread
    // ! under the daemon's account.
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.outboxFor(jobId);
      }),
      () =>
        Effect.fail(new ExecutorError({ message: 'Codex exited with status 1', retryable: false })),
    );

    expect(result).toHaveLength(1);
    expect(result[0]?.outcome).toBe('failed');
    expect(result[0]?.note).toBeUndefined();
  });

  it('owes the thread nothing while a failure is still going to retry', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => Effect.fail(new ExecutorError({ message: 'Codex died', retryable: true })),
      3,
    );

    expect(result.job?.status).toBe('retry');
    expect(result.messages).toHaveLength(0);
  });

  it('answers the thread at once when the agent itself reports a failure', async () => {
    // The defect: a capability the policy withholds came back as `failed`, and
    // the retry that bought cost the thread every word of the explanation for
    // the whole attempt budget — three identical runs of byte-identical input.
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => Effect.succeed({ status: 'failed', summary: 'issue creation is not granted here' }),
      3,
    );

    // `fail` derives the status from `retryAt`, so `failed` rather than `retry`
    // is what says no further attempt was scheduled.
    expect(result.job?.status).toBe('failed');
    expect(result.messages).toHaveLength(1);
    expect(result.messages[0]).toMatchObject({
      outcome: 'failed',
      note: 'issue creation is not granted here',
    });
  });

  it('owes the thread an outcome when policy refuses the job, without the code', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(deniedWork);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => Effect.succeed({ status: 'completed', summary: 'never runs' }),
    );

    expect(result.job?.lastError).toBe('POLICY_EXECUTION_DENIED');
    expect(result.messages).toHaveLength(1);
    expect(result.messages[0]).toMatchObject({
      outcome: 'failed',
      repository: deniedWork.repository,
    });
    expect(result.messages[0]?.note).toBeUndefined();
  });

  it('does not record a refusal as a completion', async () => {
    // The defect: `rejected` fell past the failure branch into `queue.complete`
    // and logged "Completed queued work", so an agent that declined the request
    // was stored as having done it — and pruned on the completed window.
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { counts: yield* queue.counts, job: yield* queue.job(jobId) };
      }),
      () => Effect.succeed({ status: 'rejected', summary: 'I will not do this' }),
    );

    expect(result.counts.completed).toBe(0);
    expect(result.job?.status).toBe('failed');
    expect(result.job?.outcome).toBe('rejected');
  });

  it('parks a question instead of finishing it, and does not re-ask unprompted', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        // Mixed case, and a sender outside the trusted list: the answer policy
        // is the union of the two, normalized, and a fixture where they agree
        // cannot tell that from either half alone.
        const { jobId } = yield* queue.enqueue({ ...work, sender: 'Stranger' });
        const worker = yield* Worker;
        yield* worker.runOnce;
        return {
          secondRun: yield* worker.runOnce,
          job: yield* queue.job(jobId),
          counts: yield* queue.counts,
        };
      }),
      () => Effect.succeed({ status: 'needs_input', summary: 'which branch?' }),
      3,
      true,
      undefined,
      // `Adiutriel` is the daemon's own login: trusted to send, never an
      // answerer, or she answers her own questions and runs forever.
      { repository: { trustedSenders: ['edloidas', 'friend', 'Adiutriel'] } },
    );

    expect(result.job?.status).toBe('pending');
    expect(result.job?.outcome).toBeUndefined();
    expect(result.job?.questionId).toBeString();
    // The turn that asked, plus the senders this repository trusts. Only a
    // fixture where those two differ can tell a union from either half of it.
    expect(result.job?.questionAnswerers).toEqual(['stranger', 'edloidas', 'friend']);
    // The asking attempt is spent; waiting itself costs nothing further, and
    // the claim must not pick the row up again while it is unanswered.
    expect(result.job?.attempts).toBe(1);
    expect(result.counts.failed).toBe(0);
    expect(result.secondRun).toBe(false);
  });

  it('finishes rather than parks a question it would have no attempt left to act on', async () => {
    // Parking spends no attempt and restores none, so asking on the last one
    // parks a row the next claim dead-letters. The thread would read: I need an
    // answer — answered — this did not finish.
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        yield* (yield* Worker).runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => Effect.succeed({ status: 'needs_input', summary: 'which branch?' }),
      1,
    );

    expect(result.job?.questionId).toBeUndefined();
    expect(result.job?.status).toBe('failed');
    expect(result.job?.outcome).toBe('needs_input');
    // The question still reaches the thread — it just carries no promise that
    // an answer will be acted on.
    expect(result.messages.map((message) => message.outcome)).toEqual(['needs_input']);
  });

  it('rolls the question message back when the park is fenced out', async () => {
    // The park writes the question and the message in one transaction. A write
    // that loses its fence must take the message with it — a question posted to
    // a thread where no job is waiting can never be answered.
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const parked = yield* Effect.either(
          queue.park({
            jobId,
            attemptNumber: 7,
            repository: work.repository,
            subjectNumber: work.subject.number,
            question: 'which branch?',
            answerers: ['edloidas'],
            expiresAt: 1_000_000,
          }),
        );
        return { parked, outbox: yield* queue.outboxFor(jobId), job: yield* queue.job(jobId) };
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
    );

    expect(result.parked._tag).toBe('Left');
    expect(result.outbox).toHaveLength(0);
    expect(result.job?.questionId).toBeUndefined();
  });

  it('runs a job approved after the runnable age limit instead of refusing it', async () => {
    // The job is older than `maxJobAgeMs`, but it spent that time held for
    // approval — the gate bounds runnable work, so the wait must not count.
    // Before `ready_at` the gate read `created_at` and refused this on the
    // first claim after approval, into a status `approve` rejects.
    const executed: string[] = [];
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(
          { ...work, approvalRequired: true },
          10_000,
          7 * 86_400_000,
        );
        // Twice the age limit below, so `created_at` alone condemns the job.
        yield* TestClock.adjust('48 hours');
        yield* queue.approve(jobId);
        const worker = yield* Worker;
        const worked = yield* worker.runOnce;
        return { worked, job: yield* queue.job(jobId) };
      }).pipe(Effect.provide(TestContext.TestContext)),
      (item) => {
        executed.push(item.deliveryId);
        return Effect.succeed({ status: 'completed', summary: 'done' });
      },
      3,
      true,
      undefined,
      { maxJobAgeMs: 24 * 60 * 60 * 1000 },
    );

    expect(executed).toEqual([work.deliveryId]);
    expect(result.job?.status).toBe('completed');
    expect(result.job?.lastError).toBeUndefined();
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

  // Each case closes exactly one gate, so the stored code — not the count — says which fired.
  const dropped = (item: WorkItem, policyOverrides: PolicyOverrides, beforeClaim = Effect.void) =>
    run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(item);
        yield* beforeClaim;
        const worker = yield* Worker;
        const worked = yield* worker.runOnce;
        return { worked, counts: yield* queue.counts, job: yield* queue.job(jobId) };
      }),
      () => Effect.die('the executor must not run'),
      3,
      true,
      undefined,
      policyOverrides,
    );

  it('drops a claimed job whose repository policy does not accept it', async () => {
    const result = await dropped(work, { repository: { accepted: false } });

    expect(result.worked).toBe(true);
    expect(result.counts.failed).toBe(1);
    expect(result.counts.completed).toBe(0);
    expect(result.job?.lastError).toBe('POLICY_REPOSITORY_NOT_ACCEPTED');
  });

  it('drops a claimed job whose repository policy denies execution', async () => {
    const result = await dropped(deniedWork, {});

    expect(result.worked).toBe(true);
    expect(result.counts.failed).toBe(1);
    expect(result.job?.lastError).toBe('POLICY_EXECUTION_DENIED');
  });

  it('drops a claimed job still awaiting approval under approval execution', async () => {
    const result = await dropped(work, { repository: { execution: 'approval' } });

    expect(result.worked).toBe(true);
    expect(result.counts.failed).toBe(1);
    expect(result.job?.lastError).toBe('POLICY_APPROVAL_REQUIRED');
  });

  // The other half of the same clause: without `approvalRequired !== false` this
  // job would drop too.
  it('runs an approval-execution job whose approval was already granted', async () => {
    const counts = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(approvedWork);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.counts;
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
      3,
      true,
      undefined,
      { repository: { execution: 'approval' } },
    );

    expect(counts.completed).toBe(1);
    expect(counts.failed).toBe(0);
  });

  it('drops a claimed job that sat in the queue past the maximum job age', async () => {
    const result = await dropped(work, { maxJobAgeMs: 10 }, Effect.sleep('30 millis'));

    expect(result.worked).toBe(true);
    expect(result.counts.failed).toBe(1);
    expect(result.job?.lastError).toBe('POLICY_JOB_TOO_OLD');
  });

  // The repository limit sits below the daemon-wide one, so `claimFor` still hands
  // out attempt 2 — it dead-letters only past `workerMaxAttempts`.
  it('drops a claimed job past the repository attempt limit', async () => {
    let executions = 0;
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(work);
        const worker = yield* Worker;
        yield* worker.runOnce;
        yield* Effect.sleep('150 millis');
        const second = yield* worker.runOnce;
        return { second, counts: yield* queue.counts, job: yield* queue.job(jobId) };
      }),
      () => {
        executions += 1;
        return Effect.fail(new ExecutorError({ message: 'temporary', retryable: true }));
      },
      3,
      true,
      undefined,
      { repository: { maxAttempts: 1 } },
    );

    expect(result.second).toBe(true);
    expect(executions).toBe(1);
    expect(result.counts.failed).toBe(1);
    expect(result.counts.retry).toBe(0);
    expect(result.job?.lastError).toBe('POLICY_ATTEMPTS_EXHAUSTED');
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

describe('Worker.runOnce observability', () => {
  const observe = (
    execute: InstanceType<typeof AgentExecutor>['execute'],
    items: readonly WorkItem[] = [work],
  ) => {
    const logs: LogLine[] = [];
    return run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const worker = yield* Worker;
        for (const item of items) yield* queue.enqueue(item);
        for (const _ of items) yield* worker.runOnce;
        return { logs, counts: yield* queue.counts };
      }).pipe(Effect.provide(capture(logs))),
      execute,
    );
  };

  it('reports the status and the elapsed time of a completed job', async () => {
    const { logs } = await observe(
      () => Effect.sleep('30 millis').pipe(Effect.as({ status: 'completed', summary: 'done' })),
      [work, prWork],
    );

    const completed = logs.filter((line) => line.message === 'Completed queued work');
    // Two jobs, so the annotation has to carry the claimed id rather than a constant.
    expect(completed.map((line) => line.annotations.job)).toEqual([1, 2]);
    expect(completed[0]?.annotations.status).toBe('completed');
    expect(completed[0]?.annotations.attempt).toBe(1);
    // Measured, not defaulted: the executor was held for 30ms on the real clock.
    expect(completed[0]?.annotations.durationMs).toBeGreaterThanOrEqual(25);
  });

  it('reports a job parked on the question the agent asked', async () => {
    const { logs } = await observe(() =>
      Effect.succeed({ status: 'needs_input', summary: 'which branch?' }),
    );

    const annotations = annotationsOf(logs, 'Parked queued work pending an answer');
    expect(annotations?.status).toBe('needs_input');
    expect(annotations?.answerers).toBe('edloidas');
    // The question is agent prose parsed out of Codex stdout; it stays in the
    // database rather than going through the log.
    expect(JSON.stringify(annotations)).not.toContain('which branch?');
    expect(annotationsOf(logs, 'Queued work did not complete')).toBeUndefined();
  });

  it('schedules nothing further when the agent reports its own failure', async () => {
    const { logs } = await observe(() =>
      Effect.succeed({ status: 'failed', summary: 'could not push' }),
    );

    const annotations = annotationsOf(logs, 'Queued work did not complete');
    expect(annotations?.status).toBe('failed');
    // A retry is earned by an observed cause, and this one carries none: the
    // next attempt would re-run byte-identical input for a second opinion.
    expect(annotations?.retryAt).toBeUndefined();
  });

  it('keeps the agent-authored summary out of every log line', async () => {
    const { logs } = await observe(() =>
      Effect.succeed({ status: 'failed', summary: 'cat /etc/shadow said root:x:0' }),
    );

    expect(logs.length).toBeGreaterThan(0);
    expect(JSON.stringify(logs)).not.toContain('root:x:0');
  });

  it('reports the elapsed time when the executor itself fails', async () => {
    const { logs } = await observe(() =>
      Effect.sleep('30 millis').pipe(
        Effect.zipRight(Effect.fail(new ExecutorError({ message: 'temporary', retryable: true }))),
      ),
    );

    const annotations = annotationsOf(logs, 'Queued work will retry');
    expect(annotations?.durationMs).toBeGreaterThanOrEqual(25);
    expect(annotations?.errorCode).toBe('EXECUTOR_RETRYABLE');
  });

  it('reports a permanent executor failure under its own message and code', async () => {
    const { logs } = await observe(() =>
      Effect.fail(new ExecutorError({ message: 'no write access', retryable: false })),
    );

    const annotations = annotationsOf(logs, 'Queued work failed');
    expect(annotations?.errorCode).toBe('EXECUTOR_FAILED');
    expect(annotations).not.toHaveProperty('retryAt');
  });

  it('reports a claimed job dropped before execution by repository policy', async () => {
    const { logs, counts } = await observe(
      () => Effect.die('the executor must not run'),
      [deniedWork],
    );

    const annotations = annotationsOf(logs, 'Dropped queued work denied by policy');
    expect(annotations?.errorCode).toBe('POLICY_EXECUTION_DENIED');
    expect(annotations?.durationMs).toBeTypeOf('number');
    // A dropped job is failed, never completed.
    expect(counts.failed).toBe(1);
    expect(counts.completed).toBe(0);
  });
});

describe('Worker.runOnce on a clipped request', () => {
  const trigger = {
    source: { kind: 'issue_comment', id: 200 },
    url: 'https://github.com/edloidas/lictor/issues/17#issuecomment-200',
    text: 'do the thing and then',
    clipped: true,
    poster: 'edloidas',
    revision: '2026-08-21T10:00:00Z',
    observedAt: 1_700_000_000_000,
  } as const;

  const clippedWork: WorkItem = { ...work, trigger };

  // The whole point of the guard: the agent is never handed part of a request
  // as though it were the whole one. `Effect.die` rather than a recorded flag —
  // a spy that is merely asserted-not-called still passes if the assertion is
  // the thing that breaks.
  it('parks without ever running the agent', async () => {
    const messages = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(clippedWork);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.outboxFor(jobId);
      }),
      () => Effect.die('the executor must not run on a clipped request'),
    );

    expect(messages).toHaveLength(1);
    expect(messages[0]?.outcome).toBe('clipped');
    // ! No note. The wording is the daemon's, and every note published carries
    // ! an "the agent's own summary" attribution that would then be false.
    expect(messages[0]?.note).toBeUndefined();
  });

  it('leaves the job answerable rather than finished', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(clippedWork);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return { job: yield* queue.job(jobId), messages: yield* queue.outboxFor(jobId) };
      }),
      () => Effect.die('the executor must not run on a clipped request'),
    );

    expect(result.job?.status).toBe('pending');
    // The question's identity, not merely its presence: the claim skips on this
    // column and an answer is fenced by it, so a `question_id` naming no message
    // parks a job nothing can ever resume.
    expect(result.job?.questionId).toBe(result.messages[0]?.messageId);
    expect(result.job?.questionAnswerers).toEqual(['edloidas']);
  });

  // Otherwise every claim re-asks and the job never progresses, however many
  // times the requester answers.
  it('runs the agent once an answer has come back', async () => {
    const answered: WorkItem = { ...clippedWork, answerUrl: 'https://github.com/a/b/issues/1#c-9' };
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(answered);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.job(jobId);
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
    );

    expect(result?.outcome).toBe('completed');
  });

  // Parking spends no attempt but refunds none, so with the budget gone there
  // is nothing left to ask with. It must still not run on the fragment.
  it('refuses instead of asking when no attempt is left to ask with', async () => {
    const messages = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(clippedWork);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.outboxFor(jobId);
      }),
      () => Effect.die('the executor must not run on a clipped request'),
      1,
    );

    expect(messages).toHaveLength(1);
    expect(messages[0]?.outcome).toBe('rejected');
    expect(messages[0]?.note).toBeUndefined();
  });

  it('runs normally when the record fit', async () => {
    const whole: WorkItem = {
      ...clippedWork,
      trigger: { ...trigger, clipped: false },
    };
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId } = yield* queue.enqueue(whole);
        const worker = yield* Worker;
        yield* worker.runOnce;
        return yield* queue.job(jobId);
      }),
      () => Effect.succeed({ status: 'completed', summary: 'done' }),
    );

    expect(result?.outcome).toBe('completed');
  });
});
