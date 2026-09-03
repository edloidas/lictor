import { describe, expect, it } from 'bun:test';
import {
  Chunk,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Redacted,
  TestClock,
  TestContext,
} from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import {
  credentialExpiryWatch,
  daemonTick,
  maintenanceLoop,
  supervisedMaintenanceLoop,
} from '../src/daemon-tick.ts';
import { describeCause, failureOperation } from '../src/diagnostics.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { QueueError, WorkQueue } from '../src/queue/work-queue.ts';
import type { FatalAction } from '../src/supervision.ts';
import type { WorkItem } from '../src/work-item.ts';

const config = LictorConfig.make({
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
  workerMaxAttempts: 3,
  workerRetryBaseMs: 100,
  notificationPollMs: 60_000,
});

const live: VerifiedIdentity = { login: 'adiutriel', tokenExpiresAt: undefined };

// `TestClock.adjust` supervises no fibers under a bare `TestContext`: it yields a
// few real-timer turns and warps. A double parked on a real timer ahead of the
// first sleep — none does today — would see the clock move before that sleep is
// registered. Hold until the fiber has a sleep on the clock, or has ended.
const forkSleeping = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const before = Chunk.size(yield* TestClock.sleeps());
    const fiber = yield* Effect.fork(self);
    const deadline = performance.now() + 3000;
    while (
      Chunk.size(yield* TestClock.sleeps()) === before &&
      Option.isNone(yield* Fiber.poll(fiber))
    ) {
      if (performance.now() > deadline) {
        return yield* Effect.dieMessage('The forked fiber never reached the test clock');
      }
      yield* Effect.yieldNow();
    }
    return fiber;
  });

const run = <A, E>(
  effect: Effect.Effect<A, E, WorkQueue | CredentialHealth | GitHubIdentity>,
  verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError> = Effect.succeed(live),
  overlay: (queue: WorkQueue) => WorkQueue = (queue) => queue,
) => {
  const ConfigLive = Layer.succeed(LictorConfig, config);
  const QueueLive = Layer.effect(WorkQueue, Effect.map(WorkQueue, overlay)).pipe(
    Layer.provide(WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive))),
  );
  const services = Layer.mergeAll(
    QueueLive,
    CredentialHealth.Default,
    Layer.succeed(GitHubIdentity, GitHubIdentity.make({ verified })),
  );
  return Effect.runPromise(
    Effect.scoped(effect.pipe(Effect.provide(services), Effect.provide(TestContext.TestContext))),
  );
};

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
    title: 'Tick this',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
};

describe('daemonTick', () => {
  it('reclaims a job whose worker lease has expired', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        yield* queue.claim;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return { counts: yield* queue.counts, retried: yield* queue.claim };
      }),
    );

    expect(result.counts.interrupted).toBe(1);
    expect(result.counts.running).toBe(0);
    expect(result.retried?.work.deliveryId).toBe('delivery-1');
    expect(result.retried?.attempts).toBe(2);
  });

  it('returns a delivery whose lease has expired to pending', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'stuck',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        yield* queue.claimDelivery;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return { status: yield* queue.deliveryStatus('stuck'), next: yield* queue.claimDelivery };
      }),
    );

    expect(result.status).toBe('pending');
    expect(result.next).toMatchObject({ id: 'stuck', attempts: 2 });
  });

  it('leaves work whose lease is still live untouched', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        yield* queue.claim;
        yield* queue.receiveDelivery({
          id: 'stuck',
          event: 'notification',
          body: '{}',
          source: 'notification',
        });
        yield* queue.claimDelivery;
        yield* TestClock.adjust('59 seconds');
        yield* daemonTick;
        return { counts: yield* queue.counts, status: yield* queue.deliveryStatus('stuck') };
      }),
    );

    expect(result.counts.running).toBe(1);
    expect(result.counts.interrupted).toBe(0);
    expect(result.status).toBe('processing');
  });

  it('renews daemon ownership', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const before = yield* queue.diagnostics;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return { before, after: yield* queue.diagnostics };
      }),
    );

    expect(result.before.workerHeartbeatAt).toBe(0);
    expect(result.after.workerHeartbeatAt).toBe(61_000);
  });

  it('runs the tick every ten seconds for as long as it is forked', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        // Forked off the epoch, so a tick that fires before its first sleep
        // writes a heartbeat distinguishable from the one ownership opened with.
        yield* TestClock.adjust('1 second');
        const loop = yield* forkSleeping(maintenanceLoop);
        yield* TestClock.adjust('9 seconds');
        const early = yield* queue.diagnostics;
        yield* TestClock.adjust('1 second');
        const first = yield* queue.diagnostics;
        yield* TestClock.adjust('10 seconds');
        const second = yield* queue.diagnostics;
        yield* Fiber.interrupt(loop);
        return { early, first, second };
      }),
    );

    expect(result.early.workerHeartbeatAt).toBe(0);
    expect(result.first.workerHeartbeatAt).toBe(11_000);
    expect(result.second.workerHeartbeatAt).toBe(21_000);
  });

  // `Effect.never` is the faithful double: an unreachable GitHub makes the probe
  // retry without limit rather than fail, and a tick reading its verdict stalls.
  it('renews ownership and reclaims while the identity probe is still pending', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.enqueue(work);
        yield* queue.claim;
        yield* TestClock.adjust('61 seconds');
        // A plain fork, not `forkSleeping`: the tick has no sleep to reach, and a
        // tick that stalls on the probe must fail the assertions below, not the hold.
        const tick = yield* Effect.fork(daemonTick);
        yield* TestClock.adjust('1 second');
        yield* Fiber.interrupt(tick);
        return { counts: yield* queue.counts, diagnostics: yield* queue.diagnostics };
      }),
      Effect.never,
    );

    expect(result.counts.interrupted).toBe(1);
    expect(result.diagnostics.workerHeartbeatAt).toBe(61_000);
  });
});

const queueError = (operation: string, message: string) =>
  new QueueError({ operation, cause: new Error(message) });

const ownershipLost = queueError('renew daemon ownership', 'Daemon ownership was lost');

const recorder = () => {
  const calls: { message: string; reason: string; operation: string | undefined }[] = [];
  const fatal: FatalAction = (message, cause) =>
    Effect.sync(() => {
      calls.push({
        message,
        reason: cause === undefined ? 'none' : describeCause(cause),
        operation: cause === undefined ? undefined : failureOperation(cause),
      });
    });
  return { calls, fatal };
};

describe('supervisedMaintenanceLoop', () => {
  it.each([
    [
      'the ownership heartbeat fails',
      (queue: WorkQueue) =>
        WorkQueue.make({ ...queue, heartbeatDaemon: Effect.fail(ownershipLost) }),
      { reason: 'QueueError', operation: 'renew daemon ownership' },
    ],
    [
      // Both recovery calls, because wrapping either one in `Effect.ignore` left
      // every test green before these two cases existed.
      'a job reclaim fails',
      (queue: WorkQueue) =>
        WorkQueue.make({
          ...queue,
          recoverStale: () => Effect.fail(queueError('recover stale jobs', 'Reclaim failed')),
        }),
      { reason: 'QueueError', operation: 'recover stale jobs' },
    ],
    [
      'a delivery reclaim fails',
      (queue: WorkQueue) =>
        WorkQueue.make({
          ...queue,
          recoverStaleDeliveries: () =>
            Effect.fail(queueError('recover stale deliveries', 'Reclaim failed')),
        }),
      { reason: 'QueueError', operation: 'recover stale deliveries' },
    ],
    [
      // The gap this test file existed without: a throw inside `Effect.gen` is a
      // defect, so the error channel never carries it and the fiber dies unseen.
      'the tick dies of a defect',
      (queue: WorkQueue) =>
        WorkQueue.make({
          ...queue,
          heartbeatDaemon: Effect.sync(() => {
            throw new Error('database handle closed');
          }),
        }),
      { reason: 'Defect: Error: database handle closed', operation: undefined },
    ],
  ])('stops the daemon when %s', async (_case, overlay, expected) => {
    const { calls, fatal } = recorder();
    const exit = await run(
      Effect.gen(function* () {
        const loop = yield* forkSleeping(supervisedMaintenanceLoop(fatal));
        yield* TestClock.adjust('10 seconds');
        return yield* Fiber.await(loop);
      }),
      undefined,
      overlay,
    );

    expect(calls).toEqual([{ message: 'The daemon maintenance loop stopped', ...expected }]);
    // Supervision consumes the cause rather than re-raising it: the fatal action
    // is taking the process down, so the fiber has nothing left to report.
    expect(exit).toStrictEqual(Exit.void);
  });

  it('stays silent while the tick holds', async () => {
    const { calls, fatal } = recorder();
    const beat = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* TestClock.adjust('1 second');
        const loop = yield* forkSleeping(supervisedMaintenanceLoop(fatal));
        yield* TestClock.adjust('30 seconds');
        const diagnostics = yield* queue.diagnostics;
        yield* Fiber.interrupt(loop);
        return diagnostics;
      }),
    );

    // Three cadences of a loop that really ran: without this, a wrapper that
    // never reached the loop at all would satisfy the assertion below.
    expect(beat.workerHeartbeatAt).toBe(31_000);
    expect(calls).toEqual([]);
  });
});

describe('credentialExpiryWatch', () => {
  // The watch only reads the clock on the ten-second grid, so an expiry landing
  // exactly on one is the only case that reaches the equality arm.
  it.each([
    ['an expiry between two polls', 61_000, 60_000, 70_000],
    ['an expiry on a poll', 60_000, 50_000, 60_000],
  ])('suspends the credential after %s', async (_case, tokenExpiresAt, healthyAt, rejectedAt) => {
    const result = await run(
      Effect.gen(function* () {
        const health = yield* CredentialHealth;
        const watch = yield* forkSleeping(credentialExpiryWatch);
        yield* TestClock.adjust(healthyAt);
        const before = yield* health.isRejected;
        yield* TestClock.adjust(rejectedAt - healthyAt);
        const after = yield* health.isRejected;
        yield* Fiber.interrupt(watch);
        return { before, after };
      }),
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt }),
    );

    expect(result.before).toBe(false);
    expect(result.after).toBe(true);
  });

  it.each([
    ['the token never expires', Effect.succeed(live)],
    [
      // Not transient: those retry forever inside `verified` and never escape.
      'the credential was refused',
      Effect.fail(new GitHubIdentityError({ message: 'GitHub rejected the credential' })),
    ],
  ])('returns without suspending when %s', async (_case, verified) => {
    const result = await run(
      Effect.gen(function* () {
        const health = yield* CredentialHealth;
        yield* credentialExpiryWatch;
        return yield* health.isRejected;
      }),
      verified,
    );

    expect(result).toBe(false);
  });
});
