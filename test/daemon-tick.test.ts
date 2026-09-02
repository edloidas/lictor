import { describe, expect, it } from 'bun:test';
import { Effect, Fiber, Layer, Redacted, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { credentialExpiryWatch, daemonTick, maintenanceLoop } from '../src/daemon-tick.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
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

const run = <A, E>(
  effect: Effect.Effect<A, E, WorkQueue | CredentialHealth | GitHubIdentity>,
  verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError> = Effect.succeed(live),
) => {
  const ConfigLive = Layer.succeed(LictorConfig, config);
  const services = Layer.mergeAll(
    WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive)),
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
        const loop = yield* Effect.fork(maintenanceLoop);
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
        const watch = yield* Effect.fork(credentialExpiryWatch);
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
