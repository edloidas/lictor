import { describe, expect, it } from 'bun:test';
import { Effect, Fiber, Layer, Redacted, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { daemonTick, maintenanceLoop } from '../src/daemon-tick.ts';
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

  it('suspends the credential once its verified expiry has passed', async () => {
    const result = await run(
      Effect.gen(function* () {
        const health = yield* CredentialHealth;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return yield* health.isRejected;
      }),
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt: 61_000 }),
    );

    expect(result).toBe(true);
  });

  it.each([
    ['an expiry still ahead', 120_000],
    ['no expiry at all', undefined],
  ])('leaves the credential healthy with %s', async (_case, tokenExpiresAt) => {
    const result = await run(
      Effect.gen(function* () {
        const health = yield* CredentialHealth;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return yield* health.isRejected;
      }),
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt }),
    );

    expect(result).toBe(false);
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

  it('still reclaims when the identity probe fails', async () => {
    const result = await run(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const health = yield* CredentialHealth;
        yield* queue.enqueue(work);
        yield* queue.claim;
        yield* TestClock.adjust('61 seconds');
        yield* daemonTick;
        return { counts: yield* queue.counts, rejected: yield* health.isRejected };
      }),
      Effect.fail(new GitHubIdentityError({ message: 'GitHub is unreachable', transient: true })),
    );

    expect(result.counts.interrupted).toBe(1);
    expect(result.rejected).toBe(false);
  });
});
