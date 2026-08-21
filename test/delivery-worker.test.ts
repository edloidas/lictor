import { describe, expect, it } from 'bun:test';
import { Effect, Layer, Logger, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { DeliveryWorker } from '../src/delivery-worker.ts';
import { GitHubClient } from '../src/github/client.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('pat-value'),
  expectedLogin: 'adiutriel',
  webhookSecret: Redacted.make('secret'),
  trustedSenders: ['edloidas'],
  targetUsers: ['adiutriel'],
  databasePath: ':memory:',
  policyPath: 'unused',
  controlSocketPath: '/tmp/lictor-delivery.sock',
  webhookMaxBytes: 1024 * 1024,
  executor: 'disabled',
  codexModel: 'gpt-5.6-luna',
  agentWorkdir: '.',
  executorTimeoutMs: 1000,
  executorOutputBytes: 1024,
  workerPollMs: 10,
  workerMaxAttempts: 3,
  workerRetryBaseMs: 1,
});

const body = JSON.stringify({
  action: 'assigned',
  sender: { login: 'edloidas' },
  repository: { name: 'lictor', full_name: 'edloidas/lictor' },
  installation: { id: 42 },
  issue: {
    number: 17,
    title: 'Keep the queue moving',
    html_url: 'https://github.com/edloidas/lictor/issues/17',
    updated_at: '2026-08-12T12:00:00Z',
  },
  assignee: { login: 'adiutriel' },
});

const run = (verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError>, passes = 1) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({ id: 'delivery-1', event: 'issues', body });
        const worker = yield* DeliveryWorker;
        // ! Repeated deliberately. The drain loop reclaims a pending delivery
        // ! every cycle, and `claimDelivery` increments `attempts` each time, so
        // ! a branch that merely defers condemnation looks correct on the first
        // ! pass and destroys the inbox on the third.
        let processed = false;
        for (let pass = 0; pass < passes; pass += 1) {
          processed = yield* worker.runOnce;
        }
        return {
          processed,
          status: yield* queue.deliveryStatus('delivery-1'),
          counts: yield* queue.counts,
        };
      }).pipe(
        Effect.provide(Logger.remove(Logger.defaultLogger)),
        Effect.provide(
          (() => {
            const ConfigLive = Layer.succeed(LictorConfig, config);
            const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
            const IdentityLive = Layer.succeed(GitHubIdentity, GitHubIdentity.make({ verified }));
            const GitHubLive = Layer.succeed(
              GitHubClient,
              GitHubClient.make({ authenticated: Effect.die('must not call GitHub') }),
            );
            const PolicyLive = Layer.effect(
              Policy,
              parsePolicy(
                '[defaults]\nexecution = "automatic"\n[repositories]\nallow = ["edloidas/lictor"]',
              ).pipe(Effect.map(Policy.make)),
            );
            // ! `runOnce` dispatches, so the handler's own requirements have to
            // ! be in scope at the call site too — providing them to the layer
            // ! alone satisfies the typechecker and then fails at runtime.
            const Services = Layer.mergeAll(
              ConfigLive,
              QueueLive,
              GitHubLive,
              IdentityLive,
              PolicyLive,
            );
            return Layer.merge(
              DeliveryWorker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
              Services,
            );
          })(),
        ),
      ),
    ),
  );

describe('DeliveryWorker', () => {
  it('completes a delivery it can qualify', async () => {
    const result = await run(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }));

    expect(result.processed).toBe(true);
    expect(result.status).toBe('completed');
    expect(result.counts.pending).toBe(1);
  });

  // ! A dead credential says nothing about the delivery. `failed` is terminal —
  // ! neither the startup reset nor the control plane recovers such a row — so
  // ! condemning the inbox for a daemon-side misconfiguration silently voids the
  // ! guarantee that an acknowledged delivery is durably committed.
  it.each([1, 3, 5])(
    'does not condemn a delivery when the credential cannot be verified, after %i passes',
    async (passes) => {
      const result = await run(
        Effect.fail(
          new GitHubIdentityError({
            message: 'Credential belongs to someone-else, not the expected adiutriel',
          }),
        ),
        passes,
      );

      expect(result.processed).toBe(true);
      expect(result.status).toBe('pending');
      expect(result.counts.pending).toBe(0);
    },
  );
});
