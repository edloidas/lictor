import { describe, expect, it } from 'bun:test';
import { Effect, Layer, Logger, Redacted, Schema } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { DeliveryWorker, isTerminalFailure } from '../src/delivery-worker.ts';
import { GitHubClient } from '../src/github/client.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { QueueError, WorkQueue } from '../src/queue/work-queue.ts';
import { MalformedInteraction } from '../src/webhook/qualification.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('pat-value'),
  expectedLogin: 'adiutriel',
  webhookSecret: Redacted.make('secret'),
  trustedSenders: ['edloidas'],
  targetUsers: ['adiutriel'],
  databasePath: ':memory:',
  policyPath: 'unused',
  controlSocketPath: '/tmp/lictor-delivery.sock',
  deliveryMaxBytes: 1024 * 1024,
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

const run = (
  verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError>,
  passes = 1,
  deliveryBody: string = body,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({ id: 'delivery-1', event: 'issues', body: deliveryBody });
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

  // ! Parse and schema failures are properties of the stored bytes, so they are
  // ! condemned on the first pass and stay condemned — a later pass must not
  // ! resurrect them, which is what distinguishes terminal from budgeted. A
  // ! condemned delivery is not reclaimed, so a later pass finds an empty inbox.
  it.each([1, 3])('condemns an unparseable body after %i passes', async (passes) => {
    const result = await run(
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
      passes,
      'not-json',
    );

    expect(result.processed).toBe(passes === 1);
    expect(result.status).toBe('failed');
    expect(result.counts.pending).toBe(0);
  });

  it.each([1, 3])('condemns a body the envelope schema rejects after %i passes', async (passes) => {
    const result = await run(
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
      passes,
      'null',
    );

    expect(result.processed).toBe(passes === 1);
    expect(result.status).toBe('failed');
    expect(result.counts.pending).toBe(0);
  });

  it.each([1, 3])(
    'condemns a malformed interaction without spending retries, after %i passes',
    async (passes) => {
      const result = await run(
        Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
        passes,
        JSON.stringify({
          action: 'assigned',
          sender: { login: 'edloidas' },
          repository: { name: 'lictor', full_name: 'edloidas/lictor' },
          assignee: { login: 'adiutriel' },
        }),
      );

      expect(result.processed).toBe(passes === 1);
      expect(result.status).toBe('failed');
      expect(result.counts.pending).toBe(0);
    },
  );

  // ! Anything the worker cannot classify is presumed transient and retried
  // ! within the attempt budget — this is the path a future enrichment fetch's
  // ! transport errors land on. `Effect.die` stands in for such an error,
  // ! because defects skip every named branch by construction.
  it.each([1, 2])('keeps retrying an unclassifiable failure after %i passes', async (passes) => {
    const result = await run(Effect.die(new Error('enrichment fetch exploded')), passes);

    expect(result.processed).toBe(true);
    expect(result.status).toBe('pending');
  });

  it('condemns an unclassifiable failure once the attempt budget is spent', async () => {
    const result = await run(Effect.die(new Error('enrichment fetch exploded')), 3);

    expect(result.status).toBe('failed');
  });
});

describe('isTerminalFailure', () => {
  // ! Built from a real schema rejection rather than a lookalike object: the
  // ! predicate must recognize the actual `ParseError` the decoder produces.
  const parseError = Effect.runSync(
    Effect.flip(Schema.decodeUnknown(Schema.Struct({ login: Schema.String }))({})),
  );

  it('names parse and schema failures terminal', () => {
    expect(isTerminalFailure(parseError)).toBe(true);
    expect(isTerminalFailure(new MalformedInteraction({ message: 'missing issue' }))).toBe(true);
  });

  it('leaves everything else to the retry path', () => {
    expect(isTerminalFailure(new QueueError({ operation: 'claim', cause: undefined }))).toBe(false);
    expect(isTerminalFailure(new GitHubIdentityError({ message: 'nope' }))).toBe(false);
    expect(isTerminalFailure(new Error('transport down'))).toBe(false);
    expect(isTerminalFailure(undefined)).toBe(false);
  });
});
