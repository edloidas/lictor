import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Logger, Redacted, Schema } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { DeliveryWorker, isTerminalFailure } from '../src/delivery-worker.ts';
import { GitHubClient, GitHubRequestError } from '../src/github/client.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { QueueError, WorkQueue } from '../src/queue/work-queue.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('pat-value'),
  expectedLogin: 'adiutriel',
  trustedSenders: ['edloidas'],
  databasePath: ':memory:',
  policyPath: 'unused',
  controlSocketPath: '/tmp/lictor-delivery.sock',
  deliveryMaxBytes: 1024 * 1024,
  executor: 'disabled',
  codexModel: 'gpt-5.6-luna',
  agentWorkdir: '.',
  executorTimeoutMs: 1000,
  executorOutputBytes: 1024,
  gitTimeoutMs: 180_000,
  workerPollMs: 10,
  workerMaxAttempts: 3,
  workerRetryBaseMs: 1,
  notificationPollMs: 60_000,
});

const body = JSON.stringify({
  id: '14567',
  unread: true,
  reason: 'mention',
  updated_at: '2026-08-21T10:00:00Z',
  subject: {
    title: 'Keep the queue moving',
    url: 'https://api.github.com/repos/edloidas/lictor/issues/17',
    latest_comment_url: 'https://api.github.com/repos/edloidas/lictor/issues/comments/99',
    type: 'Issue',
  },
  repository: { full_name: 'edloidas/lictor' },
});

const issue = {
  title: 'Keep the queue moving',
  html_url: 'https://github.com/edloidas/lictor/issues/17',
  body: 'no mention in the body',
  user: { login: 'edloidas' },
  created_at: '2026-08-20T08:00:00Z',
  updated_at: '2026-08-20T08:00:00Z',
};

const comments = [
  {
    id: 99,
    html_url: 'https://github.com/edloidas/lictor/issues/17#issuecomment-99',
    body: '@adiutriel please look',
    user: { login: 'edloidas' },
    created_at: '2026-08-21T10:00:00Z',
    updated_at: '2026-08-21T10:00:00Z',
  },
];

const routes: readonly (readonly [string, unknown])[] = [
  ['/issues/17/comments', comments],
  ['/pulls/17/comments', []],
  ['/issues/17', issue],
];

const run = (
  verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError>,
  passes = 1,
  deliveryBody: string = body,
  options: { readonly reactionStatus?: number } = {},
) => {
  const reactions: string[] = [];
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        yield* queue.receiveDelivery({
          id: 'delivery-1',
          event: 'notification',
          body: deliveryBody,
          source: 'notification',
        });
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
          reactions,
          audit: yield* queue.auditLog(1),
          cursor: yield* queue.notificationCursor('14567'),
        };
      }).pipe(
        Effect.provide(Logger.remove(Logger.defaultLogger)),
        Effect.provide(
          (() => {
            const ConfigLive = Layer.succeed(LictorConfig, config);
            const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
            const IdentityLive = Layer.succeed(GitHubIdentity, GitHubIdentity.make({ verified }));
            const client = HttpClient.make((request) =>
              Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify(
                      routes.find(([fragment]) => request.url.includes(fragment))?.[1] ?? {},
                    ),
                    { status: 200, headers: { 'content-type': 'application/json' } },
                  ),
                ),
              ),
            );
            const GitHubLive = Layer.succeed(
              GitHubClient,
              GitHubClient.make({
                authenticated: Effect.succeed(
                  client.pipe(
                    HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test')),
                  ),
                ),
                addReaction: (repository, target) =>
                  options.reactionStatus !== undefined && options.reactionStatus >= 400
                    ? Effect.fail(
                        new GitHubRequestError({
                          message: `Reacting returned status ${options.reactionStatus}`,
                        }),
                      )
                    : Effect.sync(() => {
                        reactions.push(`${repository}:${JSON.stringify(target)}`);
                      }).pipe(Effect.as(undefined)),
              }),
            );
            const PolicyLive = Layer.effect(
              Policy,
              parsePolicy(
                '[defaults]\nexecution = "automatic"\n[repositories]\nallow = ["edloidas/lictor"]',
              ).pipe(Effect.map(Policy.make)),
            );
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
};

describe('DeliveryWorker', () => {
  it('completes a delivery it can qualify, and records the acknowledgement', async () => {
    const result = await run(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }));

    expect(result.processed).toBe(true);
    expect(result.status).toBe('completed');
    expect(result.counts.pending).toBe(1);
    expect(result.audit.map((entry) => entry.capability)).toEqual(['react']);
    expect(result.audit[0]?.outcome).toBe('ok');
  });

  // ! The cursor is what makes the next sweep scan from the right anchor. Moving
  // ! it is not optional bookkeeping: without it the same comment is re-examined
  // ! and re-attributed on every poll.
  it('advances the thread cursor once the job is committed', async () => {
    const result = await run(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }));

    expect(result.cursor).toBe(Date.parse('2026-08-21T10:00:00Z'));
  });

  // ! A reaction is a courtesy, the job is the product. A refused reaction must
  // ! not reach the delivery worker's error channel, where a non-`QueueError`
  // ! failure marks the delivery permanently failed and throws away work that is
  // ! already durably queued.
  it('completes the delivery even when the reaction is refused', async () => {
    const result = await run(
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
      1,
      body,
      { reactionStatus: 403 },
    );

    expect(result.status).toBe('completed');
    expect(result.counts.pending).toBe(1);
    expect(result.audit[0]?.outcome).toBe('react_failed');
  });

  // ! A dead credential says nothing about the delivery. `failed` is terminal —
  // ! neither the startup reset nor the control plane recovers such a row — so
  // ! condemning the inbox for a daemon-side misconfiguration silently voids the
  // ! guarantee that a committed delivery is durable.
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

  it.each([1, 3])('condemns a body the thread schema rejects after %i passes', async (passes) => {
    const result = await run(
      Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
      passes,
      'null',
    );

    expect(result.processed).toBe(passes === 1);
    expect(result.status).toBe('failed');
    expect(result.counts.pending).toBe(0);
  });

  // ! Anything the worker cannot classify is presumed transient and retried
  // ! within the attempt budget — this is the path enrichment-fetch transport
  // ! errors land on. `Effect.die` stands in for such an error, because defects
  // ! skip every named branch by construction.
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
  });

  it('leaves everything else to the retry path', () => {
    expect(isTerminalFailure(new QueueError({ operation: 'claim', cause: undefined }))).toBe(false);
    expect(isTerminalFailure(new GitHubIdentityError({ message: 'nope' }))).toBe(false);
    expect(isTerminalFailure(new Error('transport down'))).toBe(false);
    expect(isTerminalFailure(undefined)).toBe(false);
  });
});
