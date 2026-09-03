import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import {
  Clock,
  Effect,
  Fiber,
  Layer,
  Logger,
  Redacted,
  Schema,
  TestClock,
  TestContext,
} from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { DeliveryWorker, isTerminalFailure } from '../src/delivery-worker.ts';
import { GitHubClient, GitHubRequestError } from '../src/github/client.ts';
import {
  GitHubIdentity,
  GitHubIdentityError,
  type VerifiedIdentity,
} from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { QueueError, WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('pat-value'),
  expectedLogin: 'adiutriel',
  trustedSenders: ['edloidas'],
  autoAcceptInviters: [],
  databasePath: ':memory:',
  stateDir: stateDirOf(':memory:'),
  policyPath: 'unused',
  controlSocketPath: '/tmp/lictor-delivery.sock',
  deliveryMaxBytes: 1024 * 1024,
  executor: 'disabled',
  codexModel: 'gpt-5.6-luna',
  codexHome: '',
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
    node_id: 'IC_99',
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

const services = (
  verified: Effect.Effect<VerifiedIdentity, GitHubIdentityError>,
  reactions: string[],
  options: {
    readonly reactionStatus?: number;
    readonly firstRequestDelayMs?: number;
    readonly requests?: string[];
    readonly maxQueueDepth?: number;
  } = {},
) => {
  const ConfigLive = Layer.succeed(LictorConfig, config);
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const IdentityLive = Layer.succeed(GitHubIdentity, GitHubIdentity.make({ verified }));
  let delayed = false;
  const client = HttpClient.make((request) => {
    options.requests?.push(request.url);
    const respond = Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(
          JSON.stringify(routes.find(([fragment]) => request.url.includes(fragment))?.[1] ?? {}),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      ),
    );
    if (options.firstRequestDelayMs === undefined || delayed) return respond;
    delayed = true;
    return Effect.sleep(`${options.firstRequestDelayMs} millis`).pipe(Effect.zipRight(respond));
  });
  const GitHubLive = Layer.succeed(
    GitHubClient,
    GitHubClient.make({
      authenticated: Effect.succeed(
        client.pipe(HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test'))),
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
      `[defaults]\nexecution = "automatic"\n[limits]\nmaxQueueDepth = ${options.maxQueueDepth ?? 10_000}\n[repositories]\nallow = ["edloidas/lictor"]`,
      ['edloidas'],
    ).pipe(Effect.map(Policy.make)),
  );
  const Services = Layer.mergeAll(ConfigLive, QueueLive, GitHubLive, IdentityLive, PolicyLive);
  return Layer.merge(
    DeliveryWorker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
    Services,
  );
};

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
        // Repeated deliberately. The drain loop reclaims a pending delivery
        // every cycle, and `claimDelivery` increments `attempts` each time, so
        // a branch that merely defers condemnation looks correct on the first
        // pass and destroys the inbox on the third.
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
        Effect.provide(services(verified, reactions, options)),
      ),
    ),
  );
};

describe('DeliveryWorker', () => {
  // Qualification can outlast the lease, and the sweep reads only the row. The
  // threshold sits between the claim-time lease and the one heartbeat renews,
  // so a worker that never renews is reclaimed out from under itself here.
  it('renews the delivery lease while qualification runs', async () => {
    const reactions: string[] = [];
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.receiveDelivery({
            id: 'delivery-1',
            event: 'notification',
            body,
            source: 'notification',
          });
          const worker = yield* DeliveryWorker;
          const claimedAt = yield* Clock.currentTimeMillis;
          const fiber = yield* Effect.fork(worker.runOnce);
          yield* Effect.sleep('1200 millis');
          const reclaimed = yield* queue.recoverStaleDeliveries(claimedAt + 60_500);
          const processed = yield* Fiber.join(fiber);
          return { reclaimed, processed, status: yield* queue.deliveryStatus('delivery-1') };
        }).pipe(
          Effect.provide(Logger.remove(Logger.defaultLogger)),
          Effect.provide(
            services(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }), reactions, {
              firstRequestDelayMs: 1500,
            }),
          ),
        ),
      ),
    );

    expect(result.reclaimed).toBe(0);
    expect(result.processed).toBe(true);
    expect(result.status).toBe('completed');
  });

  // The lease is a claim on the row, so losing it has to stop the work, not
  // just be noticed. The sweep steals the delivery before the first heartbeat
  // is due; the renewal that then fails is what aborts qualification. Counting
  // requests is the only way to tell that apart from a worker that merely
  // fails at the end — both leave the row `pending`, but one keeps calling
  // GitHub on a delivery it no longer owns. The cycle survives: the row is
  // already requeued, so there is nothing to write and nothing to fail over.
  it('abandons a delivery whose lease it lost mid-qualification', async () => {
    const reactions: string[] = [];
    const requests: string[] = [];
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.receiveDelivery({
            id: 'delivery-1',
            event: 'notification',
            body,
            source: 'notification',
          });
          const worker = yield* DeliveryWorker;
          const fiber = yield* Effect.fork(worker.runOnce);
          // Inside the first request's delay, and before the heartbeat at 1s.
          yield* Effect.sleep('300 millis');
          const now = yield* Clock.currentTimeMillis;
          const stolen = yield* queue.recoverStaleDeliveries(now + 120_000);
          const exit = yield* Effect.exit(Fiber.join(fiber));
          return { stolen, exit, status: yield* queue.deliveryStatus('delivery-1') };
        }).pipe(
          Effect.provide(Logger.remove(Logger.defaultLogger)),
          Effect.provide(
            services(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }), reactions, {
              firstRequestDelayMs: 1500,
              requests,
            }),
          ),
        ),
      ),
    );

    expect(result.stolen).toBe(1);
    // Absorbed, not failed: a lost lease must not cost the rest of the drain.
    expect(result.exit._tag).toBe('Success');
    // Left where the sweep put it — the worker wrote nothing on the way out.
    expect(result.status).toBe('pending');
    // One request in flight when the lease was lost, and none after.
    expect(requests).toHaveLength(1);
  });

  // The heartbeat is not the only way to learn the claim is gone. Here
  // qualification finishes inside the heartbeat interval, so `finishDelivery`
  // is what discovers it — and it has to be absorbed the same way, or a sweep
  // landing near the end of processing costs the whole drain cycle.
  it('abandons a delivery stolen just before it finished', async () => {
    const reactions: string[] = [];
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.receiveDelivery({
            id: 'delivery-1',
            event: 'notification',
            body,
            source: 'notification',
          });
          const worker = yield* DeliveryWorker;
          const fiber = yield* Effect.fork(worker.runOnce);
          // Inside the first request's 400ms delay, so processing resumes and
          // completes well before the heartbeat at 1s ever fires.
          yield* Effect.sleep('200 millis');
          const now = yield* Clock.currentTimeMillis;
          const stolen = yield* queue.recoverStaleDeliveries(now + 120_000);
          const exit = yield* Effect.exit(Fiber.join(fiber));
          return { stolen, exit, status: yield* queue.deliveryStatus('delivery-1') };
        }).pipe(
          Effect.provide(Logger.remove(Logger.defaultLogger)),
          Effect.provide(
            services(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }), reactions, {
              firstRequestDelayMs: 400,
            }),
          ),
        ),
      ),
    );

    expect(result.stolen).toBe(1);
    expect(result.exit._tag).toBe('Success');
    // Not completed by the worker that lost it — the sweep's requeue stands.
    expect(result.status).toBe('pending');
  });

  it('completes a delivery it can qualify, and records the acknowledgement', async () => {
    const result = await run(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }));

    expect(result.processed).toBe(true);
    expect(result.status).toBe('completed');
    expect(result.counts.pending).toBe(1);
    expect(result.audit.map((entry) => entry.capability)).toEqual(['react']);
    expect(result.audit[0]?.outcome).toBe('ok');
  });

  // The cursor is what makes the next sweep scan from the right anchor. Moving
  // it is not optional bookkeeping: without it the same comment is re-examined
  // and re-attributed on every poll.
  it('advances the thread cursor once the job is committed', async () => {
    const result = await run(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }));

    expect(result.cursor).toBe(Date.parse('2026-08-21T10:00:00Z'));
  });

  // A reaction is a courtesy, the job is the product. A refused reaction must
  // not reach the delivery worker's error channel, where a non-`QueueError`
  // failure marks the delivery permanently failed and throws away work that is
  // already durably queued.
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

  // A dead credential says nothing about the delivery. `failed` is terminal —
  // neither the startup reset nor the control plane recovers such a row — so
  // condemning the inbox for a daemon-side misconfiguration silently voids the
  // guarantee that a committed delivery is durable.
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

  // Driven under the test clock because the assertion is the length of the
  // sleep: every pass leaves the same refunded `pending` row behind, so the row
  // alone cannot show it.
  it('waits the backoff cap on every pass while the job queue is full', async () => {
    const filler: WorkItem = {
      deliveryId: 'filler',
      interactionId: 'interaction-filler',
      repository: 'edloidas/lictor',
      sender: 'edloidas',
      targets: ['adiutriel'],
      reasons: ['assigned'],
      subject: {
        kind: 'issue',
        number: 1,
        title: 'Occupies the only slot',
        url: 'https://github.com/edloidas/lictor/issues/1',
      },
    };
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          yield* queue.enqueue(filler);
          yield* queue.receiveDelivery({
            id: 'delivery-1',
            event: 'notification',
            body,
            source: 'notification',
          });
          const worker = yield* DeliveryWorker;
          const pass = Effect.gen(function* () {
            const fiber = yield* Effect.fork(worker.runOnce);
            yield* TestClock.adjust('59 seconds');
            const early = yield* Fiber.poll(fiber);
            yield* TestClock.adjust('1 second');
            const done = yield* Fiber.poll(fiber);
            return { early: early._tag, done: done._tag };
          });
          const passes = [yield* pass, yield* pass];
          return { passes, next: yield* queue.claimDelivery };
        }).pipe(
          Effect.provide(Logger.remove(Logger.defaultLogger)),
          Effect.provide(
            services(Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }), [], {
              maxQueueDepth: 1,
            }),
          ),
          Effect.provide(TestContext.TestContext),
        ),
      ),
    );

    expect(result.passes).toEqual([
      { early: 'None', done: 'Some' },
      { early: 'None', done: 'Some' },
    ]);
    // Two refunded passes leave the row where a fresh claim finds attempt 1.
    expect(result.next).toMatchObject({ id: 'delivery-1', status: 'processing', attempts: 1 });
  });

  // Parse and schema failures are properties of the stored bytes, so they are
  // condemned on the first pass and stay condemned — a later pass must not
  // resurrect them, which is what distinguishes terminal from budgeted. A
  // condemned delivery is not reclaimed, so a later pass finds an empty inbox.
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

  // Anything the worker cannot classify is presumed transient and retried
  // within the attempt budget — this is the path enrichment-fetch transport
  // errors land on. `Effect.die` stands in for such an error, because defects
  // skip every named branch by construction.
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
  // Built from a real schema rejection rather than a lookalike object: the
  // predicate must recognize the actual `ParseError` the decoder produces.
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
