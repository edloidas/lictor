import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Logger, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { NotificationPoller } from '../src/notifications/poller.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';

const config = (
  overrides: {
    readonly notificationPollMs?: number;
    readonly autoAcceptInviters?: readonly string[];
  } = {},
) =>
  LictorConfig.make({
    githubToken: Redacted.make('pat-value'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    databasePath: ':memory:',
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor-poller.sock',
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
    notificationPollMs: overrides.notificationPollMs ?? 60_000,
    autoAcceptInviters: [...(overrides.autoAcceptInviters ?? [])],
  });

const thread = (id: string, updatedAt = '2026-08-21T10:00:00Z') => ({
  id,
  unread: true,
  reason: 'mention',
  updated_at: updatedAt,
  last_read_at: null,
  subject: {
    title: `Thread ${id}`,
    url: `https://api.github.com/repos/edloidas/lictor/issues/${id}`,
    latest_comment_url: null,
    type: 'Issue',
  },
  repository: { full_name: 'edloidas/lictor' },
});

// `setUrlParams` is serialized at execution time, so `request.url` carries no
// query string — a matcher looking for one routes every list call elsewhere.
const isList = (url: string): boolean =>
  url.includes('/notifications') && !url.includes('/notifications/threads');

type Reply = {
  readonly status?: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
};

const isInvitationList = (url: string): boolean =>
  url.includes('/user/repository_invitations') && !/\/repository_invitations\//.test(url);

/**
 * Runs one or more sweeps against a scripted sequence of list responses.
 *
 * `listReplies[n]` answers the n-th `GET /notifications`. Running past the end
 * repeats the last entry, which is what makes a two-sweep idempotence check
 * readable. Everything that is not a list call answers 205 with no body, which
 * is what `PATCH /notifications/threads/{id}` returns.
 */
const run = (
  listReplies: readonly Reply[],
  options: {
    readonly sweeps?: number;
    readonly maxQueueDepth?: number;
    readonly notificationPollMs?: number;
    readonly markReadStatus?: number;
    readonly inviters?: readonly string[];
    readonly invitationReplies?: readonly Reply[];
    /** Runs the invitation pass once after the sweeps and reports its exit. */
    readonly acceptInvitations?: boolean;
    /** Runs the invitation pass *before* the sweeps. */
    readonly acceptFirst?: boolean;
  } = {},
) => {
  const calls: string[] = [];
  const headersSeen: Record<string, string | undefined>[] = [];
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const poller = yield* NotificationPoller;
        const queue = yield* WorkQueue;
        if (options.acceptFirst) {
          yield* poller.acceptInvitations.pipe(Effect.ignore);
        }
        const outcomes = [];
        for (let sweep = 0; sweep < (options.sweeps ?? 1); sweep += 1) {
          outcomes.push(yield* poller.pollOnce);
        }
        const accepted = options.acceptInvitations
          ? yield* Effect.either(poller.acceptInvitations)
          : undefined;
        return {
          outcomes,
          accepted,
          calls,
          headersSeen,
          cursor: yield* queue.pollerCursor,
          backlog: yield* queue.backlog,
        };
      }).pipe(
        Effect.provide(Logger.remove(Logger.defaultLogger)),
        Effect.provide(
          (() => {
            const ConfigLive = Layer.succeed(
              LictorConfig,
              config({
                ...(options.notificationPollMs === undefined
                  ? {}
                  : { notificationPollMs: options.notificationPollMs }),
                ...(options.inviters === undefined ? {} : { autoAcceptInviters: options.inviters }),
              }),
            );
            // The store is recorded into the same log as the HTTP calls, because
            // the invariant this suite exists to protect is an *ordering* — a
            // count of PATCHes cannot tell store-then-mark from mark-then-store,
            // and swapping the two lines in the poller left every test green.
            const QueueLive = Layer.effect(
              WorkQueue,
              Effect.map(WorkQueue, (queue) =>
                WorkQueue.make({
                  ...queue,
                  receiveDelivery: (delivery) =>
                    Effect.sync(() => {
                      calls.push(`STORE ${delivery.id}`);
                    }).pipe(Effect.zipRight(queue.receiveDelivery(delivery))),
                }),
              ),
            ).pipe(
              Layer.provide(WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive))),
            );
            const client = HttpClient.make((request) => {
              calls.push(`${request.method} ${request.url}`);
              if (isInvitationList(request.url)) {
                const index = Math.min(
                  calls.filter((call) => isInvitationList(call)).length - 1,
                  (options.invitationReplies?.length ?? 1) - 1,
                );
                const reply = options.invitationReplies?.[index] ?? {};
                return Effect.succeed(
                  HttpClientResponse.fromWeb(
                    request,
                    new Response(JSON.stringify(reply.body ?? []), {
                      status: reply.status ?? 200,
                      headers: { 'content-type': 'application/json', ...reply.headers },
                    }),
                  ),
                );
              }
              if (isList(request.url)) {
                headersSeen.push({ 'if-modified-since': request.headers['if-modified-since'] });
                const index = Math.min(
                  calls.filter((call) => isList(call)).length - 1,
                  listReplies.length - 1,
                );
                const reply = listReplies[index] ?? {};
                return Effect.succeed(
                  HttpClientResponse.fromWeb(
                    request,
                    new Response(reply.status === 304 ? null : JSON.stringify(reply.body ?? []), {
                      status: reply.status ?? 200,
                      headers: { 'content-type': 'application/json', ...reply.headers },
                    }),
                  ),
                );
              }
              return Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(null, { status: options.markReadStatus ?? 205 }),
                ),
              );
            });
            const GitHubLive = Layer.succeed(
              GitHubClient,
              GitHubClient.make({
                authenticated: Effect.succeed(
                  client.pipe(
                    HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test')),
                  ),
                ),
                addReaction: () => Effect.succeed(undefined),
              }),
            );
            const PolicyLive = Layer.effect(
              Policy,
              parsePolicy(
                `[limits]\nmaxQueueDepth = ${options.maxQueueDepth ?? 10_000}\n[repositories]\nallow = ["edloidas/lictor"]`,
              ).pipe(Effect.map(Policy.make)),
            );
            const Services = Layer.mergeAll(
              ConfigLive,
              QueueLive,
              GitHubLive,
              PolicyLive,
              CredentialHealth.Default,
            );
            return Layer.merge(
              NotificationPoller.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
              Services,
            );
          })(),
        ),
      ),
    ),
  );
};

const marks = (calls: readonly string[]) =>
  calls.filter((call) => call.startsWith('PATCH') && call.includes('/notifications/threads/'));

describe('NotificationPoller', () => {
  it('stores every thread it sees and marks each read once', async () => {
    const result = await run([{ body: [thread('1'), thread('2')] }]);

    expect(result.outcomes[0]?.stored).toBe(2);
    expect(marks(result.calls)).toHaveLength(2);
    expect(result.backlog).toBe(2);
  });

  // The whole point of the synthetic delivery id. GitHub replays a thread
  // whenever it gains activity, and re-reading one already stored must not
  // create a second inbox row.
  it('is idempotent across sweeps for the same thread activity', async () => {
    const result = await run([{ body: [thread('1')] }], { sweeps: 2 });

    expect(result.backlog).toBe(1);
  });

  // Stored, not filtered. `reason` describes the thread, not the activity that
  // just landed on it: GitHub does not re-key an already-unread `assign` thread
  // when a trusted mention arrives on it. Skipping on `reason` here would mark
  // it read with nothing durable behind it and lose the mention on both sides,
  // so the drop happens at qualification, where the row survives to say so.
  it('stores a thread whose reason this phase does not act on', async () => {
    const result = await run([{ body: [{ ...thread('1'), reason: 'assign' }, thread('2')] }]);

    expect(result.outcomes[0]?.stored).toBe(2);
    expect(marks(result.calls)).toHaveLength(2);
    expect(result.backlog).toBe(2);
  });

  // The code's own reason for sending `If-Modified-Since` on page 1 only: a
  // later page answering 304 must not read as end-of-list, because the cursor
  // would then advance past threads nobody fetched and the next poll answers
  // 304 forever.
  it('does not advance the cursor when a later page answers 304', async () => {
    const firstPage = Array.from({ length: 50 }, (_, index) => thread(String(index + 1)));
    const result = await run([
      { body: firstPage, headers: { 'last-modified': 'Thu, 21 Aug 2026 10:00:00 GMT' } },
      { status: 304 },
    ]);

    expect(result.outcomes[0]?.deferred).toBe(true);
    expect(result.cursor).toBeUndefined();
  });

  it('replays Last-Modified as If-Modified-Since on the next sweep', async () => {
    const result = await run(
      [{ body: [thread('1')], headers: { 'last-modified': 'Thu, 21 Aug 2026 10:00:00 GMT' } }],
      { sweeps: 2 },
    );

    expect(result.cursor).toBe('Thu, 21 Aug 2026 10:00:00 GMT');
    expect(result.headersSeen[0]?.['if-modified-since']).toBeUndefined();
    expect(result.headersSeen[1]?.['if-modified-since']).toBe('Thu, 21 Aug 2026 10:00:00 GMT');
  });

  it('stores nothing on a 304', async () => {
    const result = await run([{ status: 304 }]);

    expect(result.outcomes[0]?.stored).toBe(0);
    expect(marks(result.calls)).toHaveLength(0);
    expect(result.backlog).toBe(0);
  });

  it('honours X-Poll-Interval when it is longer than the configured floor', async () => {
    const result = await run([{ body: [], headers: { 'x-poll-interval': '120' } }], {
      notificationPollMs: 30_000,
    });

    expect(result.outcomes[0]?.waitMs).toBe(120_000);
  });

  // The floor is a floor, not a target. A header shorter than it — or a header
  // GitHub omits — must not turn the loop into a spin.
  it('never waits less than the configured floor', async () => {
    const result = await run([{ body: [], headers: { 'x-poll-interval': '1' } }], {
      notificationPollMs: 30_000,
    });

    expect(result.outcomes[0]?.waitMs).toBe(30_000);
  });

  // The whole durability guarantee, and the replacement for the webhook 202.
  // Until the mark lands GitHub still owns the item; mark first and a crash
  // between the two loses it outright.
  it('commits the row before marking the thread read', async () => {
    const result = await run([{ body: [thread('1')] }]);

    const store = result.calls.findIndex(
      (call) => call === 'STORE notification:1:2026-08-21T10:00:00Z',
    );
    const mark = result.calls.findIndex(
      (call) => call.startsWith('PATCH') && call.includes('/notifications/threads/1'),
    );
    expect(store).toBeGreaterThanOrEqual(0);
    expect(mark).toBeGreaterThan(store);
  });

  // A refused PATCH must defer the sweep. The row is already committed, so
  // nothing durable is lost — but advancing the cursor over a thread that is
  // still unread turns the next poll into a 304, and the thread is never listed
  // again until fresh activity lands on it.
  it('does not advance the cursor when marking a thread read fails', async () => {
    const result = await run(
      [{ body: [thread('1')], headers: { 'last-modified': 'Thu, 21 Aug 2026 10:00:00 GMT' } }],
      { markReadStatus: 500 },
    );

    expect(result.outcomes[0]?.stored).toBe(1);
    expect(result.outcomes[0]?.deferred).toBe(true);
    expect(result.cursor).toBeUndefined();
  });

  // A credential GitHub refuses does not heal, and repeating it every minute
  // buries the one log line that says why nothing is happening. The second
  // sweep must make no request at all.
  it('suspends permanently on a 401 and stops calling GitHub', async () => {
    const result = await run([{ status: 401, body: { message: 'Bad credentials' } }], {
      sweeps: 3,
    });

    expect(result.calls.filter((call) => isList(call))).toHaveLength(1);
    expect(result.outcomes.every((outcome) => outcome.stored === 0)).toBe(true);
  });

  it('backs off for the wait GitHub asked for on a 429', async () => {
    const result = await run([{ status: 429, headers: { 'retry-after': '300' } }], {
      notificationPollMs: 30_000,
    });

    expect(result.outcomes[0]?.waitMs).toBe(300_000);
  });

  // Marking read past the depth limit hands the overflow nowhere to sit:
  // GitHub forgets the thread and `enqueue` refuses it a stage later. Leaving
  // it unread is what makes GitHub the buffer.
  it('stores nothing and marks nothing read once the queue is full', async () => {
    const full = await run([{ body: [thread('1'), thread('2'), thread('3')] }], {
      maxQueueDepth: 2,
      sweeps: 1,
    });

    expect(full.outcomes[0]?.stored).toBe(2);
    expect(full.outcomes[0]?.deferred).toBe(true);
    expect(marks(full.calls)).toHaveLength(2);
  });

  // A deferred sweep must not advance the cursor. Advancing it turns the next
  // poll into a 304 and the threads deliberately left unread are never
  // fetched again.
  it('leaves the cursor alone when a sweep is deferred', async () => {
    const result = await run(
      [
        {
          body: [thread('1'), thread('2')],
          headers: { 'last-modified': 'Thu, 21 Aug 2026 10:00:00 GMT' },
        },
      ],
      { maxQueueDepth: 1 },
    );

    expect(result.outcomes[0]?.deferred).toBe(true);
    expect(result.cursor).toBeUndefined();
  });

  // The row is already durable, so a refused mark-read is a cosmetic problem
  // only: the thread stays bold in her inbox and the next sweep upserts the
  // same synthetic id. Failing the sweep here would lose the sweep instead.
  it('keeps going when marking a thread read fails', async () => {
    const result = await run([{ body: [thread('1')] }], { markReadStatus: 500 });

    expect(result.outcomes[0]?.stored).toBe(1);
    expect(result.backlog).toBe(1);
  });

  const invitation = (id: number, login: string, repo = 'edloidas/lictor') => ({
    id,
    inviter: { login },
    repository: { full_name: repo },
    permissions: 'write',
  });

  const accepts = (calls: readonly string[]) =>
    calls.filter(
      (call) => call.startsWith('PATCH') && call.includes('/user/repository_invitations/'),
    );

  it('accepts invitations from the allow-list and leaves the rest pending', async () => {
    const result = await run([{ body: [] }], {
      inviters: ['edloidas'],
      invitationReplies: [
        {
          body: [invitation(1, 'edloidas'), invitation(2, 'stranger'), invitation(3, 'Edloidas')],
        },
      ],
      acceptInvitations: true,
    });

    expect(result.accepted?._tag).toBe('Right');
    // Case-insensitive on the inviter, like every other login comparison.
    expect(accepts(result.calls)).toHaveLength(2);
    expect(result.calls.some((call) => call.includes('/repository_invitations/2'))).toBe(false);
  });

  it('makes no calls when the allow-list is empty', async () => {
    const result = await run([{ body: [] }], {
      invitationReplies: [{ body: [invitation(1, 'edloidas')] }],
      acceptInvitations: true,
    });

    expect(result.accepted?._tag).toBe('Right');
    expect(result.calls.some((call) => call.includes('/user/repository_invitations'))).toBe(false);
  });

  it('logs and moves on when listing invitations fails', async () => {
    const result = await run([{ body: [] }], {
      inviters: ['edloidas'],
      invitationReplies: [{ status: 404 }],
      acceptInvitations: true,
    });

    expect(result.accepted?._tag).toBe('Right');
    expect(accepts(result.calls)).toHaveLength(0);
  });

  // A refused credential suspends notification polling globally through the
  // shared ref — the invitation pass must trip that same wire, or it keeps
  // calling GitHub every cadence after the sweep went quiet.
  it('suspends on a 401 from the invitation list', async () => {
    const result = await run([{ body: [thread('1')] }], {
      inviters: ['edloidas'],
      invitationReplies: [{ status: 401 }],
      acceptFirst: true,
    });

    expect(result.calls.some((call) => call.includes('/user/repository_invitations'))).toBe(true);
    // The sweep that follows the suspension must not reach GitHub at all.
    expect(result.calls.some((call) => isList(call))).toBe(false);
  });
});
