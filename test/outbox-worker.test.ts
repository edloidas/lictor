import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientResponse } from '@effect/platform';
import { Clock, Effect, Layer, Logger, Redacted, Runtime, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { OutboxWorker } from '../src/outbox-worker.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { type OutcomeDelivery, WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const config = LictorConfig.make({
  githubToken: Redacted.make('pat-value'),
  expectedLogin: 'adiutriel',
  trustedSenders: ['edloidas'],
  autoAcceptInviters: [],
  databasePath: ':memory:',
  stateDir: stateDirOf(':memory:'),
  policyPath: 'unused',
  controlSocketPath: '/tmp/lictor-outbox.sock',
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
  workerRetryBaseMs: 1,
  notificationPollMs: 60_000,
});

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
    title: 'Queue this',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
};

const COMMENT_POLICY = `[defaults]
execution = "automatic"

[defaults.capabilities]
read = true
comment = true

[repositories]
allow = ["edloidas/lictor"]
`;

const NO_COMMENT_POLICY = `[defaults]
execution = "automatic"

[defaults.capabilities]
read = true

[repositories]
allow = ["edloidas/lictor"]
`;

const COMMENTS_URL = 'https://api.github.com/repos/edloidas/lictor/issues/17/comments';

type Reply = {
  readonly status: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
};

/**
 * Runs one delivery pass over a single queued outcome and reports both what the
 * worker asked GitHub for and what the row looks like afterwards.
 *
 * The real `GitHubClient` sits over the stub rather than being replaced by one,
 * so the status classification the worker branches on is the client's own. The
 * latch is built outside the worker's layer so a test can read it.
 *
 * `{{marker}}` in a reply body becomes the row's real marker, so a fixture can
 * only claim a comment is already on the thread by carrying the identity that
 * row was actually given.
 */
const deliverOnce = (options: {
  /** Omit to run the loop against an empty outbox. */
  readonly delivery?: OutcomeDelivery;
  readonly policy?: string;
  readonly replies: readonly Reply[];
  /** Passes before the send, each one spending an attempt. */
  readonly priorAttempts?: number;
  /** Latch the credential before the pass, as a prior 401 would have. */
  readonly credentialRejected?: boolean;
  /** Retry the job mid-flight, cancelling the row the sender already claimed. */
  readonly supersedeBeforeSend?: boolean;
}) => {
  const requests: string[] = [];
  const bodies: string[] = [];
  let call = 0;
  let marker = '';
  // Runs from inside the stub, between the reconciliation answering and the
  // POST — the window an operator retry actually lands in.
  let supersede: () => void = () => undefined;
  const stub = HttpClient.make((request) => {
    requests.push(`${request.method} ${request.url}`);
    const payload = request.body as { readonly body?: unknown };
    bodies.push(payload.body instanceof Uint8Array ? new TextDecoder().decode(payload.body) : '');
    const reply = options.replies[Math.min(call, options.replies.length - 1)];
    call += 1;
    if (options.supersedeBeforeSend === true) supersede();
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(JSON.stringify(reply?.body ?? {}).replaceAll('{{marker}}', marker), {
          status: reply?.status ?? 201,
          headers: { 'content-type': 'application/json', ...reply?.headers },
        }),
      ),
    );
  });

  const ConfigLive = Layer.succeed(LictorConfig, config);
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const HealthLive = CredentialHealth.Default;
  const PolicyLive = Layer.effect(
    Policy,
    parsePolicy(options.policy ?? COMMENT_POLICY, ['edloidas']).pipe(Effect.map(Policy.make)),
  );
  const ClientLive = GitHubClient.DefaultWithoutDependencies.pipe(
    Layer.provide(
      Layer.merge(
        Layer.succeed(HttpClient.HttpClient, stub),
        GitHubCredential.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive)),
      ),
    ),
  );
  const WorkerLive = OutboxWorker.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.mergeAll(ConfigLive, QueueLive, ClientLive, PolicyLive, HealthLive)),
  );

  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const health = yield* CredentialHealth;
        if (options.credentialRejected === true) yield* health.suspend;
        if (options.delivery !== undefined) {
          const { jobId } = yield* queue.enqueue(work);
          const claimed = yield* queue.claim;
          // `job.retry` only accepts a failed job, so the supersede case has to
          // finish this one the way the reviewer's scenario does.
          yield* options.supersedeBeforeSend === true
            ? queue.fail(jobId, claimed?.attempts ?? 0, 'no', undefined, 'rejected', {
                ...options.delivery,
                outcome: 'rejected',
              })
            : queue.complete(jobId, claimed?.attempts ?? 0, '{}', options.delivery);
          // Each pass leaves the row `sending`; recovery returns it to `pending`
          // with the attempt spent, which is the state a crashed send leaves.
          for (let pass = 0; pass < (options.priorAttempts ?? 0); pass += 1) {
            yield* queue.claimOutbox;
            yield* queue.recoverStaleOutbox(Number.MAX_SAFE_INTEGER);
          }
          const [owed] = yield* queue.outboxFor(jobId);
          marker = `<!-- lictor:${owed?.messageId ?? ''} -->`;
        }
        const runtime = yield* Effect.runtime<never>();
        supersede = () => {
          Runtime.runSync(runtime)(Effect.ignore(queue.retry(1)));
        };
        const worker = yield* OutboxWorker;
        const startedAt = yield* Clock.currentTimeMillis;
        const worked = yield* worker.runOnce;
        const [message] = yield* queue.outboxFor(1);
        return {
          worked,
          message,
          marker,
          startedAt,
          requests,
          bodies,
          rejected: yield* health.isRejected,
        };
      }).pipe(
        Effect.provide(Layer.mergeAll(WorkerLive, QueueLive, HealthLive)),
        Effect.provide(Logger.remove(Logger.defaultLogger)),
      ),
    ),
  );
};

const completed: OutcomeDelivery = {
  repository: 'edloidas/lictor',
  subjectNumber: 17,
  outcome: 'completed',
  note: 'Opened the pull request.',
};

const since = (createdAt: number | undefined) =>
  `GET ${COMMENTS_URL}?per_page=100&page=1&since=${new Date(createdAt ?? 0).toISOString()}`;

describe('OutboxWorker', () => {
  it('posts the outcome to the thread and records the comment', async () => {
    const result = await deliverOnce({
      delivery: completed,
      replies: [{ status: 201, body: { html_url: 'https://github.com/c/1' } }],
    });

    expect(result.worked).toBe(true);
    expect(result.requests).toEqual([`POST ${COMMENTS_URL}`]);
    expect(result.message).toMatchObject({
      status: 'delivered',
      commentUrl: 'https://github.com/c/1',
    });
    expect(result.bodies[0]).toContain('Opened the pull request.');
    expect(result.bodies[0]).toContain(result.marker);
  });

  it('records the outcome locally and posts nothing when policy forbids commenting', async () => {
    const result = await deliverOnce({
      delivery: completed,
      policy: NO_COMMENT_POLICY,
      replies: [{ status: 201 }],
    });

    expect(result.requests).toEqual([]);
    expect(result.message).toMatchObject({
      status: 'blocked',
      lastError: 'blocked_by_policy',
      outcome: 'completed',
      note: 'Opened the pull request.',
    });
  });

  it('recognises its own comment instead of posting a second one', async () => {
    // What a crash between the POST and the row recording it leaves behind.
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [
        {
          status: 200,
          body: [
            // A Lictor comment belonging to a different message. Matching the
            // prefix rather than the identity reconciles against this one and
            // drops the message this row still owes the thread.
            { body: 'an earlier outcome <!-- lictor:some-other-message -->' },
            { body: 'done {{marker}}', html_url: 'https://github.com/c/9' },
          ],
        },
      ],
    });

    expect(result.requests).toEqual([since(result.message?.createdAt)]);
    expect(result.message).toMatchObject({
      status: 'delivered',
      commentUrl: 'https://github.com/c/9',
    });
  });

  it('reads past a full page that does not carry the marker', async () => {
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [
        { status: 200, body: [{ body: 'unrelated chatter' }] },
        { status: 200, body: [{ body: 'done {{marker}}', html_url: 'https://github.com/c/8' }] },
      ],
    });

    expect(result.requests).toHaveLength(2);
    expect(result.requests[1]).toContain('page=2');
    expect(result.message?.commentUrl).toBe('https://github.com/c/8');
  });

  it('posts once a reconciliation proves nothing landed', async () => {
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [
        { status: 200, body: [] },
        { status: 201, body: { html_url: 'https://github.com/c/2' } },
      ],
    });

    expect(result.requests).toEqual([since(result.message?.createdAt), `POST ${COMMENTS_URL}`]);
    expect(result.message?.status).toBe('delivered');
  });

  it('does not post when the reconciliation ran out of pages', async () => {
    // Every page full and no marker means the thread outran the scan, which is
    // not the same answer as the comment not being there.
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [{ status: 200, body: [{ body: 'busy thread' }] }],
    });

    expect(result.requests).toHaveLength(10);
    expect(result.requests.every((request) => request.startsWith('GET'))).toBe(true);
    expect(result.message?.status).toBe('pending');
  });

  it('does not post a message the operator superseded while it was sending', async () => {
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      // The reconciliation comes back empty, so without the re-read the next
      // step is a POST of an outcome the operator has already replaced.
      replies: [{ status: 200, body: [] }],
      supersedeBeforeSend: true,
    });

    expect(result.requests).toHaveLength(1);
    expect(result.requests[0]).toContain('GET');
    expect(result.message?.status).toBe('canceled');
  });

  it('does not post when the reconciliation itself failed', async () => {
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [{ status: 500 }],
    });

    expect(result.requests).toHaveLength(1);
    expect(result.message?.status).toBe('pending');
  });

  it('does not post when the reconciliation answered something it cannot read', async () => {
    // A body no schema accepts leaves the loop's last recovery branch to catch
    // it — the branch a defect would otherwise take the whole worker down past.
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [{ status: 200, body: { not: 'an array' } }],
    });

    expect(result.requests).toHaveLength(1);
    expect(result.message).toMatchObject({ status: 'pending', attempts: 2 });
  });

  it('waits as long as GitHub asked when it throttles', async () => {
    const result = await deliverOnce({
      delivery: completed,
      replies: [{ status: 429, headers: { 'retry-after': '120' } }],
    });

    expect(result.message?.status).toBe('pending');
    // The header, not the local backoff, which at attempt 1 is one millisecond.
    // Measured from GitHub's answer, so it is never shorter than it asked for.
    const waited = (result.message?.availableAt ?? 0) - result.startedAt;
    expect(waited).toBeGreaterThanOrEqual(120_000);
    expect(waited).toBeLessThan(121_000);
  });

  it('backs off on its own when GitHub gives no wait', async () => {
    const result = await deliverOnce({ delivery: completed, replies: [{ status: 503 }] });

    expect(result.message?.status).toBe('pending');
    const waited = (result.message?.availableAt ?? 0) - result.startedAt;
    expect(waited).toBeGreaterThanOrEqual(config.workerRetryBaseMs);
    expect(waited).toBeLessThan(1_000);
  });

  it.each([404, 410])('gives up on a subject that answered %i', async (status) => {
    const result = await deliverOnce({ delivery: completed, replies: [{ status }] });

    expect(result.message?.status).toBe('failed');
  });

  it('refunds the attempt and latches the credential when GitHub refuses it', async () => {
    const result = await deliverOnce({ delivery: completed, replies: [{ status: 401 }] });

    // Spending the budget against a latched credential is how a committed
    // outcome ends up permanently undelivered.
    expect(result.message).toMatchObject({ status: 'pending', attempts: 0 });
    expect(result.rejected).toBe(true);
  });

  it('claims nothing while the credential is latched', async () => {
    const result = await deliverOnce({
      delivery: completed,
      credentialRejected: true,
      replies: [{ status: 201 }],
    });

    expect(result.worked).toBe(false);
    expect(result.requests).toEqual([]);
    expect(result.message).toMatchObject({ status: 'pending', attempts: 0 });
  });

  it('publishes no note for an outcome the daemon reached on its own', async () => {
    const result = await deliverOnce({
      delivery: { repository: 'edloidas/lictor', subjectNumber: 17, outcome: 'failed' },
      replies: [{ status: 201 }],
    });

    expect(result.bodies[0]).not.toContain("agent's own summary");
    expect(result.bodies[0]).toContain('This did not finish.');
  });

  it('reports no work when nothing is owed', async () => {
    const result = await deliverOnce({ replies: [{ status: 201 }] });

    expect(result.worked).toBe(false);
    expect(result.requests).toEqual([]);
  });

  it('leaves a message alone until its backoff has elapsed', async () => {
    const ConfigLive = Layer.succeed(LictorConfig, config);
    const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const { jobId } = yield* queue.enqueue(work);
          const claimed = yield* queue.claim;
          yield* queue.complete(jobId, claimed?.attempts ?? 0, '{}', completed);
          const first = yield* queue.claimOutbox;
          const now = yield* Clock.currentTimeMillis;
          yield* queue.retryOutbox(first?.id ?? 0, first?.attempts ?? 0, 'later', now + 60_000);
          const early = yield* queue.claimOutbox;
          yield* TestClock.adjust('61 seconds');
          const late = yield* queue.claimOutbox;
          return { early, late };
        }).pipe(Effect.provide(QueueLive), Effect.provide(TestContext.TestContext)),
      ),
    );

    expect(result.early).toBeUndefined();
    expect(result.late).toMatchObject({ status: 'sending', attempts: 2 });
  });
});
