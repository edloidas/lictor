import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientResponse } from '@effect/platform';
import { Clock, Effect, Layer, Logger, Redacted, Runtime, TestClock, TestContext } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { GitHubIdentity } from '../src/github/identity.ts';
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
  executorResultBytes: 1024,
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

const ACCEPTED_POLICY = `[defaults]
execution = "automatic"

[defaults.capabilities]
read = true
comment = true

[repositories]
allow = ["edloidas/lictor"]
`;

/** Commenting withheld, admission intact: a reaction must still land. */
const NO_COMMENT_POLICY = `[defaults]
execution = "automatic"

[defaults.capabilities]
read = true

[repositories]
allow = ["edloidas/lictor"]
`;

const DENIED_POLICY = `[defaults]
execution = "automatic"

[defaults.capabilities]
read = true

[repositories]
allow = ["edloidas/lictor"]
deny = ["edloidas/lictor"]
`;

const SUBJECT_URL = 'https://api.github.com/repos/edloidas/lictor/issues/17/reactions';
const COMMENT_URL = 'https://api.github.com/repos/edloidas/lictor/issues/comments/99/reactions';

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
 * identity is stubbed instead: `verified` would otherwise spend a request of its
 * own, and the login it yields is all the reconciliation reads.
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
  // Runs from the policy read, which is the last thing the worker does before
  // re-reading its claim — the window an operator retry actually lands in.
  let supersede: () => void = () => undefined;
  const stub = HttpClient.make((request) => {
    requests.push(`${request.method} ${request.url}`);
    const payload = request.body as { readonly body?: unknown };
    bodies.push(payload.body instanceof Uint8Array ? new TextDecoder().decode(payload.body) : '');
    const reply = options.replies[Math.min(call, options.replies.length - 1)];
    call += 1;
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(JSON.stringify(reply?.body ?? {}), {
          status: reply?.status ?? 201,
          headers: { 'content-type': 'application/json', ...reply?.headers },
        }),
      ),
    );
  });

  const ConfigLive = Layer.succeed(LictorConfig, config);
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const HealthLive = CredentialHealth.Default;
  const IdentityLive = Layer.succeed(
    GitHubIdentity,
    GitHubIdentity.make({
      verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
    }),
  );
  const PolicyLive = Layer.effect(
    Policy,
    parsePolicy(options.policy ?? ACCEPTED_POLICY, ['edloidas']).pipe(
      Effect.map((parsed) => {
        const made = Policy.make(parsed);
        return options.supersedeBeforeSend === true
          ? Policy.make({
              ...parsed,
              forRepository: (repository: string) => {
                supersede();
                return made.forRepository(repository);
              },
            })
          : made;
      }),
    ),
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
    Layer.provide(
      Layer.mergeAll(ConfigLive, QueueLive, ClientLive, PolicyLive, HealthLive, IdentityLive),
    ),
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
        }
        const runtime = yield* Effect.runtime<never>();
        supersede = () => {
          Runtime.runSync(runtime)(Effect.ignore(queue.retry(1)));
        };
        const worker = yield* OutboxWorker;
        const startedAt = yield* Clock.currentTimeMillis;
        const worked = yield* worker.runOnce;
        const [message] = yield* queue.outboxFor(1);
        const audit = yield* queue.auditLog(1);
        return {
          worked,
          message,
          startedAt,
          requests,
          bodies,
          audit,
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

describe('OutboxWorker', () => {
  it('resolves the acknowledgement to the outcome reaction', async () => {
    const result = await deliverOnce({
      delivery: completed,
      // The rocket, then the eyes answered as already present, then its removal.
      replies: [
        { status: 201, body: { id: 1 } },
        { status: 200, body: { id: 7 } },
        { status: 204 },
      ],
    });

    expect(result.worked).toBe(true);
    expect(result.requests).toEqual([
      `POST ${SUBJECT_URL}`,
      `POST ${SUBJECT_URL}`,
      `DELETE ${SUBJECT_URL}/7`,
    ]);
    expect(result.bodies.slice(0, 2)).toEqual([
      JSON.stringify({ content: 'rocket' }),
      JSON.stringify({ content: 'eyes' }),
    ]);
    expect(result.message?.status).toBe('delivered');
  });

  it('leaves a hand-placed reaction of the operator alone', async () => {
    // The daemon authenticates as the operator's own account, so a listing
    // filtered to "this account" cannot tell the two apart. Posting only the
    // contents the daemon itself uses never names anything else.
    const result = await deliverOnce({
      delivery: completed,
      replies: [
        { status: 201, body: { id: 1 } },
        { status: 200, body: { id: 7 } },
        { status: 204 },
      ],
    });

    expect(result.bodies.every((body) => !body.includes('+1'))).toBe(true);
    expect(result.requests.filter((request) => request.startsWith('DELETE'))).toEqual([
      `DELETE ${SUBJECT_URL}/7`,
    ]);
  });

  // Every arm, and the content itself: driving only `completed` leaves the
  // other six free to be deleted from the table with the suite still green.
  it.each([
    ['completed', 'rocket'],
    ['canceled', 'confused'],
    ['clipped', 'confused'],
    ['expired', 'confused'],
    ['failed', 'confused'],
    ['rejected', 'confused'],
    ['unanswered', 'confused'],
  ])('resolves %s to %s', async (outcome, content) => {
    const result = await deliverOnce({
      delivery: { repository: 'edloidas/lictor', subjectNumber: 17, outcome } as OutcomeDelivery,
      replies: [{ status: 201 }, { status: 200, body: [] }],
    });

    expect(result.requests[0]).toBe(`POST ${SUBJECT_URL}`);
    expect(result.bodies[0]).toBe(JSON.stringify({ content }));
    expect(result.message?.status).toBe('delivered');
  });

  it('reacts on the triggering comment when the row carries one', async () => {
    const result = await deliverOnce({
      delivery: { ...completed, context: { kind: 'issue_comment', id: 99 } },
      replies: [{ status: 201 }, { status: 200, body: [] }],
    });

    expect(result.requests[0]).toBe(`POST ${COMMENT_URL}`);
    expect(result.message?.status).toBe('delivered');
  });

  it('leaves another account’s reactions alone', async () => {
    const result = await deliverOnce({
      delivery: completed,
      replies: [
        { status: 201 },
        { status: 200, body: [{ id: 8, content: 'eyes', user: { login: 'edloidas' } }] },
      ],
    });

    expect(result.requests.some((request) => request.startsWith('DELETE'))).toBe(false);
    expect(result.message?.status).toBe('delivered');
  });

  it('clears a previous attempt\u2019s reaction as well as the acknowledgement', async () => {
    // What a retried job leaves: an earlier attempt's terminal reaction still
    // standing beside the acknowledgement. Probed only past the first attempt,
    // so an ordinary delivery never posts a wrong outcome to look for one.
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [
        { status: 201, body: { id: 1 } },
        { status: 200, body: { id: 7 } },
        { status: 204 },
        { status: 200, body: { id: 9 } },
        { status: 204 },
      ],
    });

    expect(result.bodies.filter((body) => body !== '')).toEqual([
      JSON.stringify({ content: 'rocket' }),
      JSON.stringify({ content: 'eyes' }),
      JSON.stringify({ content: 'confused' }),
    ]);
    expect(result.requests).toContain(`DELETE ${SUBJECT_URL}/7`);
    expect(result.requests).toContain(`DELETE ${SUBJECT_URL}/9`);
  });

  it('treats a reaction already gone as converged', async () => {
    const result = await deliverOnce({
      delivery: completed,
      replies: [
        { status: 201, body: { id: 1 } },
        { status: 200, body: { id: 7 } },
        { status: 404 },
      ],
    });

    expect(result.message?.status).toBe('delivered');
  });

  it('records the reaction in the capability audit', async () => {
    const result = await deliverOnce({
      delivery: completed,
      replies: [{ status: 201 }, { status: 200, body: [] }],
    });

    expect(result.audit).toContainEqual(
      expect.objectContaining({ capability: 'react', outcome: 'ok', actor: 'daemon' }),
    );
  });

  it('signals an outcome on a repository that withholds commenting', async () => {
    const result = await deliverOnce({
      delivery: completed,
      policy: NO_COMMENT_POLICY,
      replies: [{ status: 201 }, { status: 200, body: [] }],
    });

    expect(result.requests[0]).toBe(`POST ${SUBJECT_URL}`);
    expect(result.message?.status).toBe('delivered');
  });

  it('records the outcome locally and reacts nothing when the repository is denied', async () => {
    const result = await deliverOnce({
      delivery: completed,
      policy: DENIED_POLICY,
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

  it('leaves the acknowledgement in place for an outcome with nothing to signal', async () => {
    const result = await deliverOnce({
      delivery: { repository: 'edloidas/lictor', subjectNumber: 17, outcome: 'needs_input' },
      replies: [{ status: 201 }],
    });

    expect(result.requests).toEqual([]);
    expect(result.message?.status).toBe('delivered');
  });

  it('does not react for a message the operator superseded while it was sending', async () => {
    const result = await deliverOnce({
      delivery: completed,
      priorAttempts: 1,
      replies: [{ status: 201 }],
      supersedeBeforeSend: true,
    });

    expect(result.requests).toEqual([]);
    expect(result.message?.status).toBe('canceled');
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

  it.each([404, 410])('gives up on a target that answered %i', async (status) => {
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
