import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Logger, Redacted } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { DeliveryWorker } from '../src/delivery-worker.ts';
import { AgentExecutor } from '../src/executor/agent-executor.ts';
import { GitHubClient } from '../src/github/client.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { GitHubIdentity } from '../src/github/identity.ts';
import { OutboxWorker } from '../src/outbox-worker.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';
import { Worker } from '../src/worker.ts';
import { RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const asking = (number: number): WorkItem => ({
  deliveryId: `asking-${number}`,
  interactionId: `interaction-${number}`,
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['mentioned'],
  subject: {
    kind: 'issue',
    number,
    title: 'Survive a restart',
    url: `https://github.com/edloidas/lictor/issues/${number}`,
  },
});

type Execution = {
  readonly jobId: number | undefined;
  readonly attempt: number | undefined;
  readonly answerUrl: string | undefined;
};

/**
 * One daemon lifetime over the database at `path`. Every call builds its own
 * layers, so the second lifetime shares nothing with the first but the file.
 *
 * The agent on issue 17 publishes its question and asks it until it has an
 * answer; every other job completes. `answeredAt` is the time of the reply the
 * stubbed thread carries, when it carries one.
 */
const lifetime = (
  path: string,
  executions: Execution[],
  reactions: string[],
  answeredAt?: number,
) => {
  const ConfigLive = Layer.succeed(
    LictorConfig,
    LictorConfig.make({
      githubToken: Redacted.make('test-token'),
      expectedLogin: 'adiutriel',
      trustedSenders: ['edloidas'],
      autoAcceptInviters: [],
      databasePath: path,
      stateDir: stateDirOf(path),
      policyPath: 'policy.toml',
      controlSocketPath: '/tmp/lictor-restart.sock',
      deliveryMaxBytes: 1024 * 1024,
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
    }),
  );
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const ExecutorLive = Layer.effect(
    AgentExecutor,
    Effect.map(WorkQueue, (queue) =>
      AgentExecutor.make({
        enabled: true,
        execute: (work, _workdir, _timeoutMs, jobId, attempt) =>
          Effect.gen(function* () {
            executions.push({ jobId, attempt, answerUrl: work.answerUrl });
            if (work.subject.number !== 17 || work.answerUrl !== undefined) {
              return { status: 'completed' as const, summary: 'done' };
            }
            if (jobId === undefined) return yield* Effect.dieMessage('executed without a job id');
            yield* queue.recordAudit({
              jobId,
              repository: work.repository,
              capability: 'create_comment',
              input: JSON.stringify({ number: 17, body: 'which branch?' }),
              outcome: 'ok',
            });
            return { status: 'needs_input' as const, summary: 'which branch?' };
          }).pipe(Effect.orDie),
      }),
    ),
  ).pipe(Layer.provide(QueueLive));
  const PolicyLive = Layer.effect(
    Policy,
    parsePolicy(
      '[defaults]\nexecution = "automatic"\n[repositories]\nallow = ["edloidas/lictor"]',
      ['edloidas'],
    ).pipe(Effect.map(Policy.make)),
  );
  const WorkspaceLive = Layer.succeed(
    RepositoryWorkspace,
    RepositoryWorkspace.make({
      acquire: () => Effect.succeed({ path: stateDirOf(path) }),
      release: () => Effect.void,
      sweep: () => Effect.void,
      collectArtifacts: () => Effect.succeed([]),
    }),
  );

  const at = answeredAt === undefined ? undefined : new Date(answeredAt).toISOString();
  const routes: readonly (readonly [string, unknown])[] = [
    [
      '/issues/17/comments',
      at === undefined
        ? []
        : [
            {
              id: 100,
              node_id: 'IC_100',
              html_url: 'https://github.com/edloidas/lictor/issues/17#issuecomment-100',
              body: 'main',
              user: { login: 'edloidas' },
              created_at: at,
              updated_at: at,
            },
          ],
    ],
    ['/pulls/17/comments', []],
    [
      '/issues/17',
      {
        title: 'Survive a restart',
        html_url: 'https://github.com/edloidas/lictor/issues/17',
        body: 'no mention in the body',
        user: { login: 'edloidas' },
        created_at: '2026-08-20T08:00:00Z',
        updated_at: '2026-08-20T08:00:00Z',
      },
    ],
  ];
  const client = HttpClient.make((request) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(
          JSON.stringify(routes.find(([fragment]) => request.url.includes(fragment))?.[1] ?? {}),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      ),
    ),
  );
  const GitHubLive = Layer.succeed(
    GitHubClient,
    GitHubClient.make({
      authenticated: Effect.succeed(
        client.pipe(HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test'))),
      ),
      addReaction: () => Effect.succeed(undefined),
      reconcileReaction: (repository, target, content) =>
        Effect.sync(() => {
          reactions.push(`${repository}:${JSON.stringify(target)}:${content}`);
        }),
    }),
  );
  const IdentityLive = Layer.succeed(
    GitHubIdentity,
    GitHubIdentity.make({
      verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
    }),
  );

  const Services = Layer.mergeAll(
    ConfigLive,
    QueueLive,
    ExecutorLive,
    PolicyLive,
    WorkspaceLive,
    GitHubLive,
    IdentityLive,
    CredentialHealth.Default,
  );
  return Layer.mergeAll(
    Services,
    Worker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
    DeliveryWorker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
    OutboxWorker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
  );
};

type Lifetime = ReturnType<typeof lifetime>;

const live = <A, E>(effect: Effect.Effect<A, E, Layer.Layer.Success<Lifetime>>, layer: Lifetime) =>
  Effect.runPromise(
    Effect.scoped(
      effect.pipe(Effect.provide(layer), Effect.provide(Logger.remove(Logger.defaultLogger))),
    ),
  );

describe('restart over a reopened database', () => {
  // Every other suite runs against a database that never closes, so a column
  // read from memory, a write left uncommitted at shutdown, or a migration that
  // drops a parked row passes there. Only reopening the file shows the state
  // lives where the restart claim says it does.
  it('keeps a parked question answerable and an undelivered outcome owed', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-restart-'));
    const path = join(directory, 'queue.sqlite');
    const executions: Execution[] = [];
    const reactions: string[] = [];

    try {
      const before = await live(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const worker = yield* Worker;
          const parked = yield* queue.enqueue(asking(17));
          const finished = yield* queue.enqueue(asking(18));
          yield* worker.runOnce;
          yield* worker.runOnce;
          return {
            parkedId: parked.jobId,
            finishedId: finished.jobId,
            parked: yield* queue.job(parked.jobId),
            owed: yield* queue.outboxFor(finished.jobId),
          };
        }),
        lifetime(path, executions, reactions),
      );

      expect(before.parked?.status).toBe('pending');
      expect(before.parked?.attempts).toBe(1);
      expect(before.owed.map((message) => message.status)).toEqual(['pending']);

      const reopened = await live(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          return {
            job: yield* queue.job(before.parkedId),
            question: yield* queue.pendingQuestion('edloidas/lictor', 'issue', 17),
            audit: yield* queue.auditFor(before.parkedId, 10),
            attempts: yield* queue.attemptsFor(before.parkedId),
          };
        }),
        lifetime(path, executions, reactions),
      );

      const published = reopened.audit.entries.find(
        (entry) => entry.capability === 'create_comment',
      );
      expect(reopened.question?.jobId).toBe(before.parkedId);
      expect(reopened.question?.questionId).toBe(before.parked?.questionId ?? 'none');
      expect(reopened.question?.askedAt).toBe(published?.createdAt ?? Number.NaN);
      // Waiting spent nothing: the startup sweep did not count the parked row
      // as interrupted work.
      expect(reopened.job?.status).toBe('pending');
      expect(reopened.job?.attempts).toBe(1);
      expect(reopened.attempts.map((attempt) => attempt.status)).toEqual(['failed']);

      // Strictly after the question, as a reply on the thread must be. Derived
      // from the stored instant rather than the wall clock, so it cannot tie.
      const answeredAt = (reopened.question?.askedAt ?? 0) + 1000;
      const after = await live(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const outbox = yield* OutboxWorker;
          const sent = yield* outbox.runOnce;
          const sentAgain = yield* outbox.runOnce;
          const owed = yield* queue.outboxFor(before.finishedId);

          yield* queue.receiveDelivery({
            id: 'answer-delivery',
            event: 'notification',
            source: 'notification',
            body: JSON.stringify({
              id: '14567',
              unread: true,
              reason: 'mention',
              updated_at: new Date(answeredAt).toISOString(),
              subject: {
                title: 'Survive a restart',
                url: 'https://api.github.com/repos/edloidas/lictor/issues/17',
                latest_comment_url:
                  'https://api.github.com/repos/edloidas/lictor/issues/comments/100',
                type: 'Issue',
              },
              repository: { full_name: 'edloidas/lictor' },
            }),
          });
          yield* (yield* DeliveryWorker).runOnce;
          const answered = yield* queue.job(before.parkedId);
          const jobs = yield* queue.listJobs(10);
          const delivery = yield* queue.inspectDelivery('answer-delivery');

          yield* (yield* Worker).runOnce;
          return {
            sent,
            sentAgain,
            owed,
            answered,
            jobs,
            delivery,
            resumed: yield* queue.job(before.parkedId),
          };
        }),
        lifetime(path, executions, reactions, answeredAt),
      );

      expect(after.sent).toBe(true);
      expect(after.sentAgain).toBe(false);
      expect(after.owed.map((message) => message.status)).toEqual(['delivered']);
      expect(reactions).toEqual(['edloidas/lictor:{"kind":"body","number":18}:rocket']);

      expect(after.delivery?.decision).toEqual({
        decision: 'answered',
        answered: { jobId: before.parkedId, resumed: true },
      });
      expect(after.jobs).toHaveLength(2);
      expect(after.answered?.questionId).toBeUndefined();
      expect(after.answered?.attempts).toBe(1);
      expect(after.answered?.work.answerUrl).toBe(
        'https://github.com/edloidas/lictor/issues/17#issuecomment-100',
      );

      expect(executions).toEqual([
        { jobId: before.parkedId, attempt: 1, answerUrl: undefined },
        { jobId: before.finishedId, attempt: 1, answerUrl: undefined },
        {
          jobId: before.parkedId,
          attempt: 2,
          answerUrl: 'https://github.com/edloidas/lictor/issues/17#issuecomment-100',
        },
      ]);
      expect(after.resumed?.status).toBe('completed');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
