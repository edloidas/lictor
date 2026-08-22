import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Logger, Redacted, Ref, Schedule } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { DeliveryWorker } from '../src/delivery-worker.ts';
import { AgentExecutor } from '../src/executor/agent-executor.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubIdentity } from '../src/github/identity.ts';
import { NotificationPoller } from '../src/notifications/poller.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import { Worker } from '../src/worker.ts';
import { RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    databasePath: ':memory:',
    policyPath: 'policy.toml',
    controlSocketPath: '/tmp/lictor.sock',
    deliveryMaxBytes: 1024 * 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    workerPollMs: 1,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 10,
    notificationPollMs: 60_000,
  }),
);

const thread = {
  id: '14567',
  unread: true,
  reason: 'mention',
  updated_at: '2026-08-21T10:00:00Z',
  last_read_at: null,
  subject: {
    title: 'Exercise the pipeline',
    url: 'https://api.github.com/repos/edloidas/lictor/issues/17',
    latest_comment_url: 'https://api.github.com/repos/edloidas/lictor/issues/comments/99',
    type: 'Issue',
  },
  repository: { full_name: 'edloidas/lictor' },
};

const issue = {
  title: 'Exercise the pipeline',
  html_url: 'https://github.com/edloidas/lictor/issues/17',
  body: 'nothing in the body',
  user: { login: 'edloidas' },
  created_at: '2026-08-20T08:00:00Z',
  updated_at: '2026-08-20T08:00:00Z',
};

const comments = [
  {
    id: 99,
    html_url: 'https://github.com/edloidas/lictor/issues/17#issuecomment-99',
    body: 'hey @adiutriel, run the pipeline',
    user: { login: 'edloidas' },
    created_at: '2026-08-21T10:00:00Z',
    updated_at: '2026-08-21T10:00:00Z',
  },
];

// ! `setUrlParams` is serialized at execution time, so `request.url` carries no
// ! query string — matching on one silently routes every list call to the
// ! fallthrough.
const isList = (url: string): boolean =>
  url.includes('/notifications') && !url.includes('/notifications/threads');

/**
 * The first notification sweep returns the thread and every later one returns
 * nothing. A stub that kept returning it would let the test pass while the
 * poller re-stored the same delivery on every cycle.
 */
const replyFor = (url: string, listCalls: number): unknown => {
  if (isList(url)) return listCalls === 1 ? [thread] : [];
  if (url.includes('/issues/17/comments')) return comments;
  if (url.includes('/pulls/17/comments')) return [];
  if (url.includes('/issues/17')) return issue;
  return {};
};

describe('notification-to-agent pipeline', () => {
  it('polls, marks read after committing, acknowledges once, executes, and completes', async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const executions = yield* Ref.make(0);
          const calls = yield* Ref.make<string[]>([]);
          const reactions = yield* Ref.make<string[]>([]);

          const client = HttpClient.make((request) =>
            Ref.updateAndGet(calls, (items) => [...items, `${request.method} ${request.url}`]).pipe(
              Effect.map((seen) =>
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify(
                      replyFor(request.url, seen.filter((call) => isList(call)).length),
                    ),
                    {
                      status: 200,
                      headers: {
                        'content-type': 'application/json',
                        'x-poll-interval': '60',
                        'last-modified': 'Thu, 21 Aug 2026 10:00:00 GMT',
                      },
                    },
                  ),
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
                Ref.update(reactions, (items) => [
                  ...items,
                  `${repository}:${JSON.stringify(target)}`,
                ]).pipe(Effect.as(undefined)),
            }),
          );
          const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
          const IdentityLive = Layer.succeed(
            GitHubIdentity,
            GitHubIdentity.make({
              verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
            }),
          );
          const ExecutorLive = Layer.succeed(
            AgentExecutor,
            AgentExecutor.make({
              enabled: true,
              execute: () =>
                Ref.update(executions, (count) => count + 1).pipe(
                  Effect.as({ status: 'completed' as const, summary: 'agent completed' }),
                ),
            }),
          );
          const PolicyLive = Layer.effect(
            Policy,
            parsePolicy(
              '[defaults]\nexecution = "automatic"\n[repositories]\nallow = ["edloidas/lictor"]',
            ).pipe(Effect.map(Policy.make)),
          );
          const WorkspaceLive = Layer.succeed(
            RepositoryWorkspace,
            RepositoryWorkspace.make({
              create: (_jobId, work) =>
                Effect.succeed({
                  repository: work.repository,
                  clonePath: process.cwd(),
                  worktreePath: process.cwd(),
                }),
              cleanup: () => Effect.void,
              withRepositoryLock: <A, E, R>(_repository: string, effect: Effect.Effect<A, E, R>) =>
                effect,
            }),
          );
          const Services = Layer.mergeAll(
            ConfigLive,
            QueueLive,
            GitHubLive,
            IdentityLive,
            PolicyLive,
            ExecutorLive,
            WorkspaceLive,
          );
          const Runtime = Layer.mergeAll(
            Services,
            Worker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
            DeliveryWorker.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
            NotificationPoller.DefaultWithoutDependencies.pipe(Layer.provide(Services)),
          );

          return yield* Effect.gen(function* () {
            const poller = yield* NotificationPoller;
            const deliveryWorker = yield* DeliveryWorker;
            const worker = yield* Worker;
            const queue = yield* WorkQueue;

            // ! Two sweeps, not one. A replayed notification must produce no
            // ! second job and no second reaction, and one sweep cannot show that.
            const first = yield* poller.pollOnce;
            yield* poller.pollOnce;
            yield* deliveryWorker.drain;
            yield* Effect.forkScoped(worker.run);

            const counts = yield* queue.counts.pipe(
              Effect.filterOrFail(
                (current) => current.completed === 1,
                () => new Error('work not completed yet'),
              ),
              Effect.retry(
                Schedule.spaced('5 millis').pipe(Schedule.intersect(Schedule.recurs(200))),
              ),
            );

            return {
              first,
              counts,
              executions: yield* Ref.get(executions),
              reactions: yield* Ref.get(reactions),
              calls: yield* Ref.get(calls),
              deliveryStatus: yield* queue.deliveryStatus(
                'notification:14567:2026-08-21T10:00:00Z',
              ),
            };
          }).pipe(Effect.provide(Runtime), Effect.provide(Logger.remove(Logger.defaultLogger)));
        }),
      ),
    );

    expect(result.first.stored).toBe(1);
    expect(result.deliveryStatus).toBe('completed');
    expect(result.counts.completed).toBe(1);
    expect(result.counts.pending).toBe(0);
    expect(result.executions).toBe(1);
    expect(result.reactions).toEqual(['edloidas/lictor:{"kind":"issue_comment","id":99}']);
    // ! Marked read exactly once, and only after the row was committed.
    expect(
      result.calls.filter((call) => call.startsWith('PATCH') && call.includes('/threads/14567')),
    ).toHaveLength(1);
  });
});
