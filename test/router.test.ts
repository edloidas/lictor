import { describe, expect, it } from 'bun:test';
import { Effect, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { Delivery } from '../src/webhook/event.ts';
import { dispatch, type Registry } from '../src/webhook/router.ts';

/** Handlers carry `GitHubClient` in their requirements; the router never calls it. */
const stubClient = GitHubClient.make({
  forInstallation: () => Effect.die('the router must not reach GitHub'),
});
const stubPolicy = Policy.make(Effect.runSync(parsePolicy('')));

const delivery = (event: string, action?: string): Delivery => ({
  event,
  id: 'd-1',
  payload: action === undefined ? {} : { action },
  raw: action === undefined ? {} : { action },
});

/** Runs a dispatch and reports which registry keys fired, in order. */
const run = (registry: (log: Ref.Ref<string[]>) => Registry, received: Delivery) =>
  Effect.runSync(
    Effect.gen(function* () {
      const log = yield* Ref.make<string[]>([]);
      yield* dispatch(registry(log))(received);
      return yield* Ref.get(log);
    }).pipe(
      Effect.provideService(GitHubClient, stubClient),
      Effect.provideService(Policy, stubPolicy),
      Effect.provideService(
        LictorConfig,
        LictorConfig.make({
          appId: '1',
          privateKey: Redacted.make('unused'),
          webhookSecret: Redacted.make('unused'),
          trustedSenders: [],
          targetUsers: [],
          databasePath: ':memory:',
          policyPath: 'policy.toml',
          controlSocketPath: '/tmp/lictor.sock',
          webhookMaxBytes: 1024,
          executor: 'disabled',
          codexModel: 'gpt-5.6-luna',
          agentWorkdir: '.',
          executorTimeoutMs: 1000,
          executorOutputBytes: 1024,
          workerPollMs: 10,
          workerMaxAttempts: 3,
          workerRetryBaseMs: 100,
        }),
      ),
      Effect.provideService(
        WorkQueue,
        WorkQueue.make({
          receiveDelivery: () => Effect.die('the router must not receive deliveries'),
          claimDelivery: Effect.die('the router must not claim deliveries'),
          finishDelivery: () => Effect.die('the router must not finish deliveries'),
          retryDelivery: () => Effect.die('the router must not retry deliveries'),
          deliveryStatus: () => Effect.die('the router must not inspect deliveries'),
          enqueue: () => Effect.die('the router must not reach the queue'),
          claim: Effect.die('the router must not reach the queue'),
          claimFor: () => Effect.die('the router must not reach the queue'),
          heartbeat: () => Effect.die('the router must not reach the queue'),
          heartbeatDaemon: Effect.die('the router must not reach the queue'),
          complete: () => Effect.die('the router must not reach the queue'),
          fail: () => Effect.die('the router must not reach the queue'),
          recoverStale: () => Effect.die('the router must not reach the queue'),
          counts: Effect.die('the router must not reach the queue'),
          maintenance: () => Effect.die('the router must not reach the queue'),
          recordAudit: () => Effect.die('the router must not reach the queue'),
          auditLog: () => Effect.die('the router must not reach the queue'),
          listJobs: () => Effect.die('the router must not reach the queue'),
          job: () => Effect.die('the router must not reach the queue'),
          approve: () => Effect.die('the router must not reach the queue'),
          retry: () => Effect.die('the router must not reach the queue'),
          cancel: () => Effect.die('the router must not reach the queue'),
          diagnostics: Effect.die('the router must not reach the queue'),
          ownerId: '00000000-0000-0000-0000-000000000000',
        }),
      ),
    ),
  );

const record = (log: Ref.Ref<string[]>, label: string) => () =>
  Ref.update(log, (entries) => [...entries, label]);

describe('dispatch', () => {
  it('runs the handler registered for the bare event', () => {
    expect(run((log) => ({ ping: record(log, 'ping') }), delivery('ping'))).toEqual(['ping']);
  });

  it('prefers the event.action key over the bare event', () => {
    const registry = (log: Ref.Ref<string[]>) => ({
      issues: record(log, 'issues'),
      'issues.opened': record(log, 'issues.opened'),
    });

    expect(run(registry, delivery('issues', 'opened'))).toEqual(['issues.opened']);
  });

  it('falls back to the bare event when the action is not registered', () => {
    const registry = (log: Ref.Ref<string[]>) => ({ issues: record(log, 'issues') });

    expect(run(registry, delivery('issues', 'labeled'))).toEqual(['issues']);
  });

  it('drops an unregistered event without failing', () => {
    expect(run((log) => ({ ping: record(log, 'ping') }), delivery('push'))).toEqual([]);
  });

  it('runs nothing when the registry is empty', () => {
    expect(run(() => ({}), delivery('ping'))).toEqual([]);
  });
});
