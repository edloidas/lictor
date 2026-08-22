import { FetchHttpClient } from '@effect/platform';
import { BunHttpServer, BunRuntime } from '@effect/platform-bun';
import { Cause, Clock, Effect, Exit, Layer } from 'effect';
import { LictorConfig, legacyStateConflict, port } from './config.ts';
import { ControlPlane, ControlServer } from './control/control-plane.ts';
import { DeliveryWorker } from './delivery-worker.ts';
import { describeCause } from './diagnostics.ts';
import { AgentExecutor } from './executor/agent-executor.ts';
import { ProcessRunner } from './executor/process-runner.ts';
import { CapabilityBroker } from './github/capability-broker.ts';
import { GitHubClient } from './github/client.ts';
import { GitHubCredential } from './github/credential.ts';
import { GitHubIdentity } from './github/identity.ts';
import { NotificationPoller } from './notifications/poller.ts';
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { Server } from './server.ts';
import { Worker } from './worker.ts';
import { RepositoryWorkspace } from './workspace/repository-workspace.ts';

const ConfigLive = LictorConfig.Default;
const PolicyLive = Policy.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
const ExecutorLive = AgentExecutor.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ProcessRunner.Default)),
);
const CredentialLive = GitHubCredential.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
const ClientLive = GitHubClient.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(CredentialLive, FetchHttpClient.layer)),
);
const WorkspaceLive = RepositoryWorkspace.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, ProcessRunner.Default, CredentialLive)),
);
const IdentityLive = GitHubIdentity.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ClientLive)),
);
const BrokerLive = CapabilityBroker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ClientLive, IdentityLive, PolicyLive, QueueLive)),
);
const ControlLive = ControlPlane.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, BrokerLive)),
);
const ControlServerLive = ControlServer.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ControlLive)),
);
const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, ExecutorLive, WorkspaceLive)),
);
const DeliveryWorkerLive = DeliveryWorker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, ClientLive, IdentityLive, PolicyLive, QueueLive)),
);
const PollerLive = NotificationPoller.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, ClientLive, QueueLive, PolicyLive)),
);
const Services = Layer.mergeAll(
  ConfigLive,
  ClientLive,
  IdentityLive,
  PolicyLive,
  QueueLive,
  WorkspaceLive,
  BrokerLive,
  ControlLive,
  ControlServerLive,
  WorkerLive,
  DeliveryWorkerLive,
  PollerLive,
);
/**
 * Runs work that must not end quietly, and stops the daemon if it does.
 *
 * The whole exit is inspected, not just the failure channel. Each case is a
 * distinct outcome and two of them are invisible without this:
 *
 * - a defect bypasses `tapError` and `ignore` alike, so an unobserved dead fiber
 *   looks exactly like a healthy idle one while the health probe keeps answering
 *   ok and nothing is polled, drained, or run;
 * - a loop that *succeeds* has also stopped, and `forever` returning is a bug
 *   whether or not anything failed — which is what `completes` distinguishes,
 *   since one-shot startup work returning is exactly what should happen;
 * - interruption, by contrast, is how a clean shutdown reaches here, and must
 *   not log a fatal error or fire a second `SIGTERM` at a process already going
 *   down.
 */
const supervised = <A, E, R>(
  name: string,
  /** `never` for a loop, where returning at all is the bug. */
  completes: 'never' | 'once',
  work: Effect.Effect<A, E, R>,
) =>
  Effect.exit(work).pipe(
    Effect.flatMap((exit) => {
      if (Exit.isSuccess(exit)) {
        return completes === 'never' ? stop(`The ${name} stopped`) : Effect.void;
      }
      return Cause.isInterruptedOnly(exit.cause)
        ? Effect.void
        : stop(`The ${name} stopped`, exit.cause);
    }),
  );

/**
 * Logs why the daemon is stopping and stops it.
 *
 * The cause is described, not logged. Rendering it walks the nested chain, and
 * for a transport failure that chain contains the outbound request — including
 * the `Authorization` header the client just injected.
 */
const stop = (message: string, cause?: Cause.Cause<unknown>) =>
  Effect.logFatal(message)
    .pipe(Effect.annotateLogs(cause === undefined ? {} : { reason: describeCause(cause) }))
    .pipe(Effect.zipRight(Effect.sync(() => process.kill(process.pid, 'SIGTERM'))));

const Application = Layer.merge(
  Server,
  Layer.scopedDiscard(
    Effect.gen(function* () {
      const identity = yield* GitHubIdentity;
      const queue = yield* WorkQueue;
      const policy = yield* Policy;
      const control = yield* ControlServer;
      yield* Effect.logInfo('Local control socket ready').pipe(
        Effect.annotateLogs({ path: control.path }),
      );
      const counts = yield* queue.counts;
      yield* Effect.logInfo('Work queue ready').pipe(Effect.annotateLogs(counts));
      const worker = yield* Worker;
      const deliveryWorker = yield* DeliveryWorker;
      const poller = yield* NotificationPoller;
      // ! Forked, not awaited: the health socket must bind and daemon ownership
      // ! must renew whether or not GitHub is answering. But the worker starts *inside* this
      // ! fiber, after the account is confirmed — it clones and pushes with the
      // ! token, and doing that before `GET /user` agrees on who owns it is the
      // ! exact misattribution the check exists to prevent. Nothing is polled in
      // ! the meantime and jobs stay pending, which costs nothing.
      yield* Effect.forkScoped(
        identity.verified.pipe(
          Effect.tap((verified) =>
            Effect.logInfo('Authenticated as a GitHub account').pipe(
              Effect.annotateLogs({
                login: verified.login,
                expires: verified.tokenExpiresAt === undefined ? 'never' : verified.tokenExpiresAt,
              }),
            ),
          ),
          // ! All three loops behind it, for the same reason: qualification awaits
          // ! this same verdict, so a delivery it cannot qualify would be reclaimed
          // ! and retried forever for a daemon-side problem.
          // !
          // ! Forked separately rather than through one `Effect.all`, whose
          // ! fail-fast semantics make a defect in either loop interrupt the
          // ! other. `Worker.run` recovers typed failures only, so a thrown
          // ! exception escaping any `Effect.try` would take delivery processing
          // ! down with it — and a `Die` cause bypasses `tapError`, leaving the
          // ! daemon binding the socket and renewing its lease while nothing polls
          // ! GitHub or drains the inbox, silently and indefinitely.
          Effect.zipRight(
            Effect.all([
              Effect.forkScoped(supervised('worker', 'never', worker.run)),
              Effect.forkScoped(supervised('delivery worker', 'never', deliveryWorker.run)),
              // ! Behind the same verdict, and for a sharper reason than the
              // ! other two: the poller marks threads read, which is destructive
              // ! and irreversible. Doing that with a credential `GET /user` has
              // ! not yet agreed on is how a wrong token silently empties the
              // ! wrong account's inbox.
              Effect.forkScoped(supervised('notification poller', 'never', poller.run)),
            ]),
          ),
          // ! Supervised like the loops it starts, and for the same reason: a
          // ! credential that can never work is not survivable, and a *defect*
          // ! while verifying would otherwise kill this fiber before any loop is
          // ! forked, leaving a daemon that answers its health probe and does
          // ! nothing else.
          (verification) => supervised('credential verification', 'once', verification),
        ),
      );
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep('10 seconds');
            yield* queue.heartbeatDaemon;
            yield* queue.recoverStale(yield* Clock.currentTimeMillis);
          }),
        ).pipe(
          Effect.tapError((cause) => stop('Daemon ownership heartbeat failed', Cause.fail(cause))),
        ),
      );
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep('1 hour');
            const now = yield* Clock.currentTimeMillis;
            yield* queue.maintenance(
              now - policy.completedRetentionDays * 86_400_000,
              now - policy.failedRetentionDays * 86_400_000,
            );
          }),
        ),
      );
    }),
  ),
).pipe(Layer.provide(Services));

const Main = Layer.unwrapEffect(
  Effect.map(port, (bound) => Layer.provide(Application, BunHttpServer.layer({ port: bound }))),
);

/**
 * Runs before anything opens a file, because `WorkQueue` creates the database it
 * cannot find and there is no second chance to notice the old one afterwards.
 */
const guardLegacyState = Effect.flatMap(LictorConfig, (config) => {
  const conflict = legacyStateConflict(config);
  return conflict === undefined
    ? Effect.void
    : Effect.logFatal(conflict).pipe(Effect.zipRight(Effect.fail(new Error(conflict))));
}).pipe(Effect.provide(ConfigLive));

BunRuntime.runMain(Effect.zipRight(guardLegacyState, Layer.launch(Main)));
