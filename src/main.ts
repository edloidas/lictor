import { FetchHttpClient } from '@effect/platform';
import { BunHttpServer, BunRuntime } from '@effect/platform-bun';
import { Cause, Clock, Effect, Exit, Layer } from 'effect';
import { LictorConfig, legacyStateConflict, port } from './config.ts';
import { ControlPlane, ControlServer } from './control/control-plane.ts';
import { credentialExpiryWatch, maintenanceLoop } from './daemon-tick.ts';
import { DeliveryWorker } from './delivery-worker.ts';
import { describeCause, failureOperation } from './diagnostics.ts';
import { AgentExecutor } from './executor/agent-executor.ts';
import { ProcessRunner } from './executor/process-runner.ts';
import { CapabilityBroker } from './github/capability-broker.ts';
import { GitHubClient } from './github/client.ts';
import { GitHubCredential } from './github/credential.ts';
import { CredentialHealth } from './github/credential-health.ts';
import { GitHubIdentity } from './github/identity.ts';
import { NotificationPoller } from './notifications/poller.ts';
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { Server } from './server.ts';
import { Worker } from './worker.ts';
import { DiskStat, RepositoryWorkspace } from './workspace/repository-workspace.ts';

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
  Layer.provide(
    Layer.mergeAll(ConfigLive, ProcessRunner.Default, DiskStat.Default, CredentialLive, QueueLive),
  ),
);
const IdentityLive = GitHubIdentity.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ClientLive)),
);
const BrokerLive = CapabilityBroker.DefaultWithoutDependencies.pipe(
  Layer.provide(
    Layer.mergeAll(ClientLive, IdentityLive, PolicyLive, QueueLive, CredentialHealth.Default),
  ),
);
const ControlLive = ControlPlane.DefaultWithoutDependencies.pipe(
  Layer.provide(
    Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, BrokerLive, CredentialHealth.Default),
  ),
);
const ControlServerLive = ControlServer.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ControlLive)),
);
const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
  Layer.provide(
    Layer.mergeAll(
      ConfigLive,
      PolicyLive,
      QueueLive,
      ExecutorLive,
      WorkspaceLive,
      CredentialHealth.Default,
    ),
  ),
);
const DeliveryWorkerLive = DeliveryWorker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, ClientLive, IdentityLive, PolicyLive, QueueLive)),
);
const PollerLive = NotificationPoller.DefaultWithoutDependencies.pipe(
  Layer.provide(
    Layer.mergeAll(ConfigLive, ClientLive, QueueLive, PolicyLive, CredentialHealth.Default),
  ),
);
const Services = Layer.mergeAll(
  ConfigLive,
  ClientLive,
  IdentityLive,
  PolicyLive,
  QueueLive,
  WorkspaceLive,
  CredentialHealth.Default,
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
 * The whole exit is inspected: a defect bypasses `tapError` and `ignore` alike,
 * so a dead fiber looks exactly like a healthy idle one; `completes: 'never'`
 * treats even a *successful* return as the bug it is for a loop; interruption,
 * by contrast, is clean shutdown and must not fire a second `SIGTERM`.
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
 * The cause is described, not rendered: rendering walks the nested chain, which
 * for a transport failure contains the outbound request — including the
 * `Authorization` header.
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
      const workspaces = yield* RepositoryWorkspace;
      /**
       * Collects sessions whose jobs are gone.
       *
       * Liveness resolves after the listing, exhaustively and uncapped:
       * under-reporting it deletes a session executing right now, while
       * over-reporting only delays a deletion by one pass. A failed sweep costs
       * disk; killing the loop costs everything — logged, never fatal.
       */
      const sweepSessions = workspaces.sweep(queue.liveJobIds).pipe(
        // Interrupt-only causes are shutdown reaching a sweep, treated as clean
        // like `supervised`. Described rather than `.message`d: `QueueError`
        // carries no message, and the authored operation name goes beside it.
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.void
            : Effect.logError('Session sweep failed').pipe(
                Effect.annotateLogs({
                  error: describeCause(cause),
                  operation: failureOperation(cause) ?? 'unknown',
                }),
              ),
        ),
      );
      // Collect crash-orphaned sessions now, not at the first hourly tick.
      yield* sweepSessions;
      // Forked, not awaited: the socket binds whether or not GitHub answers.
      // ! But every loop waits on `identity.verified`: the worker clones with the
      // ! token, and running before `GET /user` agrees on who owns it is the
      // ! exact misattribution the check exists to prevent.
      yield* Effect.forkScoped(
        identity.verified.pipe(
          Effect.tap((verified) =>
            Effect.logInfo('Authenticated as a GitHub account').pipe(
              Effect.annotateLogs({
                login: verified.login,
                expires:
                  verified.tokenExpiresAt === undefined
                    ? 'never'
                    : new Date(verified.tokenExpiresAt).toISOString(),
              }),
            ),
          ),
          // Qualification awaits this same verdict, so a delivery it cannot
          // qualify would be reclaimed and retried forever. Forked separately —
          // one `Effect.all` fail-fasts, letting a defect in either loop take
          // the other down with it.
          Effect.zipRight(
            Effect.all([
              Effect.forkScoped(supervised('worker', 'never', worker.run)),
              Effect.forkScoped(supervised('delivery worker', 'never', deliveryWorker.run)),
              // ! The poller marks threads read — destructive and irreversible.
              // ! Doing that before `GET /user` agrees on the credential can
              // ! silently empty the wrong account's inbox.
              Effect.forkScoped(supervised('notification poller', 'never', poller.run)),
            ]),
          ),
          // Supervised like the loops: a credential that can never work is not
          // survivable, and a defect here would otherwise leave the daemon alive
          // but idle.
          (verification) => supervised('credential verification', 'once', verification),
        ),
      );
      yield* Effect.forkScoped(
        maintenanceLoop.pipe(
          Effect.tapError((cause) => stop('Daemon ownership heartbeat failed', Cause.fail(cause))),
        ),
      );
      // `CredentialHealth` is a latch, so the watch completes rather than loops.
      yield* Effect.forkScoped(
        supervised('credential expiry watch', 'once', credentialExpiryWatch),
      );
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep('1 hour');
            const now = yield* Clock.currentTimeMillis;
            // A failed pass costs one cadence; killing the loop leaks every later
            // orphan until restart. Logged, not supervised.
            yield* queue
              .maintenance(
                now - policy.completedRetentionDays * 86_400_000,
                now - policy.failedRetentionDays * 86_400_000,
              )
              .pipe(
                // Interrupt-only means shutdown, as in `sweepSessions`; described
                // because `QueueError` has no message, operation name beside it.
                Effect.catchAllCause((cause) =>
                  Cause.isInterruptedOnly(cause)
                    ? Effect.void
                    : Effect.logError('Queue maintenance failed').pipe(
                        Effect.annotateLogs({
                          error: describeCause(cause),
                          operation: failureOperation(cause) ?? 'unknown',
                        }),
                      ),
                ),
              );
            yield* sweepSessions;
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
