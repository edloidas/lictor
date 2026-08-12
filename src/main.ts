import { BunHttpServer, BunRuntime } from '@effect/platform-bun';
import { Clock, Effect, Layer } from 'effect';
import { LictorConfig, port } from './config.ts';
import { ControlPlane, ControlServer } from './control/control-plane.ts';
import { DeliveryWorker } from './delivery-worker.ts';
import { AgentExecutor } from './executor/agent-executor.ts';
import { ProcessRunner } from './executor/process-runner.ts';
import { GitHubApp } from './github/app.ts';
import { GitHubClient } from './github/client.ts';
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
const WorkspaceLive = RepositoryWorkspace.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ProcessRunner.Default, GitHubApp.Default)),
);
const ControlLive = ControlPlane.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive)),
);
const ControlServerLive = ControlServer.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ControlLive)),
);
const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, ExecutorLive, WorkspaceLive)),
);
const DeliveryWorkerLive = DeliveryWorker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, GitHubClient.Default, PolicyLive, QueueLive)),
);
const Services = Layer.mergeAll(
  ConfigLive,
  GitHubClient.Default,
  PolicyLive,
  QueueLive,
  WorkspaceLive,
  ControlLive,
  ControlServerLive,
  WorkerLive,
  DeliveryWorkerLive,
);
const Application = Layer.merge(
  Server,
  Layer.scopedDiscard(
    Effect.gen(function* () {
      const queue = yield* WorkQueue;
      const policy = yield* Policy;
      const control = yield* ControlServer;
      yield* Effect.logInfo('Local control socket ready').pipe(
        Effect.annotateLogs({ path: control.path }),
      );
      const counts = yield* queue.counts;
      yield* Effect.logInfo('Work queue ready').pipe(Effect.annotateLogs(counts));
      const worker = yield* Worker;
      yield* Effect.forkScoped(worker.run);
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep('10 seconds');
            yield* queue.heartbeatDaemon;
            yield* queue.recoverStale(yield* Clock.currentTimeMillis);
          }),
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
      const deliveryWorker = yield* DeliveryWorker;
      yield* Effect.forkScoped(deliveryWorker.run);
    }),
  ),
).pipe(Layer.provide(Services));

const Main = Layer.unwrapEffect(
  Effect.map(port, (bound) => Layer.provide(Application, BunHttpServer.layer({ port: bound }))),
);

BunRuntime.runMain(Layer.launch(Main));
