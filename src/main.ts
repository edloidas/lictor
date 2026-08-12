import { BunHttpServer, BunRuntime } from '@effect/platform-bun';
import { Effect, Layer } from 'effect';
import { LictorConfig, port } from './config.ts';
import { AgentExecutor } from './executor/agent-executor.ts';
import { ProcessRunner } from './executor/process-runner.ts';
import { GitHubClient } from './github/client.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { Server } from './server.ts';
import { Worker } from './worker.ts';

const ConfigLive = LictorConfig.Default;
const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
const ExecutorLive = AgentExecutor.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.merge(ConfigLive, ProcessRunner.Default)),
);
const WorkerLive = Worker.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(ConfigLive, QueueLive, ExecutorLive)),
);
const Services = Layer.mergeAll(ConfigLive, GitHubClient.Default, QueueLive, WorkerLive);
const Application = Layer.merge(
  Server,
  Layer.scopedDiscard(Effect.flatMap(Worker, (worker) => Effect.forkScoped(worker.run))),
).pipe(Layer.provide(Services));

const Main = Layer.unwrapEffect(
  Effect.map(port, (bound) => Layer.provide(Application, BunHttpServer.layer({ port: bound }))),
);

BunRuntime.runMain(Layer.launch(Main));
