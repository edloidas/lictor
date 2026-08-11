import { BunHttpServer, BunRuntime } from '@effect/platform-bun';
import { Effect, Layer } from 'effect';
import { port } from './config.ts';
import { ServerLive } from './server.ts';

const Main = Layer.unwrapEffect(
  Effect.map(port, (bound) => Layer.provide(ServerLive, BunHttpServer.layer({ port: bound }))),
);

BunRuntime.runMain(Layer.launch(Main));
