import { HttpMiddleware, HttpRouter, HttpServer, HttpServerResponse } from '@effect/platform';
import { Layer } from 'effect';
import { LictorConfig } from './config.ts';
import { GitHubClient } from './github/client.ts';
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';

/**
 * The whole public surface: one liveness probe.
 *
 * Nothing is delivered here any more. GitHub is polled rather than listened to,
 * because a repository webhook needs admin on the repository and is therefore
 * scoped to the operator's rights instead of the account's own reach. Readiness
 * and management stay on the owner-only local Unix socket.
 */
export const router = HttpRouter.empty.pipe(
  HttpRouter.get('/health', HttpServerResponse.text('ok')),
);

export const Server = HttpServer.serve(router, HttpMiddleware.logger).pipe(
  HttpServer.withLogAddress,
);

/**
 * Server layer for route tests and embedding without a worker.
 *
 * `GitHubIdentity` is deliberately left out: its verdict comes from a live
 * `GET /user` probe, and baking it in would put a network call behind every
 * route test that touches a credential. The caller provides it — a stub in
 * tests, the verified live layer in `main.ts`.
 */
export const ServerLive = Server.pipe(
  Layer.provide(GitHubClient.Default),
  Layer.provide(WorkQueue.Default),
  Layer.provide(Policy.Default),
  Layer.provide(LictorConfig.Default),
);
