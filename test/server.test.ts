import { describe, expect, it } from 'bun:test';
import { HttpClient } from '@effect/platform';
import { BunHttpServer } from '@effect/platform-bun';
import { ConfigProvider, Effect, Layer, Logger } from 'effect';
import { GitHubIdentity } from '../src/github/identity.ts';
import { ServerLive } from '../src/server.ts';

/**
 * The whole server on an ephemeral port, with `layerTest`'s client already
 * pointed at it. Config comes from a map rather than the process environment so
 * the suite never depends on a `.env` being present.
 */
const TestServer = ServerLive.pipe(
  Layer.provide(
    Layer.succeed(
      GitHubIdentity,
      GitHubIdentity.make({
        verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
      }),
    ),
  ),
  Layer.provide(BunHttpServer.layerTest),
  Layer.provide(Logger.remove(Logger.defaultLogger)),
  Layer.provide(
    Layer.setConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          ['LICTOR_GITHUB_TOKEN', 'test-token'],
          ['LICTOR_GITHUB_LOGIN', 'adiutriel'],
          ['LICTOR_DATABASE_PATH', ':memory:'],
          ['LICTOR_POLICY_PATH', 'policy.example.toml'],
        ]),
      ),
    ),
  ),
);

const serve = <A, E>(effect: Effect.Effect<A, E, HttpClient.HttpClient>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(effect, Layer.merge(TestServer, BunHttpServer.layerTest))),
  );

describe('GET /health', () => {
  it('reports ok', async () => {
    const response = await serve(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        return yield* client.get('/health');
      }),
    );

    expect(response.status).toBe(200);
  });
});

// The route GitHub used to deliver to is gone, not merely unused. Polling is
// the only transport, and a live inbound endpoint would be an unauthenticated
// path into the queue that nothing verifies any more.
describe('the retired webhook route', () => {
  it('is not served', async () => {
    const response = await serve(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        return yield* client.post('/webhooks/github');
      }),
    );

    expect(response.status).toBe(404);
  });
});
