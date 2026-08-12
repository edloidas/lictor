import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest } from '@effect/platform';
import { BunHttpServer } from '@effect/platform-bun';
import { ConfigProvider, Effect, Layer, Logger } from 'effect';
import { ServerLive, WEBHOOK_PATH } from '../src/server.ts';
import { sign } from '../src/webhook/signature.ts';

const secret = 'test-webhook-secret';

/**
 * The whole server on an ephemeral port, with `layerTest`'s client already
 * pointed at it. Config comes from a map rather than the process environment so
 * the suite never depends on a `.env` being present.
 */
const TestServer = ServerLive.pipe(
  Layer.provide(BunHttpServer.layerTest),
  Layer.provide(Logger.remove(Logger.defaultLogger)),
  Layer.provide(
    Layer.setConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          ['GITHUB_APP_ID', '1'],
          ['GITHUB_PRIVATE_KEY', 'unused-by-these-routes'],
          ['GITHUB_WEBHOOK_SECRET', secret],
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

const deliver = (options: {
  readonly body: string;
  readonly signature?: string;
  readonly event?: string;
  readonly deliveryId?: string | null;
}) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const headers: Record<string, string> = { 'content-type': 'application/json' };

    if (options.signature !== undefined) headers['x-hub-signature-256'] = options.signature;
    if (options.event !== undefined) headers['x-github-event'] = options.event;
    if (options.deliveryId !== null) {
      headers['x-github-delivery'] = options.deliveryId ?? 'test-delivery';
    }

    return yield* client.execute(
      HttpClientRequest.post(WEBHOOK_PATH).pipe(
        HttpClientRequest.setHeaders(headers),
        HttpClientRequest.bodyText(options.body, 'application/json'),
      ),
    );
  });

const pingBody = JSON.stringify({ zen: 'Design for failure.', repository: null });

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

describe('POST /webhooks/github', () => {
  it('accepts a correctly signed delivery', async () => {
    const response = await serve(
      deliver({ body: pingBody, signature: sign(pingBody, secret), event: 'ping' }),
    );

    expect(response.status).toBe(202);
  });

  it('rejects a delivery signed with the wrong secret', async () => {
    const response = await serve(
      deliver({ body: pingBody, signature: sign(pingBody, 'wrong'), event: 'ping' }),
    );

    expect(response.status).toBe(401);
  });

  it('rejects an unsigned delivery', async () => {
    const response = await serve(deliver({ body: pingBody, event: 'ping' }));

    expect(response.status).toBe(401);
  });

  // Verification must happen before parsing, so a body that is not even JSON
  // still fails as unauthorized rather than as malformed.
  it('rejects an unsigned non-JSON body as unauthorized, not malformed', async () => {
    const response = await serve(deliver({ body: 'not json', event: 'ping' }));

    expect(response.status).toBe(401);
  });

  it('rejects a signed delivery with no event header', async () => {
    const response = await serve(deliver({ body: pingBody, signature: sign(pingBody, secret) }));

    expect(response.status).toBe(400);
  });

  it('rejects a signed delivery with no delivery id', async () => {
    const response = await serve(
      deliver({
        body: pingBody,
        signature: sign(pingBody, secret),
        event: 'ping',
        deliveryId: null,
      }),
    );

    expect(response.status).toBe(400);
  });

  it('rejects a signed body that is not JSON', async () => {
    const body = 'not json';
    const response = await serve(deliver({ body, signature: sign(body, secret), event: 'ping' }));

    expect(response.status).toBe(400);
  });

  it('accepts an event nobody handles', async () => {
    const body = JSON.stringify({ action: 'synchronize' });
    const response = await serve(
      deliver({ body, signature: sign(body, secret), event: 'pull_request' }),
    );

    expect(response.status).toBe(202);
  });
});
