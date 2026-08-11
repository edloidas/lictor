import { describe, expect, it } from 'bun:test';
import { generateKeyPairSync } from 'node:crypto';
import { HttpClient, HttpClientResponse } from '@effect/platform';
import { ConfigProvider, Effect, Layer, Redacted, Ref, TestClock, TestContext } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubApp } from '../src/github/app.ts';

/** A real key so `createAppJwt` signs; the stubbed GitHub never verifies it. */
const { privateKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
  publicKeyEncoding: { type: 'spki', format: 'pem' },
});

const ConfigLive = LictorConfig.Default.pipe(
  Layer.provide(
    Layer.setConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          ['GITHUB_APP_ID', '12345'],
          ['GITHUB_PRIVATE_KEY', privateKey],
          ['GITHUB_WEBHOOK_SECRET', 'unused'],
        ]),
      ),
    ),
  ),
);

/**
 * A stubbed GitHub that hands out a numbered token per call and counts the
 * calls, so a test can tell a cache hit from a fresh mint.
 */
const stubGitHub = (options: { readonly expiresInMs: number }) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);

    const client = HttpClient.make((request) =>
      Effect.gen(function* () {
        const n = yield* Ref.updateAndGet(calls, (count) => count + 1);
        const now = yield* Effect.clock.pipe(Effect.flatMap((clock) => clock.currentTimeMillis));

        return HttpClientResponse.fromWeb(
          request,
          new Response(
            JSON.stringify({
              token: `ghs_token_${n}`,
              expires_at: new Date(now + options.expiresInMs).toISOString(),
            }),
            { headers: { 'content-type': 'application/json' } },
          ),
        );
      }),
    );

    return { calls, layer: Layer.succeed(HttpClient.HttpClient, client) };
  });

const run = <A, E>(
  body: (stub: { readonly calls: Ref.Ref<number> }) => Effect.Effect<A, E, GitHubApp>,
  options: { readonly expiresInMs: number },
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* stubGitHub(options);
      return yield* body(stub).pipe(
        // ! `DefaultWithoutDependencies`, not `Default`. `Default` bakes in
        // ! `FetchHttpClient.layer`, which wins over anything provided from the
        // ! outside — these tests would silently call the real api.github.com.
        Effect.provide(
          GitHubApp.DefaultWithoutDependencies.pipe(
            Layer.provide(stub.layer),
            Layer.provide(ConfigLive),
          ),
        ),
      );
    }).pipe(Effect.provide(TestContext.TestContext)),
  );

const ONE_HOUR = 60 * 60 * 1000;

describe('GitHubApp.token', () => {
  it('mints a token on the first call', async () => {
    const result = await run(
      (stub) =>
        Effect.gen(function* () {
          const app = yield* GitHubApp;
          const token = yield* app.token(1);
          return { token: Redacted.value(token), calls: yield* Ref.get(stub.calls) };
        }),
      { expiresInMs: ONE_HOUR },
    );

    expect(result).toEqual({ token: 'ghs_token_1', calls: 1 });
  });

  it('reuses the cached token while it is still fresh', async () => {
    const result = await run(
      (stub) =>
        Effect.gen(function* () {
          const app = yield* GitHubApp;
          yield* app.token(1);
          const token = yield* app.token(1);
          return { token: Redacted.value(token), calls: yield* Ref.get(stub.calls) };
        }),
      { expiresInMs: ONE_HOUR },
    );

    expect(result).toEqual({ token: 'ghs_token_1', calls: 1 });
  });

  it('caches per installation rather than globally', async () => {
    const result = await run(
      (stub) =>
        Effect.gen(function* () {
          const app = yield* GitHubApp;
          const first = yield* app.token(1);
          const second = yield* app.token(2);
          return {
            first: Redacted.value(first),
            second: Redacted.value(second),
            calls: yield* Ref.get(stub.calls),
          };
        }),
      { expiresInMs: ONE_HOUR },
    );

    expect(result).toEqual({ first: 'ghs_token_1', second: 'ghs_token_2', calls: 2 });
  });

  it('re-mints once the cached token is inside the renewal margin', async () => {
    const result = await run(
      (stub) =>
        Effect.gen(function* () {
          const app = yield* GitHubApp;
          yield* app.token(1);
          // 56 min in: 4 min of life left, inside the 5 min margin.
          yield* TestClock.adjust('56 minutes');
          const token = yield* app.token(1);
          return { token: Redacted.value(token), calls: yield* Ref.get(stub.calls) };
        }),
      { expiresInMs: ONE_HOUR },
    );

    expect(result).toEqual({ token: 'ghs_token_2', calls: 2 });
  });

  it('keeps the cached token just outside the renewal margin', async () => {
    const result = await run(
      (stub) =>
        Effect.gen(function* () {
          const app = yield* GitHubApp;
          yield* app.token(1);
          // 54 min in: 6 min of life left, still outside the margin.
          yield* TestClock.adjust('54 minutes');
          const token = yield* app.token(1);
          return { token: Redacted.value(token), calls: yield* Ref.get(stub.calls) };
        }),
      { expiresInMs: ONE_HOUR },
    );

    expect(result).toEqual({ token: 'ghs_token_1', calls: 1 });
  });

  // A revoked key answers 401 with a `message` body. That must surface as the
  // typed error, not as a decode failure leaking out of the schema.
  it('wraps an unexpected response in GitHubAppError', async () => {
    const rejecting = Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify({ message: 'Bad credentials' }), {
              status: 401,
              headers: { 'content-type': 'application/json' },
            }),
          ),
        ),
      ),
    );

    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const app = yield* GitHubApp;
        return yield* Effect.flip(app.token(1));
      }).pipe(
        Effect.provide(
          GitHubApp.DefaultWithoutDependencies.pipe(
            Layer.provide(rejecting),
            Layer.provide(ConfigLive),
          ),
        ),
      ),
    );

    expect(error._tag).toBe('GitHubAppError');
    expect(error.message).toContain('installation 1');
  });
});
