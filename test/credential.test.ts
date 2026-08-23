import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubCredential } from '../src/github/credential.ts';

const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    githubToken: Redacted.make('pat-value'),
    expectedLogin: 'adiutriel',
    trustedSenders: [],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor.sock',
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  }),
);

const CredentialLive = GitHubCredential.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));

const credential = <A, E>(effect: Effect.Effect<A, E, GitHubCredential>) =>
  Effect.runPromise(Effect.provide(effect, CredentialLive));

describe('GitHubCredential', () => {
  it('hands out the configured token', async () => {
    const token = await credential(Effect.flatMap(GitHubCredential, (self) => self.token));

    expect(Redacted.value(token)).toBe('pat-value');
  });

  it('builds an x-access-token Basic git header', async () => {
    const header = await credential(Effect.flatMap(GitHubCredential, (self) => self.gitAuthHeader));

    expect(Redacted.value(header)).toBe(
      `Basic ${Buffer.from('x-access-token:pat-value').toString('base64')}`,
    );
  });

  // ! git never parses a Bearer value and reports `invalid credentials`, which
  // ! reads like a revoked token rather than a wrong scheme. Pin the scheme.
  it('never hands git a Bearer header', async () => {
    const header = await credential(Effect.flatMap(GitHubCredential, (self) => self.gitAuthHeader));

    expect(Redacted.value(header)).not.toContain('Bearer');
  });

  it('keeps the token out of a rendered credential', async () => {
    const rendered = await credential(Effect.map(GitHubCredential, (self) => String(self.token)));

    expect(rendered).not.toContain('pat-value');
  });
});

describe('GitHubClient', () => {
  it('sends the token as Bearer against api.github.com', async () => {
    const requests = await Effect.runPromise(
      Effect.gen(function* () {
        const seen = yield* Ref.make<
          { readonly url: string; readonly headers: Record<string, string> }[]
        >([]);
        const stub = HttpClient.make((request) =>
          Ref.update(seen, (items) => [
            ...items,
            { url: request.url, headers: request.headers as Record<string, string> },
          ]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new Response('{}', { headers: { 'content-type': 'application/json' } }),
              ),
            ),
          ),
        );
        const ClientLive = GitHubClient.DefaultWithoutDependencies.pipe(
          Layer.provide(Layer.merge(CredentialLive, Layer.succeed(HttpClient.HttpClient, stub))),
        );

        yield* Effect.gen(function* () {
          const github = yield* GitHubClient;
          const client = yield* github.authenticated;
          yield* client.execute(HttpClientRequest.get('/user'));
        }).pipe(Effect.provide(ClientLive));

        return yield* Ref.get(seen);
      }),
    );

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe('https://api.github.com/user');
    expect(requests[0]?.headers.authorization).toBe('Bearer pat-value');
    expect(requests[0]?.headers['x-github-api-version']).toBe('2022-11-28');
  });

  // ! A credential that refreshes must be re-read per request. Resolving it once
  // ! per client looks identical with a static token and silently pins a stale
  // ! one as soon as the credential rotates.
  it('re-reads the credential on every request through one client', async () => {
    const authorizations = await Effect.runPromise(
      Effect.gen(function* () {
        const issued = yield* Ref.make(0);
        const seen = yield* Ref.make<(string | undefined)[]>([]);
        const stub = HttpClient.make((request) =>
          Ref.update(seen, (items) => [
            ...items,
            (request.headers as Record<string, string>).authorization,
          ]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new Response('{}', { headers: { 'content-type': 'application/json' } }),
              ),
            ),
          ),
        );
        const RotatingCredential = Layer.succeed(
          GitHubCredential,
          GitHubCredential.make({
            token: Ref.updateAndGet(issued, (count) => count + 1).pipe(
              Effect.map((count) => Redacted.make(`token-${count}`)),
            ),
            gitAuthHeader: Effect.succeed(Redacted.make('unused')),
          }),
        );
        const ClientLive = GitHubClient.DefaultWithoutDependencies.pipe(
          Layer.provide(
            Layer.merge(RotatingCredential, Layer.succeed(HttpClient.HttpClient, stub)),
          ),
        );

        yield* Effect.gen(function* () {
          const github = yield* GitHubClient;
          const client = yield* github.authenticated;
          yield* client.execute(HttpClientRequest.get('/user'));
          yield* client.execute(HttpClientRequest.get('/user'));
        }).pipe(Effect.provide(ClientLive));

        return yield* Ref.get(seen);
      }),
    );

    expect(authorizations).toEqual(['Bearer token-1', 'Bearer token-2']);
  });
});
