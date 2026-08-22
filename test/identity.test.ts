import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Logger, Redacted, Ref } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import { GitHubIdentity } from '../src/github/identity.ts';

const config = (expectedLogin: string, trustedSenders: readonly string[] = []) =>
  Layer.succeed(
    LictorConfig,
    LictorConfig.make({
      githubToken: Redacted.make('pat-value'),
      expectedLogin,
      trustedSenders: [...trustedSenders],
      databasePath: ':memory:',
      policyPath: 'unused',
      controlSocketPath: '/tmp/lictor.sock',
      deliveryMaxBytes: 1024,
      executor: 'disabled',
      codexModel: 'gpt-5.6-luna',
      agentWorkdir: '.',
      executorTimeoutMs: 1000,
      executorOutputBytes: 1024,
      workerPollMs: 10,
      workerMaxAttempts: 3,
      workerRetryBaseMs: 100,
      notificationPollMs: 60_000,
    }),
  );

type Reply = {
  readonly status?: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
};

const resolve = (options: {
  readonly expectedLogin: string;
  readonly status?: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
  /** Consumed one per request, so a transient failure can be followed by success. */
  readonly replies?: readonly Reply[];
  readonly trustedSenders?: readonly string[];
}) =>
  Effect.runPromiseExit(
    Effect.gen(function* () {
      const calls = yield* Ref.make<string[]>([]);
      const stub = HttpClient.make((request) =>
        Ref.updateAndGet(calls, (items) => [...items, request.url]).pipe(
          Effect.map((items) => {
            const reply =
              options.replies === undefined
                ? options
                : (options.replies[items.length - 1] ?? options.replies.at(-1) ?? {});
            return HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify(reply.body ?? { login: 'adiutriel' }), {
                status: reply.status ?? 200,
                headers: { 'content-type': 'application/json', ...reply.headers },
              }),
            );
          }),
        ),
      );
      const ConfigLive = config(options.expectedLogin, options.trustedSenders);
      const IdentityLive = GitHubIdentity.DefaultWithoutDependencies.pipe(
        Layer.provide(
          Layer.merge(
            ConfigLive,
            GitHubClient.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.merge(
                  GitHubCredential.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive)),
                  Layer.succeed(HttpClient.HttpClient, stub),
                ),
              ),
            ),
          ),
        ),
      );
      // ! Building the layer must not touch the network — that is the whole
      // ! point of the service. So the probe is triggered explicitly here, and a
      // ! suite that saw zero calls after construction would be the bug.
      const service = yield* Effect.provide(GitHubIdentity, IdentityLive);
      const before = yield* Ref.get(calls);
      const identity = yield* service.verified;
      return { identity, calls: yield* Ref.get(calls), callsAtBuild: before.length };
    }).pipe(Effect.provide(Logger.remove(Logger.defaultLogger))),
  );

describe('GitHubIdentity', () => {
  it('resolves the authenticated login from GET /user', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel' });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.identity.login).toBe('adiutriel');
    expect(exit.value.callsAtBuild).toBe(0);
    expect(exit.value.calls).toEqual(['https://api.github.com/user']);
  });

  it('accepts a login that differs only by case', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', body: { login: 'Adiutriel' } });

    expect(exit._tag).toBe('Success');
  });

  // ! The whole point of the check: a valid token for the wrong account must not
  // ! reach a single API call, because GitHub ignores the Basic-auth username and
  // ! nothing downstream would notice the substitution.
  it('fails when the credential belongs to another account', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', body: { login: 'edloidas' } });

    expect(exit._tag).toBe('Failure');
  });

  it('fails when GitHub rejects the credential', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', status: 401, body: {} });

    expect(exit._tag).toBe('Failure');
  });

  // ! A token for the right account with the wrong scopes passes every other
  // ! check and then 403s on the first clone, once per job, forever.
  it('refuses a classic token that lacks the repo scope', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      headers: { 'x-oauth-scopes': 'notifications, read:user' },
    });

    expect(exit._tag).toBe('Failure');
  });

  it('accepts a classic token whose scope list includes repo', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      headers: { 'x-oauth-scopes': 'notifications, project, read:user, repo, user:email' },
    });

    expect(exit._tag).toBe('Success');
  });

  // ! No scope header at all means a different credential class, which is what
  // ! the migration seam is for — warn, do not refuse.
  it('starts when GitHub reports no scopes at all', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel' });

    expect(exit._tag).toBe('Success');
  });

  it('reads the expiry header GitHub returns for a token', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      headers: { 'github-authentication-token-expiration': '2026-11-18 20:55:07 UTC' },
    });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.identity.tokenExpiresAt).toBe(Date.parse('2026-11-18T20:55:07Z'));
  });

  it('reports no expiry for a token that never expires', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel' });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.identity.tokenExpiresAt).toBeUndefined();
  });

  // ! Booting before the network is up must not need an operator. A daemon that
  // ! dies on a DNS blip stops receiving deliveries, and GitHub does not
  // ! redeliver what it could not reach.
  it('retries an unreachable GitHub and succeeds once it answers', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      replies: [{ status: 503, body: {} }, {}],
    });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.calls).toHaveLength(2);
  });

  it('does not retry a credential GitHub refuses', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', status: 401, body: {} });

    expect(exit._tag).toBe('Failure');
  });

  // ! 403 is both "credential refused" and "bucket empty". Retrying the first
  // ! wastes a boot; failing on the second needs an operator for nothing.
  it('retries a throttled 403 rather than treating it as a bad credential', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      replies: [
        {
          status: 403,
          body: {},
          // ! A reset already due, so the honoured wait is zero and the test does
          // ! not sit out the conservative default the headerless case falls to.
          headers: {
            'x-ratelimit-remaining': '0',
            'x-ratelimit-reset': String(Math.floor(Date.now() / 1000)),
          },
        },
        {},
      ],
    });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.calls).toHaveLength(2);
  });

  // ! GitHub answers a secondary limit with 403 and no rate headers, so the
  // ! headers alone would make a throttle at boot look like a dead credential.
  it('retries a headerless 403 whose body reports a secondary rate limit', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      replies: [
        {
          status: 403,
          body: { message: 'You have exceeded a secondary rate limit.' },
          headers: { 'retry-after': '0' },
        },
        {},
      ],
    });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.calls).toHaveLength(2);
  });

  it('fails a 403 that carries no throttling signal', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', status: 403, body: {} });

    expect(exit._tag).toBe('Failure');
  });

  // ! The qualification policy drops self-authored deliveries anyway, but an
  // ! operator who trusts the daemon's own account has misunderstood something
  // ! worth stopping for.
  it('refuses to start when the daemon login is a trusted sender', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      trustedSenders: ['edloidas', 'adiutriel'],
    });

    expect(exit._tag).toBe('Failure');
  });

  it('starts when only other accounts are trusted', async () => {
    const exit = await resolve({ expectedLogin: 'adiutriel', trustedSenders: ['edloidas'] });

    expect(exit._tag).toBe('Success');
  });

  // ! The header is the only thing that knows when the bucket reopens. Retrying
  // ! on the exponential schedule alone burns the budget before it does.
  it('honours the wait GitHub named on a throttled response', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      replies: [{ status: 429, body: {}, headers: { 'retry-after': '0' } }, {}],
    });

    expect(exit._tag).toBe('Success');
    if (exit._tag !== 'Success') return;
    expect(exit.value.calls).toHaveLength(2);
  });

  // ! `GITHUB_TRUSTED_SENDERS` is lowercased by `loginList`, but a caller that
  // ! builds the config by hand is not, and this refusal is the safety net.
  it('refuses a trusted sender that differs from the daemon login only by case', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      trustedSenders: ['Adiutriel'],
    });

    expect(exit._tag).toBe('Failure');
  });

  it('fails when the credential has already expired', async () => {
    const exit = await resolve({
      expectedLogin: 'adiutriel',
      headers: { 'github-authentication-token-expiration': '2020-01-01 00:00:00 UTC' },
    });

    expect(exit._tag).toBe('Failure');
  });
});
