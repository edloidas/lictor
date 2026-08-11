import { createSign } from 'node:crypto';
import { FetchHttpClient, HttpClient, HttpClientRequest } from '@effect/platform';
import { Clock, Data, Effect, Redacted, Ref, Schema } from 'effect';
import { LictorConfig, normalizePem } from '../config.ts';

/** GitHub caps App JWTs at 10 minutes; 9 leaves room for clock skew. */
const JWT_TTL_SECONDS = 540;

/** Renew an installation token this long before it actually expires. */
const RENEW_MARGIN_MS = 5 * 60 * 1000;

export class GitHubAppError extends Data.TaggedError('GitHubAppError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const base64url = (input: string | Buffer): string => Buffer.from(input).toString('base64url');

/**
 * Signs the RS256 JWT that authenticates as the App itself. This token can only
 * read App metadata and mint installation tokens — every repository call needs
 * the installation token below.
 *
 * `node:crypto` is deliberate over WebCrypto: GitHub hands out PKCS#1 PEMs
 * (`BEGIN RSA PRIVATE KEY`) and `crypto.subtle.importKey` only accepts PKCS#8.
 */
export const createAppJwt = (options: {
  readonly appId: string;
  readonly privateKey: string;
  readonly nowSeconds: number;
}): string => {
  const header = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const claims = base64url(
    JSON.stringify({
      // ! Backdated by 60s. GitHub rejects a JWT whose `iat` is in the future,
      // ! and a local clock running slightly fast is enough to trigger it.
      iat: options.nowSeconds - 60,
      exp: options.nowSeconds + JWT_TTL_SECONDS,
      iss: options.appId,
    }),
  );

  const signature = createSign('RSA-SHA256')
    .update(`${header}.${claims}`)
    .sign(normalizePem(options.privateKey));

  return `${header}.${claims}.${base64url(signature)}`;
};

const TokenResponse = Schema.Struct({
  token: Schema.String,
  expires_at: Schema.String,
});

type CachedToken = {
  readonly token: Redacted.Redacted<string>;
  readonly expiresAt: number;
};

/**
 * Mints and caches installation access tokens.
 *
 * Tokens are valid for an hour and are cached per installation until
 * {@link RENEW_MARGIN_MS} before expiry, so a busy repository does not spend a
 * round trip per delivery.
 */
export class GitHubApp extends Effect.Service<GitHubApp>()('GitHubApp', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const client = yield* HttpClient.HttpClient;
    const cache = yield* Ref.make(new Map<number, CachedToken>());

    const mint = (installationId: number) =>
      Effect.gen(function* () {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const jwt = createAppJwt({
          appId: config.appId,
          privateKey: Redacted.value(config.privateKey),
          nowSeconds,
        });

        const response = yield* client.execute(
          HttpClientRequest.post(
            `https://api.github.com/app/installations/${installationId}/access_tokens`,
          ).pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${jwt}`,
              accept: 'application/vnd.github+json',
              'x-github-api-version': '2022-11-28',
            }),
          ),
        );

        const body = yield* Schema.decodeUnknown(TokenResponse)(yield* response.json);

        return {
          token: Redacted.make(body.token),
          expiresAt: Date.parse(body.expires_at),
        } satisfies CachedToken;
      }).pipe(
        Effect.mapError(
          (cause) =>
            new GitHubAppError({
              message: `Could not mint an access token for installation ${installationId}`,
              cause,
            }),
        ),
      );

    const token = (installationId: number) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const cached = (yield* Ref.get(cache)).get(installationId);

        if (cached !== undefined && cached.expiresAt - RENEW_MARGIN_MS > now) {
          return cached.token;
        }

        const minted = yield* mint(installationId);
        yield* Ref.update(cache, (map) => new Map(map).set(installationId, minted));

        return minted.token;
      });

    return { token };
  }),
  dependencies: [LictorConfig.Default, FetchHttpClient.layer],
}) {}
