import { FetchHttpClient, HttpClient, HttpClientRequest } from '@effect/platform';
import { Effect, Redacted } from 'effect';
import { GitHubCredential } from './credential.ts';

/**
 * An `HttpClient` pointed at `api.github.com` and carrying the daemon's
 * credential.
 *
 * Handlers take this rather than the raw client so no call site has to remember
 * the base URL, the API version pin, or where the token came from. It stays an
 * `Effect` so a credential that has to be refreshed can do so per request.
 */
export class GitHubClient extends Effect.Service<GitHubClient>()('GitHubClient', {
  effect: Effect.gen(function* () {
    const credential = yield* GitHubCredential;
    const client = yield* HttpClient.HttpClient;

    // ! The credential is resolved per request, not once per client. A caller
    // ! that holds one client across many requests would otherwise bake in the
    // ! token it saw first, which is invisible with a static token and wrong the
    // ! moment the credential has to refresh.
    const authenticated = Effect.succeed(
      client.pipe(
        HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.com')),
        HttpClient.mapRequestEffect((request) =>
          Effect.map(credential.token, (token) =>
            HttpClientRequest.setHeaders(request, {
              authorization: `Bearer ${Redacted.value(token)}`,
              accept: 'application/vnd.github+json',
              'x-github-api-version': '2022-11-28',
            }),
          ),
        ),
      ),
    );

    return { authenticated };
  }),
  dependencies: [GitHubCredential.Default, FetchHttpClient.layer],
}) {}
