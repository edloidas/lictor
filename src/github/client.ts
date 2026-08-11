import { FetchHttpClient, HttpClient, HttpClientRequest } from '@effect/platform';
import { Effect, Redacted } from 'effect';
import { GitHubApp } from './app.ts';

/**
 * An `HttpClient` scoped to one installation: every request is resolved against
 * `api.github.com` and carries a fresh installation token.
 *
 * Handlers take this rather than the raw client so no call site has to remember
 * the base URL, the API version pin, or where the token came from.
 */
export class GitHubClient extends Effect.Service<GitHubClient>()('GitHubClient', {
  effect: Effect.gen(function* () {
    const app = yield* GitHubApp;
    const client = yield* HttpClient.HttpClient;

    const forInstallation = (installationId: number) =>
      Effect.map(app.token(installationId), (token) =>
        client.pipe(
          HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.com')),
          HttpClient.mapRequest(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${Redacted.value(token)}`,
              accept: 'application/vnd.github+json',
              'x-github-api-version': '2022-11-28',
            }),
          ),
        ),
      );

    return { forInstallation };
  }),
  dependencies: [GitHubApp.Default, FetchHttpClient.layer],
}) {}
