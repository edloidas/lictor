import { FetchHttpClient, HttpClient, HttpClientRequest } from '@effect/platform';
import { Data, Effect, Redacted } from 'effect';
import type { ContextRef } from '../work-item.ts';
import { GitHubCredential } from './credential.ts';

/** The three shapes a reaction target takes, each with its own endpoint. */
const reactionPath = (repository: string, target: ContextRef): string => {
  switch (target.kind) {
    case 'issue_comment':
      return `/repos/${repository}/issues/comments/${target.id}/reactions`;
    case 'review_comment':
      return `/repos/${repository}/pulls/comments/${target.id}/reactions`;
    // Reviews, assignments, and review requests all react on the issue or pull
    // request itself: no reactions endpoint for a review, and a timeline event
    // has no resource of its own to acknowledge.
    case 'review':
    case 'assigned':
    case 'review_requested':
    case 'body':
      return `/repos/${repository}/issues/${target.number}/reactions`;
  }
};

export class GitHubRequestError extends Data.TaggedError('GitHubRequestError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

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

    // Resolved per request, not once per client: a caller holding one client
    // across many requests would bake in the token it saw first — invisible with
    // a static token, wrong the moment the credential has to refresh.
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

    /**
     * Acknowledges a triggering comment or body with a reaction.
     *
     * Deliberately on the client rather than behind `CapabilityBroker`. The
     * broker refuses anything that is not a `running` job holding a live lease,
     * and a just-enqueued job is `pending` — loosening that fencing to admit a
     * daemon-side call would weaken the only thing the broker exists for. If the
     * *agent* should ever react, that becomes a normal policy-gated broker tool
     * and the two paths stay distinct.
     *
     * GitHub's reactions endpoints are idempotent per user, content, and target,
     * so a repeated call is a no-op rather than a duplicate.
     */
    const addReaction = (repository: string, target: ContextRef, content: 'eyes') =>
      Effect.gen(function* () {
        const path = reactionPath(repository, target);
        const authorized = yield* authenticated;
        const response = yield* authorized
          .execute(HttpClientRequest.bodyUnsafeJson(HttpClientRequest.post(path), { content }))
          .pipe(
            Effect.mapError(
              (cause) => new GitHubRequestError({ message: `Could not POST ${path}`, cause }),
            ),
          );
        // 200 as well as 201: already-present reaction, normal on a replay.
        if (response.status !== 200 && response.status !== 201) {
          return yield* new GitHubRequestError({
            message: `Reacting to ${target.kind} returned status ${response.status}`,
          });
        }
      });

    return { authenticated, addReaction };
  }),
  dependencies: [GitHubCredential.Default, FetchHttpClient.layer],
}) {}
