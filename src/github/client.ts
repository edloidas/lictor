import { FetchHttpClient, HttpClient, HttpClientRequest } from '@effect/platform';
import { Clock, Data, Effect, Redacted, Schema } from 'effect';
import type { ContextRef } from '../work-item.ts';
import { GitHubCredential } from './credential.ts';
import { DEFAULT_THROTTLE_WAIT_MS, isSecondaryRateLimit, retryAfterMs } from './retry-after.ts';

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
 * A response GitHub answered but refused, kept apart from a transport failure so
 * a caller can decide on the status rather than on prose. `retryAfterMs` is set
 * only where GitHub said how long to wait, or where a 429 or a secondary limit
 * means the caller must wait regardless.
 */
export class GitHubStatusError extends Data.TaggedError('GitHubStatusError')<{
  readonly message: string;
  readonly status: number;
  readonly retryAfterMs?: number;
}> {}

const CreatedComment = Schema.Struct({ html_url: Schema.optional(Schema.String) });
const CommentPage = Schema.Array(
  Schema.Struct({ body: Schema.optional(Schema.String), html_url: Schema.optional(Schema.String) }),
);

/** One posted comment, as much of it as a caller needs to record. */
export type PostedComment = { readonly url?: string };

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

    /**
     * Why GitHub refused, expressed as something a delivery loop can act on.
     *
     * The broker classifies the same three cases for the agent's calls; this is
     * the daemon's own copy, over the shared `retry-after` helpers, because the
     * broker's version is welded to `CapabilityError` and its running-job fence.
     */
    const refusal = (
      path: string,
      status: number,
      headers: Readonly<Record<string, string | undefined>>,
      body: string,
    ) =>
      Effect.map(Clock.currentTimeMillis, (now) => {
        const hinted = status === 403 || status === 429 ? retryAfterMs(headers, now) : undefined;
        const secondary =
          status === 403 &&
          hinted === undefined &&
          (headers['x-ratelimit-remaining'] === '0' || isSecondaryRateLimit(body));
        const wait = status === 429 || secondary ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : hinted;
        return new GitHubStatusError({
          message: `${path} returned status ${status}`,
          status,
          ...(wait === undefined ? {} : { retryAfterMs: wait }),
        });
      });

    const send = (request: HttpClientRequest.HttpClientRequest, path: string) =>
      Effect.gen(function* () {
        const authorized = yield* authenticated;
        const response = yield* authorized
          .execute(request)
          .pipe(
            Effect.mapError(
              (cause) => new GitHubRequestError({ message: `Could not reach ${path}`, cause }),
            ),
          );
        if (response.status < 200 || response.status >= 300) {
          const body = yield* Effect.orElseSucceed(response.text, () => '');
          const refused = yield* refusal(path, response.status, response.headers, body);
          return yield* refused;
        }
        return response;
      });

    /**
     * Posts a comment on an issue or pull request as the daemon.
     *
     * On `GitHubClient` rather than `CapabilityBroker` for the same reason
     * `addReaction` is: the broker admits only a `running` job holding a live
     * lease, and an outcome is posted once the job is terminal.
     */
    const createComment = (repository: string, number: number, body: string) =>
      Effect.gen(function* () {
        const path = `/repos/${repository}/issues/${number}/comments`;
        const response = yield* send(
          HttpClientRequest.bodyUnsafeJson(HttpClientRequest.post(path), { body }),
          path,
        );
        const decoded = yield* Schema.decodeUnknown(CreatedComment)(yield* response.json).pipe(
          Effect.orElseSucceed(() => ({ html_url: undefined })),
        );
        return {
          ...(decoded.html_url === undefined ? {} : { url: decoded.html_url }),
        } satisfies PostedComment;
      });

    /**
     * One page of an issue's comments, newest activity first, for recognising a
     * comment this daemon may already have posted. `since` bounds the scan to
     * the window the message could have landed in.
     */
    const listComments = (repository: string, number: number, sinceMs: number, page: number) =>
      Effect.gen(function* () {
        const since = new Date(sinceMs).toISOString();
        const path = `/repos/${repository}/issues/${number}/comments?per_page=100&page=${page}&since=${since}`;
        const response = yield* send(HttpClientRequest.get(path), path);
        return yield* Schema.decodeUnknown(CommentPage)(yield* response.json).pipe(
          Effect.mapError(
            (cause) =>
              new GitHubRequestError({ message: `${path} returned an unexpected body`, cause }),
          ),
        );
      });

    return { authenticated, addReaction, createComment, listComments };
  }),
  dependencies: [GitHubCredential.Default, FetchHttpClient.layer],
}) {}
