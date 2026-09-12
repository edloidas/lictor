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

const CreatedReaction = Schema.Struct({ id: Schema.optional(Schema.Number) });

/**
 * The reactions this daemon uses to say where a job got to.
 *
 * `eyes` is the acknowledgement the delivery worker places; the rest are the
 * terminal states the outbox reconciles it to.
 */
export type ReactionContent = 'eyes' | 'rocket' | 'confused';

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
     * Acknowledges a triggering comment or body with a reaction, and answers
     * with that reaction's id.
     *
     * On the client rather than behind `CapabilityBroker`: the broker admits
     * only a `running` job holding a live lease, and a just-enqueued job is
     * `pending`.
     *
     * GitHub's reactions endpoints are idempotent per user, content and target.
     * A repeat is not merely a no-op: it answers 200 with the reaction that is
     * already there, which is the only way to learn an id nothing recorded.
     */
    const addReaction = (repository: string, target: ContextRef, content: ReactionContent) =>
      Effect.gen(function* () {
        const path = reactionPath(repository, target);
        const response = yield* send(
          HttpClientRequest.bodyUnsafeJson(HttpClientRequest.post(path), { content }),
          path,
        );
        const decoded = yield* Schema.decodeUnknown(CreatedReaction)(yield* response.json).pipe(
          Effect.orElseSucceed(() => ({ id: undefined })),
        );
        return decoded.id;
      });

    const removeReaction = (repository: string, target: ContextRef, id: number) => {
      const path = `${reactionPath(repository, target)}/${id}`;
      // A reaction already gone is the state this call wanted. Only the delete
      // tolerates 404 — on the add it means the target itself is gone.
      return send(HttpClientRequest.del(path), path).pipe(
        Effect.catchIf(
          (error) => error._tag === 'GitHubStatusError' && error.status === 404,
          () => Effect.void,
        ),
        Effect.asVoid,
      );
    };

    /**
     * Leaves this account holding exactly `content` on the target.
     *
     * Add before remove. A crash between the two shows both reactions until a
     * retry converges; the other order shows none, and a thread that lost its
     * acknowledgement reads as work the daemon never saw.
     *
     * Each stale content is cleared by posting it and deleting what comes back,
     * rather than by searching a listing for this account. The listing is
     * everyone's, so finding this account in it is unbounded work on a popular
     * target — a scan that can run out of pages either dead-letters the outcome
     * or leaves a contradictory reaction standing forever, and re-reading the
     * same prefix on retry makes neither converge. Posting costs one request
     * against a known id and converges whatever anyone else has reacted.
     *
     * It also touches nothing but this daemon's own vocabulary: a reaction the
     * operator left by hand on the same comment is never a candidate.
     */
    const reconcileReaction = (
      repository: string,
      target: ContextRef,
      content: ReactionContent,
      stale: readonly ReactionContent[],
    ) =>
      Effect.gen(function* () {
        yield* addReaction(repository, target, content);
        for (const other of stale) {
          if (other === content) continue;
          // 200 means this account held it and the id is the stale reaction's;
          // 201 means the post created one this account did not have. The
          // delete is right either way, and the end state is the same.
          const id = yield* addReaction(repository, target, other);
          if (id !== undefined) yield* removeReaction(repository, target, id);
        }
      });

    return { authenticated, addReaction, reconcileReaction };
  }),
  dependencies: [GitHubCredential.Default, FetchHttpClient.layer],
}) {}
