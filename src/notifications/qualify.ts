import { type HttpClient, HttpClientRequest } from '@effect/platform';
import { Data, Effect, Schema } from 'effect';
import { GitHubClient } from '../github/client.ts';
import type { ContextRef, WorkItem } from '../work-item.ts';
import { type NotificationThread, subjectRef, UNMENTIONABLE_REASONS } from './thread.ts';

/**
 * Every failure this module produces carries this tag. In particular a schema
 * decode failure on a GitHub *response* is wrapped here, never surfaced as a
 * `ParseResult.ParseError`: the delivery worker treats `ParseError` as terminal,
 * and a truncated or changed response body is not a property of the delivery.
 */
export class NotificationError extends Data.TaggedError('NotificationError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export type QualificationPolicy = {
  /** The login the daemon authenticates as, confirmed by `GET /user`. */
  readonly selfLogin: string;
  readonly trustedSenders: readonly string[];
};

export type QualifiedNotification = {
  /** `undefined` when the thread did not become work. */
  readonly work: WorkItem | undefined;
  /** Cursor value to store once this thread is processed, epoch ms. */
  readonly lastActivityAt: number;
};

type Candidate = {
  readonly at: number;
  readonly author: string | undefined;
  readonly ref: ContextRef;
  readonly url: string;
  readonly body: string | undefined;
};

const PAGE_SIZE = 100;

const PAGE_CEILING = 10;

const User = Schema.Struct({ login: Schema.String });

const Subject = Schema.Struct({
  title: Schema.String,
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  user: Schema.optionalWith(User, { nullable: true }),
  created_at: Schema.String,
  updated_at: Schema.String,
});

/**
 * A submitted pull-request review, whose own body can carry the mention.
 *
 * ! A third comment species, and not optional. GitHub notifies on a mention in a
 * ! review body, `/pulls/{n}/comments` holds only the inline threads, and the
 * ! deleted `pull_request_review` webhook handler used to cover exactly this. It
 * ! carries `submitted_at` rather than `updated_at`, and the endpoint takes no
 * ! `since`, so the window filter is applied here instead of by GitHub.
 */
const Review = Schema.Struct({
  id: Schema.Number,
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  user: Schema.optionalWith(User, { nullable: true }),
  submitted_at: Schema.optionalWith(Schema.String, { nullable: true }),
});

const Comment = Schema.Struct({
  id: Schema.Number,
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  user: Schema.optionalWith(User, { nullable: true }),
  created_at: Schema.String,
  updated_at: Schema.String,
});

// All login comparison in this module is lowercase-trimmed.
const normalizeLogin = (login: string): string => login.trim().toLowerCase();

const parseDate = (value: string): number | undefined => {
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? undefined : ms;
};

/**
 * Strips the parts of a comment that quote or display someone else's words.
 *
 * ! Not cosmetic. GitHub's own reply button quotes the message it replies to, and
 * ! GitHub notifies on a mention inside a blockquote — so without this a reader
 * ! quote-replying to "@adiutriel do X" produces a second job that does X again,
 * ! attributed to whoever merely agreed with it. Code spans matter for the same
 * ! reason: a mention shown as an example is not an instruction.
 *
 * ! Fences are matched line-anchored, both spellings. Anchoring is what stops a
 * ! sentence containing two inline ``` runs from swallowing everything between
 * ! them, which silently ate real mentions. A blockquote is recognised inside a
 * ! list item too, because that is how a threaded quote-reply nests.
 *
 * ! Not a Markdown parser, and not trying to be. A lazy blockquote continuation —
 * ! an unprefixed line GitHub still renders inside the quote — is not recognised,
 * ! and indented code blocks are deliberately left alone: stripping four-space
 * ! indentation would eat list continuations, and losing a real mention is worse
 * ! than acting on a displayed one.
 */
const addressable = (body: string): string =>
  body
    .replace(/^[ \t]*(?:```|~~~).*$[\s\S]*?^[ \t]*(?:```|~~~)[ \t]*$/gm, ' ')
    .replace(/`[^`\n]*`/g, ' ')
    .replace(/^[ \t]*(?:[-*+][ \t]+|\d+[.)][ \t]+)?>.*$/gm, ' ');

// ! Deliberately narrow, and not to be loosened. `@adiutriel-bot` is a different
// ! account, and matching a login that merely prefixes hers would put someone
// ! else's mention into her queue.
const mentions = (body: string, login: string): boolean => {
  const escaped = login.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^a-z0-9-])@${escaped}(?![a-z0-9-])`, 'i').test(addressable(body));
};

/**
 * ! Generic over the client's error channel rather than typed `HttpClient`. The
 * ! authenticated client can also fail resolving the credential, and pinning the
 * ! narrower type here would force a cast at the only call site that has one.
 */
type Authenticated<E> = HttpClient.HttpClient.With<E>;

const fetchResponse = <E>(
  client: Authenticated<E>,
  path: string,
  query: Record<string, string | undefined>,
) => {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined) params.set(key, value);
  }
  const search = params.toString();
  const url = search === '' ? path : `${path}?${search}`;
  return client
    .execute(HttpClientRequest.get(url))
    .pipe(
      Effect.mapError(
        (cause) => new NotificationError({ message: `Could not reach GitHub for ${path}`, cause }),
      ),
    );
};

/**
 * ! A body that will not parse is wrapped like every other enrichment failure.
 * ! A truncated 200 is as transient as a request that never arrived, and letting
 * ! its `ResponseError` escape would put it outside the retry budget.
 */
const readJson = (
  response: { readonly json: Effect.Effect<unknown, unknown> },
  path: string,
): Effect.Effect<unknown, NotificationError> =>
  response.json.pipe(
    Effect.mapError(
      (cause) => new NotificationError({ message: `Could not read the body of ${path}`, cause }),
    ),
  );

const decodeJson = <A, I>(
  schema: Schema.Schema<A, I>,
  value: unknown,
  label: string,
): Effect.Effect<A, NotificationError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(schema)(value),
    catch: (cause) =>
      new NotificationError({ message: `${label} response did not match its schema`, cause }),
  });

const fetchSubject = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
): Effect.Effect<Schema.Schema.Type<typeof Subject>, NotificationError> =>
  Effect.gen(function* () {
    const path = `/repos/${repository}/issues/${number}`;
    const response = yield* fetchResponse(client, path, {});
    if (response.status < 200 || response.status >= 300) {
      return yield* new NotificationError({
        message: `GitHub returned status ${response.status} for ${path}`,
      });
    }
    return yield* Effect.flatMap(readJson(response, path), (json) =>
      decodeJson(Subject, json, path),
    );
  });

/**
 * Page number `Link: rel="last"` points at, when GitHub supplies one.
 *
 * ! The only way to read this endpoint newest-first. `GET /issues/{n}/comments`
 * ! takes no `direction` — only the repository-wide comment list does — so the
 * ! last page is the sole handle on the newest comments. Without it a truncated
 * ! scan holds the *oldest* half of the window, which is exactly the wrong half
 * ! for a rule where the newest mentioning comment decides trust.
 */
const lastPage = (link: string | undefined): number | undefined => {
  if (link === undefined) return undefined;
  const match = /[?&]page=(\d+)[^>]*>;\s*rel="last"/.exec(link);
  if (match?.[1] === undefined) return undefined;
  const page = Number(match[1]);
  return Number.isSafeInteger(page) && page > 1 ? page : undefined;
};

const readPage = <A, I, E>(
  client: Authenticated<E>,
  path: string,
  element: Schema.Schema<A, I>,
  page: number,
  since: number | undefined,
) =>
  Effect.gen(function* () {
    const response = yield* fetchResponse(client, path, {
      per_page: String(PAGE_SIZE),
      page: String(page),
      ...(since === undefined ? {} : { since: new Date(since).toISOString() }),
    });
    if (response.status < 200 || response.status >= 300) {
      return yield* new NotificationError({
        message: `GitHub returned status ${response.status} for ${path}`,
      });
    }
    const items = yield* Effect.flatMap(readJson(response, path), (json) =>
      decodeJson(Schema.Array(element), json, path),
    );
    return { items, link: response.headers.link };
  });

/**
 * Every element of a paginated list in the window, newest pages first.
 *
 * The first request establishes how many pages there are; from then on the walk
 * runs backwards, so exhausting the budget drops the oldest entries rather than
 * the newest. That makes the budget a safe truncation instead of a reason to fail
 * a delivery on a thread that is merely long.
 *
 * ! Page 1 is read before the budget is known, because `Link` is the only place
 * ! the page count appears. When the walk does not reach page 2 those hundred
 * ! entries are the oldest in the window and are dropped — one request spent to
 * ! learn the shape of the thread, which is the price of endpoints with no
 * ! `direction` parameter.
 */
const fetchNewestFirst = <A, I, E>(
  client: Authenticated<E>,
  path: string,
  element: Schema.Schema<A, I>,
  since: number | undefined,
): Effect.Effect<readonly A[], NotificationError> =>
  Effect.gen(function* () {
    const first = yield* readPage(client, path, element, 1, since);
    const last = lastPage(first.link);
    if (last === undefined) return first.items;

    const stopAt = Math.max(2, last - (PAGE_CEILING - 1) + 1);
    const newer: A[] = [];
    for (let page = last; page >= stopAt; page -= 1) {
      const next = yield* readPage(client, path, element, page, since);
      newer.unshift(...next.items);
    }
    if (stopAt === 2) return [...first.items, ...newer];

    yield* Effect.logWarning(
      'Paginated window exceeded the page budget; the oldest entries were not examined',
    ).pipe(Effect.annotateLogs({ path, pages: last, budget: PAGE_CEILING }));
    return newer;
  });

const fetchComments = <E>(
  client: Authenticated<E>,
  path: string,
  since: number | undefined,
): Effect.Effect<readonly Schema.Schema.Type<typeof Comment>[], NotificationError> =>
  fetchNewestFirst(client, path, Comment, since);

/**
 * ! Paginated newest-first like the comment streams, and for the same reason: the
 * ! endpoint returns oldest-first and takes no `since`, so reading only page 1
 * ! would hide the newest reviews on any pull request with more than a hundred of
 * ! them. The window filter is applied to the candidates instead of by GitHub.
 */
const fetchReviews = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
): Effect.Effect<readonly Schema.Schema.Type<typeof Review>[], NotificationError> =>
  fetchNewestFirst(client, `/repos/${repository}/pulls/${number}/reviews`, Review, undefined);

/**
 * ! `created_at`, not `updated_at`, and this is a security property rather than a
 * ! preference. `comment.user` is who *wrote* the comment and REST reports no
 * ! editor for it, so keying on `updated_at` lets anyone able to edit a comment —
 * ! a maintainer, on someone else's comment — insert a mention and have the
 * ! resulting job attributed to the original, possibly trusted, author. Keying on
 * ! creation is what makes the recorded sender authoritative. The cost is that a
 * ! mention added by editing an existing comment is not acted on; a new comment
 * ! still is. GitHub's GraphQL schema exposes `editor` and `lastEditedAt`, which
 * ! is what would let the edited case be attributed correctly.
 */
const commentCandidate = (
  comment: Schema.Schema.Type<typeof Comment>,
  kind: 'issue_comment' | 'review_comment',
): Candidate => ({
  at: Date.parse(comment.created_at),
  author: comment.user?.login,
  ref: { kind, id: comment.id },
  url: comment.html_url,
  body: comment.body,
});

/**
 * Order within one second, where GitHub's timestamps cannot separate candidates.
 *
 * ! A total order, not a meaningful one. A body candidate loses to any comment
 * ! because it carries no id at all, and issue-comment ids are compared before
 * ! review-comment ids only so the winner is stable — the two are disjoint
 * ! sequences, so nothing about their relative size says which came first. Two
 * ! comments of the same kind in the same second do order correctly, which is the
 * ! case this exists for.
 */
const rankOf = (candidate: Candidate): readonly [number, number] => {
  switch (candidate.ref.kind) {
    case 'body':
      return [0, 0];
    case 'issue_comment':
      return [1, candidate.ref.id];
    case 'review_comment':
      return [2, candidate.ref.id];
    case 'review':
      return [3, candidate.ref.id];
  }
};

const outranks = (candidate: Candidate, best: Candidate): boolean => {
  const [kind, id] = rankOf(candidate);
  const [bestKind, bestId] = rankOf(best);
  return kind === bestKind ? id > bestId : kind > bestKind;
};

export const qualifyNotification = (input: {
  readonly deliveryId: string;
  readonly thread: NotificationThread;
  readonly policy: QualificationPolicy;
  /** Newest activity already turned into work for this thread, epoch ms. */
  readonly cursorMs: number | undefined;
}): Effect.Effect<QualifiedNotification, NotificationError, GitHubClient> =>
  Effect.scoped(
    Effect.gen(function* () {
      const lastActivityAt = Date.parse(input.thread.updated_at);

      // ! First, because every bound below derives from it. `updated_at` is a
      // ! plain string in the schema, and an unreadable one leaves the comment
      // ! scan with no anchor — a malformed envelope then costs a full history
      // ! scan and the delivery's whole attempt budget.
      if (Number.isNaN(lastActivityAt)) {
        yield* Effect.logWarning('Notification dropped: its activity time is unreadable').pipe(
          Effect.annotateLogs({ threadId: input.thread.id, updatedAt: input.thread.updated_at }),
        );
        return { work: undefined, lastActivityAt };
      }

      if (UNMENTIONABLE_REASONS.has(input.thread.reason)) {
        yield* Effect.logDebug('Notification skipped: machine traffic carries no mention').pipe(
          Effect.annotateLogs({ threadId: input.thread.id, reason: input.thread.reason }),
        );
        return { work: undefined, lastActivityAt };
      }

      const ref = subjectRef(input.thread);
      if (ref === undefined) {
        yield* Effect.logDebug(
          'Notification skipped: the subject has no issue number to act on',
        ).pipe(Effect.annotateLogs({ threadId: input.thread.id, type: input.thread.subject.type }));
        return { work: undefined, lastActivityAt };
      }

      // ! `undefined` is allowed here: with no cursor and no read mark the whole
      // ! thread is new to her, and `fetchComments` walks newest-first, so an
      // ! unbounded window truncates the *oldest* comments rather than losing the
      // ! one that triggered the notification.
      const since =
        input.cursorMs ??
        (input.thread.last_read_at === undefined
          ? undefined
          : parseDate(input.thread.last_read_at));

      const github = yield* GitHubClient;
      const client = yield* github.authenticated;
      const repository = input.thread.repository.full_name;

      // Serves pull requests too; its html_url points at the pull request.
      const subject = yield* fetchSubject(client, repository, ref.number);

      const conversationComments = yield* fetchComments(
        client,
        `/repos/${repository}/issues/${ref.number}/comments`,
        since,
      );

      let reviewComments: readonly Schema.Schema.Type<typeof Comment>[] = [];
      let reviews: readonly Schema.Schema.Type<typeof Review>[] = [];
      if (ref.kind === 'pull_request') {
        reviewComments = yield* fetchComments(
          client,
          `/repos/${repository}/pulls/${ref.number}/comments`,
          since,
        );
        reviews = yield* fetchReviews(client, repository, ref.number);
      }

      const candidates: Candidate[] = [
        ...conversationComments.map((comment) => commentCandidate(comment, 'issue_comment')),
        ...reviewComments.map((comment) => commentCandidate(comment, 'review_comment')),
        // ! Targeted at the pull request rather than the review. GitHub has no
        // ! reactions endpoint for a review, so the pull request itself is the
        // ! only place an acknowledgement can land — while `contextUrl` still
        // ! points at the review, which is what the agent needs to read.
        ...reviews.map((review) => ({
          at: review.submitted_at === undefined ? Number.NaN : Date.parse(review.submitted_at),
          author: review.user?.login,
          ref: { kind: 'review', id: review.id, number: ref.number } as const,
          url: review.html_url,
          body: review.body,
        })),
      ];
      // ! Gated on when the issue was opened, not on any later change, for the
      // ! reason spelled out on `commentCandidate`: `subject.user` is whoever
      // ! opened it and REST names no editor.
      const openedAt = Date.parse(subject.created_at);
      if (since === undefined || openedAt >= since) {
        candidates.push({
          at: openedAt,
          author: subject.user?.login,
          ref: { kind: 'body', number: ref.number },
          url: subject.html_url,
          body: subject.body,
        });
      }

      // ! The reviews endpoint takes no `since`, so its candidates are held to the
      // ! same window GitHub applied to the comment streams. Without this a review
      // ! submitted long before the cursor would re-trigger on every sweep.
      const windowed =
        since === undefined ? candidates : candidates.filter((candidate) => candidate.at >= since);

      const usable = windowed.flatMap((candidate) =>
        candidate.author === undefined || candidate.author === '' || Number.isNaN(candidate.at)
          ? []
          : [{ ...candidate, author: candidate.author }],
      );

      const selfLogin = normalizeLogin(input.policy.selfLogin);
      const matching = usable.filter(
        (candidate) => candidate.body !== undefined && mentions(candidate.body, selfLogin),
      );

      if (matching.length === 0) {
        yield* Effect.logDebug(
          'Notification did not become work: no candidate mentions the target user',
        ).pipe(Effect.annotateLogs({ threadId: input.thread.id }));
        return { work: undefined, lastActivityAt };
      }

      // ! The newest *trusted* mention triggers, not the newest mention. Letting
      // ! an untrusted one win and then refusing it hands every repository
      // ! participant a mute button. Her own activity is excluded structurally
      // ! rather than by configuration, so a loop cannot be configured into
      // ! existence.
      const trusted = new Set(input.policy.trustedSenders.map(normalizeLogin));
      const eligible = matching.filter((candidate) => {
        const author = normalizeLogin(candidate.author);
        return author !== selfLogin && trusted.has(author);
      });

      if (eligible.length === 0) {
        yield* Effect.logInfo('Notification dropped: no trusted sender mentioned her').pipe(
          Effect.annotateLogs({
            repository,
            senders: [...new Set(matching.map((candidate) => normalizeLogin(candidate.author)))]
              .sort()
              .join(','),
          }),
        );
        return { work: undefined, lastActivityAt };
      }

      const triggering = eligible.reduce((best, candidate) =>
        candidate.at > best.at || (candidate.at === best.at && outranks(candidate, best))
          ? candidate
          : best,
      );
      const sender = normalizeLogin(triggering.author);

      const work: WorkItem = {
        deliveryId: input.deliveryId,
        // Identities only: one job per thread per activity window means no
        // timestamp and no html url, or every edit becomes a new job.
        interactionId: JSON.stringify([
          repository,
          ref.kind,
          ref.number,
          'mentioned',
          triggering.ref,
          sender,
          selfLogin,
        ]),
        repository,
        sender,
        targets: [selfLogin],
        reasons: ['mentioned'],
        subject: {
          kind: ref.kind,
          number: ref.number,
          title: subject.title,
          url: subject.html_url,
        },
        contextUrl: triggering.url,
        context: triggering.ref,
      };

      return { work, lastActivityAt };
    }),
  ).pipe(
    Effect.mapError((cause) =>
      cause instanceof NotificationError
        ? cause
        : new NotificationError({ message: 'Could not fetch notification context', cause }),
    ),
  );
