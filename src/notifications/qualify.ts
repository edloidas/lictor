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
  /** GraphQL's `lastEditedAt`, verbatim, once an edit has been attributed. */
  readonly editedAt?: string;
};

type Edit = {
  readonly editor: string | undefined;
  readonly lastEditedAt: string | undefined;
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
  // 'open' | 'closed'. A closed subject ends its thread's live window.
  state: Schema.optionalWith(Schema.String, { nullable: true }),
});

/**
 * A submitted pull-request review, whose own body can carry the mention.
 *
 * A third comment species GitHub notifies on; `/pulls/{n}/comments` holds only
 * inline threads.
 */
const Review = Schema.Struct({
  id: Schema.Number,
  node_id: Schema.String,
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  user: Schema.optionalWith(User, { nullable: true }),
  submitted_at: Schema.optionalWith(Schema.String, { nullable: true }),
});

const Comment = Schema.Struct({
  id: Schema.Number,
  node_id: Schema.String,
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  user: Schema.optionalWith(User, { nullable: true }),
  created_at: Schema.String,
  updated_at: Schema.String,
});

/**
 * One issue-timeline event — the only record of who assigned her or requested
 * her review: an assignment carries no comment whose author could be trusted.
 * A `review_requested` event naming a team says nothing about her, so the
 * requested reviewer is checked rather than trusted blindly.
 */
const TimelineEvent = Schema.Struct({
  id: Schema.Number,
  event: Schema.String,
  actor: Schema.optionalWith(User, { nullable: true }),
  assignee: Schema.optionalWith(User, { nullable: true }),
  review_requester: Schema.optionalWith(User, { nullable: true }),
  requested_reviewer: Schema.optionalWith(User, { nullable: true }),
  created_at: Schema.String,
});

// All login comparison in this module is lowercase-trimmed.
const normalizeLogin = (login: string): string => login.trim().toLowerCase();

const parseDate = (value: string): number | undefined => {
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? undefined : ms;
};

// A real HTML comment renders away; `<code>` and `<pre>` written as raw HTML
// display what they wrap. Both are matched on the rendered text rather than
// per callback, because a raw HTML block arrives as one chunk carrying its own
// markup, and an attribute may contain `>`. An unterminated one takes the rest
// of the body, which is the direction that loses a mention rather than invents
// one.
const HTML_COMMENT = /<!--[\s\S]*?(?:-->|$)/g;
const DISPLAYED_HTML = /<(code|pre)\b[\s\S]*?(?:<\/\1[ \t]*>|$)/gi;
const RAW_TAG = /<\/?[a-z][^>]*>/gi;
// ! `Bun.markdown` does not implement GFM's bare-URL autolink, so a pasted
// ! `https://host/@login` arrives as prose and reads as addressed. GitHub links
// ! no mention inside an autolink, and none is ever written in one.
const BARE_URL = /(?:https?:\/\/|www\.)\S+/gi;

/**
 * Reduces a body to the text GitHub would apply its mention linkifier to.
 *
 * GitHub renders first and linkifies the result, so every rule that depends on
 * what a delimiter *renders to* — an underscore consumed as emphasis, a
 * backslash consumed as an escape, a backtick run that never pairs — resolves
 * for free here and cannot be expressed on the raw Markdown at all.
 *
 * Code and code spans are dropped because GitHub links nothing inside them.
 * Quoted prose is dropped for a different reason, and it is lictor's rule
 * rather than GitHub's: GitHub's reply button quotes what it replies to and
 * does link a mention inside the quote, so without this a reader agreeing with
 * "@her do X" produces a second job attributed to whoever agreed.
 *
 * ! Every element becomes a boundary, whether its text is kept or dropped.
 * ! GitHub linkifies each text node separately, so a mention opening one is
 * ! addressed however the previous node ended — `a**b**@her` links. Join the
 * ! nodes instead and that `b` runs into the `@` and silently swallows it.
 */
const addressable = (body: string): string => {
  const drop = (): string => ' ';
  const keep = (children: string): string => ` ${children} `;
  const block = (children: string): string => `${children}\n`;

  const rendered = Bun.markdown.render(body, {
    code: drop,
    codespan: drop,
    // The quote's prose goes, but a `<code>` or `<pre>` tag it opened stays: an
    // unclosed one displays every line after the quote on GitHub too, and
    // dropping it here would leave that text looking addressed.
    blockquote: (children) => ` ${(children.match(RAW_TAG) ?? []).join(' ')} `,
    // A mention in link text is displayed, never linked: GitHub does not nest
    // one anchor inside another.
    link: drop,
    image: drop,
    strong: keep,
    emphasis: keep,
    strikethrough: keep,
    html: block,
    paragraph: block,
    heading: block,
    listItem: block,
    th: block,
    td: block,
    hr: block,
  });

  return rendered
    .replace(HTML_COMMENT, ' ')
    .replace(DISPLAYED_HTML, ' ')
    .replace(RAW_TAG, ' ')
    .replace(BARE_URL, ' ');
};

// Measured against `POST /markdown` rather than taken from the spec: GitHub
// opens a mention after anything but a word character or a backtick, and ends
// it before anything but a word character, `-`, `/`, or a backtick. `/` is the
// team-mention separator, and `@org/team` names no user.
const MENTION_OPENS_AFTER = '[^a-z0-9_`]';
const MENTION_CLOSES_BEFORE = '(?![a-z0-9_/`-])';

// ! Deliberately narrow, not to be loosened: matching a login that merely
// ! prefixes hers would put someone else's mention into her queue.
const mentionPattern = (login: string): RegExp => {
  const escaped = login.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|${MENTION_OPENS_AFTER})@${escaped}${MENTION_CLOSES_BEFORE}`, 'i');
};

const mentions = (body: string, login: string): Effect.Effect<boolean, NotificationError> =>
  // A throw inside `Effect.gen` is a defect that `catchAll` never sees, and a
  // trusted sender's prose is still prose nobody here wrote.
  Effect.try({
    try: () => mentionPattern(login).test(addressable(body)),
    catch: (cause) => new NotificationError({ message: 'Failed to render a body', cause }),
  });

// Generic over the client's error channel: the authenticated client can also
// fail resolving the credential, and a narrower type forces a cast at the only
// call site that has one.
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

// An unparseable body wraps like every other enrichment failure: a truncated
// 200 is as transient as a request that never arrived, and letting its error
// escape would put it outside the retry budget.
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
 * The only way to read the endpoint newest-first — it takes no `direction`, so
 * the last page is the sole handle on the newest comments. Without it a
 * truncated scan holds the oldest half of the window, the wrong half when the
 * newest mentioning comment decides trust.
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
 * Page 1 fixes the page count (`Link` is the only place it appears); from then
 * on the walk runs backwards, so exhausting the budget drops the oldest entries,
 * which makes truncation safe instead of failing the delivery. When the walk
 * never reaches page 2 those hundred entries are the oldest and are dropped.
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

// Paginated newest-first like the comment streams: the endpoint returns
// oldest-first, takes no `since`, and reading only page 1 hides the newest
// reviews past a hundred.
const fetchReviews = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
): Effect.Effect<readonly Schema.Schema.Type<typeof Review>[], NotificationError> =>
  fetchNewestFirst(client, `/repos/${repository}/pulls/${number}/reviews`, Review, undefined);

/**
 * The newest in-window timeline event that put her on this thread.
 *
 * `assigned` matches when she is the assignee; `review_requested` when she is
 * the requested reviewer. Returns the newest event whose causer is a trusted
 * sender — trust first, newest second, exactly like the mention path — or
 * `undefined` when there is nothing worth acting on.
 */
const fetchTriggeringEvent = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
  reason: 'assign' | 'review_requested',
  selfLogin: string,
  trusted: ReadonlySet<string>,
  since: number | undefined,
): Effect.Effect<Schema.Schema.Type<typeof TimelineEvent> | undefined, NotificationError> =>
  Effect.gen(function* () {
    const wanted = reason === 'assign' ? 'assigned' : 'review_requested';
    const events = yield* fetchNewestFirst(
      client,
      `/repos/${repository}/issues/${number}/events`,
      TimelineEvent,
      undefined,
    );
    const inWindow = events.filter((event) => {
      if (event.event !== wanted) return false;
      const at = parseDate(event.created_at);
      if (at === undefined || (since !== undefined && at < since)) return false;
      return reason === 'assign'
        ? normalizeLogin(event.assignee?.login ?? '') === selfLogin
        : // A team request names no individual — skipped rather than guessed.
          normalizeLogin(event.requested_reviewer?.login ?? '') === selfLogin;
    });
    // Trust first, newest second: trusting the newest match lets an untrusted
    // assigner silence a trusted one with assign → unassign → assign.
    const eligible = inWindow.filter((event) => {
      const causer = reason === 'assign' ? event.actor?.login : event.review_requester?.login;
      const author = normalizeLogin(causer ?? '');
      return author !== '' && author !== selfLogin && trusted.has(author);
    });
    const chosen = eligible.at(-1);
    if (chosen === undefined) return undefined;
    // Authority withdrawn after the trigger cancels it — acting anyway spends
    // work nobody asked for. Compared by position, not timestamp: the timeline
    // is chronological while timestamps have only second granularity.
    const chosenIndex = events.indexOf(chosen);
    const rescinded = events.some((event, index) => {
      if (index <= chosenIndex) return false;
      return reason === 'assign'
        ? event.event === 'unassigned' && normalizeLogin(event.assignee?.login ?? '') === selfLogin
        : event.event === 'review_request_removed' &&
            normalizeLogin(event.requested_reviewer?.login ?? '') === selfLogin;
    });
    return rescinded ? undefined : chosen;
  });

const EditNode = Schema.Struct({
  id: Schema.String,
  editor: Schema.optionalWith(User, { nullable: true }),
  lastEditedAt: Schema.optionalWith(Schema.String, { nullable: true }),
});

const EditsResponse = Schema.Struct({
  data: Schema.optionalWith(
    Schema.Struct({
      nodes: Schema.optionalWith(Schema.Array(Schema.NullOr(EditNode)), { nullable: true }),
      repository: Schema.optionalWith(
        Schema.Struct({
          issueOrPullRequest: Schema.optionalWith(EditNode, { nullable: true }),
        }),
        { nullable: true },
      ),
    }),
    { nullable: true },
  ),
  errors: Schema.optionalWith(Schema.Array(Schema.Struct({ message: Schema.String })), {
    nullable: true,
  }),
});

const EDITS_QUERY =
  'query($ids:[ID!]!,$owner:String!,$name:String!,$number:Int!,$nodes:Boolean!,$subject:Boolean!){nodes(ids:$ids) @include(if:$nodes){id ... on IssueComment{editor{login} lastEditedAt} ... on PullRequestReviewComment{editor{login} lastEditedAt} ... on PullRequestReview{editor{login} lastEditedAt}} repository(owner:$owner,name:$name) @include(if:$subject){issueOrPullRequest(number:$number){... on Issue{id editor{login} lastEditedAt} ... on PullRequest{id editor{login} lastEditedAt}}}}';

const toEdit = (node: Schema.Schema.Type<typeof EditNode>): Edit => ({
  editor: node.editor?.login,
  lastEditedAt: node.lastEditedAt ?? undefined,
});

const isEdited = (created: string, updated: string): boolean =>
  Date.parse(created) !== Date.parse(updated);

type Edits = { readonly nodes: ReadonlyMap<string, Edit>; readonly subject: Edit | undefined };

// GitHub's ceiling on one `nodes(ids:)` lookup. Reviews are asked about
// unconditionally, so a first sweep of a busy pull request can exceed it.
const EDITS_PAGE_SIZE = 100;

/**
 * Who last edited each candidate that may have been edited, and when.
 *
 * REST names no editor, so GraphQL is the only source of that identity. One
 * request serves the whole thread: comments and reviews by node id, the subject
 * by number. A subject's `updated_at` moves on any activity, not only a body
 * edit, so it is asked about on nearly every thread; a comment only when its
 * timestamps disagree; a review always, since REST gives it no `updated_at`.
 */
const fetchEdits = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
  nodeIds: readonly string[],
  subjectEdited: boolean,
): Effect.Effect<Edits, NotificationError> =>
  Effect.gen(function* () {
    const nodes = new Map<string, Edit>();
    if (nodeIds.length === 0 && !subjectEdited) return { nodes, subject: undefined };

    let subject: Edit | undefined;
    for (let start = 0; start === 0 || start < nodeIds.length; start += EDITS_PAGE_SIZE) {
      const page = yield* fetchEditsPage(
        client,
        repository,
        number,
        nodeIds.slice(start, start + EDITS_PAGE_SIZE),
        subjectEdited && start === 0,
      );
      for (const [id, edit] of page.nodes) nodes.set(id, edit);
      if (start === 0) subject = page.subject;
    }
    return { nodes, subject };
  });

const fetchEditsPage = <E>(
  client: Authenticated<E>,
  repository: string,
  number: number,
  nodeIds: readonly string[],
  subjectEdited: boolean,
): Effect.Effect<Edits, NotificationError> =>
  Effect.gen(function* () {
    const nodes = new Map<string, Edit>();
    const path = '/graphql';
    const [owner, name] = repository.split('/');
    const body = {
      query: EDITS_QUERY,
      variables: {
        ids: nodeIds,
        owner,
        name,
        number,
        nodes: nodeIds.length > 0,
        subject: subjectEdited,
      },
    };
    const response = yield* client
      .execute(HttpClientRequest.bodyUnsafeJson(HttpClientRequest.post(path), body))
      .pipe(
        Effect.mapError(
          (cause) =>
            new NotificationError({ message: `Could not reach GitHub for ${path}`, cause }),
        ),
      );
    if (response.status < 200 || response.status >= 300) {
      return yield* new NotificationError({
        message: `GitHub returned status ${response.status} for ${path}`,
      });
    }
    const parsed = yield* Effect.flatMap(readJson(response, path), (json) =>
      decodeJson(EditsResponse, json, path),
    );
    // A 200 with no `data` is the GraphQL shape of a failed request. Partial
    // data beside `errors` is kept: a node GitHub could not resolve comes back
    // null and its candidate keeps its creation, which is the safe direction.
    if (parsed.data === undefined) {
      return yield* new NotificationError({
        message: `GitHub returned no data for ${path}`,
        cause: parsed.errors,
      });
    }
    for (const node of parsed.data.nodes ?? []) {
      if (node !== null) nodes.set(node.id, toEdit(node));
    }
    const subjectNode = parsed.data.repository?.issueOrPullRequest;
    return { nodes, subject: subjectNode === undefined ? undefined : toEdit(subjectNode) };
  });

/**
 * ! An edited text belongs to the less trusted of its author and its editor.
 * ! REST names no editor, so keying on `updated_at` alone would attribute a
 * ! stranger's edit to the original, possibly trusted, author; and a trusted
 * ! user tidying a stranger's comment did not write the instruction in it, so
 * ! a foreign edit never lifts a body above the trust it was posted with. With
 * ! no named editor the candidate keeps its creation: lost, not misattributed.
 */
const attributed = (
  candidate: Candidate,
  edit: Edit | undefined,
  trusted: ReadonlySet<string>,
): Candidate => {
  if (edit?.editor === undefined || edit.lastEditedAt === undefined) return candidate;
  const at = parseDate(edit.lastEditedAt);
  if (at === undefined) return candidate;
  const original = candidate.author;
  const elevates =
    trusted.has(normalizeLogin(edit.editor)) &&
    (original === undefined || !trusted.has(normalizeLogin(original)));
  return {
    ...candidate,
    at,
    author: elevates ? original : edit.editor,
    editedAt: edit.lastEditedAt,
  };
};

const commentCandidate = (
  comment: Schema.Schema.Type<typeof Comment>,
  kind: 'issue_comment' | 'review_comment',
  edit: Edit | undefined,
  trusted: ReadonlySet<string>,
): Candidate =>
  attributed(
    {
      at: Date.parse(comment.created_at),
      author: comment.user?.login,
      ref: { kind, id: comment.id },
      url: comment.html_url,
      body: comment.body,
    },
    edit,
    trusted,
  );

/**
 * Order within one second, where GitHub's timestamps cannot separate candidates.
 * A total order, not a meaningful one: body candidates carry no id at all, and
 * issue/review-comment ids are disjoint sequences compared only so the winner
 * stays stable. Same-kind pairs do order correctly — the case this exists for.
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
    case 'assigned':
      return [4, candidate.ref.id];
    case 'review_requested':
      return [5, candidate.ref.id];
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
  /**
   * Whether a trusted trigger has armed this thread's live window. While true,
   * replies from participants this policy does not trust continue the work.
   */
  readonly live?: boolean;
}): Effect.Effect<QualifiedNotification, NotificationError, GitHubClient> =>
  Effect.scoped(
    Effect.gen(function* () {
      const lastActivityAt = Date.parse(input.thread.updated_at);

      // Checked first: every bound below derives from it, and an unreadable
      // `updated_at` leaves the comment scan with no anchor — a full history
      // scan and the delivery's whole attempt budget.
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

      // `undefined` allowed: with no cursor and no read mark the thread is new
      // to her, and newest-first walking makes an unbounded window drop the
      // oldest comments, not the triggering one.
      const since =
        input.cursorMs ??
        (input.thread.last_read_at === undefined
          ? undefined
          : parseDate(input.thread.last_read_at));

      const selfLogin = normalizeLogin(input.policy.selfLogin);
      const trusted = new Set(input.policy.trustedSenders.map(normalizeLogin));

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
        // The endpoint takes no `since`; unwindowed, every old review would
        // re-trigger on each sweep and be asked about on each.
        reviews = (yield* fetchReviews(client, repository, ref.number)).filter(
          (review) =>
            since === undefined ||
            (review.submitted_at !== undefined && Date.parse(review.submitted_at) >= since),
        );
      }

      const edits = yield* fetchEdits(
        client,
        repository,
        ref.number,
        [...conversationComments, ...reviewComments]
          .filter((comment) => isEdited(comment.created_at, comment.updated_at))
          .map((comment) => comment.node_id)
          .concat(reviews.map((review) => review.node_id)),
        isEdited(subject.created_at, subject.updated_at),
      );

      const candidates: Candidate[] = [
        ...conversationComments.map((comment) =>
          commentCandidate(comment, 'issue_comment', edits.nodes.get(comment.node_id), trusted),
        ),
        ...reviewComments.map((comment) =>
          commentCandidate(comment, 'review_comment', edits.nodes.get(comment.node_id), trusted),
        ),
        // Targeted at the pull request, not the review: GitHub has no reactions
        // endpoint for a review, while `contextUrl` still points at the review.
        ...reviews.map((review) =>
          attributed(
            {
              at: review.submitted_at === undefined ? Number.NaN : Date.parse(review.submitted_at),
              author: review.user?.login,
              ref: { kind: 'review', id: review.id, number: ref.number },
              url: review.html_url,
              body: review.body,
            },
            edits.nodes.get(review.node_id),
            trusted,
          ),
        ),
        attributed(
          {
            at: Date.parse(subject.created_at),
            author: subject.user?.login,
            ref: { kind: 'body', number: ref.number },
            url: subject.html_url,
            body: subject.body,
          },
          edits.subject,
          trusted,
        ),
      ];

      const windowed =
        since === undefined ? candidates : candidates.filter((candidate) => candidate.at >= since);

      const usable = windowed.flatMap((candidate) =>
        candidate.author === undefined || candidate.author === '' || Number.isNaN(candidate.at)
          ? []
          : [{ ...candidate, author: candidate.author }],
      );

      // The newest *trusted* mention triggers. Letting an untrusted one win and
      // then refusing hands every participant a mute button; self-activity is
      // excluded structurally, so a loop cannot be configured into existence.
      //
      // ! Trust is settled before the body is read, not after. Rendering is the
      // ! expensive half of qualification and `Bun.markdown` is superlinear on
      // ! unresolved link brackets, so deciding first is what keeps a stranger
      // ! from choosing how long this thread's single runtime is busy. Bounding
      // ! the cost instead would mean predicting the parser — an escaped or
      // ! code-spanned `]` closes a bracket for one counter and not the other.
      const addressed = usable.filter((candidate) => {
        const author = normalizeLogin(candidate.author);
        return author !== selfLogin && trusted.has(author);
      });
      const eligible = yield* Effect.filter(addressed, (candidate) =>
        candidate.body === undefined ? Effect.succeed(false) : mentions(candidate.body, selfLogin),
      );
      const mentionTrigger = eligible.reduce<(typeof usable)[number] | undefined>(
        (best, candidate) =>
          best === undefined ||
          candidate.at > best.at ||
          (candidate.at === best.at && outranks(candidate, best))
            ? candidate
            : best,
        undefined,
      );

      // Assignments carry no comment, so their actor comes from the timeline;
      // fetched even when a mention matched, because an untrusted participant
      // must not mute a trusted assignment with one throwaway mention.
      const reason = input.thread.reason;
      const assignedEvent =
        reason === 'assign' || reason === 'review_requested'
          ? yield* fetchTriggeringEvent(
              client,
              repository,
              ref.number,
              reason,
              selfLogin,
              trusted,
              since,
            )
          : undefined;

      // While live, any non-self reply continues at continuation strength —
      // trust was gated when the trigger armed liveness. A closed subject ends
      // it regardless of the stored window; opening an issue is not replying.
      const live = input.live === true && subject.state !== 'closed';
      const continuationTrigger = live
        ? usable.reduce<(typeof usable)[number] | undefined>((best, candidate) => {
            if (candidate.ref.kind === 'body') return best;
            if (normalizeLogin(candidate.author) === selfLogin) return best;
            return best === undefined || candidate.at > best.at ? candidate : best;
          }, undefined)
        : undefined;

      if (
        mentionTrigger === undefined &&
        assignedEvent === undefined &&
        continuationTrigger === undefined
      ) {
        // Names who tried, and decides nothing — so it reads the raw body
        // rather than rendering an untrusted one. It over-reports a mention
        // shown as code, which is the harmless way for a log line to be wrong.
        const attempted = mentionPattern(selfLogin);
        const untrustedSenders = [
          ...new Set(
            usable.flatMap((candidate) =>
              candidate.body !== undefined && attempted.test(candidate.body)
                ? [normalizeLogin(candidate.author)]
                : [],
            ),
          ),
        ].sort();

        if (untrustedSenders.length > 0) {
          yield* Effect.logInfo('Notification dropped: no trusted sender mentioned her').pipe(
            Effect.annotateLogs({ repository, senders: untrustedSenders.join(',') }),
          );
        } else {
          yield* Effect.logDebug(
            'Notification did not become work: no candidate mentions the target user',
          ).pipe(Effect.annotateLogs({ threadId: input.thread.id }));
        }
        return { work: undefined, lastActivityAt };
      }

      // ! A trusted trigger outranks any continuation whatever the timestamps:
      // ! continuations inherit authority from the arming trigger, so letting a
      // ! stranger's later comment demote a trusted command strips it unearned.
      // ! Between trusted triggers, newest wins — the timeline event on ties.
      const assignedAt =
        assignedEvent === undefined ? -1 : (parseDate(assignedEvent.created_at) ?? -1);
      const useAssigned = assignedEvent !== undefined && assignedAt >= (mentionTrigger?.at ?? -1);
      const hasTrustedTrigger = assignedEvent !== undefined || mentionTrigger !== undefined;
      const useContinuation = !hasTrustedTrigger && continuationTrigger !== undefined;

      const common = {
        deliveryId: input.deliveryId,
        repository,
        targets: [selfLogin],
        subject: {
          kind: ref.kind,
          number: ref.number,
          title: subject.title,
          url: subject.html_url,
        },
      } as const;

      if (useAssigned && assignedEvent !== undefined) {
        // Trust was already applied inside `fetchTriggeringEvent`.
        const causer =
          reason === 'assign' ? assignedEvent.actor?.login : assignedEvent.review_requester?.login;
        const sender = normalizeLogin(causer ?? '');
        const contextKind = reason === 'assign' ? 'assigned' : 'review_requested';
        const work: WorkItem = {
          ...common,
          interactionId: JSON.stringify([
            repository,
            ref.kind,
            ref.number,
            contextKind,
            assignedEvent.id,
            sender,
            selfLogin,
          ]),
          sender,
          reasons: [contextKind],
          contextUrl: subject.html_url,
          context: { kind: contextKind, id: assignedEvent.id, number: ref.number },
        };
        return { work, lastActivityAt };
      }

      const triggering = useContinuation ? continuationTrigger : mentionTrigger;
      if (triggering === undefined) return { work: undefined, lastActivityAt };
      const sender = normalizeLogin(triggering.author);
      const continued = useContinuation;
      const work: WorkItem = {
        ...common,
        // Identities, plus the edit time only where an edit was attributed: an
        // unedited candidate keeps a timestamp-free identity so a replay dedupes,
        // while a second edit of the same text is a second instruction.
        interactionId: JSON.stringify([
          repository,
          ref.kind,
          ref.number,
          continued ? 'continued' : 'mentioned',
          triggering.ref,
          sender,
          selfLogin,
          ...(triggering.editedAt === undefined ? [] : [triggering.editedAt]),
        ]),
        sender,
        reasons: [continued ? 'continued' : 'mentioned'],
        ...(continued ? { continuation: true } : {}),
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
