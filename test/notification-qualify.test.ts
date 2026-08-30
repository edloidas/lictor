import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Exit, Layer, Ref } from 'effect';
import { GitHubClient } from '../src/github/client.ts';
import { qualifyNotification } from '../src/notifications/qualify.ts';
import type { NotificationThread } from '../src/notifications/thread.ts';

const policy = { selfLogin: 'adiutriel', trustedSenders: ['edloidas', 'friend'] };

const thread = (overrides: Partial<NotificationThread> = {}): NotificationThread => ({
  id: '14567',
  unread: true,
  reason: 'mention',
  updated_at: '2026-08-21T10:00:00Z',
  subject: {
    title: 'Something broke',
    url: 'https://api.github.com/repos/edloidas/sandbox/issues/7',
    latest_comment_url: 'https://api.github.com/repos/edloidas/sandbox/issues/comments/99',
    type: 'Issue',
  },
  repository: { full_name: 'edloidas/sandbox' },
  ...overrides,
});

const issue = (overrides: Record<string, unknown> = {}) => ({
  title: 'Something broke',
  html_url: 'https://github.com/edloidas/sandbox/issues/7',
  body: 'plain description',
  user: { login: 'edloidas' },
  created_at: '2026-08-20T08:00:00Z',
  updated_at: '2026-08-20T08:00:00Z',
  ...overrides,
});

// `created_at` mirrors `updated_at` by default. Candidates key on creation, so
// a fixture that omitted it would silently fall outside every window.
const comment = (overrides: Record<string, unknown> = {}) => {
  const base = {
    id: 99,
    html_url: 'https://github.com/edloidas/sandbox/issues/7#issuecomment-99',
    body: 'hey @adiutriel take a look',
    user: { login: 'edloidas' },
    created_at: '2026-08-21T10:00:00Z',
    updated_at: '2026-08-21T10:00:00Z',
  };
  const merged = { ...base, ...overrides };
  return 'updated_at' in overrides && !('created_at' in overrides)
    ? { ...merged, created_at: overrides.updated_at }
    : merged;
};

/**
 * Routes by path so one stub serves the subject fetch and both comment streams.
 * The first matching entry wins, and an unmatched path is a 404 rather than a
 * silent empty body — a route the code reaches unexpectedly must fail loudly.
 */
type Reply = {
  readonly status?: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
};

const run = <A, E>(
  effect: Effect.Effect<A, E, GitHubClient>,
  routes: readonly (readonly [string, Reply])[],
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const requests = yield* Ref.make<string[]>([]);
        const client = HttpClient.make((request) =>
          Ref.update(requests, (items) => [...items, request.url]).pipe(
            Effect.as(
              (() => {
                const match = routes.find(([fragment]) => request.url.includes(fragment));
                const reply = match?.[1] ?? { status: 404, body: { message: 'Not Found' } };
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(JSON.stringify(reply.body ?? {}), {
                    status: reply.status ?? 200,
                    headers: { 'content-type': 'application/json', ...reply.headers },
                  }),
                );
              })(),
            ),
          ),
        );
        const GitHubLive = Layer.succeed(
          GitHubClient,
          GitHubClient.make({
            authenticated: Effect.succeed(
              client.pipe(
                HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test')),
              ),
            ),
            addReaction: () => Effect.succeed(undefined),
          }),
        );
        const exit = yield* Effect.exit(Effect.provide(effect, GitHubLive));
        return { exit, requests: yield* Ref.get(requests) };
      }),
    ),
  );

const issueRoutes = (
  subject: Record<string, unknown>,
  comments: readonly Record<string, unknown>[],
) =>
  [
    ['/issues/7/comments', { body: comments }],
    ['/pulls/7/comments', { body: [] }],
    ['/issues/7', { body: subject }],
  ] as const;

const qualified = <A>(exit: Exit.Exit<A, unknown>): A => {
  if (Exit.isFailure(exit)) throw new Error(`qualification failed: ${String(exit.cause)}`);
  return exit.value;
};

describe('qualifyNotification', () => {
  // `reason` is an exclusion list, not an allow list. It describes the thread,
  // not the activity that just landed on it, so a thread that went unread as
  // `assign` and then received a mention still reports `assign` — refusing to
  // scan it would discard the mention outright.
  it('scans a thread whose reason is not mention', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      // The assignment path runs alongside the mention scan now, so the
      // timeline route must answer even when a mention decides the trigger.
      [['/issues/7/events', { body: [] }], ...issueRoutes(issue(), [comment()])],
    );

    expect(qualified(result.exit).work?.sender).toBe('edloidas');
  });

  it('skips machine traffic without spending a request, and still advances the cursor', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'notification:14567:2026-08-21T10:00:00Z',
        thread: thread({ reason: 'ci_activity' }),
        policy,
        cursorMs: undefined,
      }),
      [],
    );

    const value = qualified(result.exit);
    expect(value.work).toBeUndefined();
    expect(value.lastActivityAt).toBe(Date.parse('2026-08-21T10:00:00Z'));
    expect(result.requests).toEqual([]);
  });

  it('returns no work when the subject url carries no issue number', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({
          subject: {
            title: 'A discussion',
            url: 'https://api.github.com/repos/edloidas/sandbox/discussions/3',
            type: 'Discussion',
          },
        }),
        policy,
        cursorMs: undefined,
      }),
      [],
    );

    expect(qualified(result.exit).work).toBeUndefined();
    expect(result.requests).toEqual([]);
  });

  it('turns a trusted mention in a comment into work', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment()]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('edloidas');
    expect(work?.repository).toBe('edloidas/sandbox');
    expect(work?.reasons).toEqual(['mentioned']);
    expect(work?.targets).toEqual(['adiutriel']);
    expect(work?.subject).toEqual({
      kind: 'issue',
      number: 7,
      title: 'Something broke',
      url: 'https://github.com/edloidas/sandbox/issues/7',
    });
    expect(work?.context).toEqual({ kind: 'issue_comment', id: 99 });
    expect(work?.contextUrl).toBe('https://github.com/edloidas/sandbox/issues/7#issuecomment-99');
  });

  // An identity, not an event. A timestamp or an html url in here makes every
  // edit of the same comment a second job.
  it('builds an interaction id from identities only', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment()]),
    );

    const id = qualified(result.exit).work?.interactionId ?? '';
    expect(id).toContain('issue_comment');
    expect(id).not.toContain('2026-');
    expect(id).not.toContain('github.com');
  });

  it('treats a body mention with no comments as the trigger', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(
        issue({
          body: 'cc @adiutriel please look',
          // A freshly opened issue: the notification is about its creation.
          created_at: '2026-08-21T10:00:00Z',
          updated_at: '2026-08-21T10:00:00Z',
        }),
        [],
      ),
    );

    const work = qualified(result.exit).work;
    expect(work?.context).toEqual({ kind: 'body', number: 7 });
    expect(work?.sender).toBe('edloidas');
  });

  it('lets the newest mentioning comment in one window decide the sender', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [
        comment({ id: 98, updated_at: '2026-08-21T09:00:00Z', user: { login: 'edloidas' } }),
        comment({ id: 99, updated_at: '2026-08-21T10:00:00Z', user: { login: 'friend' } }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('friend');
    expect(work?.context).toEqual({ kind: 'issue_comment', id: 99 });
  });

  // An untrusted mention must not suppress a trusted one. Letting the newest
  // mention win and then refusing it hands every repository participant a mute
  // button: mention her after a trusted request and the whole thread is dropped,
  // the cursor advances, and the real request is never rescanned.
  it('ignores an untrusted mention layered over a trusted one', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [
        comment({ id: 98, updated_at: '2026-08-21T09:00:00Z', user: { login: 'edloidas' } }),
        comment({ id: 99, updated_at: '2026-08-21T10:00:00Z', user: { login: 'stranger' } }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('edloidas');
    expect(work?.context).toEqual({ kind: 'issue_comment', id: 98 });
  });

  it('returns no work when only untrusted senders mentioned her', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment({ user: { login: 'stranger' } })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // `comment.user` is who wrote the comment; REST reports no editor. Keying a
  // candidate on `updated_at` would let a maintainer rewrite someone else's
  // comment and have the job attributed to that trusted author.
  it('ignores a mention inserted by editing an existing comment', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: Date.parse('2026-08-21T09:00:00Z'),
      }),
      issueRoutes(issue(), [
        comment({ created_at: '2026-08-20T08:00:00Z', updated_at: '2026-08-21T10:00:00Z' }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  it('drops her own activity structurally', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment({ user: { login: 'Adiutriel' } })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  it('returns no work when nothing in the window mentions her', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment({ body: 'unrelated chatter' })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // A near-miss must not match. `@adiutriel-bot` is a different account, and
  // matching it would put someone else's mention into her queue.
  it('does not match a login that merely prefixes hers', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment({ body: 'ping @adiutriel-bot instead' })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // `subject.user` is who opened the issue, and REST exposes no editor
  // identity for a body. Keying the body candidate on `updated_at` would let
  // anyone able to edit the body insert a mention and have it attributed to the
  // original — possibly trusted — author.
  it('ignores a body mention inserted after the issue was opened', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: Date.parse('2026-08-21T09:00:00Z'),
      }),
      issueRoutes(
        issue({
          body: 'cc @adiutriel',
          created_at: '2026-08-20T08:00:00Z',
          updated_at: '2026-08-21T10:00:00Z',
        }),
        [],
      ),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // `GET /issues/{n}/comments` takes no `direction`, so `Link: rel="last"` is
  // the only handle on the newest comments. Walking back from the last page is
  // what makes the page budget a safe truncation: it drops the oldest comments,
  // never the one that decides trust.
  it('walks back from the last page to reach the newest comments', async () => {
    const oldest = comment({ id: 10, updated_at: '2026-08-21T08:00:00Z', body: 'no mention' });
    const newest = comment({ id: 900, updated_at: '2026-08-21T10:00:00Z' });
    const link = '<https://api.github.test/x?page=3>; rel="last"';
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      [
        ['/issues/7/comments?per_page=100&page=3', { body: [newest], headers: { link } }],
        ['/issues/7/comments?per_page=100&page=2', { body: [], headers: { link } }],
        ['/issues/7/comments?per_page=100&page=1', { body: [oldest], headers: { link } }],
        ['/pulls/7/comments', { body: [] }],
        ['/issues/7', { body: issue() }],
      ],
    );

    expect(qualified(result.exit).work?.context).toEqual({ kind: 'issue_comment', id: 900 });
    // Within budget, so page 1 is kept rather than discarded — the arithmetic
    // that decides this is off by one in the obvious wrong direction.
    expect(result.requests.filter((url) => url.includes('/issues/7/comments'))).toHaveLength(3);
  });

  // Past the budget the oldest pages go, and page 1 with them. This is the
  // branch that must never be reached by an in-budget thread, and the only one
  // that logs; the assertion below pins which half of the window survives.
  it('drops the oldest pages once the page budget is exhausted', async () => {
    const link = '<https://api.github.test/x?page=15>; rel="last"';
    const trigger = comment({ id: 1500, updated_at: '2026-08-21T10:00:00Z' });
    const buried = comment({ id: 1, updated_at: '2026-08-21T07:00:00Z' });
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      [
        ['/issues/7/comments?per_page=100&page=15', { body: [trigger], headers: { link } }],
        ['/issues/7/comments?per_page=100&page=1', { body: [buried], headers: { link } }],
        ['/issues/7/comments', { body: [], headers: { link } }],
        ['/pulls/7/comments', { body: [] }],
        ['/issues/7', { body: issue() }],
      ],
    );

    const pages = result.requests.filter((url) => url.includes('/issues/7/comments'));
    // Page 1 to learn the shape, then nine more walking back from the last: the
    // budget counts total requests, so pages 15 down to 7 and no further.
    expect(pages).toHaveLength(10);
    expect(pages.some((url) => url.includes('page=15'))).toBe(true);
    expect(pages.some((url) => url.includes('page=5'))).toBe(false);
    expect(qualified(result.exit).work?.context).toEqual({ kind: 'issue_comment', id: 1500 });
  });

  // Every bound in the scan derives from `updated_at`, and the schema accepts
  // any string. Without a guard an unreadable one leaves the comment scan with
  // no anchor, which turns a malformed envelope into a full history scan.
  it('drops a thread whose activity time is unreadable', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ updated_at: 'not a date' }),
        policy,
        cursorMs: undefined,
      }),
      [],
    );

    expect(qualified(result.exit).work).toBeUndefined();
    expect(result.requests).toEqual([]);
  });

  it('fails with NotificationError when GitHub refuses the subject fetch', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      [['/issues/7', { status: 502, body: { message: 'Bad gateway' } }]],
    );

    expect(Exit.isFailure(result.exit)).toBe(true);
    expect(String(result.exit)).toContain('NotificationError');
  });

  // The scan is never unbounded. A cursor anchors it; failing that the read
  // mark does; failing that a short grace around the notification's own
  // `updated_at`, because scanning an old issue whole would put every comment
  // it ever received into one window and overflow the page ceiling.
  it('anchors the comment scan on the cursor when there is one', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: Date.parse('2026-08-21T09:30:00Z'),
      }),
      issueRoutes(issue(), [comment()]),
    );

    expect(result.requests.find((url) => url.includes('/issues/7/comments')) ?? '').toContain(
      'since=2026-08-21T09%3A30%3A00.000Z',
    );
  });

  it('falls back to the read mark, and sends no since when there is neither', async () => {
    const read = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ last_read_at: '2026-08-20T09:00:00Z' }),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment()]),
    );
    const unread = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment()]),
    );

    const commentsCall = (requests: readonly string[]) =>
      requests.find((url) => url.includes('/issues/7/comments')) ?? '';
    expect(commentsCall(read.requests)).toContain('since=2026-08-20T09%3A00%3A00.000Z');
    expect(commentsCall(unread.requests)).not.toContain('since=');
  });

  const workFor = async (body: string) => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue(), [comment({ body })]),
    );
    return qualified(result.exit).work;
  };

  // GitHub's reply button quotes what it replies to, and GitHub notifies on a
  // mention inside a blockquote. Without stripping, a trusted reader agreeing
  // with "@adiutriel do X" produces a second job that does X again. The quote
  // runs past its `>` lines: GitHub renders an unprefixed line following one
  // inside the quote, so a lazy continuation would re-issue the instruction
  // under the quoter's own name.
  it.each([
    ['a strict quote', '> hey @adiutriel take a look\n\nagreed'],
    ['a lazy continuation', '> please close this\n@adiutriel please close this\n\nagreed'],
    ['a lazy multi-line quote', '> the ask was\nrelayed as\n@adiutriel do X\n\nagreed'],
    ['a quote inside a list item', '- > @adiutriel do X\nstill quoted'],
    ['an inline code span', 'write `@adiutriel` to summon her'],
    ['a fenced block', '```\n@adiutriel do X\n```\nplain'],
    ['a fence nobody closed', 'plain\n```\n@adiutriel do X'],
    ['a tilde line inside a backtick fence', '```\n~~~\n@adiutriel do X\n```'],
    ['a CRLF fence', 'a\r\n```\r\n@adiutriel do X\r\n```\r\nb'],
    ['a fence inside a list item', '- ```\n  @adiutriel do X\n  ```'],
    ['a quote nested in two list items', '- - > @adiutriel do X'],
    // GitHub keeps both inside the quote: four spaces past the marker is code,
    // and a marker that cannot interrupt a paragraph does not end one.
    ['a lazy line indented to code depth', '> quoted\n    - @adiutriel do X'],
    ['a lazy line under a marker that cannot interrupt', '> quoted\n2. @adiutriel do X'],
    ['a fence whose closer sits past the code indent', '```\n    ```\n@adiutriel do X\n```'],
    // Dedenting out of the list does not close that fence — GitHub reads the
    // line as opening a new one, and swallows what follows as code.
    ['a list fence closed from the margin', '- ```\n  code\n```\nafter @adiutriel do Y'],
  ])('does not treat a mention displayed in %s as a fresh one', async (_shape, body) => {
    expect(await workFor(body)).toBeUndefined();
  });

  // The other half of the same property: a quote that never ends swallows the
  // reply carrying the real instruction. It ends where CommonMark ends a
  // paragraph — a blank line, or a line opening a new block.
  it.each([
    ['a blank line', '> quoted\n\n@adiutriel do X'],
    ['a heading', '> quoted\n# next\n@adiutriel do X'],
    ['a list item', '> quoted\n- @adiutriel do X'],
    ['a fence', '> quoted\n```\ncode\n```\n@adiutriel do X'],
  ])('ends a quote at %s and reads the mention after it', async (_shape, body) => {
    expect((await workFor(body))?.sender).toBe('edloidas');
  });

  // Held to a fixed indent instead of its opener's, an indented fence never
  // closes and swallows every mention after it in the comment.
  it.each([
    ['indented under a list item', '1.  Do this:\n\n    ```\n    code\n    ```\n\n@adiutriel do X'],
    ['opened on a list marker', '- ```\n  code\n  ```\n\n@adiutriel do X'],
    ['indented with a tab', '\t```\n\tcode\n\t```\n\n@adiutriel do X'],
  ])('closes a fence %s and reads the mention after it', async (_shape, body) => {
    expect((await workFor(body))?.sender).toBe('edloidas');
  });

  it('reads review comments for a pull request and can trigger on one', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({
          subject: {
            title: 'Fix the thing',
            url: 'https://api.github.com/repos/edloidas/sandbox/pulls/12',
            type: 'PullRequest',
          },
        }),
        policy,
        cursorMs: undefined,
      }),
      [
        ['/issues/12/comments', { body: [] }],
        [
          '/pulls/12/comments',
          {
            body: [
              comment({
                id: 501,
                html_url: 'https://github.com/edloidas/sandbox/pull/12#discussion_r501',
              }),
            ],
          },
        ],
        ['/pulls/12/reviews', { body: [] }],
        [
          '/issues/12',
          { body: issue({ html_url: 'https://github.com/edloidas/sandbox/pull/12' }) },
        ],
      ],
    );

    const work = qualified(result.exit).work;
    expect(work?.subject.kind).toBe('pull_request');
    expect(work?.context).toEqual({ kind: 'review_comment', id: 501 });
    expect(result.requests.some((url) => url.includes('/pulls/12/comments'))).toBe(true);
  });

  // `/pulls/{n}/comments` holds only the inline threads. A mention in the body
  // of a submitted review lives on `/pulls/{n}/reviews`, notifies just the same,
  // and was covered by the deleted `pull_request_review` webhook handler.
  it('triggers on a mention in a submitted review body', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({
          subject: {
            title: 'Fix the thing',
            url: 'https://api.github.com/repos/edloidas/sandbox/pulls/12',
            type: 'PullRequest',
          },
        }),
        policy,
        cursorMs: undefined,
      }),
      [
        ['/issues/12/comments', { body: [] }],
        ['/pulls/12/comments', { body: [] }],
        [
          '/pulls/12/reviews',
          {
            body: [
              {
                id: 7001,
                html_url: 'https://github.com/edloidas/sandbox/pull/12#pullrequestreview-7001',
                body: 'looks off, @adiutriel can you check',
                user: { login: 'friend' },
                submitted_at: '2026-08-21T10:00:00Z',
              },
            ],
          },
        ],
        [
          '/issues/12',
          { body: issue({ html_url: 'https://github.com/edloidas/sandbox/pull/12' }) },
        ],
      ],
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('friend');
    expect(work?.contextUrl).toContain('pullrequestreview-7001');
    // Keyed on the review's own id even though it reacts on the pull request:
    // collapsing reviews into `body` would give every review on one pull request
    // the same `interactionId`, and a reviewer's second instruction would be
    // deduped away as a replay of the first.
    expect(work?.context).toEqual({ kind: 'review', id: 7001, number: 12 });
  });

  it('ignores a comment with no author', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
      }),
      issueRoutes(issue({ body: 'no mention here' }), [comment({ user: null })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  const event = (overrides: Record<string, unknown> = {}) => ({
    id: 3001,
    event: 'assigned',
    actor: { login: 'edloidas' },
    assignee: { login: 'adiutriel' },
    created_at: '2026-08-21T10:00:00Z',
    ...overrides,
  });
  const eventsRoute = (events: readonly Record<string, unknown>[]) =>
    // Before `/issues/7`: the fragment matcher picks the first substring hit,
    // and the events path contains the subject path.
    [['/issues/7/events', { body: events }], ...issueRoutes(issue(), [])] as const;

  // The whole point of the timeline fetch: an assignment carries no comment,
  // so the mention scan cannot see it and the actor must come from `assigned`.
  it('acts on a trusted assignment with no mention anywhere', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([event()]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('edloidas');
    expect(work?.reasons).toEqual(['assigned']);
    expect(work?.context).toEqual({ kind: 'assigned', id: 3001, number: 7 });
    expect(work?.contextUrl).toBe('https://github.com/edloidas/sandbox/issues/7');
  });

  it('drops an assignment from an untrusted actor', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([event({ actor: { login: 'stranger' } })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  it('drops her own assignment structurally', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([event({ actor: { login: 'adiutriel' } })]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  it('ignores an assignment that predates the window', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: Date.parse('2026-08-22T00:00:00Z'),
      }),
      eventsRoute([event()]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  it('acts on a trusted review request naming her individually', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'review_requested' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({
          event: 'review_requested',
          review_requester: { login: 'friend' },
          requested_reviewer: { login: 'adiutriel' },
          assignee: null,
        }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('friend');
    expect(work?.reasons).toEqual(['review_requested']);
    expect(work?.context).toEqual({ kind: 'review_requested', id: 3001, number: 7 });
  });

  // A team request names no individual reviewer, so there is nobody to
  // attribute and nobody to trust — skipped rather than guessed.
  it('skips a review request aimed at a team', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'review_requested' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({
          event: 'review_requested',
          review_requester: { login: 'edloidas' },
          requested_reviewer: null,
          assignee: null,
        }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // A withdrawal of the other kind does not cancel: she can be assigned and
  // review-requested on the same pull request, and dropping one must not
  // silence the other.
  it('keeps a review request alive when her assignment is withdrawn', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'review_requested' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({ created_at: '2026-08-21T09:00:00Z' }),
        event({
          id: 3002,
          event: 'unassigned',
          actor: { login: 'edloidas' },
          created_at: '2026-08-21T10:30:00Z',
        }),
        event({
          id: 3003,
          event: 'review_requested',
          review_requester: { login: 'friend' },
          requested_reviewer: { login: 'adiutriel' },
          assignee: null,
          created_at: '2026-08-21T10:00:00Z',
        }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('friend');
    expect(work?.reasons).toEqual(['review_requested']);
  });

  // Same-second events cannot be ordered by GitHub's timestamps, so the
  // withdrawal scan compares positions in the timeline instead.
  it('cancels a same-second assignment withdrawn after it', async () => {
    const stamp = '2026-08-21T10:00:00Z';
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({ created_at: stamp }),
        event({
          id: 3002,
          event: 'unassigned',
          actor: { login: 'edloidas' },
          created_at: stamp,
        }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // Bot churn — assign, unassign, reassign inside one second. The final
  // assignment stands because its withdrawal precedes it positionally.
  it('keeps a same-second assignment reassigned after a withdrawal', async () => {
    const stamp = '2026-08-21T10:00:00Z';
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({ created_at: stamp }),
        event({
          id: 3002,
          event: 'unassigned',
          actor: { login: 'edloidas' },
          created_at: stamp,
        }),
        event({ id: 3003, created_at: stamp }),
      ]),
    );

    expect(qualified(result.exit).work?.sender).toBe('edloidas');
  });

  // Authority that was withdrawn is not authority. The thread is marked read
  // once committed, so acting on a rescinded assignment spends work nobody
  // asked for by the time anyone could notice.
  it('drops an assignment that was rescinded after it was made', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event(),
        event({
          id: 3002,
          event: 'unassigned',
          actor: { login: 'edloidas' },
          created_at: '2026-08-21T10:30:00Z',
        }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // Trust first, newest second. Newest-then-trust would let an untrusted
  // assigner silence a trusted one with assign → unassign → assign.
  it('lets the newest trusted assignment win when an untrusted one is newer', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      eventsRoute([
        event({ created_at: '2026-08-21T09:00:00Z' }),
        event({
          id: 3011,
          actor: { login: 'stranger' },
          created_at: '2026-08-21T11:00:00Z',
        }),
      ]),
    );

    expect(qualified(result.exit).work?.sender).toBe('edloidas');
  });

  // The mute-button guard, timeline edition: an untrusted throwaway mention
  // must not suppress a trusted assignment in the same window.
  it('prefers a trusted assignment over an untrusted mention in the window', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread({ reason: 'assign' }),
        policy,
        cursorMs: undefined,
      }),
      [
        ['/issues/7/events', { body: [event()] }],
        ...issueRoutes(issue(), [comment({ user: { login: 'stranger' } })]),
      ],
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('edloidas');
    expect(work?.reasons).toEqual(['assigned']);
  });

  // The whole point of liveness: a reply this policy does not trust keeps the
  // work going inside the armed window, at continuation strength.
  it('continues live work from an untrusted non-mentioning reply', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
        live: true,
      }),
      issueRoutes(issue(), [
        comment({ id: 200, user: { login: 'stranger' }, body: 'the fix goes in src/x.ts' }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('stranger');
    expect(work?.reasons).toEqual(['continued']);
    expect(work?.continuation).toBe(true);
    expect(work?.context).toEqual({ kind: 'issue_comment', id: 200 });
  });

  it('ignores an untrusted reply when the thread is not live', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
        live: false,
      }),
      issueRoutes(issue(), [
        comment({ id: 200, user: { login: 'stranger' }, body: 'the fix goes in src/x.ts' }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });

  // A closed subject ends the conversation regardless of the stored window:
  // the work is done and nobody should be able to reopen it by commenting.
  // A trusted trigger outranks any continuation, whatever the timestamps: a
  // stranger's later comment must not demote a trusted command to a stripped
  // continuation turn.
  it('keeps a trusted mention authoritative when an untrusted reply is newer', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: Date.parse('2026-08-21T09:00:00Z'),
        live: true,
      }),
      issueRoutes(issue(), [
        comment({ created_at: '2026-08-21T10:00:00Z' }),
        comment({
          id: 200,
          user: { login: 'stranger' },
          body: 'btw unrelated',
          created_at: '2026-08-21T10:05:00Z',
          updated_at: '2026-08-21T10:05:00Z',
        }),
      ]),
    );

    const work = qualified(result.exit).work;
    expect(work?.sender).toBe('edloidas');
    expect(work?.reasons).toEqual(['mentioned']);
    expect(work?.continuation).toBeUndefined();
  });

  it('stops continuing once the subject is closed', async () => {
    const result = await run(
      qualifyNotification({
        deliveryId: 'delivery',
        thread: thread(),
        policy,
        cursorMs: undefined,
        live: true,
      }),
      issueRoutes(issue({ state: 'closed' }), [
        comment({ id: 200, user: { login: 'stranger' }, body: 'please also do X' }),
      ]),
    );

    expect(qualified(result.exit).work).toBeUndefined();
  });
});
