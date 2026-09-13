import type { HttpClient, HttpClientResponse } from '@effect/platform';
import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Effect } from 'effect';
import { bounded } from '../bounded.ts';
import { Policy } from '../policy.ts';
import { type QueuedJob, WorkQueue } from '../queue/work-queue.ts';
import { GitHubClient } from './client.ts';
import { CredentialHealth } from './credential-health.ts';
import {
  type BrokerTool,
  type GrantCapabilities,
  grantCapabilities,
  grantedTools,
  intersectCapabilities,
  toolCapabilities,
} from './grant.ts';
import { GitHubIdentity } from './identity.ts';
import { DEFAULT_THROTTLE_WAIT_MS, isSecondaryRateLimit, retryAfterMs } from './retry-after.ts';

export class CapabilityError extends Data.TaggedError('CapabilityError')<{
  readonly code: string;
  readonly message: string;
  /** How long to wait before retrying, when GitHub said so. */
  readonly retryAfterMs?: number;
  readonly cause?: unknown;
}> {}

const issueNumber = { type: 'integer', minimum: 1, description: 'Issue number.' } as const;
const commentableNumber = {
  type: 'integer',
  minimum: 1,
  description: 'Issue or pull request number; both are addressed as issues here.',
} as const;
const pullNumber = { type: 'integer', minimum: 1, description: 'Pull request number.' } as const;
const pageNumber = {
  type: 'integer',
  minimum: 1,
  description: 'Page of results, three per page. Defaults to 1.',
} as const;
const repositoryProperty = {
  type: 'string',
  description:
    'Optional `owner/name`. It must match the repository the job was created for; any other value is denied.',
} as const;
const reviewId = {
  type: 'integer',
  minimum: 1,
  description: 'Review id, from `list_reviews`.',
} as const;
const reviewEvent = {
  type: 'string',
  enum: ['APPROVE', 'REQUEST_CHANGES', 'COMMENT'],
  description:
    'Verdict to submit. `APPROVE` and `REQUEST_CHANGES` are denied on a pull request this account opened, and on a turn that continues earlier work; `COMMENT` always stands.',
} as const;
const threadId = {
  type: 'string',
  description: 'Thread node id — the `id` of a thread from `list_review_threads`.',
} as const;

/**
 * The whole input object becomes the request body, so a property this table
 * does not name is one the agent has to guess. What it advertises has to agree
 * with what `route` enforces, `ref` patterns included.
 */
const toolSchemas: Readonly<
  Record<
    BrokerTool,
    {
      readonly description: string;
      readonly properties: Readonly<Record<string, unknown>>;
      readonly required?: readonly string[];
    }
  >
> = {
  get_repository: {
    // `permissions` in the response is the daemon account's rights on the
    // repository, which are wider than this job's grants. The advertised tool
    // list is what this job may do; this says nothing about it.
    description:
      "Read the job repository: default branch, visibility, and the account's own repository rights — which are not this job's permissions.",
    properties: {},
  },
  get_issue: {
    description: 'Read one issue: title, body, state, labels and assignees.',
    properties: { number: issueNumber },
    required: ['number'],
  },
  get_pull_request: {
    description: 'Read one pull request: title, body, state, head and base refs, mergeability.',
    properties: { number: pullNumber },
    required: ['number'],
  },
  list_comments: {
    description: 'List comments on an issue or pull request, three per page, oldest first.',
    properties: { number: commentableNumber, page: pageNumber },
    required: ['number'],
  },
  list_review_threads: {
    description:
      'List review threads on a pull request, ten per page: per thread its node id, resolution, whether it is outdated and where it points; per comment its node id, its `databaseId` — which is what `reply_review_comment` takes — its author and the diff hunk it hangs on. The pull request carries its own author and `viewerCanUpdate`, which says whether resolving is permitted here at all.',
    properties: {
      number: pullNumber,
      after: {
        type: 'string',
        description:
          'Cursor from the previous page, `pageInfo.endCursor`. Omit for the first page.',
      },
    },
    required: ['number'],
  },
  list_reviews: {
    description:
      "List reviews on a pull request, three per page: id, state — `PENDING`, `APPROVED`, `CHANGES_REQUESTED`, `COMMENTED` or `DISMISSED` — body, author and the commit reviewed. Read it before reviewing or replying: this account's own `PENDING` review holds a reply invisible until it is submitted, and a review an earlier attempt created is found here rather than posted a second time.",
    properties: { number: pullNumber, page: pageNumber },
    required: ['number'],
  },
  list_review_comments: {
    description: 'List review comments on a pull request, three per page.',
    properties: { number: pullNumber, page: pageNumber },
    required: ['number'],
  },
  create_comment: {
    description: 'Post a comment on an issue or pull request.',
    properties: {
      number: commentableNumber,
      body: { type: 'string', description: 'Comment text, GitHub-flavored markdown.' },
    },
    required: ['number', 'body'],
  },
  create_issue: {
    description: 'Open a new issue on the job repository.',
    properties: {
      title: { type: 'string' },
      body: { type: 'string', description: 'Description, GitHub-flavored markdown.' },
    },
    required: ['title'],
  },
  update_issue: {
    description: 'Update an issue. Only the fields sent are changed.',
    properties: {
      number: issueNumber,
      title: { type: 'string' },
      body: { type: 'string' },
      state: { type: 'string', enum: ['open', 'closed'] },
      state_reason: { type: 'string', enum: ['completed', 'not_planned', 'reopened'] },
      labels: {
        type: 'array',
        items: { type: 'string' },
        description: 'Replaces the label set; it is not additive.',
      },
      assignees: {
        type: 'array',
        items: { type: 'string' },
        description: 'Replaces the assignee set; it is not additive.',
      },
    },
    required: ['number'],
  },
  create_branch: {
    description: 'Create a branch pointing at an existing commit.',
    properties: {
      ref: {
        type: 'string',
        pattern: '^refs/heads/(?!.*\\.\\.)[A-Za-z0-9._/-]+$',
        description:
          'Full ref of the new branch, `refs/heads/<name>`. Note the `refs/` prefix — `update_branch` takes the short form instead. The name allows only letters, digits, `.`, `_`, `-` and `/`, and may not contain `..`.',
      },
      sha: { type: 'string', description: 'Commit SHA the new branch points at.' },
    },
    required: ['ref', 'sha'],
  },
  update_branch: {
    description: 'Move an existing branch to another commit.',
    properties: {
      ref: {
        type: 'string',
        pattern: '^heads/(?!.*\\.\\.)[A-Za-z0-9._/-]+$',
        description:
          'Short ref of the branch to move, `heads/<name>`, with no `refs/` prefix — unlike `create_branch`. The name allows only letters, digits, `.`, `_`, `-` and `/`, and may not contain `..`.',
      },
      sha: { type: 'string', description: 'Commit SHA to move the branch to.' },
      force: {
        type: 'boolean',
        description:
          'Move the branch even when the new commit is not a descendant. Denied unless repository policy grants force pushes, and denied outright on a continuation whatever policy says.',
      },
    },
    required: ['ref', 'sha'],
  },
  create_blob: {
    description: 'Store file content as a blob and return its SHA, for use in a tree entry.',
    properties: {
      content: {
        type: 'string',
        description:
          'File content, encoded as `encoding` says. The 256 KiB cap is on the whole serialized call, not on this field, so a large blob may need splitting.',
      },
      encoding: { type: 'string', enum: ['utf-8', 'base64'], description: 'Defaults to `utf-8`.' },
    },
    required: ['content'],
  },
  create_tree: {
    description: 'Build a tree from entries, usually over an existing tree.',
    properties: {
      tree: {
        type: 'array',
        description:
          'Entries to write. The whole call is capped at 256 KiB once serialized, so send bulk content through `create_blob` and reference the SHA here.',
        items: {
          type: 'object',
          properties: {
            path: { type: 'string', description: 'Path relative to the repository root.' },
            mode: {
              type: 'string',
              enum: ['100644', '100755', '040000', '160000', '120000'],
              description: 'File mode: `100644` for a regular file, `100755` for an executable.',
            },
            type: { type: 'string', enum: ['blob', 'tree', 'commit'] },
            sha: {
              type: ['string', 'null'],
              description: 'Blob SHA from `create_blob`. Null deletes the path.',
            },
            content: {
              type: 'string',
              description: 'Inline content, as an alternative to `sha`.',
            },
          },
          required: ['path', 'mode', 'type'],
          anyOf: [{ required: ['sha'] }, { required: ['content'] }],
        },
      },
      base_tree: {
        type: 'string',
        description: 'Tree SHA to layer the entries onto. Omit it and every unlisted path is gone.',
      },
    },
    required: ['tree'],
  },
  create_commit: {
    description:
      'Create a commit object. Authorship is pinned to the account the daemon runs as; `author` and `committer` are dropped.',
    properties: {
      message: { type: 'string', description: 'Commit message.' },
      tree: { type: 'string', description: 'Tree SHA from `create_tree`.' },
      parents: {
        type: 'array',
        items: { type: 'string' },
        description: 'Parent commit SHAs, the branch tip first.',
      },
    },
    required: ['message', 'tree', 'parents'],
  },
  create_pull_request: {
    description: 'Open a pull request between two branches of the job repository.',
    properties: {
      title: { type: 'string' },
      head: { type: 'string', description: 'Branch the changes are on, as a bare name.' },
      base: { type: 'string', description: 'Branch to merge into, as a bare name.' },
      body: { type: 'string', description: 'Description, GitHub-flavored markdown.' },
      draft: { type: 'boolean' },
    },
    required: ['title', 'head', 'base'],
  },
  create_review: {
    description:
      'Open one review on a pull request, optionally with comments anchored to lines of the diff. Omitting `event` leaves the review `PENDING`, which is how a review is held for a person to finish; giving one submits it outright. Not idempotent: an attempt that landed and then failed leaves a review `list_reviews` shows, and that is where a retry looks instead of posting a second one.',
    properties: {
      number: pullNumber,
      body: { type: 'string', description: 'Review summary, GitHub-flavored markdown.' },
      event: reviewEvent,
      comments: {
        type: 'array',
        description: 'Comments anchored to lines of the diff.',
        items: {
          type: 'object',
          properties: {
            path: { type: 'string', description: 'File path, as the diff names it.' },
            line: {
              type: 'integer',
              minimum: 1,
              description: 'Line the comment ends on, in the file as `side` sees it.',
            },
            side: {
              type: 'string',
              enum: ['LEFT', 'RIGHT'],
              description: '`RIGHT` is the head of the diff, `LEFT` the base. Defaults to `RIGHT`.',
            },
            start_line: {
              type: 'integer',
              minimum: 1,
              description: 'First line of a multi-line comment.',
            },
            start_side: { type: 'string', enum: ['LEFT', 'RIGHT'] },
            body: { type: 'string', description: 'Comment text, GitHub-flavored markdown.' },
          },
          required: ['path', 'body'],
        },
      },
      commit_id: {
        type: 'string',
        description: 'Head SHA the review is against. Defaults to the latest commit.',
      },
    },
    required: ['number'],
  },
  submit_review: {
    description: 'Submit a review `create_review` left pending.',
    properties: {
      number: pullNumber,
      review_id: reviewId,
      event: reviewEvent,
      body: { type: 'string', description: 'Review summary, GitHub-flavored markdown.' },
    },
    required: ['number', 'review_id', 'event'],
  },
  delete_pending_review: {
    description:
      'Discard a pending review. GitHub allows one per account per pull request and refuses a second, so this is the only way past a pending review an earlier attempt left behind. A submitted review cannot be deleted.',
    properties: { number: pullNumber, review_id: reviewId },
    required: ['number', 'review_id'],
  },
  reply_review_comment: {
    description:
      'Reply inside a review thread, addressed by the `databaseId` of a comment in it — the numeric one, not the node `id`. A reply posted while this account holds a pending review on the pull request is attached to that review and stays invisible until it is submitted, so read `list_reviews` first. To post on the pull request itself rather than in a thread, use `create_comment`.',
    properties: {
      number: pullNumber,
      comment_id: {
        type: 'integer',
        minimum: 1,
        description:
          'The `databaseId` of the thread’s **first** comment, from `list_review_threads`. GitHub takes only a top-level review comment here; the id of a reply already in the thread is refused.',
      },
      body: { type: 'string', description: 'Reply text, GitHub-flavored markdown.' },
    },
    required: ['number', 'comment_id', 'body'],
  },
  resolve_review_thread: {
    description:
      'Mark a review thread resolved. Needs write access on the repository, which `list_review_threads` reports as the pull request’s `viewerCanUpdate`.',
    properties: { thread_id: threadId },
    required: ['thread_id'],
  },
  unresolve_review_thread: {
    description: 'Reopen a resolved review thread. Needs the access `resolve_review_thread` does.',
    properties: { thread_id: threadId },
    required: ['thread_id'],
  },
  merge_pull_request: {
    description: 'Merge a pull request.',
    properties: {
      number: pullNumber,
      merge_method: {
        type: 'string',
        enum: ['merge', 'squash', 'rebase'],
        description: 'Defaults to `merge`. The repository may not allow every method.',
      },
      commit_title: { type: 'string' },
      commit_message: { type: 'string' },
      sha: {
        type: 'string',
        description: 'Head SHA the merge expects; the merge fails if the branch has moved past it.',
      },
    },
    required: ['number'],
  },
};

const number = (input: Readonly<Record<string, unknown>>, name: string): number => {
  const value = input[name];
  if (!Number.isInteger(value) || Number(value) < 1)
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: `${name} must be a positive integer`,
    });
  return Number(value);
};

const branchRef = (input: Readonly<Record<string, unknown>>): string => {
  const value = input.ref;
  if (typeof value !== 'string' || !/^heads\/[a-z0-9._/-]+$/i.test(value) || value.includes('..')) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: 'ref must name a branch under heads/',
    });
  }
  return value;
};

const createdBranchRef = (input: Readonly<Record<string, unknown>>): void => {
  const value = input.ref;
  if (
    typeof value !== 'string' ||
    !/^refs\/heads\/[a-z0-9._/-]+$/i.test(value) ||
    value.includes('..')
  ) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: 'ref must name a branch under refs/heads/',
    });
  }
};

const boundedInput = (input: Readonly<Record<string, unknown>>): void => {
  const bytes = Buffer.byteLength(JSON.stringify(input));
  if (bytes > 256 * 1024) {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_TOO_LARGE',
      message: 'Capability input exceeds 256 KiB',
    });
  }
};

/**
 * Commit authorship is GitHub's to assign, not the agent's.
 *
 * The tool schema allows additional properties, so `create_commit` would
 * otherwise forward whatever `author` and `committer` the agent supplied. That
 * was cosmetic while every commit was visibly `lictor[bot]`; once commits carry
 * a person's account it becomes a way to attribute work to someone who did not
 * do it. Dropping the fields makes GitHub fall back to the authenticated user.
 */
const pinCommitIdentity = (
  tool: BrokerTool,
  input: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => {
  if (tool !== 'create_commit') return input;
  const { author: _author, committer: _committer, ...rest } = input;
  return rest;
};

const REVIEW_THREADS_QUERY =
  'query($owner:String!,$name:String!,$number:Int!,$after:String){repository(owner:$owner,name:$name){pullRequest(number:$number){author{login} viewerCanUpdate reviewThreads(first:10,after:$after){pageInfo{hasNextPage endCursor} nodes{id isResolved isOutdated path line originalLine diffSide subjectType comments(first:10){pageInfo{hasNextPage endCursor} nodes{id databaseId body author{login __typename} authorAssociation viewerDidAuthor createdAt diffHunk url}}}}}}}';

/** Thread resolution is a GraphQL mutation in both directions; REST has no equivalent. */
const THREAD_RESOLUTION = {
  resolve_review_thread:
    'mutation($threadId:ID!){resolveReviewThread(input:{threadId:$threadId}){thread{id isResolved}}}',
  unresolve_review_thread:
    'mutation($threadId:ID!){unresolveReviewThread(input:{threadId:$threadId}){thread{id isResolved}}}',
} as const;

const isGraphqlMutation = (tool: BrokerTool): tool is keyof typeof THREAD_RESOLUTION =>
  tool in THREAD_RESOLUTION;

const reviewThreadId = (input: Readonly<Record<string, unknown>>): string => {
  const value = input.thread_id;
  if (typeof value !== 'string' || value === '') {
    throw new CapabilityError({
      code: 'CAPABILITY_INPUT_INVALID',
      message: 'thread_id must name a review thread',
    });
  }
  return value;
};

const graphqlBody = (
  repository: string,
  tool: BrokerTool,
  input: Readonly<Record<string, unknown>>,
) => {
  if (isGraphqlMutation(tool)) {
    return { query: THREAD_RESOLUTION[tool], variables: { threadId: reviewThreadId(input) } };
  }
  if (tool !== 'list_review_threads') return undefined;
  const [owner, name] = repository.split('/');
  return {
    query: REVIEW_THREADS_QUERY,
    variables: {
      owner,
      name,
      number: number(input, 'number'),
      after: typeof input.after === 'string' ? input.after : null,
    },
  };
};

/**
 * GraphQL answers 200 whatever it did, so a mutation it refused is told from one
 * it performed only by the envelope — and the audit row would otherwise record
 * an outcome the call never had.
 */
const graphqlRefusal = (payload: unknown): string | undefined => {
  const errors = (payload as { readonly errors?: unknown } | null)?.errors;
  if (!Array.isArray(errors) || errors.length === 0) return undefined;
  const message = (errors[0] as { readonly message?: unknown }).message;
  return typeof message === 'string' ? message : 'the mutation was refused';
};

const THREAD_OWNER_QUERY =
  'query($id:ID!){node(id:$id){... on PullRequestReviewThread{pullRequest{repository{nameWithOwner}}}}}';

/**
 * The repository a review thread belongs to, lowercased, or `undefined` where
 * GitHub would not say.
 *
 * ! A node id is global, so the two thread mutations are the only tools whose
 * ! target the broker does not pin by building the path — nothing in their input
 * ! names a repository for the guard to compare. This is why they ask, and why
 * ! it fails closed: GitHub would resolve a thread in any repository the
 * ! credential reaches.
 */
const reviewThreadRepository = <E, R>(client: HttpClient.HttpClient.With<E, R>, thread: string) =>
  Effect.gen(function* () {
    const response = yield* client.execute(
      HttpClientRequest.bodyUnsafeJson(HttpClientRequest.post('/graphql'), {
        query: THREAD_OWNER_QUERY,
        variables: { id: thread },
      }),
    );
    // Handed back unclassified rather than collapsed into "no owner": a revoked
    // credential answering here would otherwise read as another repository.
    if (response.status < 200 || response.status >= 300) return { failed: response };
    const body = (yield* response.json) as {
      readonly data?: {
        readonly node?: {
          readonly pullRequest?: { readonly repository?: { readonly nameWithOwner?: unknown } };
        } | null;
      } | null;
    } | null;
    const owner = body?.data?.node?.pullRequest?.repository?.nameWithOwner;
    return { owner: typeof owner === 'string' ? owner.trim().toLowerCase() : undefined };
  });

/** The verdict a call would publish, or `undefined` — a `COMMENT` publishes none. */
const reviewVerdict = (
  tool: BrokerTool,
  input: Readonly<Record<string, unknown>>,
): 'APPROVE' | 'REQUEST_CHANGES' | undefined => {
  if (tool !== 'create_review' && tool !== 'submit_review') return undefined;
  const event = input.event;
  return event === 'APPROVE' || event === 'REQUEST_CHANGES' ? event : undefined;
};

/**
 * Who opened the pull request, lowercased, or `undefined` where GitHub did not
 * say. Read only to refuse a self-review with a reason — GitHub refuses one
 * anyway, so it fails open rather than withhold work over a momentary failure.
 */
const pullRequestAuthor = <E, R>(
  client: HttpClient.HttpClient.With<E, R>,
  repository: string,
  pull: unknown,
) =>
  Effect.gen(function* () {
    if (typeof pull !== 'number' || !Number.isInteger(pull) || pull < 1) return undefined;
    const response = yield* client.execute(
      HttpClientRequest.get(`/repos/${repository}/pulls/${pull}`),
    );
    if (response.status < 200 || response.status >= 300) return undefined;
    const body = (yield* response.json) as { readonly user?: { readonly login?: unknown } } | null;
    const login = body?.user?.login;
    return typeof login === 'string' ? login.trim().toLowerCase() : undefined;
  }).pipe(Effect.orElseSucceed(() => undefined));

const route = (repository: string, tool: BrokerTool, input: Readonly<Record<string, unknown>>) => {
  const base = `/repos/${repository}`;
  switch (tool) {
    case 'get_repository':
      return { method: 'GET', path: base } as const;
    case 'get_issue':
      return { method: 'GET', path: `${base}/issues/${number(input, 'number')}` } as const;
    case 'get_pull_request':
      return { method: 'GET', path: `${base}/pulls/${number(input, 'number')}` } as const;
    case 'list_comments':
      return {
        method: 'GET',
        path: `${base}/issues/${number(input, 'number')}/comments?per_page=3&page=${input.page === undefined ? 1 : number(input, 'page')}`,
      } as const;
    case 'list_review_threads':
    case 'resolve_review_thread':
    case 'unresolve_review_thread':
      return { method: 'POST', path: '/graphql' } as const;
    case 'list_reviews':
      return {
        method: 'GET',
        path: `${base}/pulls/${number(input, 'number')}/reviews?per_page=3&page=${input.page === undefined ? 1 : number(input, 'page')}`,
      } as const;
    case 'list_review_comments':
      return {
        method: 'GET',
        path: `${base}/pulls/${number(input, 'number')}/comments?per_page=3&page=${input.page === undefined ? 1 : number(input, 'page')}`,
      } as const;
    case 'create_comment':
      return {
        method: 'POST',
        path: `${base}/issues/${number(input, 'number')}/comments`,
      } as const;
    case 'create_issue':
      return { method: 'POST', path: `${base}/issues` } as const;
    case 'update_issue':
      return { method: 'PATCH', path: `${base}/issues/${number(input, 'number')}` } as const;
    case 'create_branch':
      createdBranchRef(input);
      return { method: 'POST', path: `${base}/git/refs` } as const;
    case 'update_branch':
      return { method: 'PATCH', path: `${base}/git/refs/${branchRef(input)}` } as const;
    case 'create_blob':
      return { method: 'POST', path: `${base}/git/blobs` } as const;
    case 'create_tree':
      return { method: 'POST', path: `${base}/git/trees` } as const;
    case 'create_commit':
      return { method: 'POST', path: `${base}/git/commits` } as const;
    case 'create_pull_request':
      return { method: 'POST', path: `${base}/pulls` } as const;
    case 'create_review':
      return { method: 'POST', path: `${base}/pulls/${number(input, 'number')}/reviews` } as const;
    case 'submit_review':
      // POST, not the PUT that `/dismissals` and the review body take.
      return {
        method: 'POST',
        path: `${base}/pulls/${number(input, 'number')}/reviews/${number(input, 'review_id')}/events`,
      } as const;
    case 'delete_pending_review':
      return {
        method: 'DELETE',
        path: `${base}/pulls/${number(input, 'number')}/reviews/${number(input, 'review_id')}`,
      } as const;
    case 'reply_review_comment':
      return {
        method: 'POST',
        path: `${base}/pulls/${number(input, 'number')}/comments/${number(input, 'comment_id')}/replies`,
      } as const;
    case 'merge_pull_request':
      return { method: 'PUT', path: `${base}/pulls/${number(input, 'number')}/merge` } as const;
  }
};

/** `remaining quota N` when GitHub reported the bucket, for non-2xx diagnostics. */
const quotaNote = (headers: Record<string, string | undefined>): string | undefined => {
  const remaining = headers['x-ratelimit-remaining'];
  return remaining === undefined ? undefined : `remaining quota ${remaining}`;
};

/** What one audited string field may contribute, so the whole row stays small. */
const AUDIT_FIELD_BYTES = 512;
const AUDIT_ROW_BYTES = 4096;

/**
 * The call as the audit records it: secrets masked, long prose cut.
 *
 * Bounded per field, never by cutting the encoded row: a cut landing inside a
 * string stores JSON nothing can parse, and these rows are read back as
 * evidence, not only written.
 */
const sanitized = (input: Readonly<Record<string, unknown>>): string => {
  const auditValue = (key: string, value: unknown): unknown => {
    if (/token|secret|authorization|private.?key/i.test(key)) return '[REDACTED]';
    return typeof value === 'string' ? bounded(value, AUDIT_FIELD_BYTES) : value;
  };
  const clean = Object.fromEntries(
    Object.entries(input).map(([key, value]) => [key, auditValue(key, value)]),
  );
  const encoded = JSON.stringify(clean);
  if (Buffer.byteLength(encoded) <= AUDIT_ROW_BYTES) return encoded;
  // Enough fields to overrun even bounded. Keep the scalars the audit is
  // queried on and say the rest was dropped, rather than store a fragment.
  const scalars = Object.fromEntries(
    Object.entries(clean).filter(([, value]) => typeof value !== 'string'),
  );
  return JSON.stringify({ ...scalars, truncated: true });
};

const boundedJson = (value: unknown): unknown => {
  const encoded = Buffer.from(
    JSON.stringify(value, (_key, item) =>
      typeof item === 'string' && Buffer.byteLength(item) > 4096
        ? Buffer.from(item)
            .subarray(0, 4096)
            .toString('utf8')
            .replace(/\uFFFD$/u, '')
        : item,
    ),
  );
  if (encoded.byteLength > 64 * 1024) throw new Error('GitHub response exceeds broker limit');
  return JSON.parse(encoded.toString('utf8')) as unknown;
};

export class CapabilityBroker extends Effect.Service<CapabilityBroker>()('CapabilityBroker', {
  effect: Effect.gen(function* () {
    const github = yield* GitHubClient;
    const identity = yield* GitHubIdentity;
    const health = yield* CredentialHealth;
    // Resolved per call, not construction: building the broker costs no network;
    // the first call pays for the probe.
    const actor = identity.verified.pipe(
      Effect.map(({ login }) => login),
      Effect.mapError(
        (cause) =>
          new CapabilityError({
            code: 'CAPABILITY_CREDENTIAL_REJECTED',
            message: cause.message,
            cause,
          }),
      ),
    );
    const policy = yield* Policy;
    const queue = yield* WorkQueue;

    const effectiveCapabilities = (job: QueuedJob): GrantCapabilities => {
      const live = grantCapabilities(
        policy.forRepository(job.work.repository.toLowerCase()).capabilities,
      );
      return job.grant === undefined ? live : intersectCapabilities(job.grant.capabilities, live);
    };

    /**
     * GitHub's non-2xx answer, classified. Every request a call makes routes
     * here, the ownership probe included: one that swallowed a failure would
     * report a revoked credential or a closed bucket as its own absent answer.
     */
    const githubFailure = (
      response: HttpClientResponse.HttpClientResponse,
    ): Effect.Effect<never, CapabilityError> =>
      Effect.gen(function* () {
        // An installation token heals by re-minting; a revoked PAT never
        // does. Collapsing a 401 into a generic failure spends every attempt,
        // each one a full clone cycle.
        if (response.status === 401) {
          yield* health.suspend;
          return yield* new CapabilityError({
            code: 'CAPABILITY_CREDENTIAL_REJECTED',
            message: 'GitHub rejected the daemon credential',
            ...(quotaNote(response.headers) === undefined
              ? {}
              : { cause: quotaNote(response.headers) }),
          });
        }
        // A 429 is definitive alone; 403 needs evidence — headers first,
        // prose second (a secondary limit answers with neither rate header).
        // Retrying either against a closed bucket is a retry storm.
        const hinted =
          response.status === 403 || response.status === 429
            ? retryAfterMs(response.headers, yield* Clock.currentTimeMillis)
            : undefined;
        const secondary =
          response.status === 403 &&
          hinted === undefined &&
          // No reset time still means exhausted; secondary limits say so only
          // in prose.
          (response.headers['x-ratelimit-remaining'] === '0' ||
            isSecondaryRateLimit(yield* Effect.orElseSucceed(response.text, () => '')));
        const wait =
          response.status === 429 || secondary ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : hinted;
        if (wait !== undefined) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_RATE_LIMITED',
            message: `GitHub rate limit reached; retry in ${Math.ceil(wait / 1000)}s${
              quotaNote(response.headers) === undefined ? '' : `, ${quotaNote(response.headers)}`
            }`,
            retryAfterMs: wait,
          });
        }
        return yield* new CapabilityError({
          code: 'CAPABILITY_GITHUB_FAILED',
          message: `GitHub returned status ${response.status}`,
          ...(quotaNote(response.headers) === undefined
            ? {}
            : { cause: quotaNote(response.headers) }),
        });
      });

    const callTool = (request: {
      readonly jobId: number;
      readonly attemptNumber: number;
      readonly workerId: string;
      /** Ignored legacy field; persisted queue state is authoritative. */
      readonly work?: unknown;
      readonly name: BrokerTool;
      readonly input: Readonly<Record<string, unknown>>;
    }) =>
      Effect.gen(function* () {
        const auditInput = sanitized(request.input);
        const job = yield* queue.job(request.jobId);
        if (job === undefined || job.status !== 'running') {
          return yield* new CapabilityError({
            code: 'CAPABILITY_JOB_INACTIVE',
            message: 'Capability calls require an active persisted job',
          });
        }
        if (job.attempts !== request.attemptNumber || job.workerId !== request.workerId) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_ATTEMPT_STALE',
            message: 'Capability session belongs to a stale job attempt',
          });
        }
        const now = yield* Clock.currentTimeMillis;
        if (job.leaseExpiresAt === undefined || job.leaseExpiresAt <= now) {
          return yield* new CapabilityError({
            code: 'CAPABILITY_LEASE_EXPIRED',
            message: 'Capability session lease has expired',
          });
        }
        const auditIdentity = {
          jobId: request.jobId,
          repository: job.work.repository,
          actor: yield* actor,
          capability: request.name,
          input: auditInput,
        };
        yield* queue.recordAudit({ ...auditIdentity, outcome: 'started' });
        const invoke = Effect.gen(function* () {
          yield* Effect.try({
            try: () => boundedInput(request.input),
            catch: (cause) => cause as CapabilityError,
          });
          const expectedRepository = job.work.repository.toLowerCase();
          const requestedRepository = request.input.repository;
          if (
            requestedRepository !== undefined &&
            String(requestedRepository).toLowerCase() !== expectedRepository
          ) {
            return yield* new CapabilityError({
              code: 'CAPABILITY_REPOSITORY_DENIED',
              message: 'Tool request targets another repository',
            });
          }
          const effective = effectiveCapabilities(job);
          const capability = toolCapabilities[request.name];
          // A continuation inherits its authority from the arming trigger: it
          // never reaches escalation capabilities however generous policy is.
          const narrowed = job.work.continuation === true;
          const forcePushDenied =
            request.name === 'update_branch' &&
            request.input.force === true &&
            (!effective.forcePush || narrowed);
          if (
            !policy.forRepository(expectedRepository).accepted ||
            !effective[capability] ||
            (narrowed && capability === 'merge') ||
            forcePushDenied
          ) {
            return yield* new CapabilityError({
              code: 'CAPABILITY_DENIED',
              message: `${request.name} is denied by repository policy`,
            });
          }
          const client = yield* github.authenticated;
          // A verdict is an argument, not a tool, so it is gated per call the
          // way `update_branch`'s force is.
          const verdict = reviewVerdict(request.name, request.input);
          if (verdict !== undefined) {
            if (narrowed) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_DENIED',
                message: `${verdict} is withheld from a turn that continues earlier work`,
              });
            }
            const author = yield* pullRequestAuthor(
              client,
              expectedRepository,
              request.input.number,
            );
            if (author !== undefined && author === (yield* actor)) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_DENIED',
                message: `${verdict} is withheld on a pull request this account opened`,
              });
            }
          }
          if (isGraphqlMutation(request.name)) {
            const thread = yield* Effect.try({
              try: () => reviewThreadId(request.input),
              catch: (cause) =>
                cause instanceof CapabilityError
                  ? cause
                  : new CapabilityError({
                      code: 'CAPABILITY_INPUT_INVALID',
                      message: 'Capability input is invalid',
                      cause,
                    }),
            });
            const owned = yield* reviewThreadRepository(client, thread);
            if ('failed' in owned) return yield* githubFailure(owned.failed);
            if (owned.owner !== expectedRepository) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_REPOSITORY_DENIED',
                message: 'Review thread belongs to another repository',
              });
            }
          }
          const target = yield* Effect.try({
            try: () => route(expectedRepository, request.name, request.input),
            catch: (cause) =>
              cause instanceof CapabilityError
                ? cause
                : new CapabilityError({
                    code: 'CAPABILITY_INPUT_INVALID',
                    message: 'Capability input is invalid',
                    cause,
                  }),
          });
          let baseRequest = HttpClientRequest.get(target.path);
          if (target.method === 'POST') baseRequest = HttpClientRequest.post(target.path);
          if (target.method === 'PATCH') baseRequest = HttpClientRequest.patch(target.path);
          if (target.method === 'PUT') baseRequest = HttpClientRequest.put(target.path);
          if (target.method === 'DELETE') baseRequest = HttpClientRequest.del(target.path);
          const body = yield* Effect.try({
            try: () =>
              graphqlBody(expectedRepository, request.name, request.input) ??
              pinCommitIdentity(request.name, request.input),
            catch: (cause) =>
              cause instanceof CapabilityError
                ? cause
                : new CapabilityError({
                    code: 'CAPABILITY_INPUT_INVALID',
                    message: 'Capability input is invalid',
                    cause,
                  }),
          });
          const httpRequest =
            target.method === 'GET' || target.method === 'DELETE'
              ? baseRequest
              : HttpClientRequest.bodyUnsafeJson(baseRequest, body);
          const response = yield* client.execute(httpRequest);
          if (response.status < 200 || response.status >= 300) {
            return yield* githubFailure(response);
          }
          // A delete answers with the record it removed, but 204 is legal on
          // any of these and `response.json` has nothing to parse.
          if (response.status === 204) return {};
          const payload = boundedJson(yield* response.json);
          if (isGraphqlMutation(request.name)) {
            const refusal = graphqlRefusal(payload);
            if (refusal !== undefined) {
              return yield* new CapabilityError({
                code: 'CAPABILITY_GITHUB_FAILED',
                message: 'GitHub refused the GraphQL mutation',
                cause: refusal,
              });
            }
          }
          return payload;
        }).pipe(
          Effect.mapError((cause) =>
            cause instanceof CapabilityError
              ? cause
              : new CapabilityError({
                  code: 'CAPABILITY_FAILED',
                  message: 'Capability call failed',
                  cause,
                }),
          ),
        );

        return yield* Effect.matchEffect(invoke, {
          onFailure: (error) =>
            queue
              .recordAudit({
                ...auditIdentity,
                outcome: error.code,
              })
              .pipe(Effect.zipRight(Effect.fail(error))),
          onSuccess: (result) =>
            // A branch she created outlives the session — durable state the next
            // interaction continues from, never failed after a successful call.
            (request.name === 'create_branch' &&
            typeof request.input.ref === 'string' &&
            request.input.ref.startsWith('refs/heads/')
              ? queue
                  .recordSubjectBranch({
                    repository: job.work.repository,
                    subjectKind: job.work.subject.kind,
                    subjectNumber: job.work.subject.number,
                    branch: request.input.ref.slice('refs/heads/'.length),
                  })
                  .pipe(
                    Effect.catchAll((cause) =>
                      Effect.logError('Could not record created branch').pipe(
                        Effect.annotateLogs({ job: request.jobId, error: cause.message }),
                      ),
                    ),
                  )
              : Effect.void
            ).pipe(
              Effect.zipRight(
                queue
                  .recordAudit({ ...auditIdentity, outcome: 'ok' })
                  .pipe(
                    Effect.catchAll((cause) =>
                      Effect.logError('Could not finalize capability audit', cause),
                    ),
                  ),
              ),
              Effect.as(result),
            ),
        });
      });

    const listTools = (Object.keys(toolCapabilities) as BrokerTool[]).map((name) => {
      const { description, properties, required } = toolSchemas[name];
      return {
        name,
        description,
        // ! The executor runs `approval_policy: never`, where Codex refuses an
        // ! unannotated tool outright: without these it is unreachable, and the
        // ! agent reports a summary having done nothing. The hints describe this
        // ! surface, not GitHub's — one repository, an enumerated operation set,
        // ! re-gated per call against the job's policy and lease.
        annotations: {
          readOnlyHint: toolCapabilities[name] === 'read',
          destructiveHint: false,
          openWorldHint: false,
        },
        inputSchema: {
          type: 'object',
          properties: { repository: repositoryProperty, ...properties },
          ...(required === undefined ? {} : { required }),
          additionalProperties: true,
        },
      };
    });

    // Discovery is scoped to the job's own authority: offering a tool that could
    // only ever fail invites attempts whose denial is the feature. Same function
    // the prompt enumerates from, so the two cannot advertise different sets.
    const visibleTools = (job: QueuedJob) => {
      const granted = grantedTools(effectiveCapabilities(job), job.work.continuation === true);
      return listTools.filter((tool) => granted.includes(tool.name));
    };

    const handleMcp = (
      jobId: number,
      attemptOrRequest: number | unknown,
      workerOrRequest?: string | unknown,
      sessionRequest?: {
        readonly jsonrpc: '2.0';
        readonly id: string | number;
        readonly method: string;
        readonly params?: Readonly<Record<string, unknown>>;
      },
    ) => {
      const request = (sessionRequest ?? workerOrRequest ?? attemptOrRequest) as {
        readonly jsonrpc: '2.0';
        readonly id: string | number;
        readonly method: string;
        readonly params?: Readonly<Record<string, unknown>>;
      };
      if (request.method === 'initialize') {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'lictor', version: '1.0.0' },
          },
        });
      }
      if (request.method === 'tools/list') {
        return Effect.flatMap(queue.job(jobId), (job) =>
          Effect.succeed({
            jsonrpc: '2.0' as const,
            id: request.id,
            result: {
              tools: job === undefined || job.status !== 'running' ? [] : visibleTools(job),
            },
          }),
        );
      }
      if (request.method !== 'tools/call') {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          error: { code: -32601, message: 'Method not found' },
        });
      }
      const name = request.params?.name;
      if (typeof name !== 'string' || !(name in toolCapabilities)) {
        return Effect.succeed({
          jsonrpc: '2.0' as const,
          id: request.id,
          error: { code: -32602, message: 'Unknown tool' },
        });
      }
      const args = request.params?.arguments;
      const input =
        typeof args === 'object' && args !== null
          ? (args as Readonly<Record<string, unknown>>)
          : {};
      return Effect.match(
        callTool({
          jobId,
          attemptNumber: Number(attemptOrRequest),
          workerId: String(workerOrRequest),
          name: name as BrokerTool,
          input,
        }),
        {
          // The code is the whole contract — what the agent branches on. `data`
          // carries only what the code cannot: a closed bucket without a wait
          // leaves the agent guessing, and its guess is a retry storm.
          onFailure: (error) => ({
            jsonrpc: '2.0' as const,
            id: request.id,
            error: {
              code: -32000,
              message: error instanceof CapabilityError ? error.code : 'CAPABILITY_AUDIT_FAILED',
              ...(error instanceof CapabilityError && error.retryAfterMs !== undefined
                ? { data: { detail: error.message, retryAfterMs: error.retryAfterMs } }
                : {}),
            },
          }),
          onSuccess: (result) => ({
            jsonrpc: '2.0' as const,
            id: request.id,
            result: { content: [{ type: 'text', text: JSON.stringify(result) }] },
          }),
        },
      );
    };

    return { callTool, handleMcp, listTools };
  }),
  dependencies: [
    GitHubClient.Default,
    GitHubIdentity.Default,
    Policy.Default,
    WorkQueue.Default,
    CredentialHealth.Default,
  ],
}) {}
