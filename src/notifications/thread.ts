import { createHash } from 'node:crypto';
import { Schema } from 'effect';

export const NotificationThread = Schema.Struct({
  id: Schema.String,
  unread: Schema.Boolean,
  reason: Schema.String,
  updated_at: Schema.String,
  last_read_at: Schema.optionalWith(Schema.String, { nullable: true }),
  subject: Schema.Struct({
    title: Schema.String,
    url: Schema.String,
    latest_comment_url: Schema.optionalWith(Schema.String, { nullable: true }),
    // GitHub sends types we do not enumerate (Discussion, CheckSuite, ...);
    // rejecting one would drop the whole poll page.
    type: Schema.String,
  }),
  repository: Schema.Struct({
    full_name: Schema.String,
  }),
});

export type NotificationThread = Schema.Schema.Type<typeof NotificationThread>;

export const NotificationThreads = Schema.Array(NotificationThread);

export const decodeThreads = Schema.decodeUnknown(NotificationThreads);

/** For a single thread read back out of the durable inbox. */
export const decodeThread = Schema.decodeUnknown(NotificationThread);

/**
 * Reasons that can never carry a mention, and are therefore not worth fetching.
 *
 * An exclusion list, not an allow list: `reason` describes the thread, not the
 * activity that landed on it. An allow list would discard exactly the thread
 * that went unread as `assign` and then received a trusted mention — and after
 * the poller's read mark it would be gone from both sides.
 */
export const UNMENTIONABLE_REASONS: ReadonlySet<string> = new Set([
  'ci_activity',
  'security_alert',
  'invitation',
]);

// `updated_at` has second resolution; the digest keeps two same-second
// observations apart. `body` must be the exact string the row stores.
export const deliveryIdFor = (thread: NotificationThread, body: string): string => {
  const digest = createHash('sha256').update(body).digest('hex').slice(0, 6);
  return `notification:${thread.id}:${thread.updated_at}:${digest}`;
};

export type SubjectRef = {
  readonly kind: 'issue' | 'pull_request';
  readonly number: number;
};

// A PullRequest-typed subject can report its url under `/issues/{n}`, so the
// path segment alone does not decide the kind.
export const subjectRef = (thread: NotificationThread): SubjectRef | undefined => {
  const match = /\/repos\/[^/]+\/[^/]+\/(issues|pulls)\/(\d+)/.exec(thread.subject.url);
  if (!match) return undefined;
  const [, segment, number] = match;
  return {
    kind: segment === 'pulls' || thread.subject.type === 'PullRequest' ? 'pull_request' : 'issue',
    number: Number(number),
  };
};
