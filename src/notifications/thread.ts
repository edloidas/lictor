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
 * ! An exclusion list, not an allow list, and that direction is the whole point.
 * ! `reason` describes the *thread*, not the activity that just landed on it, and
 * ! GitHub does not re-key an already-unread thread — so a thread that went unread
 * ! as `assign` and then received a trusted mention still reports `assign`.
 * ! Accepting only `mention` would discard exactly that thread, and since the
 * ! poller has already marked it read, the mention would be gone from both sides.
 * ! What remains here is machine traffic that has no author to trust and no prose
 * ! to scan, which is also the high-volume kind worth not fetching.
 */
export const UNMENTIONABLE_REASONS: ReadonlySet<string> = new Set([
  'ci_activity',
  'security_alert',
  'invitation',
]);

export const deliveryIdFor = (thread: NotificationThread): string =>
  `notification:${thread.id}:${thread.updated_at}`;

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
