import { Schema } from 'effect';

/**
 * Why an item qualified. One job can carry several — an assignment whose body
 * also mentions her is one activity window, not two. `continued` marks a turn
 * that keeps already-triggered work going rather than starting any: it comes
 * from a reply this policy does not trust, allowed only while the thread is
 * live, and carries narrower capabilities than a triggering turn.
 */
export type WorkReason = 'assigned' | 'mentioned' | 'review_requested' | 'continued';

/**
 * What triggered the item, addressably.
 *
 * Reactions, and later replies, target distinct GitHub resources with distinct
 * endpoints, and a url string cannot be dispatched on. The body case carries the
 * subject number rather than a comment id because an issue or pull request opened
 * with a mention in its description has no comment at all.
 */
export type ContextRef =
  | { readonly kind: 'issue_comment'; readonly id: number }
  | { readonly kind: 'review_comment'; readonly id: number }
  /**
   * ! A submitted review carries its own id even though it reacts on the pull
   * ! request, because `interactionId` is built from this ref. Collapsing reviews
   * ! into `body` would make every review on one pull request share an identity,
   * ! and the second instruction a reviewer sends would be deduped away as a
   * ! replay of the first.
   */
  | { readonly kind: 'review'; readonly id: number; readonly number: number }
  /**
   * ! An assignment or review request carries the timeline event's id for the
   * ! same reason a review does: two assignments in one window are two jobs,
   * ! not a replay. Both react on the issue itself — there is nothing else to
   * ! acknowledge.
   */
  | { readonly kind: 'assigned'; readonly id: number; readonly number: number }
  | { readonly kind: 'review_requested'; readonly id: number; readonly number: number }
  | { readonly kind: 'body'; readonly number: number };

export type WorkItem = {
  readonly deliveryId: string;
  readonly interactionId: string;
  readonly repository: string;
  readonly approvalRequired?: boolean;
  /**
   * A turn that continues live work instead of triggering it. The broker strips
   * the escalation capabilities (`merge`, `forcePush`, `deleteBranches`) from
   * these even where repository policy grants them.
   */
  readonly continuation?: boolean;
  readonly sender: string;
  readonly targets: readonly string[];
  readonly reasons: readonly WorkReason[];
  readonly subject: {
    readonly kind: 'issue' | 'pull_request';
    readonly number: number;
    readonly title: string;
    readonly url: string;
  };
  readonly contextUrl?: string;
  readonly context?: ContextRef;
};

export const ContextRefSchema: Schema.Schema<ContextRef> = Schema.Union(
  Schema.Struct({ kind: Schema.Literal('issue_comment'), id: Schema.Number }),
  Schema.Struct({ kind: Schema.Literal('review_comment'), id: Schema.Number }),
  Schema.Struct({ kind: Schema.Literal('review'), id: Schema.Number, number: Schema.Number }),
  Schema.Struct({ kind: Schema.Literal('assigned'), id: Schema.Number, number: Schema.Number }),
  Schema.Struct({
    kind: Schema.Literal('review_requested'),
    id: Schema.Number,
    number: Schema.Number,
  }),
  Schema.Struct({ kind: Schema.Literal('body'), number: Schema.Number }),
);

/**
 * ! `context` is optional, not required. The schema decodes payloads already
 * ! stored, and a job queued before this field existed would otherwise fail
 * ! `decodeJob` and be dead-lettered as an invalid payload at claim time.
 */
export const WorkItemSchema: Schema.Schema<WorkItem> = Schema.Struct({
  deliveryId: Schema.String,
  interactionId: Schema.String,
  repository: Schema.String,
  approvalRequired: Schema.optionalWith(Schema.Boolean, { exact: true }),
  continuation: Schema.optionalWith(Schema.Boolean, { exact: true }),
  sender: Schema.String,
  targets: Schema.Array(Schema.String),
  reasons: Schema.Array(Schema.Literal('assigned', 'mentioned', 'review_requested', 'continued')),
  subject: Schema.Struct({
    kind: Schema.Literal('issue', 'pull_request'),
    number: Schema.Number,
    title: Schema.String,
    url: Schema.String,
  }),
  contextUrl: Schema.optionalWith(Schema.String, { exact: true }),
  context: Schema.optionalWith(ContextRefSchema, { exact: true }),
});
