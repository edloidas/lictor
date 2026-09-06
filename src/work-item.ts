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

/**
 * The request this job was accepted on, as qualification observed it.
 *
 * `context` says where the trigger lives and `contextUrl` how to reach it, but
 * both resolve to whatever the sender last edited. GitHub prose is mutable and
 * the broker offers no fetch by id, so a job that reconstructs its own intent
 * runs on text nobody accepted — or, once the comment is deleted, on nothing.
 * This is the copy that stops moving.
 */
export type TriggerRecord = {
  readonly source: ContextRef;
  readonly url: string;
  /** Bounded body. Empty for an assignment or review request, which have none. */
  readonly text: string;
  /**
   * ! Set when the bound could not hold the request. The worker refuses to run
   * ! a clipped job: truncated instructions read as complete ones, and the part
   * ! that was cut is exactly the part nothing can judge the loss of.
   */
  readonly clipped: boolean;
  /**
   * Who wrote it, before `attributed()` decided whose trust it carries. That
   * decision belongs to `sender`; this field is the record, and the two differ
   * whenever an edit did not elevate.
   */
  readonly poster: string;
  /** Present only where an edit was attributed to a named editor. */
  readonly editor?: string;
  /** The version observed: the edit time where there was one, else creation. */
  readonly revision: string;
  readonly observedAt: number;
};

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
  readonly trigger?: TriggerRecord;
  /**
   * Where the answer to this job's earlier question was posted. The only field
   * an answer adds: everything authority is derived from stays as the asking
   * turn left it, so resuming cannot grant more than the question was asked
   * under.
   */
  readonly answerUrl?: string;
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

export const TriggerRecordSchema: Schema.Schema<TriggerRecord> = Schema.Struct({
  source: ContextRefSchema,
  url: Schema.String,
  text: Schema.String,
  clipped: Schema.Boolean,
  poster: Schema.String,
  editor: Schema.optionalWith(Schema.String, { exact: true }),
  revision: Schema.String,
  observedAt: Schema.Number,
});

/**
 * ! `context` and `trigger` are optional, not required. The schema decodes
 * ! payloads already stored, and a job queued before either field existed would
 * ! otherwise fail `decodeJob` and be dead-lettered as an invalid payload at
 * ! claim time.
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
  trigger: Schema.optionalWith(TriggerRecordSchema, { exact: true }),
  answerUrl: Schema.optionalWith(Schema.String, { exact: true }),
});
