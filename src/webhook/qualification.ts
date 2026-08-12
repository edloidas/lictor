import { Data, Effect, type ParseResult, Schema } from 'effect';
import type { Delivery } from './event.ts';

const Actor = Schema.Struct({ login: Schema.String });
const Subject = Schema.Struct({
  number: Schema.Number,
  title: Schema.String,
  html_url: Schema.String,
  updated_at: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  pull_request: Schema.optional(Schema.Unknown),
});
const Comment = Schema.Struct({
  html_url: Schema.String,
  body: Schema.optionalWith(Schema.String, { nullable: true }),
  updated_at: Schema.optional(Schema.String),
  submitted_at: Schema.optional(Schema.String),
});
const BodyChange = Schema.Struct({
  body: Schema.optional(
    Schema.Struct({ from: Schema.optionalWith(Schema.String, { nullable: true }) }),
  ),
});

const InteractionPayload = Schema.Struct({
  action: Schema.String,
  sender: Actor,
  repository: Schema.Struct({ full_name: Schema.String }),
  installation: Schema.optional(Schema.Struct({ id: Schema.Number })),
  issue: Schema.optional(Subject),
  pull_request: Schema.optional(Subject),
  comment: Schema.optional(Comment),
  review: Schema.optional(Comment),
  assignee: Schema.optional(Actor),
  requested_reviewer: Schema.optional(Actor),
  changes: Schema.optional(BodyChange),
});
type InteractionPayload = Schema.Schema.Type<typeof InteractionPayload>;

export type QualificationPolicy = {
  readonly trustedSenders: readonly string[];
  readonly targetUsers: readonly string[];
};

export type WorkReason = 'assigned' | 'mentioned' | 'review_requested';

export type WorkItem = {
  readonly deliveryId: string;
  readonly interactionId: string;
  readonly event: string;
  readonly action: string;
  readonly repository: string;
  readonly installationId?: number;
  readonly approvalRequired?: boolean;
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
};

export class MalformedInteraction extends Data.TaggedError('MalformedInteraction')<{
  readonly message: string;
}> {}

const normalize = (login: string): string => login.toLowerCase();

const mentions = (body: string | undefined, targets: readonly string[]): readonly string[] => {
  if (body === undefined) return [];

  return targets.filter((target) => {
    const escaped = target.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`(^|[^a-z0-9-])@${escaped}(?![a-z0-9-])`, 'i').test(body);
  });
};

const newlyMentioned = (
  current: string | undefined,
  previous: string | undefined,
  targets: readonly string[],
): readonly string[] => {
  const before = new Set(mentions(previous, targets));
  return mentions(current, targets).filter((target) => !before.has(target));
};

const mentionBody = (event: string, payload: InteractionPayload): string | undefined => {
  if (event === 'issues') return payload.issue?.body;
  if (event === 'pull_request') return payload.pull_request?.body;
  if (event === 'pull_request_review') return payload.review?.body;
  return payload.comment?.body;
};

const supportsMentions = (event: string, action: string): boolean => {
  if (event === 'issues' || event === 'pull_request')
    return action === 'opened' || action === 'edited';
  if (event === 'pull_request_review') return action === 'submitted' || action === 'edited';
  return (
    (event === 'issue_comment' || event === 'pull_request_review_comment') &&
    (action === 'created' || action === 'edited')
  );
};

export const supportsInteraction = (event: string, action: string | undefined): boolean => {
  if (action === undefined) return false;
  if ((event === 'issues' || event === 'pull_request') && action === 'assigned') return true;
  if (event === 'pull_request' && action === 'review_requested') return true;
  return supportsMentions(event, action);
};

const validatePayload = (
  event: string,
  payload: InteractionPayload,
): Effect.Effect<InteractionPayload, MalformedInteraction> => {
  let missing: string | undefined;
  if ((event === 'issues' || event === 'issue_comment') && payload.issue === undefined) {
    missing = 'issue';
  } else if (event.startsWith('pull_request') && payload.pull_request === undefined) {
    missing = 'pull_request';
  } else if (
    (event === 'issue_comment' || event === 'pull_request_review_comment') &&
    payload.comment === undefined
  ) {
    missing = 'comment';
  } else if (event === 'pull_request_review' && payload.review === undefined) {
    missing = 'review';
  } else if (payload.action === 'assigned' && payload.assignee === undefined) {
    missing = 'assignee';
  } else if (payload.action === 'review_requested' && payload.requested_reviewer === undefined) {
    missing = 'requested_reviewer';
  }

  return missing === undefined
    ? Effect.succeed(payload)
    : Effect.fail(new MalformedInteraction({ message: `${event} payload is missing ${missing}` }));
};

export const qualifyDelivery = (
  delivery: Delivery,
  policy: QualificationPolicy,
): Effect.Effect<WorkItem | undefined, ParseResult.ParseError | MalformedInteraction> =>
  Schema.decodeUnknown(InteractionPayload)(delivery.raw).pipe(
    Effect.flatMap((payload) => validatePayload(delivery.event, payload)),
    Effect.map((payload) => {
      const sender = normalize(payload.sender.login);
      const trusted = new Set(policy.trustedSenders.map(normalize));
      const targets = policy.targetUsers.map(normalize);
      if (!trusted.has(sender) || targets.length === 0) return undefined;

      const matched = new Set<string>();
      const reasons = new Set<WorkReason>();
      const addDirectTarget = (login: string | undefined, reason: WorkReason) => {
        if (login === undefined) return;
        const target = normalize(login);
        if (!targets.includes(target)) return;
        matched.add(target);
        reasons.add(reason);
      };

      if (payload.action === 'assigned') addDirectTarget(payload.assignee?.login, 'assigned');
      if (payload.action === 'review_requested') {
        addDirectTarget(payload.requested_reviewer?.login, 'review_requested');
      }

      if (supportsMentions(delivery.event, payload.action)) {
        const current = mentionBody(delivery.event, payload);
        let found: readonly string[] = [];
        if (payload.action !== 'edited') {
          found = mentions(current, targets);
        } else if (payload.changes?.body !== undefined) {
          found = newlyMentioned(current, payload.changes.body.from, targets);
        }
        for (const target of found) matched.add(target);
        if (found.length > 0) reasons.add('mentioned');
      }

      if (matched.size === 0) return undefined;

      const subject = payload.pull_request ?? payload.issue;
      if (subject === undefined) return undefined;
      const isPullRequest =
        payload.pull_request !== undefined ||
        delivery.event.startsWith('pull_request') ||
        payload.issue?.pull_request !== undefined;
      const context = payload.comment ?? payload.review;
      const interactionId = JSON.stringify([
        delivery.event,
        payload.action,
        payload.repository.full_name,
        isPullRequest ? 'pull_request' : 'issue',
        subject.number,
        context?.html_url ?? subject.html_url,
        context?.updated_at ?? context?.submitted_at ?? subject.updated_at,
        [...matched].sort(),
        [...reasons].sort(),
      ]);

      return {
        deliveryId: delivery.id,
        interactionId,
        event: delivery.event,
        action: payload.action,
        repository: payload.repository.full_name,
        ...(payload.installation === undefined ? {} : { installationId: payload.installation.id }),
        sender,
        targets: [...matched].sort(),
        reasons: [...reasons].sort(),
        subject: {
          kind: isPullRequest ? 'pull_request' : 'issue',
          number: subject.number,
          title: subject.title,
          url: subject.html_url,
        },
        ...(context === undefined ? {} : { contextUrl: context.html_url }),
      };
    }),
  );
