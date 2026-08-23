import { Cause, Data, Effect, type ParseResult, Schedule } from 'effect';
import { isTagged } from 'effect/Predicate';
import { LictorConfig } from './config.ts';
import { describeCause } from './diagnostics.ts';
import { GitHubClient } from './github/client.ts';
import { GitHubIdentity, type GitHubIdentityError } from './github/identity.ts';
import { type NotificationError, qualifyNotification } from './notifications/qualify.ts';
import { decodeThread } from './notifications/thread.ts';
import { Policy } from './policy.ts';
import {
  type DeliverySource,
  type InboxDelivery,
  type QueueError,
  WorkQueue,
} from './queue/work-queue.ts';
import type { ContextRef, WorkItem } from './work-item.ts';

class InvalidStoredDelivery extends Data.TaggedError('InvalidStoredDelivery')<{
  readonly cause: unknown;
}> {}

/**
 * Whether a failed delivery is condemned on first sight.
 *
 * A body no decoder accepts describes the delivery alone: it fails identically
 * on every retry, so spending the attempt budget on it only delays the
 * inevitable by a fixed number of passes. Everything else — transport errors,
 * throttles, defects, tags this file has never seen — takes the retry path,
 * because the moment qualification grows enrichment fetches, a transient
 * GitHub 502 must cost an attempt, not the delivery.
 */
export const isTerminalFailure = (
  error: unknown,
): error is InvalidStoredDelivery | ParseResult.ParseError =>
  isTagged('InvalidStoredDelivery')(error) || isTagged('ParseError')(error);

type ProcessRequirements = GitHubClient | GitHubIdentity | LictorConfig | Policy | WorkQueue;

/**
 * One decoder per stored-delivery source. Adding a producer means adding a
 * `DeliverySource` member and this map forces its decoder into existence —
 * nothing downstream assumes an envelope again.
 */
/**
 * Reacts to the triggering comment, and records that it did.
 *
 * Strictly best-effort: every failure is logged and swallowed. This runs inside
 * the delivery worker, where a non-`QueueError` failure marks the delivery
 * permanently `failed` — so a reaction GitHub refused would throw away work that
 * is already durably queued and about to run. The eyes are a courtesy; the job
 * is the product.
 */
const acknowledge = (work: WorkItem, context: ContextRef, jobId: number) =>
  Effect.gen(function* () {
    const github = yield* GitHubClient;
    const queue = yield* WorkQueue;
    const input = JSON.stringify({ target: context, content: 'eyes' });
    const outcome = yield* github.addReaction(work.repository, context, 'eyes').pipe(
      Effect.as('ok'),
      Effect.catchAll((error) =>
        Effect.logWarning('Could not acknowledge queued work').pipe(
          Effect.annotateLogs({ job: jobId, repository: work.repository, reason: error.message }),
          Effect.as('react_failed'),
        ),
      ),
    );
    yield* queue.recordAudit({
      jobId,
      repository: work.repository,
      actor: 'daemon',
      capability: 'react',
      input,
      outcome,
    });
  }).pipe(
    Effect.catchAllCause((cause) =>
      Cause.isInterruptedOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning('Acknowledgement failed').pipe(
            Effect.annotateLogs({ job: jobId, reason: describeCause(cause) }),
          ),
    ),
  );

const processBySource: Record<
  DeliverySource,
  (
    stored: InboxDelivery,
  ) => Effect.Effect<
    void,
    | InvalidStoredDelivery
    | ParseResult.ParseError
    | NotificationError
    | GitHubIdentityError
    | QueueError,
    ProcessRequirements
  >
> = {
  notification: (stored) =>
    Effect.gen(function* () {
      // ! `Effect.try`, not a bare `JSON.parse`. A throw inside `Effect.gen`
      // ! is a defect, which none of the recovery branches below would see.
      const raw = yield* Effect.try({
        try: () => JSON.parse(stored.body) as unknown,
        catch: (cause) => new InvalidStoredDelivery({ cause }),
      });
      // ! A `ParseError` here is terminal, and correctly so: the body is one the
      // ! poller serialized from a thread it had already decoded, so a shape this
      // ! schema rejects fails identically on every retry. Enrichment failures
      // ! are a different tag — see `NotificationError` — precisely so a GitHub
      // ! 502 mid-qualification costs an attempt rather than the delivery.
      const thread = yield* decodeThread(raw);
      const identity = yield* GitHubIdentity;
      const { login } = yield* identity.verified;
      const config = yield* LictorConfig;
      const queue = yield* WorkQueue;
      const cursorMs = yield* queue.notificationCursor(thread.id);
      const { work, lastActivityAt } = yield* qualifyNotification({
        deliveryId: stored.id,
        thread,
        policy: { selfLogin: login, trustedSenders: config.trustedSenders },
        cursorMs,
      });
      // ! Advanced only after the job is committed, never before. A cursor moved
      // ! ahead of a failed `enqueue` makes the retry scan from the wrong anchor
      // ! and miss the comment that caused the notification in the first place.
      const advance = Number.isFinite(lastActivityAt)
        ? queue.advanceNotificationCursor(thread.id, lastActivityAt)
        : Effect.void;

      if (work === undefined) return yield* advance;

      const policy = yield* Policy;
      const repositoryPolicy = policy.forRepository(work.repository);
      if (!repositoryPolicy.accepted || repositoryPolicy.execution === 'denied') {
        yield* Effect.logInfo('Dropped notification denied by repository policy').pipe(
          Effect.annotateLogs({ delivery: stored.id, repository: work.repository }),
        );
        return yield* advance;
      }

      const enqueued = yield* queue.enqueue(
        {
          ...work,
          ...(repositoryPolicy.execution === 'approval' ? { approvalRequired: true } : {}),
        },
        policy.maxQueueDepth,
      );
      yield* advance;

      yield* Effect.logInfo(
        enqueued.inserted ? 'Queued GitHub interaction' : 'Ignored duplicate notification',
      ).pipe(
        Effect.annotateLogs({
          job: enqueued.jobId,
          delivery: stored.id,
          repository: work.repository,
          sender: work.sender,
          targets: work.targets.join(','),
          reasons: work.reasons.join(','),
          subject: `${work.subject.kind}#${work.subject.number}`,
        }),
      );

      // ! Only on a fresh insert. `enqueue` reports `inserted: false` for a
      // ! replayed notification, and reacting again would be a second write for
      // ! work that already exists — GitHub's endpoint is idempotent, but the
      // ! audit row would not be.
      if (enqueued.inserted && work.context !== undefined) {
        yield* acknowledge(work, work.context, enqueued.jobId);
      }
    }),
};

export class DeliveryWorker extends Effect.Service<DeliveryWorker>()('DeliveryWorker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const queue = yield* WorkQueue;
    const runOnce = Effect.gen(function* () {
      const stored = yield* queue.claimDelivery;
      if (stored === undefined) return false;
      const backoff = Effect.sleep(
        Math.min(config.workerRetryBaseMs * 2 ** Math.max(0, stored.attempts - 1), 60_000),
      );
      const process = Effect.gen(function* () {
        yield* processBySource[stored.source](stored);
        yield* queue.finishDelivery(stored.id, 'completed');
      });
      yield* process.pipe(
        // ! A dead credential says nothing about the delivery, so it must not
        // ! condemn one. `finishDelivery(..., 'failed')` is terminal — nothing
        // ! recovers a `failed` row, not the startup reset and not the control
        // ! plane — and the verdict is memoized, so without this the drain loop
        // ! would burn through the whole durable inbox at SQLite speed while the
        // ! shutdown signal is still propagating. Retry instead and let the
        // ! deliveries outlive the misconfiguration that stalled them.
        Effect.catchTag('GitHubIdentityError', (error) =>
          // ! `false`, so the attempt budget never applies. With `true` this
          // ! branch only *defers* the condemnation — `retryDelivery` flips the
          // ! row to terminal `failed` once attempts reach the limit, and the
          // ! drain loop reclaims the same delivery every cycle, so the whole
          // ! inbox is destroyed a few passes later instead of immediately. The
          // ! budget exists to stop one poisonous delivery retrying forever, and
          // ! a credential the daemon cannot verify is not a property of any
          // ! delivery. Same reasoning as the `QueueError` branch below.
          queue
            .retryDelivery(stored.id, String(error), false)
            // ! A flat wait, not a backoff: this branch is not retry-counted at
            // ! all, so there is no curve to climb — the pause only keeps the
            // ! drain loop from spinning against a credential that cannot work.
            .pipe(Effect.zipRight(Effect.sleep(config.workerRetryBaseMs))),
        ),
        Effect.catchTag('QueueError', (error) =>
          queue
            .retryDelivery(
              stored.id,
              String(error),
              (error.cause as Error | undefined)?.message !== 'QUEUE_DEPTH_LIMIT',
            )
            .pipe(Effect.zipRight(backoff)),
        ),
        Effect.catchIf(isTerminalFailure, (error) =>
          // ! `failed` is terminal, so something has to be logged here or the
          // ! reason is lost: nothing downstream ever reads the row again.
          Effect.logError('Delivery failed permanently').pipe(
            Effect.annotateLogs({
              delivery: stored.id,
              event: stored.event,
              reason: describeCause(Cause.fail(error)),
            }),
            Effect.zipRight(
              queue.finishDelivery(stored.id, 'failed', 'DELIVERY_PROCESSING_FAILED'),
            ),
          ),
        ),
        // ! The catch-all retries rather than condemns. Only parse and schema
        // ! failures are provably permanent, and those are named above; every
        // ! other failure is presumed transient until proven otherwise. The
        // ! attempt budget keeps a genuinely poisonous delivery from looping
        // ! forever — `retryDelivery` flips it to terminal `failed` at the limit.
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.interrupt
            : // ! Logged per retry or the reason is lost: the eventual terminal
              // ! flip happens inside `retryDelivery` and writes only the row.
              Effect.logWarning('Retrying failed delivery')
                .pipe(
                  Effect.annotateLogs({
                    delivery: stored.id,
                    event: stored.event,
                    reason: describeCause(cause),
                  }),
                )
                .pipe(
                  Effect.zipRight(
                    queue
                      .retryDelivery(stored.id, describeCause(cause), true)
                      .pipe(Effect.zipRight(backoff)),
                  ),
                ),
        ),
      );
      return true;
    });
    const drain = Effect.repeat(runOnce, { until: (processed) => !processed }).pipe(Effect.asVoid);
    const run = drain.pipe(
      Effect.catchAllCause((cause) => Effect.logError('Delivery worker cycle failed', cause)),
      Effect.repeat(Schedule.spaced(`${config.workerPollMs} millis`)),
    );
    return { runOnce, drain, run };
  }),
  dependencies: [
    LictorConfig.Default,
    WorkQueue.Default,
    GitHubClient.Default,
    GitHubIdentity.Default,
    Policy.Default,
  ],
}) {}
