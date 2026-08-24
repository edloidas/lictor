import { Cause, Data, Effect, type ParseResult, Schedule } from 'effect';
import { isTagged } from 'effect/Predicate';
import { LictorConfig } from './config.ts';
import { describeCause } from './diagnostics.ts';
import { GitHubClient } from './github/client.ts';
import { GitHubIdentity, type GitHubIdentityError } from './github/identity.ts';
import { type NotificationError, qualifyNotification } from './notifications/qualify.ts';
import { decodeThread, subjectRef } from './notifications/thread.ts';
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
 * Eyes reaction on the triggering comment, and the audit row recording it.
 * Strictly best-effort: a non-`QueueError` failure here marks the delivery
 * permanently `failed`, so no reaction GitHub refused may throw away durably
 * queued work.
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
  /** One decoder per `DeliverySource`; the map forces each into existence. */
  notification: (stored) =>
    Effect.gen(function* () {
      // ! A throw inside `Effect.gen` is a defect, which none of the recovery
      // ! branches below would see — so `JSON.parse` goes through `Effect.try`.
      const raw = yield* Effect.try({
        try: () => JSON.parse(stored.body) as unknown,
        catch: (cause) => new InvalidStoredDelivery({ cause }),
      });
      const thread = yield* decodeThread(raw);
      const identity = yield* GitHubIdentity;
      const { login } = yield* identity.verified;
      const queue = yield* WorkQueue;
      const policy = yield* Policy;
      // Trust is resolved per repository, not globally: trusted for one owner's
      // repositories is not trusted for every other one she can access.
      const threadPolicy = policy.forRepository(thread.repository.full_name);
      const cursorMs = yield* queue.notificationCursor(thread.id);
      const subject = subjectRef(thread);
      const live =
        subject === undefined
          ? false
          : yield* queue.livenessFor(thread.repository.full_name, subject.kind, subject.number);
      const { work, lastActivityAt } = yield* qualifyNotification({
        deliveryId: stored.id,
        thread,
        policy: { selfLogin: login, trustedSenders: threadPolicy.trustedSenders },
        cursorMs,
        live,
      });
      // ! Advanced only after the job commits, never before: a cursor moved
      // ! past a failed `enqueue` loses the comment that caused it all.
      const advance = Number.isFinite(lastActivityAt)
        ? queue.advanceNotificationCursor(thread.id, lastActivityAt)
        : Effect.void;

      if (work === undefined) return yield* advance;

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

      // Fresh inserts only: a replayed notification must not acknowledge twice.
      if (enqueued.inserted && work.context !== undefined) {
        yield* acknowledge(work, work.context, enqueued.jobId);
      }

      // ! Armed by triggering turns only, never by continuations: an untrusted
      // ! reply must not extend anyone's live window. Best-effort like the
      // ! acknowledgement — the job is already committed, so arming must not
      // ! burn the delivery's attempt budget.
      if (enqueued.inserted && work.continuation !== true && work.context !== undefined) {
        yield* queue
          .markLive({
            repository: work.repository,
            subjectKind: work.subject.kind,
            subjectNumber: work.subject.number,
            expiresAt: Date.now() + policy.livenessMs,
          })
          .pipe(
            Effect.catchAll((error) =>
              Effect.logWarning('Could not arm the thread live window').pipe(
                Effect.annotateLogs({
                  job: enqueued.jobId,
                  reason: describeCause(Cause.fail(error)),
                }),
              ),
            ),
          );
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
        // ! A dead credential says nothing about the delivery, and `failed` is
        // ! terminal and memoized — condemning one would destroy the whole inbox
        // ! at SQLite speed while shutdown propagates. Retry instead.
        Effect.catchTag('GitHubIdentityError', (error) =>
          queue
            .retryDelivery(stored.id, String(error), false)
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
          // Terminal row: nothing reads it again, so log the reason here.
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
        // Retry rather than condemn: only parse/schema failures are provably
        // permanent (named above); the attempt budget stops the rest looping.
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.interrupt
            : // Logged per retry: the eventual terminal flip writes only the row.
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
