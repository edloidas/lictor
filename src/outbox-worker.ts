import { Cause, Clock, Data, Effect, Schedule } from 'effect';
import { isTagged } from 'effect/Predicate';
import { LictorConfig } from './config.ts';
import { describeCause } from './diagnostics.ts';
import { GitHubClient } from './github/client.ts';
import { CredentialHealth } from './github/credential-health.ts';
import { outcomeMarker, renderOutcome } from './github/outcome-comment.ts';
import { Policy } from './policy.ts';
import {
  OUTBOX_FENCED_OPERATIONS,
  type OutboxMessage,
  type QueueError,
  WorkQueue,
} from './queue/work-queue.ts';

const OUTBOX_BACKOFF_CAP_MS = 300_000;
/** Well inside the row's lease, so a hung request cannot outlive its claim. */
const REQUEST_TIMEOUT = '30 seconds';
/** Pages of an issue's comments a reconciliation will read before giving up. */
const RECONCILE_PAGES = 10;
/** The whole scan. Ten pages at the per-request timeout would outlast the lease. */
const RECONCILE_TIMEOUT = '20 seconds';

/** A scan that ran out of pages, which is not the same answer as "not there". */
class ReconcileUnresolved extends Data.TaggedError('ReconcileUnresolved')<{
  readonly messageId: string;
}> {}

const claimLost: ReadonlySet<string> = new Set(Object.values(OUTBOX_FENCED_OPERATIONS));

const isClaimLost = (error: unknown): error is QueueError =>
  isTagged('QueueError')(error) && claimLost.has((error as QueueError).operation);

/**
 * Whether this message is already on the thread.
 *
 * ! Only an exhausted thread answers "not there". A scan that failed, and one
 * ! that ran out of pages while the thread kept going, are both unresolved —
 * ! they fail the caller rather than reporting absence and licensing a second
 * ! post.
 */
const findPosted = (github: GitHubClient, message: OutboxMessage) =>
  Effect.gen(function* () {
    const marker = outcomeMarker(message.messageId);
    for (let page = 1; page <= RECONCILE_PAGES; page += 1) {
      const comments = yield* github
        .listComments(message.repository, message.subjectNumber, message.createdAt, page)
        .pipe(Effect.timeout(REQUEST_TIMEOUT));
      const found = comments.find((comment) => comment.body?.includes(marker) === true);
      if (found !== undefined) return found.html_url ?? '';
      if (comments.length === 0) return undefined;
    }
    return yield* new ReconcileUnresolved({ messageId: message.messageId });
  }).pipe(Effect.timeout(RECONCILE_TIMEOUT));

/**
 * Delivers the message a terminal outcome owes its GitHub thread.
 *
 * Separate from the job it describes on purpose: the job is finished before the
 * row is claimed, so no delivery failure can spend an execution attempt or rerun
 * an agent whose side effects already landed.
 */
export class OutboxWorker extends Effect.Service<OutboxWorker>()('OutboxWorker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const queue = yield* WorkQueue;
    const github = yield* GitHubClient;
    const policy = yield* Policy;
    const health = yield* CredentialHealth;

    const runOnce = Effect.gen(function* () {
      // Nothing is claimed against a credential GitHub has already refused: the
      // latch never clears, so every claim past it would spend a message's
      // budget on a request that cannot succeed.
      if (yield* health.isRejected) return false;
      const message = yield* queue.claimOutbox;
      if (message === undefined) return false;
      // Read when the failure happens, never at the claim: the request itself
      // takes time, and a wait GitHub measured from its own response shrinks by
      // that much if it is added to a timestamp from before the request.
      const waitUntil = (extraMs?: number) =>
        Effect.map(Clock.currentTimeMillis, (now) =>
          extraMs === undefined
            ? now +
              Math.min(
                config.workerRetryBaseMs * 2 ** Math.max(0, message.attempts - 1),
                OUTBOX_BACKOFF_CAP_MS,
              )
            : now + extraMs,
        );

      const deliver = Effect.gen(function* () {
        const repositoryPolicy = policy.forRepository(message.repository);
        // Admission and capability both, as the broker checks them: a repository
        // policy denies can still carry `comment: true`.
        if (!repositoryPolicy.accepted || repositoryPolicy.capabilities.comment !== true) {
          yield* queue.finishOutbox(message.id, message.attempts, 'blocked', 'blocked_by_policy');
          yield* Effect.logWarning('Outcome cannot be posted; policy forbids commenting').pipe(
            Effect.annotateLogs({
              job: message.jobId,
              repository: message.repository,
              outcome: message.outcome,
            }),
          );
          return;
        }
        // Attempt 1 is the first send; anything above it follows an attempt that
        // may have reached GitHub before losing its claim.
        if (message.attempts > 1) {
          const posted = yield* findPosted(github, message);
          if (posted !== undefined) {
            yield* queue.deliverOutbox(message.id, message.attempts, posted);
            yield* Effect.logInfo('Reconciled an outcome already on the thread').pipe(
              Effect.annotateLogs({ job: message.jobId, outcome: message.outcome }),
            );
            return;
          }
        }
        // ! Re-read immediately before the request, because the fenced write
        // ! comes after it: an operator retry cancels this row mid-reconcile,
        // ! and `deliverOutbox` only notices once the comment is already there.
        const claim = yield* queue.outboxHeld(message.id, message.attempts);
        if (!claim.held) {
          // A lost claim is loud because a sender losing it every time goes
          // quiet forever, which is the failure this whole feature prevents.
          yield* claim.superseded
            ? Effect.logInfo('Dropped an outbox message superseded while sending').pipe(
                Effect.annotateLogs({ job: message.jobId, outcome: message.outcome }),
              )
            : Effect.logWarning('Lost an outbox claim before sending; another pass owns it').pipe(
                Effect.annotateLogs({
                  job: message.jobId,
                  attempt: message.attempts,
                  outcome: message.outcome,
                  status: claim.status ?? 'gone',
                }),
              );
          return;
        }
        const comment = yield* github
          .createComment(message.repository, message.subjectNumber, renderOutcome(message))
          .pipe(Effect.timeout(REQUEST_TIMEOUT));
        yield* queue.deliverOutbox(message.id, message.attempts, comment.url);
        yield* Effect.logInfo('Posted the outcome to the thread').pipe(
          Effect.annotateLogs({
            job: message.jobId,
            repository: message.repository,
            outcome: message.outcome,
          }),
        );
      });

      yield* deliver.pipe(
        Effect.catchIf(isClaimLost, (error) =>
          Effect.logWarning('Abandoned an outbox message this worker no longer holds').pipe(
            Effect.annotateLogs({
              job: message.jobId,
              attempt: message.attempts,
              operation: error.operation,
            }),
          ),
        ),
        Effect.catchTag('GitHubStatusError', (error) => {
          if (error.status === 401) {
            // The latch stops the loop, so the attempt is refunded: a dead
            // credential must not spend a message's budget while it is set.
            return health.suspend.pipe(
              Effect.zipRight(waitUntil()),
              Effect.flatMap((at) =>
                queue.retryOutbox(message.id, message.attempts, error.message, at, false),
              ),
            );
          }
          // The subject is gone. Nothing can be posted to it, ever.
          if (error.status === 404 || error.status === 410) {
            return queue.finishOutbox(message.id, message.attempts, 'failed', error.message);
          }
          return Effect.flatMap(waitUntil(error.retryAfterMs), (at) =>
            queue.retryOutbox(message.id, message.attempts, error.message, at),
          );
        }),
        Effect.catchTag('QueueError', (error) =>
          Effect.logError('Could not record an outbox delivery').pipe(
            Effect.annotateLogs({ job: message.jobId, operation: error.operation }),
          ),
        ),
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning('Retrying an undelivered outcome').pipe(
                Effect.annotateLogs({
                  job: message.jobId,
                  attempt: message.attempts,
                  reason: describeCause(cause),
                }),
                Effect.zipRight(
                  Effect.flatMap(waitUntil(), (at) =>
                    queue
                      .retryOutbox(message.id, message.attempts, describeCause(cause), at)
                      .pipe(Effect.ignore),
                  ),
                ),
              ),
        ),
      );
      return true;
    });

    const drain = Effect.repeat(runOnce, { until: (delivered) => !delivered }).pipe(Effect.asVoid);
    const run = drain.pipe(
      Effect.catchAllCause((cause) => Effect.logError('Outbox worker cycle failed', cause)),
      Effect.repeat(Schedule.spaced(`${config.workerPollMs} millis`)),
    );
    return { runOnce, drain, run };
  }),
  dependencies: [
    LictorConfig.Default,
    WorkQueue.Default,
    GitHubClient.Default,
    Policy.Default,
    CredentialHealth.Default,
  ],
}) {}
