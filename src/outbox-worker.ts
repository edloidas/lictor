import { Cause, Clock, Effect, Schedule } from 'effect';
import { isTagged } from 'effect/Predicate';
import { LictorConfig } from './config.ts';
import { describeCause } from './diagnostics.ts';
import { GitHubClient } from './github/client.ts';
import { CredentialHealth } from './github/credential-health.ts';
import { outcomeReaction, staleContents } from './github/outcome-reaction.ts';
import { Policy } from './policy.ts';
import {
  OUTBOX_FENCED_OPERATIONS,
  type OutboxMessage,
  type QueueError,
  WorkQueue,
} from './queue/work-queue.ts';
import type { ContextRef } from './work-item.ts';

const OUTBOX_BACKOFF_CAP_MS = 300_000;
/**
 * The whole reconciliation — an add, a listing, and a delete per stale reaction.
 * Bounded well inside `OUTBOX_LEASE_MS` so a slow target cannot let a second
 * sender claim the row out from under this one.
 */
const DELIVERY_TIMEOUT = '45 seconds';

const claimLost: ReadonlySet<string> = new Set(Object.values(OUTBOX_FENCED_OPERATIONS));

const isClaimLost = (error: unknown): error is QueueError =>
  isTagged('QueueError')(error) && claimLost.has((error as QueueError).operation);

const reactionTarget = (message: OutboxMessage): ContextRef =>
  message.context ?? { kind: 'body', number: message.subjectNumber };

/**
 * Resolves the acknowledgement a terminal outcome left on its GitHub thread.
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
        Effect.map(
          Clock.currentTimeMillis,
          (now) =>
            now +
            (extraMs ??
              Math.min(
                config.workerRetryBaseMs * 2 ** Math.max(0, message.attempts - 1),
                OUTBOX_BACKOFF_CAP_MS,
              )),
        );

      const deliver = Effect.gen(function* () {
        const repositoryPolicy = policy.forRepository(message.repository);
        // Admission alone, the same gate the acknowledgement keyed on: a
        // reaction is the daemon's own state signal, not agent authority, and
        // a capability check here would leave eyes that never resolve.
        if (!repositoryPolicy.accepted) {
          yield* queue.finishOutbox(message.id, message.attempts, 'blocked', 'blocked_by_policy');
          yield* Effect.logWarning('Outcome cannot be signalled; repository is not accepted').pipe(
            Effect.annotateLogs({
              job: message.jobId,
              repository: message.repository,
              outcome: message.outcome,
            }),
          );
          return;
        }
        const content = outcomeReaction(message.outcome);
        // An outcome with nothing to say leaves the acknowledgement alone. The
        // row is still owed a terminal status, so it settles here rather than
        // being claimed again every poll.
        if (content === undefined) {
          yield* queue.deliverOutbox(message.id, message.attempts);
          return;
        }
        // ! Re-read immediately before the request, because the fenced write
        // ! comes after it: an operator retry cancels this row mid-reconcile,
        // ! and `deliverOutbox` only notices once the reaction is already there.
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
        const target = reactionTarget(message);
        const firstAttempt = message.attempt <= 1 && message.attempts <= 1;
        yield* github
          .reconcileReaction(message.repository, target, content, staleContents(firstAttempt))
          .pipe(Effect.timeout(DELIVERY_TIMEOUT));
        yield* queue.deliverOutbox(message.id, message.attempts);
        yield* queue
          .recordAudit({
            jobId: message.jobId,
            repository: message.repository,
            actor: 'daemon',
            capability: 'react',
            input: JSON.stringify({ target, content }),
            outcome: 'ok',
          })
          .pipe(Effect.ignore);
        yield* Effect.logInfo('Signalled the outcome on the thread').pipe(
          Effect.annotateLogs({
            job: message.jobId,
            repository: message.repository,
            outcome: message.outcome,
            reaction: content,
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
          // The target is gone — a deleted trigger comment, or the subject
          // itself — so nothing can be reacted on, ever. A stale reaction
          // already removed never reaches here; that 404 means converged, and
          // `reconcileReaction` swallows it.
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
