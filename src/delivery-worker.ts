import { Cause, Data, Effect, Schedule } from 'effect';
import { LictorConfig } from './config.ts';
import { describeCause } from './diagnostics.ts';
import { GitHubClient } from './github/client.ts';
import { GitHubIdentity } from './github/identity.ts';
import { registry } from './handlers/index.ts';
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { decodePayload } from './webhook/event.ts';
import { dispatch } from './webhook/router.ts';

class InvalidStoredDelivery extends Data.TaggedError('InvalidStoredDelivery')<{
  readonly cause: unknown;
}> {}

export class DeliveryWorker extends Effect.Service<DeliveryWorker>()('DeliveryWorker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const queue = yield* WorkQueue;
    const runOnce = Effect.gen(function* () {
      const stored = yield* queue.claimDelivery;
      if (stored === undefined) return false;
      const process = Effect.gen(function* () {
        const raw = yield* Effect.try({
          try: () => JSON.parse(stored.body) as unknown,
          catch: (cause) => new InvalidStoredDelivery({ cause }),
        });
        const payload = yield* decodePayload(raw);
        yield* dispatch(registry)({ id: stored.id, event: stored.event, payload, raw });
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
            .pipe(Effect.zipRight(Effect.sleep(Math.min(config.workerRetryBaseMs, 60_000)))),
        ),
        Effect.catchTag('QueueError', (error) =>
          queue
            .retryDelivery(
              stored.id,
              String(error),
              (error.cause as Error | undefined)?.message !== 'QUEUE_DEPTH_LIMIT',
            )
            .pipe(
              Effect.zipRight(
                Effect.sleep(
                  Math.min(
                    config.workerRetryBaseMs * 2 ** Math.max(0, stored.attempts - 1),
                    60_000,
                  ),
                ),
              ),
            ),
        ),
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.interrupt
            : // ! `failed` is terminal, so something has to be logged here or the
              // ! reason is lost: nothing downstream ever reads the row again.
              Effect.logError('Delivery failed permanently')
                .pipe(
                  Effect.annotateLogs({
                    delivery: stored.id,
                    event: stored.event,
                    reason: describeCause(cause),
                  }),
                )
                .pipe(
                  Effect.zipRight(
                    queue.finishDelivery(stored.id, 'failed', 'DELIVERY_PROCESSING_FAILED'),
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
