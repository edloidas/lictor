import { Cause, Data, Effect, Schedule } from 'effect';
import { LictorConfig } from './config.ts';
import { GitHubClient } from './github/client.ts';
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
        Effect.catchTag('QueueError', (error) => queue.retryDelivery(stored.id, String(error))),
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.interrupt
            : queue.finishDelivery(stored.id, 'failed', 'DELIVERY_PROCESSING_FAILED'),
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
  dependencies: [LictorConfig.Default, WorkQueue.Default, GitHubClient.Default, Policy.Default],
}) {}
