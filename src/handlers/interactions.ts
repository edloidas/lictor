import { Effect } from 'effect';
import { LictorConfig } from '../config.ts';
import { WorkQueue } from '../queue/work-queue.ts';
import { qualifyDelivery, supportsInteraction } from '../webhook/qualification.ts';
import type { Handler } from '../webhook/router.ts';

export const handleInteraction: Handler = (delivery) =>
  Effect.gen(function* () {
    if (!supportsInteraction(delivery.event, delivery.payload.action)) {
      yield* Effect.logDebug('Dropped unsupported interaction').pipe(
        Effect.annotateLogs({ delivery: delivery.id, event: delivery.event }),
      );
      return;
    }

    const config = yield* LictorConfig;
    const work = yield* qualifyDelivery(delivery, config);
    if (work === undefined) return;
    const queue = yield* WorkQueue;
    const enqueued = yield* queue.enqueue(work);

    yield* Effect.logInfo(
      enqueued.inserted ? 'Queued GitHub interaction' : 'Ignored duplicate delivery',
    ).pipe(
      Effect.annotateLogs({
        job: enqueued.jobId,
        delivery: work.deliveryId,
        repository: work.repository,
        sender: work.sender,
        targets: work.targets.join(','),
        reasons: work.reasons.join(','),
        subject: `${work.subject.kind}#${work.subject.number}`,
      }),
    );
  }).pipe(
    Effect.catchAll((error) =>
      Effect.logWarning(
        error._tag === 'QueueError'
          ? 'Could not queue GitHub interaction'
          : 'Dropped malformed interaction payload',
        error,
      ).pipe(Effect.annotateLogs({ delivery: delivery.id, event: delivery.event })),
    ),
  );
