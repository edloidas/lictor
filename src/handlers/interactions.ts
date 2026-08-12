import { Effect } from 'effect';
import { LictorConfig } from '../config.ts';
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

    yield* Effect.logInfo('Qualified GitHub interaction').pipe(
      Effect.annotateLogs({
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
      Effect.logWarning('Dropped malformed interaction payload', error).pipe(
        Effect.annotateLogs({ delivery: delivery.id, event: delivery.event }),
      ),
    ),
  );
