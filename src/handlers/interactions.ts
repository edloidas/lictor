import { Effect } from 'effect';
import { LictorConfig } from '../config.ts';
import { Policy } from '../policy.ts';
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
    const policy = yield* Policy;
    const repositoryPolicy = policy.forRepository(work.repository);
    if (!repositoryPolicy.accepted || repositoryPolicy.execution === 'denied') {
      yield* Effect.logInfo('Dropped interaction denied by repository policy').pipe(
        Effect.annotateLogs({ delivery: work.deliveryId, repository: work.repository }),
      );
      return;
    }
    const queue = yield* WorkQueue;
    const enqueued = yield* queue.enqueue(
      {
        ...work,
        ...(repositoryPolicy.execution === 'approval' ? { approvalRequired: true } : {}),
      },
      policy.maxQueueDepth,
    );

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
  });
