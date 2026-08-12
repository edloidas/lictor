import { Clock, Effect } from 'effect';
import { LictorConfig } from './config.ts';
import { AgentExecutor, ExecutorError } from './executor/agent-executor.ts';
import { WorkQueue } from './queue/work-queue.ts';

export class Worker extends Effect.Service<Worker>()('Worker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const executor = yield* AgentExecutor;
    const queue = yield* WorkQueue;

    const runOnce = Effect.gen(function* () {
      if (!executor.enabled) return false;
      const job = yield* queue.claim;
      if (job === undefined) return false;
      yield* Effect.logInfo('Claimed queued work').pipe(
        Effect.annotateLogs({ job: job.id, attempt: job.attempts }),
      );

      const keepLease = Effect.forever(
        Effect.sleep('30 seconds').pipe(
          Effect.zipRight(queue.heartbeat(job.id, job.attempts, job.workerId ?? queue.ownerId)),
        ),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ExecutorError({ message: 'Worker lease renewal failed', retryable: true, cause }),
        ),
      );
      const result = yield* Effect.either(Effect.raceFirst(executor.execute(job.work), keepLease));
      if (result._tag === 'Right') {
        yield* queue.complete(job.id, job.attempts, result.right);
        yield* Effect.logInfo('Completed queued work').pipe(
          Effect.annotateLogs({ job: job.id, attempt: job.attempts }),
        );
        return true;
      }

      const retry = result.left.retryable && job.attempts < config.workerMaxAttempts;
      const now = yield* Clock.currentTimeMillis;
      const retryAt = retry
        ? now + config.workerRetryBaseMs * 2 ** Math.max(0, job.attempts - 1)
        : undefined;
      yield* queue.fail(job.id, job.attempts, result.left.message, retryAt);
      yield* Effect.logWarning(retry ? 'Queued work will retry' : 'Queued work failed').pipe(
        Effect.annotateLogs({
          job: job.id,
          attempt: job.attempts,
          error: result.left.message,
          ...(retryAt === undefined ? {} : { retryAt }),
        }),
      );
      return true;
    });

    const run = Effect.forever(
      runOnce.pipe(
        Effect.flatMap((worked) => (worked ? Effect.void : Effect.sleep(config.workerPollMs))),
        Effect.catchAll((error) =>
          Effect.logError('Worker loop failed', error).pipe(
            Effect.zipRight(Effect.sleep(config.workerPollMs)),
          ),
        ),
      ),
    );

    return { runOnce, run };
  }),
  dependencies: [LictorConfig.Default, AgentExecutor.Default, WorkQueue.Default],
}) {}
