import { Clock, Effect, Exit, Ref } from 'effect';
import { LictorConfig } from './config.ts';
import { AgentExecutor, ExecutorError } from './executor/agent-executor.ts';
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { RepositoryWorkspace, WorkspaceError } from './workspace/repository-workspace.ts';

export class Worker extends Effect.Service<Worker>()('Worker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const executor = yield* AgentExecutor;
    const queue = yield* WorkQueue;
    const policy = yield* Policy;
    const workspaces = yield* RepositoryWorkspace;

    const runOnce = Effect.gen(function* () {
      if (!executor.enabled) return false;
      const job = yield* queue.claimFor(queue.ownerId, policy.maxJobAgeMs);
      if (job === undefined) return false;
      yield* Effect.logInfo('Claimed queued work').pipe(
        Effect.annotateLogs({ job: job.id, attempt: job.attempts }),
      );

      const keepLease = Effect.forever(
        Effect.sleep('1 second').pipe(
          Effect.zipRight(queue.heartbeat(job.id, job.attempts, job.workerId ?? queue.ownerId)),
        ),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ExecutorError({ message: 'Worker lease renewal failed', retryable: true, cause }),
        ),
      );
      const repositoryPolicy = policy.forRepository(job.work.repository);
      const policyTime = yield* Clock.currentTimeMillis;
      if (
        !repositoryPolicy.accepted ||
        repositoryPolicy.execution === 'denied' ||
        (repositoryPolicy.execution === 'approval' && job.work.approvalRequired !== false) ||
        job.attempts > repositoryPolicy.maxAttempts ||
        policyTime - job.createdAt > policy.maxJobAgeMs
      ) {
        yield* queue.fail(job.id, job.attempts, 'POLICY_COST_LIMIT');
        return true;
      }
      const retainWorkspace = yield* Ref.make(false);
      const execution = workspaces
        .withRepositoryLock(
          job.work.repository,
          Effect.acquireUseRelease(
            workspaces.create(job.id, job.work, repositoryPolicy),
            (workspace) =>
              executor
                .execute(
                  job.work,
                  workspace.worktreePath,
                  repositoryPolicy.maxDurationMs,
                  job.id,
                  job.attempts,
                  job.workerId,
                )
                .pipe(
                  Effect.tap((result) =>
                    result.status === 'failed' && job.attempts < repositoryPolicy.maxAttempts
                      ? Ref.set(retainWorkspace, true)
                      : Effect.void,
                  ),
                ),
            (workspace, exit) =>
              Ref.get(retainWorkspace)
                .pipe(
                  Effect.flatMap((retain) =>
                    workspaces.cleanup(workspace, retain || Exit.isFailure(exit)),
                  ),
                )
                .pipe(
                  Effect.catchAll((cause) =>
                    Effect.logError('Workspace cleanup failed').pipe(
                      Effect.annotateLogs({ job: job.id, error: cause.message }),
                    ),
                  ),
                ),
          ),
        )
        .pipe(
          Effect.mapError((cause) =>
            cause instanceof ExecutorError
              ? cause
              : new ExecutorError({
                  message:
                    cause instanceof WorkspaceError
                      ? cause.message
                      : 'Could not prepare or clean up the isolated workspace',
                  // ! A refused credential never heals, and each retry pays for
                  // ! another clone. Only genuinely transient workspace failures
                  // ! are worth another attempt.
                  retryable: cause instanceof WorkspaceError ? cause.retryable !== false : true,
                  ...(cause instanceof WorkspaceError && cause.retryAfterMs !== undefined
                    ? { retryAfterMs: cause.retryAfterMs }
                    : {}),
                  cause,
                }),
          ),
        );
      const result = yield* Effect.either(Effect.raceFirst(execution, keepLease));
      if (result._tag === 'Right') {
        if (result.right.status === 'failed' || result.right.status === 'needs_input') {
          const retryAt =
            result.right.status === 'failed' && job.attempts < repositoryPolicy.maxAttempts
              ? (yield* Clock.currentTimeMillis) +
                config.workerRetryBaseMs * 2 ** Math.max(0, job.attempts - 1)
              : undefined;
          yield* queue.fail(job.id, job.attempts, result.right.summary, retryAt);
          return true;
        }
        yield* queue.complete(job.id, job.attempts, JSON.stringify(result.right));
        yield* Effect.logInfo('Completed queued work').pipe(
          Effect.annotateLogs({ job: job.id, attempt: job.attempts }),
        );
        return true;
      }

      const retry = result.left.retryable && job.attempts < config.workerMaxAttempts;
      const now = yield* Clock.currentTimeMillis;
      const retryAt = retry
        ? now +
          (result.left.retryAfterMs ??
            config.workerRetryBaseMs * 2 ** Math.max(0, job.attempts - 1))
        : undefined;
      yield* queue.fail(job.id, job.attempts, result.left.message, retryAt);
      yield* Effect.logWarning(retry ? 'Queued work will retry' : 'Queued work failed').pipe(
        Effect.annotateLogs({
          job: job.id,
          attempt: job.attempts,
          error: result.left.message,
          errorCode: result.left.retryable ? 'EXECUTOR_RETRYABLE' : 'EXECUTOR_FAILED',
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
  dependencies: [
    LictorConfig.Default,
    AgentExecutor.Default,
    WorkQueue.Default,
    Policy.Default,
    RepositoryWorkspace.Default,
  ],
}) {}
