import { Clock, Effect, Exit, PartitionedSemaphore, Ref } from 'effect';
import { LictorConfig } from './config.ts';
import { AgentExecutor, ExecutorError } from './executor/agent-executor.ts';
import { CredentialHealth } from './github/credential-health.ts';
import { canonicalRepository, Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { RepositoryWorkspace, WorkspaceError } from './workspace/repository-workspace.ts';

export class Worker extends Effect.Service<Worker>()('Worker', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const executor = yield* AgentExecutor;
    const queue = yield* WorkQueue;
    const policy = yield* Policy;
    const workspaces = yield* RepositoryWorkspace;
    const health = yield* CredentialHealth;
    // PartitionedSemaphore permits are global, shared across all keys
    // round-robin: `permits: 1` is a daemon-wide mutex with fair queuing, not
    // one permit per repository. Safe — each job's session is its own — but it
    // only orders work; genuine per-repository serialization needs a different
    // construct before any concurrent worker fiber exists.
    const locks = yield* PartitionedSemaphore.make<string>({ permits: 1 });

    const runOnce = Effect.gen(function* () {
      if (!executor.enabled) return false;
      // Before the claim, not after: a claimed job spends its attempt the moment
      // it runs, and a dead credential makes every claim burn it on a clone that
      // cannot push.
      if (yield* health.isRejected) return false;
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
      // PR jobs clone at their head (`refs/pull/<n>/head` works for forks too;
      // the default branch would review the wrong tree). A branch a previous
      // interaction created wins over both: continuing her own work beats
      // re-reading a moved head.
      const priorBranch = yield* queue.branchForSubject(
        job.work.repository,
        job.work.subject.kind,
        job.work.subject.number,
      );
      let ref: string | undefined;
      if (priorBranch !== undefined) {
        // Stored bare; the workspace fetches full refs.
        ref = `refs/heads/${priorBranch}`;
      } else if (job.work.subject.kind === 'pull_request') {
        ref = `refs/pull/${job.work.subject.number}/head`;
      }
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
      const execution = locks
        .withPermits(
          canonicalRepository(job.work.repository),
          1,
        )(
          Effect.acquireUseRelease(
            workspaces.acquire(
              {
                id: job.id,
                repository: job.work.repository,
                ...(ref === undefined ? {} : { ref }),
              },
              repositoryPolicy,
            ),
            (workspace) =>
              executor
                .execute(
                  job.work,
                  workspace.path,
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
            (_workspace, exit) =>
              Ref.get(retainWorkspace)
                .pipe(
                  Effect.flatMap((retain) =>
                    workspaces.release(job.id, { retain: retain || Exit.isFailure(exit) }),
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
                  // A refused credential never heals and every retry pays another
                  // clone; only transient workspace failures are worth an attempt.
                  retryable: cause instanceof WorkspaceError ? cause.retryable !== false : true,
                  ...(cause instanceof WorkspaceError && cause.retryAfterMs !== undefined
                    ? { retryAfterMs: cause.retryAfterMs }
                    : {}),
                  cause,
                }),
          ),
        );
      const result = yield* Effect.either(Effect.raceFirst(execution, keepLease));
      // Git classifies a refused credential from stderr prose; latch the
      // daemon-wide breaker here so nothing claims while it is dead.
      if (
        result._tag === 'Left' &&
        result.left.cause instanceof WorkspaceError &&
        result.left.cause.code === 'WORKSPACE_CREDENTIAL_REJECTED'
      ) {
        yield* health.suspend;
      }
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
