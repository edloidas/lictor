import { Clock, Effect, Exit, PartitionedSemaphore, Ref } from 'effect';
import { LictorConfig } from './config.ts';
import { AgentExecutor, ExecutorError } from './executor/agent-executor.ts';
import { CredentialHealth } from './github/credential-health.ts';
import { canonicalRepository, Policy, policyRefusal } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { RepositoryWorkspace, WorkspaceError } from './workspace/repository-workspace.ts';

/**
 * Who may answer a question this job asks, fixed when it is asked rather than
 * resolved when a reply arrives — resolving it later would let a change of
 * policy hand an old question to someone it was never asked of.
 */
const answerersFor = (
  sender: string,
  trustedSenders: readonly string[],
  selfLogin: string,
): readonly string[] => [
  ...new Set(
    [sender, ...trustedSenders]
      .map((login) => login.toLowerCase())
      .filter((login) => login !== '' && login !== selfLogin.toLowerCase()),
  ),
];

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
      const job = yield* queue.claimFor(queue.ownerId);
      if (job === undefined) return false;
      yield* Effect.logInfo('Claimed queued work').pipe(
        Effect.annotateLogs({ job: job.id, attempt: job.attempts }),
      );
      // Measured from the claim, not the child spawn, so clone and cleanup
      // count toward every outcome's duration.
      const claimedAt = yield* Clock.currentTimeMillis;

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
      const refusal = policyRefusal({
        repository: repositoryPolicy,
        attempts: job.attempts,
        readyAt: job.readyAt,
        approvalRequired: job.work.approvalRequired,
        maxJobAgeMs: policy.maxJobAgeMs,
        now: policyTime,
      });
      if (refusal !== undefined) {
        yield* queue.fail(job.id, job.attempts, refusal, undefined, 'failed', {
          repository: job.work.repository,
          subjectNumber: job.work.subject.number,
          outcome: 'failed',
        });
        yield* Effect.logWarning('Dropped queued work denied by policy').pipe(
          Effect.annotateLogs({
            job: job.id,
            attempt: job.attempts,
            errorCode: refusal,
            durationMs: policyTime - claimedAt,
          }),
        );
        return true;
      }

      // Parking spends no attempt but restores none, so a question asked with
      // the budget already gone parks a row the next claim dead-letters on
      // sight — the thread would read: I need an answer, answered, this did
      // not finish. Finish now and say so instead.
      const attemptsLeft =
        job.attempts < Math.min(repositoryPolicy.maxAttempts, config.workerMaxAttempts);

      // ! Before the workspace is acquired, so a request the record could not
      // ! hold never reaches the agent at all. Truncated instructions read as
      // ! whole ones, and the cut part is precisely what nothing downstream can
      // ! weigh the absence of. Asked once: a resumed job carries an answer and
      // ! runs on it, or this would re-ask every claim and never progress.
      if (job.work.trigger?.clipped === true && job.work.answerUrl === undefined) {
        const clippedAt = yield* Clock.currentTimeMillis;
        const reason = 'Request exceeded the recordable bound; refused to act on part of it';
        if (attemptsLeft) {
          yield* queue.park({
            jobId: job.id,
            attemptNumber: job.attempts,
            repository: job.work.repository,
            subjectNumber: job.work.subject.number,
            question: reason,
            answerers: answerersFor(
              job.work.sender,
              repositoryPolicy.trustedSenders,
              config.expectedLogin,
            ),
            expiresAt: clippedAt + policy.answerExpiryMs,
            outcome: 'clipped',
          });
        } else {
          // No attempt left to ask with. `rejected` is the honest outcome: the
          // daemon decided not to carry this out, and the reason stays in the
          // row rather than on the thread.
          yield* queue.fail(job.id, job.attempts, reason, undefined, 'rejected', {
            repository: job.work.repository,
            subjectNumber: job.work.subject.number,
            outcome: 'rejected',
          });
        }
        yield* Effect.logWarning('Refused queued work recorded from a clipped request').pipe(
          Effect.annotateLogs({
            job: job.id,
            attempt: job.attempts,
            asked: attemptsLeft,
            durationMs: clippedAt - claimedAt,
          }),
        );
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
                  // Quarantined for the post-mortem, not for a rerun: a retry
                  // clones afresh, and this attempt is now the last one. The
                  // pool is capped, so keeping every failure costs nothing the
                  // sweep does not reclaim.
                  Effect.tap((result) =>
                    result.status === 'failed' ? Ref.set(retainWorkspace, true) : Effect.void,
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
        const finishedAt = yield* Clock.currentTimeMillis;
        if (result.right.status === 'completed') {
          yield* queue.complete(job.id, job.attempts, JSON.stringify(result.right), {
            repository: job.work.repository,
            subjectNumber: job.work.subject.number,
            outcome: 'completed',
            note: result.right.summary,
          });
          yield* Effect.logInfo('Completed queued work').pipe(
            Effect.annotateLogs({
              job: job.id,
              attempt: job.attempts,
              status: result.right.status,
              durationMs: finishedAt - claimedAt,
            }),
          );
          return true;
        }
        if (result.right.status === 'needs_input' && attemptsLeft) {
          const answerers = answerersFor(
            job.work.sender,
            repositoryPolicy.trustedSenders,
            config.expectedLogin,
          );
          yield* queue.park({
            jobId: job.id,
            attemptNumber: job.attempts,
            repository: job.work.repository,
            subjectNumber: job.work.subject.number,
            question: result.right.summary,
            answerers,
            expiresAt: finishedAt + policy.answerExpiryMs,
          });
          yield* Effect.logInfo('Parked queued work pending an answer').pipe(
            Effect.annotateLogs({
              job: job.id,
              attempt: job.attempts,
              status: result.right.status,
              durationMs: finishedAt - claimedAt,
              answerers: answerers.join(','),
            }),
          );
          return true;
        }
        // ! Every status the agent returns is terminal, `failed` included. A
        // ! retry is earned by an observed cause — an exit code, a signature in
        // ! stderr, a refused credential — and those all arrive on the failure
        // ! branch below with `retryable` computed from evidence. `failed` here
        // ! is only the agent's opinion of a run whose input the next attempt
        // ! would reproduce byte for byte, and scheduling one costs the thread
        // ! its answer: `queue.fail` withholds the outbox row until an attempt
        // ! is final, so a mislabelled capability denial used to buy silence for
        // ! the whole budget instead of the explanation the agent had in hand.
        yield* queue.fail(
          job.id,
          job.attempts,
          result.right.summary,
          undefined,
          result.right.status,
          {
            repository: job.work.repository,
            subjectNumber: job.work.subject.number,
            outcome: result.right.status,
            note: result.right.summary,
          },
        );
        // `summary` is parsed out of Codex stdout and stays in the database:
        // logging it would echo whatever the repository made the agent say.
        yield* Effect.logWarning('Queued work did not complete').pipe(
          Effect.annotateLogs({
            job: job.id,
            attempt: job.attempts,
            status: result.right.status,
            durationMs: finishedAt - claimedAt,
          }),
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
      yield* queue.fail(job.id, job.attempts, result.left.message, retryAt, 'failed', {
        repository: job.work.repository,
        subjectNumber: job.work.subject.number,
        outcome: 'failed',
      });
      yield* Effect.logWarning(retry ? 'Queued work will retry' : 'Queued work failed').pipe(
        Effect.annotateLogs({
          job: job.id,
          attempt: job.attempts,
          error: result.left.message,
          errorCode: result.left.retryable ? 'EXECUTOR_RETRYABLE' : 'EXECUTOR_FAILED',
          durationMs: now - claimedAt,
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
