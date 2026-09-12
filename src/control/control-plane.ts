import { chmodSync, mkdirSync, statfsSync, unlinkSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect } from 'effect';
import { LictorConfig } from '../config.ts';
import { CredentialHealth } from '../github/credential-health.ts';
import { grantedTools, mintGrant } from '../github/grant.ts';
import { Policy } from '../policy.ts';
import { WorkQueue } from '../queue/work-queue.ts';

export class ControlError extends Data.TaggedError('ControlError')<{
  readonly code: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export type ControlRequest = {
  readonly command: string;
  readonly args?: readonly string[];
};

/**
 * Where the answer was written, which is all the resumed agent is given —
 * bounded and `https` only, because it is published into the job payload and
 * read back as a link.
 */
const answerLocation = (value: string | undefined): Effect.Effect<string, ControlError> =>
  value?.startsWith('https://') && value.length <= 2048
    ? Effect.succeed(value)
    : Effect.fail(
        new ControlError({
          code: 'CONTROL_ANSWER_URL_INVALID',
          message: 'An https URL to the answer is required',
        }),
      );

const positiveId = (value: string | undefined): Effect.Effect<number, ControlError> => {
  const id = Number(value);
  return Number.isInteger(id) && id > 0
    ? Effect.succeed(id)
    : Effect.fail(
        new ControlError({
          code: 'CONTROL_JOB_ID_INVALID',
          message: 'A positive job id is required',
        }),
      );
};

export class ControlPlane extends Effect.Service<ControlPlane>()('ControlPlane', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const policy = yield* Policy;
    const queue = yield* WorkQueue;
    const health = yield* CredentialHealth;

    const mutate = (action: 'approve' | 'cancel' | 'retry', id: number) =>
      Effect.gen(function* () {
        const before = yield* queue.job(id);
        if (before === undefined)
          return yield* new ControlError({
            code: 'CONTROL_JOB_NOT_FOUND',
            message: `Job ${id} was not found`,
          });
        // The approval is the authorization decision, so the scope is minted
        // against policy as it stands now and recorded with it. Minting at the
        // first claim instead would record whatever policy a restart in between
        // loaded, labelled as the scope the operator released.
        //
        // Gated on the same state `mutateJob` approves from, not on the verb: an
        // `approve` the row will no-op is inert and audited as `no_change`, and
        // refusing it below would make it an error instead.
        const now = yield* Clock.currentTimeMillis;
        const authorized =
          action === 'approve' &&
          before.work.approvalRequired === true &&
          before.status === 'pending'
            ? mintGrant(
                policy.forRepository(before.work.repository),
                { ...before.work, approvalRequired: false },
                now,
              )
            : undefined;
        // ! An empty ceiling is never overwritten once written, so recording one
        // ! here would leave the row unrunnable and unreleasable by any policy
        // ! correction. Refuse the approval instead; the operator still holds it.
        if (
          authorized !== undefined &&
          grantedTools(authorized.capabilities, before.work.continuation === true).length === 0
        )
          return yield* new ControlError({
            code: 'CONTROL_APPROVAL_WITHOUT_CAPABILITY',
            message: `Repository policy leaves job ${id} no capability to act with`,
          });
        const changed = yield* {
          approve: () => queue.approve(id, authorized),
          // Retry re-parks a job still awaiting approval, so it needs the window
          // to date the new hold from.
          retry: () => queue.retry(id, policy.approvalExpiryMs),
          cancel: () => queue.cancel(id),
        }[action]();
        yield* queue.recordAudit({
          jobId: id,
          repository: before.work.repository,
          capability: `control.${action}`,
          input: '{}',
          outcome: changed ? 'ok' : 'no_change',
        });
        return { changed, jobId: id };
      });

    const execute = (
      request: ControlRequest,
    ): Effect.Effect<unknown, ControlError | import('../queue/work-queue.ts').QueueError> =>
      Effect.gen(function* () {
        const args = request.args ?? [];
        switch (request.command) {
          case 'status': {
            const diagnostics = yield* queue.diagnostics;
            const diskAvailableBytes = yield* Effect.try({
              try: () =>
                Number(statfsSync(dirname(config.databasePath)).bavail) *
                Number(statfsSync(dirname(config.databasePath)).bsize),
              catch: (cause) =>
                new ControlError({
                  code: 'CONTROL_DISK_FAILED',
                  message: 'Could not inspect state disk',
                  cause,
                }),
            });
            return {
              ...diagnostics,
              executor: config.executor,
              diskAvailableBytes,
              credentialRejected: yield* health.isRejected,
            };
          }
          case 'job.list':
            return yield* queue.listJobs(Number(args[0] ?? 100));
          case 'job.show': {
            const jobId = yield* positiveId(args[0]);
            // The outbox is where an outcome's `note` and a `blocked` delivery's
            // reason live. Nothing is published to the thread, so this is the
            // only place either can be read.
            //
            // ! Read first, and survive a job that will not decode. A payload
            // ! the schema no longer accepts is dead-lettered *and* owed a
            // ! message, and `queue.job` is the one call that fails on exactly
            // ! those rows — reading it first made the record unreachable for
            // ! the case it was added for.
            const outbox = yield* queue.outboxFor(jobId);
            const found = yield* Effect.orElseSucceed(queue.job(jobId), () => undefined);
            if (found !== undefined) return { ...found, outbox };
            return outbox.length === 0 ? undefined : { id: jobId, undecodable: true, outbox };
          }
          case 'job.approve':
            return yield* mutate('approve', yield* positiveId(args[0]));
          case 'job.retry':
            return yield* mutate('retry', yield* positiveId(args[0]));
          case 'job.cancel':
            return yield* mutate('cancel', yield* positiveId(args[0]));
          case 'job.answer': {
            const jobId = yield* positiveId(args[0]);
            const answerUrl = yield* answerLocation(args[1]);
            const parked = yield* queue.job(jobId);
            if (parked === undefined)
              return yield* new ControlError({
                code: 'CONTROL_JOB_NOT_FOUND',
                message: `Job ${jobId} was not found`,
              });
            if (parked.questionId === undefined)
              return yield* new ControlError({
                code: 'CONTROL_JOB_NOT_WAITING',
                message: `Job ${jobId} is not waiting on an answer`,
              });
            // The recorded answerers bind GitHub replies, not this socket. An
            // operator holding it already has `approve` and `cancel` on the same
            // row, so a list of logins here would lock them out of their own
            // lever without withholding anything they cannot already do.
            const changed = yield* queue.answerQuestion({
              jobId,
              questionId: parked.questionId,
              answerUrl,
            });
            yield* queue.recordAudit({
              jobId,
              repository: parked.work.repository,
              capability: 'control.answer',
              input: JSON.stringify({ answerUrl }),
              outcome: changed ? 'ok' : 'no_change',
            });
            return { changed, jobId };
          }
          case 'repository.list': {
            const jobs = yield* queue.listJobs(1000);
            return [...new Set(jobs.map((job) => job.work.repository))].sort();
          }
          case 'repository.inspect': {
            const repository = args[0]?.toLowerCase();
            if (repository === undefined)
              return yield* new ControlError({
                code: 'CONTROL_REPOSITORY_REQUIRED',
                message: 'A repository is required',
              });
            return policy.forRepository(repository);
          }
          case 'policy.check': {
            const repository = args[0];
            if (repository === undefined)
              return yield* new ControlError({
                code: 'CONTROL_REPOSITORY_REQUIRED',
                message: 'A repository is required',
              });
            return policy.forRepository(repository);
          }
          case 'prune': {
            const now = yield* Clock.currentTimeMillis;
            return yield* queue.maintenance(
              now - policy.completedRetentionDays * 86_400_000,
              now - policy.failedRetentionDays * 86_400_000,
            );
          }
          case 'backup': {
            const destination = args[0];
            if (destination === undefined) {
              return yield* new ControlError({
                code: 'CONTROL_BACKUP_PATH_REQUIRED',
                message: 'A backup destination is required',
              });
            }
            return yield* queue.backup(destination);
          }
          default:
            return yield* new ControlError({
              code: 'CONTROL_COMMAND_UNKNOWN',
              message: `Unknown command: ${request.command}`,
            });
        }
      });
    return { execute };
  }),
  dependencies: [LictorConfig.Default, Policy.Default, WorkQueue.Default, CredentialHealth.Default],
}) {}

export class ControlServer extends Effect.Service<ControlServer>()('ControlServer', {
  scoped: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const control = yield* ControlPlane;
    yield* Effect.try({
      try: () => {
        mkdirSync(dirname(config.controlSocketPath), { recursive: true, mode: 0o700 });
        try {
          unlinkSync(config.controlSocketPath);
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== 'ENOENT') throw cause;
        }
      },
      catch: (cause) =>
        new ControlError({
          code: 'CONTROL_SOCKET_PREPARE_FAILED',
          message: 'Could not prepare control socket',
          cause,
        }),
    });
    const server = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          Bun.listen<{ buffer: string }>({
            unix: config.controlSocketPath,
            socket: {
              open(socket) {
                socket.data = { buffer: '' };
              },
              data(socket, chunk) {
                socket.data.buffer += Buffer.from(chunk).toString('utf8');
                if (Buffer.byteLength(socket.data.buffer) > 256 * 1024) {
                  socket.end();
                  return;
                }
                const newline = socket.data.buffer.indexOf('\n');
                if (newline < 0) return;
                const source = socket.data.buffer.slice(0, newline);
                Effect.runFork(
                  Effect.try({
                    try: () => JSON.parse(source) as ControlRequest,
                    catch: (cause) =>
                      new ControlError({
                        code: 'CONTROL_REQUEST_INVALID',
                        message: 'Invalid control request',
                        cause,
                      }),
                  }).pipe(
                    Effect.flatMap(control.execute),
                    Effect.match({
                      onFailure: (error) => ({
                        ok: false,
                        error: {
                          code: error._tag === 'ControlError' ? error.code : 'CONTROL_QUEUE_FAILED',
                          message:
                            error._tag === 'ControlError'
                              ? error.message
                              : 'Queue operation failed',
                        },
                      }),
                      onSuccess: (result) => ({ ok: true, result }),
                    }),
                    Effect.tap((response) =>
                      Effect.sync(() => {
                        socket.write(`${JSON.stringify(response)}\n`);
                        socket.end();
                      }),
                    ),
                  ),
                );
              },
            },
          }),
        catch: (cause) =>
          new ControlError({
            code: 'CONTROL_SOCKET_LISTEN_FAILED',
            message: 'Could not listen on control socket',
            cause,
          }),
      }),
      (listener) => Effect.sync(() => listener.stop(true)),
    );
    yield* Effect.sync(() => chmodSync(config.controlSocketPath, 0o600));
    return { path: config.controlSocketPath, server };
  }),
  dependencies: [LictorConfig.Default, ControlPlane.Default],
}) {}
