import { chmodSync, mkdirSync, statfsSync, unlinkSync } from 'node:fs';
import { dirname } from 'node:path';
import { Clock, Data, Effect } from 'effect';
import { LictorConfig } from '../config.ts';
import { CredentialHealth } from '../github/credential-health.ts';
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
        // Retry re-parks a job still awaiting approval, so it needs the window
        // to date the new hold from.
        const changed =
          action === 'retry'
            ? yield* queue.retry(id, policy.approvalExpiryMs)
            : yield* queue[action](id);
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
          case 'job.show':
            return yield* queue.job(yield* positiveId(args[0]));
          case 'job.approve':
            return yield* mutate('approve', yield* positiveId(args[0]));
          case 'job.retry':
            return yield* mutate('retry', yield* positiveId(args[0]));
          case 'job.cancel':
            return yield* mutate('cancel', yield* positiveId(args[0]));
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
