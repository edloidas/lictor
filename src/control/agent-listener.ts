import { randomBytes } from 'node:crypto';
import { chmodSync, mkdirSync, readdirSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { Data, Effect, Fiber, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { CapabilityBroker } from '../github/capability-broker.ts';
import type { QueueError } from '../queue/work-queue.ts';

export class AgentListenerError extends Data.TaggedError('AgentListenerError')<{
  readonly code: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

const McpRequest = Schema.Struct({
  jsonrpc: Schema.Literal('2.0'),
  id: Schema.Union(Schema.String, Schema.Number),
  method: Schema.String,
  params: Schema.optional(Schema.Record({ key: Schema.String, value: Schema.Unknown })),
});

/**
 * Carries only the request — job identity comes from the listener that
 * accepted the connection, never from a field here, so a compromised agent
 * has no way to name another job's.
 */
const AgentRequest = Schema.Struct({ mcp: McpRequest });

/** `sun_path` is 108 bytes including the terminator. */
const SOCKET_PATH_MAX = 107;

/**
 * Job ids are SQLite rowids and attempts are capped by `LICTOR_WORKER_MAX_ATTEMPTS`
 * — sized to that reality, not to `Number.MAX_SAFE_INTEGER`, which would refuse
 * state directories every real socket fits in.
 */
const JOB_DIGITS = 12;
const ATTEMPT_DIGITS = 3;
const NONCE_BYTES = 6;
export const LONGEST_SOCKET_NAME = `job-${'9'.repeat(JOB_DIGITS)}-${'9'.repeat(ATTEMPT_DIGITS)}-${'f'.repeat(NONCE_BYTES * 2)}.sock`;

// The nonce makes a concurrent attempt's socket unguessable, not unreachable:
// the directory is readable at the daemon's own uid, which the agent shares.
const socketName = (jobId: number, attemptNumber: number) =>
  `job-${jobId}-${attemptNumber}-${randomBytes(NONCE_BYTES).toString('hex')}.sock`;

export class AgentListener extends Effect.Service<AgentListener>()('AgentListener', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const broker = yield* CapabilityBroker;
    const directory = join(config.stateDir, 'agent');
    const enabled = config.executor !== 'disabled';

    // A path that can't hold a socket fails every job forever; checked once here
    // rather than per attempt, so that shows up as a boot error, not endless retries.
    if (enabled && Buffer.byteLength(join(directory, LONGEST_SOCKET_NAME)) > SOCKET_PATH_MAX)
      return yield* new AgentListenerError({
        code: 'AGENT_SOCKET_PATH_TOO_LONG',
        message: `Agent socket paths under ${directory} exceed ${SOCKET_PATH_MAX} bytes — point LICTOR_DATABASE_PATH at a shorter path`,
      });

    const unlinkQuietly = (path: string) => {
      try {
        unlinkSync(path);
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== 'ENOENT') throw cause;
      }
    };

    if (enabled) {
      yield* Effect.try({
        try: () => mkdirSync(directory, { recursive: true, mode: 0o700 }),
        catch: (cause) =>
          new AgentListenerError({
            code: 'AGENT_SOCKET_DIR_FAILED',
            message: 'Could not prepare the agent socket directory',
            cause,
          }),
      });
      // One daemon owns this directory, so anything already here at startup is
      // dead — cleared best-effort, worth a log line, never a boot failure.
      yield* Effect.try(() => {
        for (const entry of readdirSync(directory, { withFileTypes: true })) {
          if (!entry.isSocket()) continue;
          try {
            unlinkSync(join(directory, entry.name));
          } catch {}
        }
      }).pipe(
        Effect.catchAll((cause) =>
          Effect.logWarning('Could not clear stale agent sockets').pipe(
            Effect.annotateLogs({ path: directory, error: String(cause) }),
          ),
        ),
      );
    }

    const open = (jobId: number, attemptNumber: number, workerId: string) =>
      Effect.gen(function* () {
        const path = join(directory, socketName(jobId, attemptNumber));
        if (Buffer.byteLength(path) > SOCKET_PATH_MAX)
          return yield* new AgentListenerError({
            code: 'AGENT_SOCKET_PATH_TOO_LONG',
            message: `Agent socket path ${path} exceeds ${SOCKET_PATH_MAX} bytes`,
          });

        const answer = (source: string): Effect.Effect<unknown, AgentListenerError | QueueError> =>
          Effect.gen(function* () {
            const parsed = yield* Effect.try({
              try: () => JSON.parse(source) as unknown,
              catch: (cause) =>
                new AgentListenerError({
                  code: 'AGENT_REQUEST_INVALID',
                  message: 'Invalid agent request',
                  cause,
                }),
            });
            const request = yield* Schema.decodeUnknown(AgentRequest)(parsed).pipe(
              Effect.mapError(
                (cause) =>
                  new AgentListenerError({
                    code: 'AGENT_REQUEST_INVALID',
                    message: 'Invalid agent request',
                    cause,
                  }),
              ),
            );
            // Rebuilt rather than passed through: the schema makes `params`
            // optional-or-undefined, and the broker's type is exact-optional.
            const { mcp } = request;
            return yield* broker.handleMcp(jobId, attemptNumber, workerId, {
              jsonrpc: mcp.jsonrpc,
              id: mcp.id,
              method: mcp.method,
              ...(mcp.params === undefined ? {} : { params: mcp.params }),
            });
          });

        const inFlight = new Set<Fiber.RuntimeFiber<unknown, never>>();

        yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              Bun.listen<{ chunks: Buffer[]; size: number; dispatched: boolean }>({
                unix: path,
                socket: {
                  open(socket) {
                    socket.data = { chunks: [], size: 0, dispatched: false };
                  },
                  data(socket, chunk) {
                    if (socket.data.dispatched) return;
                    // Kept whole until the newline — decoding each chunk separately
                    // would corrupt a multi-byte character split across a boundary.
                    const bytes = Buffer.from(chunk);
                    socket.data.chunks.push(bytes);
                    socket.data.size += bytes.byteLength;
                    if (socket.data.size > 256 * 1024) {
                      socket.end();
                      return;
                    }
                    const buffer = Buffer.concat(socket.data.chunks);
                    const newline = buffer.indexOf(0x0a);
                    if (newline < 0) return;
                    socket.data.dispatched = true;
                    const fiber = Effect.runFork(
                      answer(buffer.subarray(0, newline).toString('utf8')).pipe(
                        Effect.match({
                          onFailure: (error) => ({
                            ok: false,
                            error: {
                              code:
                                error._tag === 'AgentListenerError'
                                  ? error.code
                                  : 'AGENT_CAPABILITY_FAILED',
                              message:
                                error._tag === 'AgentListenerError'
                                  ? error.message
                                  : 'Capability request failed',
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
                    inFlight.add(fiber);
                    fiber.addObserver(() => inFlight.delete(fiber));
                  },
                },
              }),
            catch: (cause) =>
              new AgentListenerError({
                code: 'AGENT_SOCKET_LISTEN_FAILED',
                message: 'Could not listen on the agent socket',
                cause,
              }),
          }),
          (listener) =>
            Effect.sync(() => {
              listener.stop(true);
              try {
                unlinkQuietly(path);
              } catch {}
            }).pipe(
              // The listener stop alone leaves an in-flight capability call running,
              // which could land a GitHub mutation after the attempt ends —
              // interruptAll so a later call can't outlive an earlier one's wait.
              Effect.zipRight(Fiber.interruptAll(inFlight)),
            ),
        );
        yield* Effect.try({
          try: () => chmodSync(path, 0o600),
          catch: (cause) =>
            new AgentListenerError({
              code: 'AGENT_SOCKET_CHMOD_FAILED',
              message: 'Could not restrict the agent socket',
              cause,
            }),
        });
        return { path };
      });

    return { open };
  }),
  dependencies: [LictorConfig.Default, CapabilityBroker.Default],
}) {}
