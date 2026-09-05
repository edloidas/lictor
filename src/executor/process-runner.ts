import { Data, Effect } from 'effect';

export type ProcessRequest = {
  readonly command: readonly string[];
  readonly cwd: string;
  readonly input: string;
  readonly timeoutMs: number;
  readonly outputLimitBytes: number;
  readonly env?: Readonly<Record<string, string>>;
};

export type ProcessResult = {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
};

export class ProcessError extends Data.TaggedError('ProcessError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

type Captured = { readonly text: string; readonly truncated: boolean };

const capture = (
  stream: ReadableStream<Uint8Array>,
  limit: number,
): Effect.Effect<Captured, ProcessError> => {
  const reader = stream.getReader();
  let bytes = 0;
  const chunks: Uint8Array[] = [];
  let truncated = false;

  const read: Effect.Effect<Captured, ProcessError> = Effect.suspend(() =>
    Effect.tryPromise({
      try: () => reader.read(),
      catch: (cause) => new ProcessError({ message: 'Could not read process output', cause }),
    }).pipe(
      Effect.flatMap((result) => {
        if (result.done) {
          let kept = Buffer.concat(chunks);
          let text = kept.toString('utf8');
          while (Buffer.byteLength(text) > limit && kept.length > 0) {
            kept = kept.subarray(0, -1);
            text = kept.toString('utf8');
          }
          return Effect.succeed({ text, truncated });
        }

        const chunk = result.value;
        const remaining = Math.max(0, limit - bytes);
        if (remaining > 0) chunks.push(chunk.subarray(0, remaining));
        bytes += chunk.byteLength;
        if (bytes > limit) truncated = true;
        return read;
      }),
    ),
  );

  return read;
};

export class ProcessRunner extends Effect.Service<ProcessRunner>()('ProcessRunner', {
  effect: Effect.succeed({
    run: (request: ProcessRequest) =>
      Effect.acquireUseRelease(
        Effect.try({
          try: () =>
            Bun.spawn([...request.command], {
              cwd: request.cwd,
              detached: true,
              stdin: new Blob([request.input]),
              stdout: 'pipe',
              stderr: 'pipe',
              ...(request.env === undefined ? {} : { env: request.env }),
            }),
          catch: (cause) => new ProcessError({ message: 'Could not start process', cause }),
        }),
        (process) =>
          Effect.all(
            {
              exitCode: Effect.tryPromise({
                try: () => process.exited,
                catch: (cause) => new ProcessError({ message: 'Process wait failed', cause }),
              }),
              stdout: capture(process.stdout, request.outputLimitBytes),
              stderr: capture(process.stderr, request.outputLimitBytes),
            },
            { concurrency: 'unbounded' },
          ).pipe(
            Effect.timeoutFail({
              duration: request.timeoutMs,
              onTimeout: () =>
                new ProcessError({ message: `Process timed out after ${request.timeoutMs}ms` }),
            }),
            Effect.map(({ exitCode, stdout, stderr }) => ({
              exitCode,
              stdout: stdout.text,
              stderr: stderr.text,
              stdoutTruncated: stdout.truncated,
              stderrTruncated: stderr.truncated,
            })),
          ),
        (child) =>
          child.exitCode !== null
            ? Effect.void
            : Effect.sync(() => {
                try {
                  process.kill(-child.pid, 'SIGTERM');
                } catch {
                  child.kill('SIGTERM');
                }
              }).pipe(
                Effect.zipRight(Effect.sleep('2 seconds')),
                Effect.zipRight(
                  Effect.sync(() => {
                    if (child.exitCode !== null) return;
                    try {
                      process.kill(-child.pid, 'SIGKILL');
                    } catch {
                      child.kill('SIGKILL');
                    }
                  }),
                ),
              ),
      ),
  }),
}) {}
