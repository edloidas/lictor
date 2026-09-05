import { Data, Effect } from 'effect';

export type ProcessRequest = {
  readonly command: readonly string[];
  readonly cwd: string;
  readonly input: string;
  readonly timeoutMs: number;
  readonly outputLimitBytes: number;
  /**
   * Which end of stderr survives the budget. Only the caller knows its
   * producer: a process that reports its failure and exits puts the evidence
   * at the end, one that refuses at startup puts it at the beginning.
   */
  readonly stderrRetention: 'head' | 'tail';
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

const sequenceWidth = (lead: number): number => {
  if (lead < 0x80) return 1;
  if (lead < 0xe0) return 2;
  if (lead < 0xf0) return 3;
  return 4;
};

/** Drops the partial character a cut sized in bytes leaves at the retained edge. */
const repairEdge = (bytes: Buffer, keep: 'head' | 'tail'): Buffer => {
  const continuation = (index: number) => ((bytes[index] ?? 0) & 0xc0) === 0x80;

  if (keep === 'tail') {
    let start = 0;
    while (start < bytes.length && continuation(start)) start += 1;
    return bytes.subarray(start);
  }

  let lead = bytes.length - 1;
  while (lead >= 0 && continuation(lead)) lead -= 1;
  if (lead < 0) return bytes.subarray(0, 0);
  const width = sequenceWidth(bytes[lead] ?? 0);
  return lead + width > bytes.length ? bytes.subarray(0, lead) : bytes;
};

const fitToBudget = (bytes: Buffer, limit: number, keep: 'head' | 'tail'): string => {
  const cut = (source: Buffer) =>
    source.length <= limit
      ? source
      : repairEdge(
          keep === 'head' ? source.subarray(0, limit) : source.subarray(source.length - limit),
          keep,
        );

  const text = cut(bytes).toString('utf8');
  // Each byte that is not valid UTF-8 decodes to a 3-byte U+FFFD, so a cut
  // sized in raw bytes can still overshoot. Re-encoding yields valid UTF-8,
  // where one more cut is exact — never a byte-at-a-time search.
  return Buffer.byteLength(text) <= limit ? text : cut(Buffer.from(text, 'utf8')).toString('utf8');
};

const capture = (
  stream: ReadableStream<Uint8Array>,
  limit: number,
  keep: 'head' | 'tail',
): Effect.Effect<Captured, ProcessError> => {
  const reader = stream.getReader();
  let bytes = 0;
  let held = 0;
  const chunks: Uint8Array[] = [];
  let truncated = false;

  const read: Effect.Effect<Captured, ProcessError> = Effect.suspend(() =>
    Effect.tryPromise({
      try: () => reader.read(),
      catch: (cause) => new ProcessError({ message: 'Could not read process output', cause }),
    }).pipe(
      Effect.flatMap((result) => {
        if (result.done) {
          // The ring evicts whole chunks, so what is held can overshoot the
          // budget by a full chunk — up to 256 KiB from a Bun pipe.
          return Effect.succeed({
            text: fitToBudget(Buffer.concat(chunks), limit, keep),
            truncated,
          });
        }

        const chunk = result.value;
        if (keep === 'head') {
          const remaining = Math.max(0, limit - bytes);
          if (remaining > 0) chunks.push(chunk.subarray(0, remaining));
        } else {
          chunks.push(chunk);
          held += chunk.byteLength;
          // Evicts whole chunks the budget no longer needs, so what is held
          // stays within one chunk of the limit rather than growing with the stream.
          while (chunks.length > 1 && held - (chunks[0]?.byteLength ?? 0) >= limit) {
            held -= chunks.shift()?.byteLength ?? 0;
          }
        }
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
              // stdout is parsed, so it must stay a contiguous head.
              stdout: capture(process.stdout, request.outputLimitBytes, 'head'),
              stderr: capture(process.stderr, request.outputLimitBytes, request.stderrRetention),
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
