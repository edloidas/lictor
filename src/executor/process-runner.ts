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
  /**
   * Durable record of the child's process group, kept for as long as the child
   * runs. Supplied only where a child can outlive the process that started it:
   * this one spawns detached and is killed from a finalizer, and a `bun --watch`
   * reload reaches neither.
   */
  readonly register?: ProcessGroupRecord;
};

export type ProcessGroupRecord = {
  readonly record: (pgid: number) => Effect.Effect<void, ProcessError>;
  /** Unfailing: a row left behind costs one signal to a group already gone. */
  readonly forget: (pgid: number) => Effect.Effect<void>;
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

/**
 * Signals the child's whole process group, reporting whether anything there
 * received it. Falls back to the child alone where the spawn produced no group
 * of its own to signal.
 */
const sweep = (child: Bun.Subprocess, signal: 'SIGTERM' | 'SIGKILL'): boolean => {
  try {
    process.kill(-child.pid, signal);
    return true;
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== 'ESRCH') return true;
    if (child.exitCode !== null) return false;
    try {
      child.kill(signal);
      return true;
    } catch {
      return false;
    }
  }
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
        (child) =>
          // Recorded here rather than in the acquire, so a record that fails
          // still reaches the release that kills what it could not record.
          Effect.zipRight(
            request.register?.record(child.pid) ?? Effect.void,
            Effect.all(
              {
                exitCode: Effect.tryPromise({
                  try: () => child.exited,
                  catch: (cause) => new ProcessError({ message: 'Process wait failed', cause }),
                }),
                // Drained, not read: no caller parses stdout any more, but an
                // unread pipe fills and blocks the child until the timeout.
                stdout: capture(child.stdout, request.outputLimitBytes, 'head'),
                stderr: capture(child.stderr, request.outputLimitBytes, request.stderrRetention),
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
          ),
        (child) =>
          // ! Swept whether the child exited or survived, and forgotten only
          // ! after. A process group outlives its leader, so a descendant runs
          // ! on while `exitCode` reports success and this row is the last
          // ! durable way to reach it; a release that dies between the two then
          // ! leaves a stale row rather than an agent nothing can find. An empty
          // ! group is `ESRCH`, so an ordinary run pays one syscall and no grace.
          Effect.zipRight(
            Effect.suspend(() =>
              sweep(child, 'SIGTERM')
                ? Effect.sleep('2 seconds').pipe(
                    Effect.zipRight(Effect.sync(() => sweep(child, 'SIGKILL'))),
                  )
                : Effect.void,
            ),
            request.register?.forget(child.pid) ?? Effect.void,
          ),
      ),
  }),
}) {}
