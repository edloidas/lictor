import { describe, expect, it } from 'bun:test';
import { Effect } from 'effect';
import { ProcessError, ProcessRunner } from '../src/executor/process-runner.ts';

/** The errno a signalling call raised, or `undefined` if it raised nothing. */
const errnoOf = (attempt: () => void): string | undefined => {
  try {
    attempt();
    return undefined;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code;
  }
};

describe('ProcessRunner', () => {
  it('captures only the configured number of bytes while draining the process', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stdout.write('abcdefghij')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 5,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result).toEqual({
      exitCode: 0,
      stdout: 'abcde',
      stderr: '',
      stdoutTruncated: true,
      stderrTruncated: false,
    });
  });

  it('keeps multibyte output within the byte limit', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stdout.write('€')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 1,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stdout).toBe('');
    expect(result.stdoutTruncated).toBe(true);
  });

  it('keeps the valid prefix when a head cut splits a character', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stdout.write('a€')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 2,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stdout).toBe('a');
    expect(result.stdoutTruncated).toBe(true);
  });

  it('does not report truncation when output exactly fills the budget', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stdout.write('abcde')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 5,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stdout).toBe('abcde');
    expect(result.stdoutTruncated).toBe(false);
  });

  it('reports truncation per stream', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stdout.write('ab');process.stderr.write('abcdefghij')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 5,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stdoutTruncated).toBe(false);
    expect(result.stderrTruncated).toBe(true);
  });

  it('keeps the end of stderr when the caller asks for the tail', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stderr.write('abcdefghij')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 5,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('fghij');
    expect(result.stderrTruncated).toBe(true);
  });

  // A `codex exec` session arrives as many chunks over minutes, not one write,
  // so the eviction accounting is what runs in production.
  it('keeps the end of stderr across many chunks', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: [
            'bun',
            '-e',
            "for (const c of 'abcdefghijklmnopqrstuvwxyz') { process.stderr.write(c); await Bun.sleep(2); }",
          ],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 5,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('vwxyz');
    expect(result.stderrTruncated).toBe(true);
  });

  it('retains each stream from its own end in one run', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: [
            'bun',
            '-e',
            "process.stdout.write('abcdefghij');process.stderr.write('ABCDEFGHIJ')",
          ],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 4,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stdout).toBe('abcd');
    expect(result.stderr).toBe('GHIJ');
  });

  it('keeps a tail within the byte limit when the cut splits a character', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stderr.write('a€b')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 3,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('b');
  });

  it('invents no replacement character when a tail cut splits a wide character', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stderr.write('\\u{1F600}abc')"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 6,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('abc');
  });

  // A stray byte decodes to a 3-byte U+FFFD, so a byte-at-a-time trim ran once
  // per stray byte and re-decoded the whole buffer each time. 1 MiB of this
  // shape took 12.7s; the timeout below is what fails if it comes back.
  it.each(['head', 'tail'] as const)(
    'trims a %s carrying invalid UTF-8 in bounded time',
    async (stderrRetention) => {
      const script =
        'const b = Buffer.alloc(2_097_152, 0x78); for (let i = 0; i < b.length; i += 100) b[i] = 0xe9; process.stderr.write(b)';

      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const runner = yield* ProcessRunner;
          return yield* runner.run({
            command: ['bun', '-e', script],
            cwd: process.cwd(),
            input: '',
            timeoutMs: 20_000,
            outputLimitBytes: 1024 * 1024,
            stderrRetention,
          });
        }).pipe(Effect.provide(ProcessRunner.Default)),
      );

      expect(Buffer.byteLength(result.stderr)).toBeLessThanOrEqual(1024 * 1024);
      expect(result.stderr).toContain('�');
      expect(result.stderrTruncated).toBe(true);
    },
    5_000,
  );

  it('keeps a head of invalid UTF-8 within the byte budget', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', 'process.stderr.write(Buffer.from([0xe9, 0xe9, 0xe9]))'],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 4,
          stderrRetention: 'head',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('�');
    expect(result.stderrTruncated).toBe(false);
  });

  it('keeps a tail of invalid UTF-8 within the byte budget', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', 'process.stderr.write(Buffer.from([0xe9, 0xe9, 0xe9]))'],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 4,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result.stderr).toBe('�');
    expect(result.stderrTruncated).toBe(false);
  });

  // Trimming a byte per full decode took 37s on this shape; the default test
  // timeout is what fails if it comes back.
  it('trims a large tail overshoot without re-decoding per byte', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['bun', '-e', "process.stderr.write('x'.repeat(4_000_000))"],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 20_000,
          outputLimitBytes: 1024 * 1024,
          stderrRetention: 'tail',
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(Buffer.byteLength(result.stderr)).toBe(1024 * 1024);
    expect(result.stderrTruncated).toBe(true);
  });

  it('preserves a signature emitted past the budget', async () => {
    const signature = 'ERROR codex_login::auth::manager: token_expired';
    const script = `process.stderr.write('x'.repeat(20000) + '\\n' + ${JSON.stringify(signature)} + '\\n')`;

    const capturing = (stderrRetention: 'head' | 'tail') =>
      Effect.runPromise(
        Effect.gen(function* () {
          const runner = yield* ProcessRunner;
          return yield* runner.run({
            command: ['bun', '-e', script],
            cwd: process.cwd(),
            input: '',
            timeoutMs: 5000,
            outputLimitBytes: 4096,
            stderrRetention,
          });
        }).pipe(Effect.provide(ProcessRunner.Default)),
      );

    const [tail, head] = await Promise.all([capturing('tail'), capturing('head')]);

    expect(tail.stderr).toContain(signature);
    expect(head.stderr).not.toContain(signature);
    expect(tail.stderrTruncated).toBe(true);
    expect(Buffer.byteLength(tail.stderr)).toBeLessThanOrEqual(4096);
  });

  /**
   * ! The child reports its process *group*, never its pid. They are equal only
   * ! because `detached: true` makes it a group leader, which is the premise the
   * ! registry rests on — drop the flag and the recorded number names a group
   * ! holding the daemon itself. A child printing `process.pid` reads the same
   * ! either way and measures none of it.
   */
  it('records the spawned child’s own process group, not the daemon’s', async () => {
    const events: string[] = [];
    let spawned = 0;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          command: ['sh', '-c', 'ps -o pgid= -p $$'],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 5000,
          outputLimitBytes: 64,
          stderrRetention: 'head',
          register: {
            record: (pgid) =>
              Effect.sync(() => {
                spawned = pgid;
                events.push('record');
              }),
            forget: (pgid) =>
              Effect.sync(() => {
                events.push(pgid === spawned ? 'forget' : 'forget-other');
              }),
          },
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(Number(result.stdout.trim())).toBe(spawned);
    expect(spawned).not.toBe(process.pid);
    expect(events).toEqual(['record', 'forget']);
  });

  // Codex exiting cleanly while a command it started runs on is a success by
  // `exitCode` and a live writer in the workspace in fact. See the release in
  // `process-runner.ts` for why the sweep cannot be conditional on the leader.
  it('sweeps a group that outlived its leader before forgetting it', async () => {
    let pgid = 0;
    let survivor = 0;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runner = yield* ProcessRunner;
        return yield* runner.run({
          // Exits 0 at once, leaving `sleep` behind in its group. The
          // grandchild's output is redirected so it does not hold the pipe the
          // runner drains — otherwise the run waits out its timeout instead of
          // reaching the clean-exit case this pins.
          command: ['sh', '-c', 'sleep 30 >/dev/null 2>&1 & echo $!; exit 0'],
          cwd: process.cwd(),
          input: '',
          timeoutMs: 10_000,
          outputLimitBytes: 64,
          stderrRetention: 'head',
          register: {
            record: (recorded) =>
              Effect.sync(() => {
                pgid = recorded;
              }),
            forget: () => Effect.void,
          },
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );
    survivor = Number(result.stdout.trim());

    expect(result.exitCode).toBe(0);
    expect(pgid).toBeGreaterThan(1);
    expect(survivor).toBeGreaterThan(1);
    expect(errnoOf(() => process.kill(survivor, 0))).toBe('ESRCH');
  }, 20_000);

  // The record is taken inside `use`, not the acquire, for exactly this: a
  // child the daemon cannot write down is one it could never kill later, so the
  // failure has to reach the release that kills it now.
  it('kills a child it could not record, rather than leaving it unrecorded', async () => {
    let pgid = 0;
    // `flip`, not `exit`: a run that succeeded here would be the defect, and
    // the message distinguishes this failure from a spawn error or a timeout,
    // which reach the same `Failure` tag by a different route.
    const error = await Effect.runPromise(
      Effect.flip(
        Effect.gen(function* () {
          const runner = yield* ProcessRunner;
          return yield* runner.run({
            command: ['sleep', '30'],
            cwd: process.cwd(),
            input: '',
            timeoutMs: 30_000,
            outputLimitBytes: 64,
            stderrRetention: 'head',
            register: {
              record: (recorded) =>
                Effect.zipRight(
                  Effect.sync(() => {
                    pgid = recorded;
                  }),
                  Effect.fail(new ProcessError({ message: 'Could not record it' })),
                ),
              forget: () => Effect.void,
            },
          });
        }).pipe(Effect.provide(ProcessRunner.Default)),
      ),
    );

    expect(error.message).toBe('Could not record it');
    expect(pgid).toBeGreaterThan(1);
    // ESRCH, not merely a throw: EPERM would mean the group is alive and this
    // user may not signal it, which is the opposite of what is claimed.
    expect(errnoOf(() => process.kill(-pgid, 0))).toBe('ESRCH');
  }, 20_000);
});
