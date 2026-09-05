import { describe, expect, it } from 'bun:test';
import { Effect } from 'effect';
import { ProcessRunner } from '../src/executor/process-runner.ts';

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
});
