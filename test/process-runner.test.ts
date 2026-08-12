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
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(result).toEqual({
      exitCode: 0,
      stdout: 'abcde',
      stderr: '',
      outputTruncated: true,
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
        });
      }).pipe(Effect.provide(ProcessRunner.Default)),
    );

    expect(Buffer.byteLength(result.stdout)).toBeLessThanOrEqual(1);
    expect(result.outputTruncated).toBe(true);
  });
});
