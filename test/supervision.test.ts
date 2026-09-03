import { describe, expect, it } from 'bun:test';
import { Cause, Effect, Fiber, FiberId } from 'effect';
import { describeCause } from '../src/diagnostics.ts';
import { type FatalAction, supervisor } from '../src/supervision.ts';

const recorder = () => {
  const calls: { message: string; reason: string | undefined }[] = [];
  const fatal: FatalAction = (message, cause) =>
    Effect.sync(() => {
      calls.push({ message, reason: cause === undefined ? undefined : describeCause(cause) });
    });
  return { calls, supervised: supervisor(fatal) };
};

describe('supervisor', () => {
  it('stops the daemon when a loop returns at all', async () => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(supervised('test loop', 'never', Effect.void));

    expect(calls).toEqual([{ message: 'The test loop stopped', reason: undefined }]);
  });

  it('stays silent when work that may complete completes', async () => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(supervised('test probe', 'once', Effect.void));

    expect(calls).toEqual([]);
  });

  it('stops the daemon when work fails', async () => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(supervised('test loop', 'never', Effect.fail(new Error('refused'))));

    expect(calls).toEqual([{ message: 'The test loop stopped', reason: 'Error: refused' }]);
  });

  // The reason this helper inspects the whole exit rather than tapping the error
  // channel: a defect never reaches `tapError`, so the fiber would die unnoticed.
  it('stops the daemon when work dies of a defect', async () => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(supervised('test loop', 'never', Effect.die(new Error('boom'))));

    expect(calls).toEqual([{ message: 'The test loop stopped', reason: 'Defect: Error: boom' }]);
  });

  // The work is interrupted, never the supervised fiber: interrupting the latter
  // short-circuits the continuation, so `calls` stays empty whatever this decides.
  // A cause carrying a failure *and* an interrupt is not clean shutdown: the
  // failure happened, so `isInterruptedOnly` must reject it where `isInterrupted`
  // would swallow it.
  it('stops the daemon when work fails while being interrupted', async () => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(
      supervised(
        'test loop',
        'never',
        Effect.failCause(
          Cause.sequential(Cause.fail(new Error('refused')), Cause.interrupt(FiberId.none)),
        ),
      ),
    );

    expect(calls).toEqual([{ message: 'The test loop stopped', reason: 'Error: refused' }]);
  });

  it.each([
    ['work interrupts itself', Effect.interrupt],
    [
      'work joins an interrupted child',
      Effect.gen(function* () {
        const child = yield* Effect.fork(Effect.never);
        yield* Fiber.interrupt(child);
        return yield* Fiber.join(child);
      }),
    ],
  ])('stays silent when %s', async (_case, work) => {
    const { calls, supervised } = recorder();
    await Effect.runPromise(supervised('test loop', 'never', work));

    expect(calls).toEqual([]);
  });
});
