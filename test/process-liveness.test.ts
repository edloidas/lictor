import { describe, expect, it } from 'bun:test';
import { processAlive } from '../src/process-liveness.ts';

describe('processAlive', () => {
  it('reports a running process as alive', () => {
    expect(processAlive(process.pid)).toBe(true);
  });

  it('reports a reaped process as gone', () => {
    // A pid that has certainly exited, rather than a number guessed to be free:
    // `spawnSync` has reaped the child by the time it returns.
    expect(processAlive(Bun.spawnSync(['true']).pid)).toBe(false);
  });

  // The branch the two suites that rely on pid 1 depend on, asserted directly.
  // Run as root, `kill(1, 0)` succeeds instead of raising `EPERM`, and those
  // suites would keep passing while no longer exercising this rule at all — so
  // this fails loudly there rather than degrading in silence.
  it('reads a process it may not signal as alive', () => {
    let raised: string | undefined;
    try {
      process.kill(1, 0);
    } catch (cause) {
      raised = (cause as NodeJS.ErrnoException).code;
    }

    expect(raised).toBe('EPERM');
    expect(processAlive(1)).toBe(true);
  });
});
