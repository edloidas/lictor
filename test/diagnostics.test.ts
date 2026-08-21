import { describe, expect, it } from 'bun:test';
import { Cause, Data, FiberId } from 'effect';
import { describeCause } from '../src/diagnostics.ts';

class Sample extends Data.TaggedError('Sample')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

describe('describeCause', () => {
  it('names the tag and the authored message', () => {
    expect(describeCause(Cause.fail(new Sample({ message: 'credential expired' })))).toBe(
      'Sample: credential expired',
    );
  });

  // ! The reason this exists. `GitHubClient` injects the token as a plain
  // ! `Authorization` header, so a rendered request carries the credential —
  // ! `Redacted` guards the config value and stops guarding it at the wire.
  it('never renders the nested cause, where the credential lives', () => {
    const described = describeCause(
      Cause.fail(
        new Sample({
          message: 'Could not reach GitHub',
          cause: { request: { headers: { authorization: 'Bearer ghp_supersecret' } } },
        }),
      ),
    );

    expect(described).toBe('Sample: Could not reach GitHub');
    expect(described).not.toContain('ghp_supersecret');
    expect(described).not.toContain('authorization');
  });

  it('does not leak a rejected payload value through a nested schema error', () => {
    const described = describeCause(
      Cause.fail(
        new Sample({
          message: 'Could not decode the delivery',
          cause: new Error('Expected number, actual "private customer note"'),
        }),
      ),
    );

    expect(described).not.toContain('private customer note');
  });

  it('marks a defect as one and keeps its message', () => {
    expect(describeCause(Cause.die(new Error('Service not found: LictorConfig')))).toBe(
      'Defect: Error: Service not found: LictorConfig',
    );
  });

  it('reports interruption as itself, not as a failure', () => {
    expect(describeCause(Cause.interrupt(FiberId.none))).toBe('Interrupted');
  });

  it('falls back to a tag when there is no message', () => {
    expect(describeCause(Cause.fail({ _tag: 'Bare' }))).toBe('Bare');
  });

  it('describes a failure that is not a tagged error at all', () => {
    expect(describeCause(Cause.fail('a bare string'))).toBe('UnknownFailure');
  });
});
