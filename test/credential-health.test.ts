import { describe, expect, it } from 'bun:test';
import { Effect, Logger } from 'effect';
import { CredentialHealth } from '../src/github/credential-health.ts';

describe('CredentialHealth', () => {
  // ! Never cleared on purpose — a dead credential heals only across an
  // ! operator restart, and auto-clearing would re-burn every attempt budget
  // ! against a token that is still rejected.
  it('latches on suspend and stays set', async () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* CredentialHealth;
        expect(yield* health.isRejected).toBe(false);

        yield* health.suspend.pipe(Effect.zipRight(health.suspend));

        expect(yield* health.isRejected).toBe(true);
      }).pipe(
        Effect.provide(CredentialHealth.Default),
        Effect.provide(Logger.remove(Logger.defaultLogger)),
      ),
    ));
});
