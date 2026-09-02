import { Clock, Effect, Option } from 'effect';
import { CredentialHealth } from './github/credential-health.ts';
import { GitHubIdentity } from './github/identity.ts';
import { WorkQueue } from './queue/work-queue.ts';

/**
 * One pass of the daemon's short-cadence upkeep. Kept out of `main.ts`, which
 * ends in `BunRuntime.runMain` — importing it to reach this starts the daemon.
 */
export const daemonTick = Effect.gen(function* () {
  const identity = yield* GitHubIdentity;
  const health = yield* CredentialHealth;
  const queue = yield* WorkQueue;
  // `verified` is memoized for process lifetime, so expiry is noticed from the verdict
  // already carried — revocation surfaces via the next 401 latching the breaker.
  const verified = yield* identity.verified.pipe(Effect.option);
  const expiresAt = Option.getOrUndefined(verified)?.tokenExpiresAt;
  if (expiresAt !== undefined && expiresAt <= (yield* Clock.currentTimeMillis)) {
    yield* health.suspend;
  }
  yield* queue.heartbeatDaemon;
  const tick = yield* Clock.currentTimeMillis;
  yield* queue.recoverStale(tick);
  yield* queue.recoverStaleDeliveries(tick);
});

export const maintenanceLoop = Effect.forever(
  Effect.zipRight(Effect.sleep('10 seconds'), daemonTick),
);
