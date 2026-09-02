import { Clock, Effect, Option, Schedule } from 'effect';
import { CredentialHealth } from './github/credential-health.ts';
import { GitHubIdentity } from './github/identity.ts';
import { WorkQueue } from './queue/work-queue.ts';

/**
 * One pass of the daemon's short-cadence upkeep. Kept out of `main.ts`, which
 * ends in `BunRuntime.runMain` — importing it to reach this starts the daemon.
 */
export const daemonTick = Effect.gen(function* () {
  const queue = yield* WorkQueue;
  yield* queue.heartbeatDaemon;
  const tick = yield* Clock.currentTimeMillis;
  yield* queue.recoverStale(tick);
  yield* queue.recoverStaleDeliveries(tick);
});

export const maintenanceLoop = Effect.forever(
  Effect.zipRight(Effect.sleep('10 seconds'), daemonTick),
);

/**
 * Suspends the credential once its verified expiry has passed.
 *
 * ! Watched on its own fiber, never from `daemonTick`: `verified` retries an
 * ! unreachable GitHub without limit, and `Effect.option` bounds a failure, not
 * ! a fiber still waiting on one. Read inside the tick it defers the ownership
 * ! heartbeat for the length of the outage, the lease lapses, and a second
 * ! daemon claims the database out from under this one.
 */
export const credentialExpiryWatch = Effect.gen(function* () {
  const identity = yield* GitHubIdentity;
  const health = yield* CredentialHealth;
  const verified = yield* Effect.option(identity.verified);
  const expiresAt = Option.getOrUndefined(verified)?.tokenExpiresAt;
  if (expiresAt === undefined) {
    return;
  }
  // Polled rather than slept through to the moment: a classic token's expiry is
  // months out, and `setTimeout` past ~24.8 days fires immediately.
  yield* Effect.repeat(Clock.currentTimeMillis, {
    schedule: Schedule.spaced('10 seconds'),
    until: (now) => now >= expiresAt,
  });
  yield* health.suspend;
});
