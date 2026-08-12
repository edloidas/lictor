import { Effect } from 'effect';
import type { LictorConfig } from '../config.ts';
import type { GitHubClient } from '../github/client.ts';
import { type Delivery, deliveryKey } from './event.ts';

/**
 * A handler must not fail: it runs detached from the request that triggered it,
 * so there is nothing left to report an error to. Recover inside the handler and
 * log what you swallowed.
 */
export type Handler = (
  delivery: Delivery,
) => Effect.Effect<void, never, GitHubClient | LictorConfig>;

/**
 * Handlers are keyed by `X-GitHub-Event`, optionally narrowed with the payload
 * action — `issues.opened` wins over `issues` for the same delivery.
 */
export type Registry = Readonly<Record<string, Handler>>;

/**
 * Dispatches one delivery. An event nobody registered for is logged and
 * dropped, not an error: a GitHub App subscribes to whole event types, so
 * receiving actions you do not care about is the normal case.
 */
export const dispatch =
  (registry: Registry) =>
  (delivery: Delivery): Effect.Effect<void, never, GitHubClient | LictorConfig> =>
    Effect.gen(function* () {
      const key = deliveryKey(delivery);
      const handler = registry[key] ?? registry[delivery.event];

      if (handler === undefined) {
        yield* Effect.logDebug('No handler registered').pipe(
          Effect.annotateLogs({ event: key, delivery: delivery.id }),
        );
        return;
      }

      yield* handler(delivery).pipe(Effect.annotateLogs({ event: key, delivery: delivery.id }));
    });
