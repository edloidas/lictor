import { Effect, type ParseResult } from 'effect';
import type { LictorConfig } from '../config.ts';
import type { GitHubClient } from '../github/client.ts';
import type { GitHubIdentity, GitHubIdentityError } from '../github/identity.ts';
import type { Policy } from '../policy.ts';
import type { QueueError, WorkQueue } from '../queue/work-queue.ts';
import { type Delivery, deliveryKey } from './event.ts';
import type { MalformedInteraction } from './qualification.ts';

/**
 * Handler failures are recorded by the durable delivery worker.
 */
export type Handler = (
  delivery: Delivery,
) => Effect.Effect<
  void,
  GitHubIdentityError | MalformedInteraction | ParseResult.ParseError | QueueError,
  GitHubClient | GitHubIdentity | LictorConfig | Policy | WorkQueue
>;

/**
 * Handlers are keyed by `X-GitHub-Event`, optionally narrowed with the payload
 * action — `issues.opened` wins over `issues` for the same delivery.
 */
export type Registry = Readonly<Record<string, Handler>>;

/**
 * Dispatches one delivery. An event nobody registered for is logged and
 * dropped, not an error: a webhook subscribes to whole event types, so
 * receiving actions you do not care about is the normal case.
 */
export const dispatch =
  (registry: Registry) =>
  (
    delivery: Delivery,
  ): Effect.Effect<
    void,
    GitHubIdentityError | MalformedInteraction | ParseResult.ParseError | QueueError,
    GitHubClient | GitHubIdentity | LictorConfig | Policy | WorkQueue
  > =>
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
