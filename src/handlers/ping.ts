import { Effect } from 'effect';
import type { Handler } from '../webhook/router.ts';

/**
 * GitHub sends `ping` once, when a webhook is first configured. Handling it is
 * how you confirm the tunnel, the secret, and the route all line up before any
 * real event arrives.
 */
export const handlePing: Handler = (delivery) =>
  Effect.logInfo('Webhook reachable').pipe(
    Effect.annotateLogs({
      repository: delivery.payload.repository?.full_name ?? '(none)',
      sender: delivery.payload.sender?.login ?? '(none)',
    }),
  );
