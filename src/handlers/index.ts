import type { Registry } from '../webhook/router.ts';
import { handleInteraction } from './interactions.ts';
import { handlePing } from './ping.ts';

/**
 * Every event lictor acts on. Add an entry here and the router picks it up —
 * anything absent is logged and dropped.
 */
export const registry: Registry = {
  ping: handlePing,
  issues: handleInteraction,
  pull_request: handleInteraction,
  issue_comment: handleInteraction,
  pull_request_review: handleInteraction,
  pull_request_review_comment: handleInteraction,
};
