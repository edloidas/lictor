import type { Registry } from '../webhook/router.ts';
import { handlePing } from './ping.ts';

/**
 * Every event lictor acts on. Add an entry here and the router picks it up —
 * anything absent is logged and dropped.
 */
export const registry: Registry = {
  ping: handlePing,
};
