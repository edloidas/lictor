import type { JobOutcome } from '../queue/work-queue.ts';
import type { ReactionContent } from './client.ts';

/**
 * What the thread is told, and the whole of it.
 *
 * Every reason a job ended — an `ExecutorError` message, a policy refusal code,
 * the agent's own summary — stays in the row and the log. The thread gets the
 * one bit it can act on: this finished, or it did not.
 *
 * `needs_input` is absent deliberately. A parked job has not ended, and the
 * acknowledgement already says the daemon is looking at it; the question itself
 * is the agent's own comment.
 */
const OUTCOME_REACTIONS: Partial<Record<JobOutcome, ReactionContent>> = {
  completed: 'rocket',
  canceled: 'confused',
  clipped: 'confused',
  expired: 'confused',
  failed: 'confused',
  rejected: 'confused',
  unanswered: 'confused',
};

/** The reaction an outcome resolves its acknowledgement to, if any. */
export const outcomeReaction = (outcome: JobOutcome): ReactionContent | undefined =>
  OUTCOME_REACTIONS[outcome];

/**
 * The contents a delivery clears out of the way, in the order it clears them.
 *
 * The acknowledgement is always a candidate: `acknowledge` places it on every
 * fresh insert. A terminal reaction is one only where a previous attempt could
 * have left it — `job.retry` requeues without re-acknowledging, and a redelivery
 * follows a send that may already have landed. Probing for one that cannot be
 * there would post it and immediately delete it again, showing a wrong outcome
 * on the thread for as long as that takes.
 */
export const staleContents = (firstAttempt: boolean): readonly ReactionContent[] =>
  firstAttempt ? ['eyes'] : ['eyes', 'rocket', 'confused'];
