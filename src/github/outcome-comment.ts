import type { JobOutcome, OutboxMessage } from '../queue/work-queue.ts';

/**
 * The message's durable identity, carried in the posted body.
 *
 * ! Not an exactly-once guarantee — GitHub has no idempotency key for a comment.
 * ! It is what lets the sender recognise its own post after a crash between the
 * ! POST and the row that records it, instead of posting a second one.
 */
export const outcomeMarker = (messageId: string): string => `<!-- lictor:${messageId} -->`;

/**
 * The outer wording, which is the part the daemon can actually promise. It
 * fixes the structure and the vocabulary; it cannot vouch for whatever prose
 * the agent supplies alongside it.
 */
const headline: Readonly<Record<JobOutcome, string>> = {
  completed: 'Done.',
  needs_input: 'I need an answer before I can continue.',
  rejected: 'I did not carry this out.',
  failed: 'This did not finish.',
  expired: 'This needed approval and the approval window closed before it came.',
  canceled: 'This was canceled.',
};

/** One sentence's worth. Longer than a summary needs and shorter than a log. */
const NOTE_BOUND_BYTES = 500;

/**
 * The agent's own contribution, reduced to one line of quotable prose.
 *
 * Bounding and attribution are what this can promise. They do not make
 * arbitrary agent text safe — it is prose the repository influenced — so it is
 * published as a quotation from the agent rather than as the daemon's words.
 */
const publicNote = (note: string): string | undefined => {
  // ! Before the marker is appended, and repeated to a fixed point. One pass is
  // ! not enough: removing the `-->` from `<-->!--` splices the halves into a
  // ! `<!--` that was not there, and an unclosed one hides the attribution line
  // ! and the marker after it. Each pass shortens the string, so it terminates.
  let stripped = note;
  let previous: string;
  do {
    previous = stripped;
    stripped = previous.replaceAll('<!--', '').replaceAll('-->', '');
  } while (stripped !== previous);
  const collapsed = stripped.replace(/\s+/gu, ' ').trim();
  if (collapsed === '') return undefined;
  return Buffer.from(collapsed)
    .subarray(0, NOTE_BOUND_BYTES)
    .toString('utf8')
    .replace(/\uFFFD$/u, '')
    .trim();
};

/**
 * The public comment for one terminal outcome.
 *
 * An outcome with no agent note is one the daemon reached on its own — a
 * timeout, a crash, a policy refusal, an expiry. Those carry no explanation at
 * all rather than an excerpt of a diagnostic: the strings the daemon holds on
 * those paths are error prose and capability codes, and a thread is not where
 * either belongs.
 */
export const renderOutcome = (message: OutboxMessage): string => {
  const note = message.note === undefined ? undefined : publicNote(message.note);
  // Every other outcome stands on its headline alone. `needs_input` cannot: its
  // headline promises a question, and the agent is free to return the status
  // with nothing in `summary` — so a thread would be told to answer something
  // the comment never asked.
  const lines = [
    message.outcome === 'needs_input' && note === undefined
      ? 'I need an answer before I can continue, but did not say what I need.'
      : headline[message.outcome],
  ];
  if (note !== undefined) {
    lines.push('', `> ${note}`, '', "*Quoted above is the agent's own summary, not Lictor's.*");
  }
  lines.push('', outcomeMarker(message.messageId));
  return lines.join('\n');
};
