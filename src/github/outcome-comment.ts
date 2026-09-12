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
  // Carries its own question, because it is asked with no note: the words are
  // the daemon's, and every note published here is attributed to the agent.
  clipped:
    'This request is longer than I can record, and I will not act on part of one. Please restate what you need in a reply.',
  rejected: 'I did not carry this out.',
  failed: 'This did not finish.',
  expired: 'This needed approval and the approval window closed before it came.',
  unanswered: 'I asked a question here and no answer came, so I have stopped waiting.',
  canceled: 'This was canceled.',
};

/** One sentence's worth. Longer than a summary needs and shorter than a log. */
const NOTE_BOUND_BYTES = 500;

/**
 * The agent's own contribution, reduced to one line of prose.
 *
 * Bounding is what this can promise. It does not make arbitrary agent text
 * safe — it is prose the repository influenced, and it is published in the
 * daemon's own message with nothing marking where it starts. The collapse is
 * load-bearing for that: a note that kept its newlines could open headings and
 * lists, and restructure the comment around itself.
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
 * One message, and for every outcome but `needs_input` the opening line is all
 * of it. That covers the paths the daemon reached on its own — a timeout, a
 * crash, a policy refusal, an expiry — where the only strings it holds are
 * error prose and capability codes, and a thread is not where either belongs.
 */
export const renderOutcome = (message: OutboxMessage): string => {
  const note = message.note === undefined ? undefined : publicNote(message.note);
  // `needs_input` is the one outcome that publishes the note, because there the
  // note is the question the headline promises and a thread cannot answer one it
  // cannot read. The agent is free to return the status with nothing in
  // `summary`, which is what the replacement opening covers.
  const asksForInput = message.outcome === 'needs_input';
  const lines = [
    asksForInput && note === undefined
      ? 'I need an answer before I can continue, but did not say what I need.'
      : headline[message.outcome],
  ];
  if (asksForInput && note !== undefined) lines.push('', note);
  lines.push('', outcomeMarker(message.messageId));
  return lines.join('\n');
};
