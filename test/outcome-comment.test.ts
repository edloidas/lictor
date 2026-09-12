import { describe, expect, it } from 'bun:test';
import { renderOutcome } from '../src/github/outcome-comment.ts';
import type { JobOutcome, OutboxMessage } from '../src/queue/work-queue.ts';

const message = (overrides: Partial<OutboxMessage> = {}): OutboxMessage => ({
  id: 1,
  messageId: 'aa11bb22',
  jobId: 7,
  attempt: 1,
  repository: 'edloidas/lictor',
  subjectNumber: 17,
  outcome: 'completed',
  status: 'sending',
  attempts: 1,
  createdAt: 1_000,
  availableAt: 1_000,
  ...overrides,
});

/**
 * The literal wording, not a call back into the renderer's own table. An
 * expectation computed from the code under test agrees with any mapping,
 * including one where two outcomes have swapped their lines.
 */
const openings: Readonly<Record<JobOutcome, string>> = {
  completed: 'Done.',
  needs_input: 'I need an answer before I can continue.',
  clipped:
    'This request is longer than I can record, and I will not act on part of one. Please restate what you need in a reply.',
  rejected: 'I did not carry this out.',
  failed: 'This did not finish.',
  expired: 'This needed approval and the approval window closed before it came.',
  unanswered: 'I asked a question here and no answer came, so I have stopped waiting.',
  canceled: 'This was canceled.',
};

describe('renderOutcome', () => {
  it('carries the message identity so a repeated send can recognise its own post', () => {
    // Literal, not `outcomeMarker('ffee00')`: the marker is the whole
    // reconciliation, and asserting it against its own formula would pass a
    // marker that carried no identity at all.
    expect(renderOutcome(message({ messageId: 'ffee00' }))).toContain('<!-- lictor:ffee00 -->');
  });

  it('gives two messages two different markers', () => {
    const first = renderOutcome(message({ messageId: 'one' }));
    const second = renderOutcome(message({ messageId: 'two' }));

    expect(first).toContain('<!-- lictor:one -->');
    expect(second).toContain('<!-- lictor:two -->');
    expect(first).not.toContain('<!-- lictor:two -->');
  });

  it.each(Object.entries(openings))('opens a %s outcome with its own line', (outcome, opening) => {
    const body = renderOutcome(message({ outcome: outcome as JobOutcome, note: 'a summary' }));

    expect(body.split('\n')[0]).toBe(opening);
  });

  it('does not promise a question a note-less needs_input never asked', () => {
    const body = renderOutcome(message({ outcome: 'needs_input' }));

    expect(body.split('\n')[0]).toBe(
      'I need an answer before I can continue, but did not say what I need.',
    );
  });

  it('cannot be made to open an HTML comment by splicing the strip', () => {
    // One strip pass turns `<-->!--` into `<!--`, and an unclosed one hides the
    // marker after it when GitHub renders the comment. On `needs_input`, the
    // one outcome that still publishes a note and so the only one that reaches
    // the strip at all.
    const body = renderOutcome(
      message({ messageId: 'real', outcome: 'needs_input', note: 'quiet <-->!-- swallow' }),
    );

    expect(body).toContain('quiet swallow');
    expect(body.match(/<!--/gu)).toHaveLength(1);
    expect(body).toContain('<!-- lictor:real -->');
  });

  it('publishes the question inline so the thread can answer it', () => {
    const body = renderOutcome(
      message({ messageId: 'ask', outcome: 'needs_input', note: 'Which branch?' }),
    );

    expect(body).toBe(
      'I need an answer before I can continue.\n\nWhich branch?\n\n<!-- lictor:ask -->',
    );
  });

  // The note is the agent's, and nothing in the rendered comment separates it
  // from the daemon's own wording — so an outcome that is not asking a question
  // must not reach it. `completed` is the one an agent always supplies a
  // summary for, which makes it the case that would regress.
  it('publishes no agent note on an outcome that is not asking for input', () => {
    const body = renderOutcome(message({ outcome: 'completed', note: 'I rewrote the parser.' }));

    expect(body).toBe('Done.\n\n<!-- lictor:aa11bb22 -->');
    expect(body).not.toContain('I rewrote the parser.');
  });

  it('publishes nothing at all for an outcome the daemon reached on its own', () => {
    const body = renderOutcome(message({ outcome: 'failed' }));

    expect(body).toBe('This did not finish.\n\n<!-- lictor:aa11bb22 -->');
  });

  // ! Published into the daemon's own message, so a note that kept its newlines
  // ! could open a heading or a list and restructure the comment around itself.
  it('collapses a multi-line note onto one line', () => {
    const body = renderOutcome(
      message({ outcome: 'needs_input', note: 'First line.\n\n# A heading\n- a list item' }),
    );

    expect(body).toContain('First line. # A heading - a list item');
    expect(body.split('\n')).toHaveLength(5);
  });

  it('bounds the note to its first 500 bytes', () => {
    const body = renderOutcome(message({ outcome: 'needs_input', note: `${'x'.repeat(600)}TAIL` }));

    expect(body).toContain('x'.repeat(500));
    expect(body).not.toContain('TAIL');
    expect(body).not.toContain('x'.repeat(501));
  });

  it('strips comment delimiters from the note before the marker is appended', () => {
    const forged = 'done <!-- lictor:forged --> more';
    const body = renderOutcome(
      message({ messageId: 'real', outcome: 'needs_input', note: forged }),
    );

    expect(body).not.toContain('lictor:forged -->');
    expect(body).toContain('<!-- lictor:real -->');
    // Exactly one marker survives, and it is the daemon's own.
    expect(body.match(/<!--/gu)).toHaveLength(1);
  });

  it('treats a note that is only whitespace as no note', () => {
    expect(renderOutcome(message({ outcome: 'needs_input', note: '   \n\t ' }))).toBe(
      'I need an answer before I can continue, but did not say what I need.\n\n<!-- lictor:aa11bb22 -->',
    );
  });

  // ! A clipped request is the daemon asking its own question, and `needs_input`
  // ! is the only outcome whose note is published. Were `clipped` ever to route
  // ! its wording through the note instead of the headline, it would go out on a
  // ! path that publishes nothing, and the thread would be told to reply with no
  // ! question in front of it.
  it('asks the clipped question from the headline, not the note', () => {
    expect(renderOutcome(message({ outcome: 'clipped' }))).toBe(
      `${openings.clipped}\n\n<!-- lictor:aa11bb22 -->`,
    );
  });
});
