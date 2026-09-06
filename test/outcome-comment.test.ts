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
    // attribution line and the marker after it when GitHub renders the comment.
    const body = renderOutcome(message({ messageId: 'real', note: 'quiet <-->!-- swallow' }));
    const quoted = body.split('\n').find((line) => line.startsWith('> ')) ?? '';

    expect(quoted).not.toContain('<!--');
    expect(body.match(/<!--/gu)).toHaveLength(1);
    expect(body).toContain("*Quoted above is the agent's own summary, not Lictor's.*");
  });

  it('quotes the agent note and says whose words they are', () => {
    const body = renderOutcome(message({ outcome: 'needs_input', note: 'Which branch?' }));

    expect(body).toContain('> Which branch?');
    expect(body).toContain("the agent's own summary");
  });

  it('publishes nothing at all for an outcome the daemon reached on its own', () => {
    const body = renderOutcome(message({ outcome: 'failed' }));

    expect(body).toBe('This did not finish.\n\n<!-- lictor:aa11bb22 -->');
  });

  it('collapses a multi-line note onto one quoted line', () => {
    const body = renderOutcome(message({ note: 'First line.\n\n# A heading\n- a list item' }));

    expect(body).toContain('> First line. # A heading - a list item');
    expect(body.split('\n').filter((line) => line.startsWith('>'))).toHaveLength(1);
  });

  it('bounds the note to its first 500 bytes', () => {
    const note = `${'x'.repeat(600)}TAIL`;
    const quoted = renderOutcome(message({ note }))
      .split('\n')
      .find((line) => line.startsWith('> '));

    expect(quoted).toBe(`> ${'x'.repeat(500)}`);
  });

  it('strips comment delimiters from the note before the marker is appended', () => {
    const forged = 'done <!-- lictor:forged --> more';
    const body = renderOutcome(message({ messageId: 'real', note: forged }));

    expect(body).not.toContain('lictor:forged -->');
    expect(body).toContain('<!-- lictor:real -->');
    // Exactly one marker survives, and it is the daemon's own.
    expect(body.match(/<!--/gu)).toHaveLength(1);
  });

  it('treats a note that is only whitespace as no note', () => {
    expect(renderOutcome(message({ note: '   \n\t ' }))).toBe('Done.\n\n<!-- lictor:aa11bb22 -->');
  });
});
