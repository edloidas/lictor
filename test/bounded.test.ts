import { describe, expect, it } from 'bun:test';
import { bounded, exceeds } from '../src/bounded.ts';

describe('bounded', () => {
  it('returns a value that fits unchanged', () => {
    expect(bounded('short', 64)).toBe('short');
  });

  it('cuts to the byte budget, not the character count', () => {
    // Four bytes each, so ten characters is forty bytes and the budget takes two.
    expect(bounded('🎉'.repeat(10), 8)).toBe('🎉🎉');
  });

  it('drops the partial character a cut lands inside', () => {
    // The budget ends one byte into the second emoji; it goes whole or not at all.
    expect(bounded('🎉🎉', 5)).toBe('🎉');
    expect(bounded('🎉🎉', 5)).not.toContain('\uFFFD');
  });

  // ! U+FFFD is a character a sender can type. Stripping one from a value that
  // ! fit would make the recorded request differ from the accepted request while
  // ! `exceeds` still reports that nothing was lost — a silent edit of evidence.
  it('keeps a replacement character the value genuinely ends with', () => {
    expect(bounded('text�', 100)).toBe('text�');
    expect(exceeds('text�', 100)).toBe(false);
  });

  it('keeps a trailing replacement character that survives the cut', () => {
    // 'ab' plus U+FFFD is five bytes; a budget of five takes all of it, so the
    // character is present because the sender wrote it, not because of a cut.
    expect(bounded('ab�cd', 5)).toBe('ab�');
  });
});

describe('exceeds', () => {
  it('measures bytes rather than characters', () => {
    expect(exceeds('🎉', 3)).toBe(true);
    expect(exceeds('🎉', 4)).toBe(false);
  });

  it('agrees with what bounded actually does', () => {
    for (const [value, max] of [
      ['plain', 64],
      ['🎉🎉', 5],
      ['text�', 100],
      ['', 8],
    ] as const) {
      expect(exceeds(value, max)).toBe(bounded(value, max) !== value);
    }
  });
});
