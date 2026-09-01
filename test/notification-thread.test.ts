import { describe, expect, it } from 'bun:test';
import { Effect, Either } from 'effect';
import { decodeThreads, deliveryIdFor, subjectRef } from '../src/notifications/thread.ts';

const decode = (input: unknown) => Effect.runSync(Effect.either(decodeThreads(input)));

type ThreadOverrides = {
  id?: string;
  unread?: boolean;
  reason?: string;
  updated_at?: string;
  last_read_at?: string;
  subject?: {
    title: string;
    url: string;
    latest_comment_url?: string;
    type: string;
  };
  repository?: { full_name: string };
};

const thread = (overrides: ThreadOverrides = {}) => ({
  id: '14567',
  unread: true,
  reason: 'mention',
  updated_at: '2026-08-21T10:00:00Z',
  last_read_at: '2026-08-20T09:00:00Z',
  subject: {
    title: 'Something broke',
    url: 'https://api.github.com/repos/edloidas/sandbox/issues/7',
    latest_comment_url: 'https://api.github.com/repos/edloidas/sandbox/issues/comments/99',
    type: 'Issue',
  },
  repository: { full_name: 'edloidas/sandbox' },
  ...overrides,
});

describe('decodeThreads', () => {
  it('decodes a full thread', () => {
    expect(Either.getOrThrow(decode([thread()]))).toEqual([thread()]);
  });

  it('reads an explicit null as absent for nullable fields', () => {
    const input = {
      ...thread(),
      last_read_at: null,
      subject: { ...thread().subject, latest_comment_url: null },
    };

    expect(Either.getOrThrow(decode([input]))).toEqual([
      {
        ...thread(),
        last_read_at: undefined,
        subject: { ...thread().subject, latest_comment_url: undefined },
      },
    ]);
  });

  it('accepts an unknown subject type without failing', () => {
    const input = thread({ subject: { ...thread().subject, type: 'CheckSuite' } });

    expect(Either.getOrThrow(decode([input]))[0]?.subject.type).toBe('CheckSuite');
  });

  it('fails when a named field has the wrong type', () => {
    expect(Either.isLeft(decode([{ ...thread(), id: 14567 }]))).toBe(true);
  });
});

describe('deliveryIdFor', () => {
  const idFor = (input: ReturnType<typeof thread>) => deliveryIdFor(input, JSON.stringify(input));

  it('keeps the thread id and updated_at greppable ahead of a 6-hex body hash', () => {
    expect(idFor(thread())).toMatch(/^notification:14567:2026-08-21T10:00:00Z:[0-9a-f]{6}$/);
  });

  it('is stable for an identical body', () => {
    expect(idFor(thread())).toBe(idFor(thread()));
  });

  // Same second, different payload: the case the digest exists for.
  it('differs for a different body with the same updated_at', () => {
    const later = thread({
      subject: {
        ...thread().subject,
        latest_comment_url: 'https://api.github.com/repos/edloidas/sandbox/issues/comments/100',
      },
    });

    expect(idFor(later)).not.toBe(idFor(thread()));
    const withoutDigest = (id: string) => id.replace(/:[0-9a-f]{6}$/, '');
    expect(withoutDigest(idFor(later))).toBe(withoutDigest(idFor(thread())));
  });
});

describe('subjectRef', () => {
  it('parses an issue url', () => {
    expect(subjectRef(thread())).toEqual({ kind: 'issue', number: 7 });
  });

  it('parses a pulls url', () => {
    const input = thread({
      subject: {
        ...thread().subject,
        type: 'PullRequest',
        url: 'https://api.github.com/repos/edloidas/sandbox/pulls/12',
      },
    });

    expect(subjectRef(input)).toEqual({ kind: 'pull_request', number: 12 });
  });

  it('treats a PullRequest-typed /issues/ url as a pull request', () => {
    const input = thread({
      subject: { ...thread().subject, type: 'PullRequest' },
    });

    expect(subjectRef(input)).toEqual({ kind: 'pull_request', number: 7 });
  });

  it('returns undefined for an unparseable url', () => {
    const input = thread({
      subject: {
        ...thread().subject,
        type: 'Discussion',
        url: 'https://api.github.com/repos/edloidas/sandbox/discussions/3',
      },
    });

    expect(subjectRef(input)).toBeUndefined();
  });
});
