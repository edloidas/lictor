import { describe, expect, it } from 'bun:test';
import { Effect, Either } from 'effect';
import type { Delivery } from '../src/webhook/event.ts';
import { qualifyDelivery } from '../src/webhook/qualification.ts';

const policy = { trustedSenders: ['edloidas'], targetUsers: ['adiutriel'], selfLogin: 'adiutriel' };
const subject = {
  number: 17,
  title: 'Keep the queue moving',
  html_url: 'https://github.com/edloidas/lictor/issues/17',
  updated_at: '2026-08-12T12:00:00Z',
};

const delivery = (
  event: string,
  raw: Record<string, unknown>,
  action = String(raw.action ?? 'opened'),
): Delivery => ({
  event,
  id: 'delivery-1',
  payload: {
    action,
    sender: { login: String((raw.sender as { login?: string } | undefined)?.login ?? 'edloidas') },
    repository: { name: 'lictor', full_name: 'edloidas/lictor' },
  },
  raw: {
    action,
    sender: { login: 'edloidas' },
    repository: { full_name: 'edloidas/lictor' },
    installation: { id: 42 },
    ...raw,
  },
});

const run = (input: Delivery, customPolicy = policy) =>
  Effect.runSync(Effect.either(qualifyDelivery(input, customPolicy)));

describe('qualifyDelivery', () => {
  // ! An App actor never received webhooks for its own actions; a real account
  // ! does. The drop must not depend on the operator having left the daemon's
  // ! login out of `trustedSenders`.
  it('drops a delivery the daemon itself authored, even when that login is trusted', () => {
    const result = run(
      delivery('issue_comment', {
        action: 'created',
        sender: { login: 'adiutriel' },
        issue: subject,
        comment: { body: 'following up @adiutriel', html_url: 'https://github.com/c/1' },
      }),
      {
        trustedSenders: ['edloidas', 'adiutriel'],
        targetUsers: ['adiutriel'],
        selfLogin: 'adiutriel',
      },
    );

    expect(Either.getOrThrow(result)).toBeUndefined();
  });

  it('matches the daemon login case-insensitively when dropping', () => {
    const result = run(
      delivery('issue_comment', {
        action: 'created',
        sender: { login: 'Adiutriel' },
        issue: subject,
        comment: { body: 'ping @adiutriel', html_url: 'https://github.com/c/2' },
      }),
      { trustedSenders: ['Adiutriel'], targetUsers: ['adiutriel'], selfLogin: 'adiutriel' },
    );

    expect(Either.getOrThrow(result)).toBeUndefined();
  });

  it('qualifies an issue assigned by a trusted sender to a configured target', () => {
    const result = run(
      delivery('issues', { action: 'assigned', issue: subject, assignee: { login: 'Adiutriel' } }),
    );

    expect(Either.getOrThrow(result)).toEqual({
      deliveryId: 'delivery-1',
      interactionId:
        '["issues","assigned","edloidas/lictor","issue",17,"https://github.com/edloidas/lictor/issues/17","2026-08-12T12:00:00Z",["adiutriel"],["assigned"]]',
      event: 'issues',
      action: 'assigned',
      repository: 'edloidas/lictor',
      installationId: 42,
      sender: 'edloidas',
      targets: ['adiutriel'],
      reasons: ['assigned'],
      subject: {
        kind: 'issue',
        number: 17,
        title: 'Keep the queue moving',
        url: 'https://github.com/edloidas/lictor/issues/17',
      },
    });
  });

  it('qualifies a pull request review request', () => {
    const result = run(
      delivery('pull_request', {
        action: 'review_requested',
        pull_request: { ...subject, html_url: 'https://github.com/edloidas/lictor/pull/17' },
        requested_reviewer: { login: 'adiutriel' },
      }),
    );

    expect(Either.getOrThrow(result)?.reasons).toEqual(['review_requested']);
    expect(Either.getOrThrow(result)?.subject.kind).toBe('pull_request');
  });

  it('qualifies a direct mention in a new issue body case-insensitively', () => {
    const result = run(
      delivery('issues', { issue: { ...subject, body: 'Could @ADIUTRIEL take this?' } }),
    );

    expect(Either.getOrThrow(result)?.reasons).toEqual(['mentioned']);
    expect(Either.getOrThrow(result)?.targets).toEqual(['adiutriel']);
  });

  it('qualifies a mention added by an issue edit', () => {
    const result = run(
      delivery('issues', {
        action: 'edited',
        issue: { ...subject, body: 'Now asking @adiutriel' },
        changes: { body: { from: 'No target yet' } },
      }),
    );

    expect(Either.getOrThrow(result)?.reasons).toEqual(['mentioned']);
  });

  it('drops an edit when the target was already mentioned', () => {
    const result = run(
      delivery('issues', {
        action: 'edited',
        issue: { ...subject, body: 'Still asking @adiutriel, with more detail' },
        changes: { body: { from: 'Asking @adiutriel' } },
      }),
    );

    expect(Either.getOrThrow(result)).toBeUndefined();
  });

  it('drops a title-only edit when the unchanged body contains a target mention', () => {
    const result = run(
      delivery('issues', {
        action: 'edited',
        issue: { ...subject, body: 'Already asking @adiutriel' },
        changes: { title: { from: 'Old title' } },
      }),
    );

    expect(Either.getOrThrow(result)).toBeUndefined();
  });

  it('qualifies a pull request comment mention and records its URL', () => {
    const result = run(
      delivery('issue_comment', {
        action: 'created',
        issue: { ...subject, pull_request: { url: 'https://api.github.com/pulls/17' } },
        comment: {
          body: '@adiutriel please review',
          html_url: 'https://github.com/edloidas/lictor/pull/17#issuecomment-1',
        },
      }),
    );

    expect(Either.getOrThrow(result)?.subject.kind).toBe('pull_request');
    expect(Either.getOrThrow(result)?.contextUrl).toEndWith('#issuecomment-1');
  });

  it('drops activity from an untrusted sender', () => {
    const input = delivery('issues', {
      action: 'assigned',
      sender: { login: 'mallory' },
      issue: subject,
      assignee: { login: 'adiutriel' },
    });

    expect(Either.getOrThrow(run(input))).toBeUndefined();
  });

  it('trusts nobody when the sender list is empty', () => {
    const input = delivery('issues', {
      action: 'assigned',
      issue: subject,
      assignee: { login: 'adiutriel' },
    });

    expect(
      Either.getOrThrow(
        run(input, { trustedSenders: [], targetUsers: ['adiutriel'], selfLogin: 'adiutriel' }),
      ),
    ).toBeUndefined();
  });

  it('does not treat a longer login as a target mention', () => {
    const result = run(
      delivery('issues', { issue: { ...subject, body: 'Asking @adiutriel-bot instead' } }),
    );

    expect(Either.getOrThrow(result)).toBeUndefined();
  });

  it('fails decoding when supported event metadata is malformed', () => {
    const result = run(
      delivery('issues', { issue: { ...subject, number: 'seventeen' }, body: '@adiutriel' }),
    );

    expect(Either.isLeft(result)).toBe(true);
  });

  it('fails when a supported action is missing its subject', () => {
    const result = run(
      delivery('issues', { action: 'assigned', assignee: { login: 'adiutriel' } }),
    );

    expect(Either.isLeft(result)).toBe(true);
  });
});
