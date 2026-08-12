import { describe, expect, it } from 'bun:test';
import { Effect, Either, Schema } from 'effect';
import { type Delivery, decodePayload, deliveryKey, WebhookPayload } from '../src/webhook/event.ts';

const decode = (input: unknown) => Effect.runSync(Effect.either(decodePayload(input)));

describe('decodePayload', () => {
  it('reads the envelope fields it names', () => {
    const result = decode({
      action: 'opened',
      installation: { id: 42 },
      repository: { name: 'lictor', full_name: 'edloidas/lictor' },
      sender: { login: 'edloidas' },
    });

    expect(Either.getOrThrow(result)).toEqual({
      action: 'opened',
      installation: { id: 42 },
      repository: { name: 'lictor', full_name: 'edloidas/lictor' },
      sender: { login: 'edloidas' },
    });
  });

  it('accepts a payload with none of the optional fields', () => {
    expect(Either.getOrThrow(decode({}))).toEqual({});
  });

  // Organization-scoped deliveries send an explicit null rather than omitting
  // the key. Treating that as absent is what keeps those events from 400ing.
  it('reads an explicit null field as absent', () => {
    const result = Either.getOrThrow(decode({ action: 'created', repository: null }));

    expect(result).toEqual({ action: 'created', repository: undefined });
    expect(result.repository).toBeUndefined();
  });

  it('drops event-specific fields it does not name', () => {
    const result = Either.getOrThrow(decode({ action: 'opened', issue: { number: 7 } }));

    expect(result).toEqual({ action: 'opened' });
  });

  it('fails when a named field has the wrong type', () => {
    expect(Either.isLeft(decode({ installation: { id: 'forty-two' } }))).toBe(true);
  });

  it('fails on a non-object payload', () => {
    expect(Either.isLeft(decode('not a payload'))).toBe(true);
  });

  it('round-trips through the schema encoder', () => {
    const payload = Schema.decodeUnknownSync(WebhookPayload)({ action: 'closed' });

    expect(Schema.encodeSync(WebhookPayload)(payload)).toEqual({ action: 'closed' });
  });
});

describe('deliveryKey', () => {
  const delivery = (payload: Delivery['payload']): Delivery => ({
    event: 'issues',
    id: 'd-1',
    payload,
    raw: payload,
  });

  it('joins the event and action', () => {
    expect(deliveryKey(delivery({ action: 'opened' }))).toBe('issues.opened');
  });

  it('falls back to the bare event when there is no action', () => {
    expect(deliveryKey(delivery({}))).toBe('issues');
  });
});
