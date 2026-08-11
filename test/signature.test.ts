import { describe, expect, it } from 'bun:test';
import { sign, verifySignature } from '../src/webhook/signature.ts';

const secret = 'a-shared-secret';
const body = '{"action":"opened","number":7}';

describe('sign', () => {
  it('produces the sha256= prefixed hex digest GitHub sends', () => {
    expect(sign(body, secret)).toMatch(/^sha256=[0-9a-f]{64}$/);
  });

  it('is stable for the same body and secret', () => {
    expect(sign(body, secret)).toBe(sign(body, secret));
  });
});

describe('verifySignature', () => {
  it('accepts a signature it produced', () => {
    expect(verifySignature({ body, signature: sign(body, secret), secret })).toBe(true);
  });

  it('rejects a body altered after signing', () => {
    const signature = sign(body, secret);
    const tampered = '{"action":"closed","number":7}';

    expect(verifySignature({ body: tampered, signature, secret })).toBe(false);
  });

  it('rejects a signature made with a different secret', () => {
    expect(verifySignature({ body, signature: sign(body, 'other'), secret })).toBe(false);
  });

  it('rejects a missing signature header', () => {
    expect(verifySignature({ body, signature: undefined, secret })).toBe(false);
  });

  // Guards the `timingSafeEqual` length check — without it this throws instead
  // of returning false, and the route answers 400 rather than 401.
  it('rejects a truncated signature without throwing', () => {
    expect(verifySignature({ body, signature: 'sha256=abc', secret })).toBe(false);
  });

  it('rejects an unprefixed digest', () => {
    const digest = sign(body, secret).slice('sha256='.length);

    expect(verifySignature({ body, signature: digest, secret })).toBe(false);
  });
});
