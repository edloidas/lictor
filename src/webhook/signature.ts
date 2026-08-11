import { createHmac, timingSafeEqual } from 'node:crypto';

/** Header GitHub puts the HMAC in. Lowercase — Effect normalizes header names. */
export const SIGNATURE_HEADER = 'x-hub-signature-256';

/** Header carrying the event name, e.g. `issues`, `pull_request`, `ping`. */
export const EVENT_HEADER = 'x-github-event';

/** Header carrying the delivery UUID — the id to quote when replaying from GitHub. */
export const DELIVERY_HEADER = 'x-github-delivery';

/** Computes the `sha256=<hex>` value GitHub sends for a given body and secret. */
export const sign = (body: string, secret: string): string =>
  `sha256=${createHmac('sha256', secret).update(body).digest('hex')}`;

/**
 * Constant-time check of a delivery signature.
 *
 * The body must be the exact bytes GitHub sent. Re-serializing the parsed JSON
 * changes key order and whitespace, which changes the digest — always verify
 * before parsing.
 */
export const verifySignature = (options: {
  readonly body: string;
  readonly signature: string | undefined;
  readonly secret: string;
}): boolean => {
  if (options.signature === undefined) return false;

  const expected = Buffer.from(sign(options.body, options.secret));
  const actual = Buffer.from(options.signature);

  // ! `timingSafeEqual` throws on a length mismatch rather than returning false,
  // ! so the guard is required — and it leaks nothing: the expected length is a
  // ! constant of the algorithm, not a secret.
  if (expected.length !== actual.length) return false;

  return timingSafeEqual(expected, actual);
};
