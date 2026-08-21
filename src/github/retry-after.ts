/**
 * How long GitHub wants the caller to wait, in milliseconds.
 *
 * `retry-after` is seconds; `x-ratelimit-reset` is an absolute epoch second.
 * Returns `undefined` when neither is usable, which is how a 403 that means
 * "forbidden" is told apart from a 403 that means "slow down".
 *
 * Shared by the broker and the startup identity check on purpose: both back off
 * against the same account-wide bucket, and a header GitHub supplied always
 * beats an exponential guess at when it reopens.
 */
export const retryAfterMs = (
  headers: Readonly<Record<string, string | undefined>>,
  nowMs: number,
): number | undefined => {
  const retryAfter = headers['retry-after'];
  if (retryAfter !== undefined) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1000);
  }
  const reset = headers['x-ratelimit-reset'];
  if (reset !== undefined && headers['x-ratelimit-remaining'] === '0') {
    const epochSeconds = Number(reset);
    if (Number.isFinite(epochSeconds)) return Math.max(0, epochSeconds * 1000 - nowMs);
  }
  return undefined;
};

/**
 * Wait to assume when GitHub throttles without saying for how long.
 *
 * A 429 is definitive even with no usable header, so the caller must still back
 * off rather than collapse it into a generic failure. One minute is the shortest
 * window GitHub's secondary limits are documented to use.
 */
export const DEFAULT_THROTTLE_WAIT_MS = 60_000;

/**
 * Whether a 403 body is GitHub saying "slow down" rather than "no".
 *
 * Secondary rate limits are documented to answer 403 with neither `retry-after`
 * nor `x-ratelimit-remaining`, so the headers cannot tell this apart from a
 * genuine permission failure and the prose is the only signal left. Guessing
 * wrong in the other direction is worse: a throttle read as a dead credential
 * fails startup, and a dead credential read as a throttle burns the retry
 * budget — so the match is deliberately narrow.
 */
export const isSecondaryRateLimit = (body: string): boolean =>
  /secondary rate limit|exceeded a secondary|abuse detection/i.test(body);
