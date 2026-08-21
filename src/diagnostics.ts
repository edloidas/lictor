import { Cause, Option } from 'effect';

/**
 * One safe line describing why something failed.
 *
 * Never renders the nested cause chain, and that restriction is the whole point.
 * `GitHubClient` injects the access token as a plain `Authorization` header, so a
 * rendered HTTP request carries the credential in full — `Redacted` protects the
 * value in config and stops protecting it the moment it reaches the wire. The
 * same chain also renders schema rejections together with the value they
 * rejected, which is how a comment body reaches a log line.
 *
 * An error's own `message` is authored text everywhere in this codebase, so it
 * stays: a tag alone turns a diagnosable failure into a mystery.
 */
export const describeCause = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.failureOption(cause);
  if (Option.isSome(failure)) return describe(failure.value, 'UnknownFailure');
  const defect = Cause.dieOption(cause);
  if (Option.isSome(defect)) return `Defect: ${describe(defect.value, 'unknown')}`;
  return Cause.isInterrupted(cause) ? 'Interrupted' : 'Empty';
};

const describe = (error: unknown, fallback: string): string => {
  const shape = error as { readonly _tag?: unknown; readonly message?: unknown };
  // ! A class name is safe to print and is the only label a plain `Error` has.
  const named = error instanceof Error ? error.name : fallback;
  const tag = typeof shape?._tag === 'string' ? shape._tag : named;
  return typeof shape?.message === 'string' && shape.message.length > 0
    ? `${tag}: ${shape.message}`
    : tag;
};
