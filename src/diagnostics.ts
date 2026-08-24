import { Cause, Option } from 'effect';

/**
 * One safe line describing why something failed.
 *
 * Never renders the nested cause chain: `GitHubClient` injects the token as a
 * plain `Authorization` header, and a rendered chain carries both that request
 * and schema rejections' rejected values (comment bodies included). An error's
 * own `message` is authored text everywhere, so it stays.
 */
export const describeCause = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.failureOption(cause);
  if (Option.isSome(failure)) return describe(failure.value, 'UnknownFailure');
  const defect = Cause.dieOption(cause);
  if (Option.isSome(defect)) return `Defect: ${describe(defect.value, 'unknown')}`;
  return Cause.isInterrupted(cause) ? 'Interrupted' : 'Empty';
};

/**
 * The authored operation name carried by the cause's failure, when its error
 * has one. `QueueError` has no `message`, so a described cause is a bare tag
 * and this is what names the statement that failed.
 */
export const failureOperation = (cause: Cause.Cause<unknown>): string | undefined => {
  const failure = Cause.failureOption(cause);
  if (Option.isNone(failure)) return undefined;
  const operation = (failure.value as { readonly operation?: unknown }).operation;
  return typeof operation === 'string' ? operation : undefined;
};

const describe = (error: unknown, fallback: string): string => {
  const shape = error as { readonly _tag?: unknown; readonly message?: unknown };
  // Class name: safe to print, and the only label a plain `Error` has.
  const named = error instanceof Error ? error.name : fallback;
  const tag = typeof shape?._tag === 'string' ? shape._tag : named;
  return typeof shape?.message === 'string' && shape.message.length > 0
    ? `${tag}: ${shape.message}`
    : tag;
};
