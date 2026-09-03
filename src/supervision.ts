import { Cause, Effect, Exit } from 'effect';

export type FatalAction = (message: string, cause?: Cause.Cause<unknown>) => Effect.Effect<void>;

/**
 * Builds the supervisor that runs work which must not end quietly, and stops
 * the daemon when it does.
 *
 * The whole exit is inspected: a defect bypasses `tapError` and `ignore` alike,
 * so a dead fiber looks exactly like a healthy idle one; `completes: 'never'`
 * treats even a *successful* return as the bug it is for a loop; interruption,
 * by contrast, is clean shutdown and must not fire a second `SIGTERM`.
 */
export const supervisor =
  (fatal: FatalAction) =>
  <A, E, R>(
    name: string,
    /** `never` for a loop, where returning at all is the bug. */
    completes: 'never' | 'once',
    work: Effect.Effect<A, E, R>,
  ) =>
    Effect.exit(work).pipe(
      Effect.flatMap((exit) => {
        if (Exit.isSuccess(exit)) {
          return completes === 'never' ? fatal(`The ${name} stopped`) : Effect.void;
        }
        return Cause.isInterruptedOnly(exit.cause)
          ? Effect.void
          : fatal(`The ${name} stopped`, exit.cause);
      }),
    );
