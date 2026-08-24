import { Effect, Ref } from 'effect';

/**
 * Daemon-wide "the credential is dead" latch.
 *
 * Set by whichever path first sees GitHub refuse the credential — the poller,
 * a broker call, a workspace clone — and read by the worker, which stops
 * claiming while it is set, and by the control socket, which reports it.
 *
 * Never cleared on purpose: a revoked or expired token heals only when an
 * operator replaces `LICTOR_GITHUB_TOKEN`, and that happens across a restart.
 * Clearing automatically would turn one stale success into a fresh burn of
 * every job's attempt budget against a credential that is still dead.
 */
export class CredentialHealth extends Effect.Service<CredentialHealth>()('CredentialHealth', {
  effect: Effect.gen(function* () {
    const rejected = yield* Ref.make(false);
    // Loud exactly once: every poll cycle repeating a fatal-grade line buries
    // everything else.
    const suspend = Ref.getAndSet(rejected, true).pipe(
      Effect.flatMap((already) =>
        already
          ? Effect.void
          : Effect.logError(
              'Credential rejected: GitHub refused the configured token. Replace LICTOR_GITHUB_TOKEN and restart the daemon; queued work is retained unclaimed.',
            ),
      ),
      Effect.asVoid,
    );
    const isRejected = Ref.get(rejected);
    return { suspend, isRejected };
  }),
}) {}
