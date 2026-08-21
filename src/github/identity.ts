import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Duration, Effect, Ref, Schedule, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { GitHubClient } from './client.ts';
import { DEFAULT_THROTTLE_WAIT_MS, isSecondaryRateLimit, retryAfterMs } from './retry-after.ts';

export class GitHubIdentityError extends Data.TaggedError('GitHubIdentityError')<{
  readonly message: string;
  /**
   * Whether retrying could plausibly succeed. A rejected or mismatched
   * credential never heals; an unreachable or throttled GitHub always does.
   */
  readonly transient?: boolean;
  /** Wait GitHub asked for, when it said so. Beats guessing at the reset. */
  readonly retryAfterMs?: number;
  readonly cause?: unknown;
}> {}

const User = Schema.Struct({ login: Schema.String });

/** Warn this far ahead of expiry, so a rotation can be scheduled rather than rushed. */
const EXPIRY_WARNING_MS = 7 * 24 * 60 * 60 * 1000;

/** Longest gap between attempts while GitHub is unreachable and naming no wait. */
const MAX_PROBE_GAP_MS = 30_000;

/**
 * Scope a classic token needs to clone, push, and comment.
 *
 * `repo` is a single grant covering all of it; there is no narrower classic
 * scope that reaches private repository contents.
 */
const REQUIRED_SCOPE = 'repo';

export type VerifiedIdentity = {
  readonly login: string;
  readonly tokenExpiresAt: number | undefined;
};

/**
 * The account the daemon is actually acting as.
 *
 * One `GET /user` covers both a wrong token and a revoked one, and it yields a
 * login GitHub confirmed rather than one an operator typed — which is what makes
 * it safe to compare senders against.
 *
 * **Building this layer performs no I/O.** The probe runs on first use of
 * `verified` and its result is memoized. That is deliberate and load-bearing:
 * everything that must survive a GitHub outage — binding the webhook socket,
 * claiming daemon ownership and renewing its lease, answering the control
 * socket — would otherwise be serialized behind a network call none of them
 * need. A delivery refused at an unbound socket is lost, because GitHub does not
 * redeliver a failed delivery on its own.
 *
 * Transient failures retry without limit rather than against a budget, for the
 * same reason: the inbox stays reachable throughout, so waiting costs nothing
 * while giving up costs every later delivery. Only a credential that can never
 * work — wrong account, refused, expired — fails, and `src/main.ts` turns that
 * into a deliberate shutdown.
 */
export class GitHubIdentity extends Effect.Service<GitHubIdentity>()('GitHubIdentity', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const github = yield* GitHubClient;
    const client = yield* github.authenticated;

    const probe = Effect.gen(function* () {
      const response = yield* client.execute(HttpClientRequest.get('/user')).pipe(
        Effect.mapError(
          (cause) =>
            new GitHubIdentityError({
              message: 'Could not reach GitHub to verify the configured credential',
              transient: true,
              cause,
            }),
        ),
      );

      // ! GitHub answers 403 both for a credential it will not accept and for a
      // ! bucket that is momentarily empty. Only the first should be fatal, and
      // ! a secondary limit is documented to arrive with neither rate header —
      // ! so the body has to be read before calling a 403 a dead credential.
      const hinted = retryAfterMs(response.headers, yield* Clock.currentTimeMillis);
      const throttled =
        response.status === 403 &&
        (hinted !== undefined ||
          // ! An exhausted bucket reported without a reset time is still an
          // ! exhausted bucket. `retryAfterMs` returns nothing here because it
          // ! has no time to offer, not because there is no throttle.
          response.headers['x-ratelimit-remaining'] === '0' ||
          isSecondaryRateLimit(yield* Effect.orElseSucceed(response.text, () => '')));

      if (response.status === 401 || (response.status === 403 && !throttled)) {
        return yield* new GitHubIdentityError({
          message: `GitHub rejected the configured credential with status ${response.status}`,
        });
      }
      if (response.status === 429 || response.status === 403 || response.status >= 500) {
        const wait =
          response.status === 429 || throttled ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : hinted;
        return yield* new GitHubIdentityError({
          message: `Verifying the configured credential returned status ${response.status}`,
          transient: true,
          ...(wait === undefined ? {} : { retryAfterMs: wait }),
        });
      }
      if (response.status < 200 || response.status >= 300) {
        return yield* new GitHubIdentityError({
          message: `Verifying the configured credential returned status ${response.status}`,
        });
      }

      // ! Decoded inside the probe, not after it. A 200 whose body arrives
      // ! truncated is as transient as a request that never arrived, and outside
      // ! the retry it would be indistinguishable from a bad token.
      const body = yield* Effect.flatMap(response.json, Schema.decodeUnknown(User)).pipe(
        Effect.mapError(
          (cause) =>
            new GitHubIdentityError({
              message: 'Could not read the authenticated user',
              transient: true,
              cause,
            }),
        ),
      );

      return { body, headers: response.headers };
    });

    const verify = Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const { body, headers } = yield* probe.pipe(
        // ! Every wait happens here and none in the schedule. A schedule sees its
        // ! own output, not the error that carried the header, so honouring the
        // ! header there is impossible — and doing it in both places composes
        // ! them, turning a requested 30s into 30s plus whatever the schedule
        // ! adds. Retrying sooner than GitHub asked is what turns a throttle into
        // ! a blocked integration; retrying later is merely slow.
        Effect.tapError((error) =>
          error.transient === true
            ? Ref.updateAndGet(attempts, (count) => count + 1).pipe(
                Effect.flatMap((count) => {
                  const backoff = Math.min(1000 * 2 ** (count - 1), MAX_PROBE_GAP_MS);
                  const wait = error.retryAfterMs ?? backoff;
                  return Effect.logWarning('Retrying GitHub credential verification')
                    .pipe(Effect.annotateLogs({ reason: error.message, waitMs: wait }))
                    .pipe(Effect.zipRight(Effect.sleep(Duration.millis(wait))));
                }),
              )
            : Effect.void,
        ),
        // ! No attempt or elapsed limit on purpose — see the class comment.
        Effect.retry({
          while: (error: GitHubIdentityError) => error.transient === true,
          schedule: Schedule.forever,
        }),
      );

      const login = body.login.trim().toLowerCase();

      if (login !== config.expectedLogin) {
        return yield* new GitHubIdentityError({
          message: `Credential belongs to ${login}, not the expected ${config.expectedLogin}`,
        });
      }

      // ! Trusting your own account is a self-trigger loop with extra steps. The
      // ! qualification policy drops self-authored deliveries regardless, but a
      // ! configuration this wrong is worth refusing rather than quietly ignoring.
      if (config.trustedSenders.some((sender) => sender.trim().toLowerCase() === login)) {
        return yield* new GitHubIdentityError({
          message: `${login} authenticates this daemon and must not be a trusted sender`,
        });
      }

      // ! `x-oauth-scopes` is returned for classic tokens only. Absent means the
      // ! credential is a different class — a fine-grained PAT, which was ruled
      // ! out because it cannot cross resource owners, or a future App
      // ! user-to-server token, which is the migration this seam exists for. So
      // ! a missing header warns and a present-but-insufficient one fails: the
      // ! second is a token that will 403 on the first clone, and finding that
      // ! out at boot beats finding it out per job.
      const scopes = headers['x-oauth-scopes'];
      if (scopes === undefined) {
        yield* Effect.logWarning('GitHub reported no token scopes').pipe(
          Effect.annotateLogs({ login, expected: REQUIRED_SCOPE }),
        );
      } else if (
        !scopes
          .split(',')
          .map((scope) => scope.trim())
          .includes(REQUIRED_SCOPE)
      ) {
        return yield* new GitHubIdentityError({
          message: `The configured credential lacks the ${REQUIRED_SCOPE} scope; GitHub reports "${scopes}"`,
        });
      }

      const header = headers['github-authentication-token-expiration'];
      const parsed = header === undefined ? Number.NaN : Date.parse(header);
      const tokenExpiresAt = Number.isNaN(parsed) ? undefined : parsed;

      if (tokenExpiresAt !== undefined) {
        const remaining = tokenExpiresAt - (yield* Clock.currentTimeMillis);
        if (remaining <= 0) {
          return yield* new GitHubIdentityError({
            message: 'The configured credential has expired',
          });
        }
        if (remaining < EXPIRY_WARNING_MS) {
          yield* Effect.logWarning('GitHub credential expires soon').pipe(
            Effect.annotateLogs({
              login,
              days: Math.floor(remaining / (24 * 60 * 60 * 1000)),
            }),
          );
        }
      }

      return { login, tokenExpiresAt } satisfies VerifiedIdentity;
    });

    // ! Memoized, so the many callers that need the login share one probe and
    // ! one verdict. Caching the fatal outcome too is correct: nothing reaching
    // ! that branch heals without an operator changing the token.
    const verified = yield* Effect.cached(verify);

    return { verified };
  }),
  dependencies: [LictorConfig.Default, GitHubClient.Default],
}) {}
