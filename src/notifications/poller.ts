import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Duration, Effect, Schema } from 'effect';
import { LictorConfig } from '../config.ts';
import { GitHubClient } from '../github/client.ts';
import { CredentialHealth } from '../github/credential-health.ts';
import {
  DEFAULT_THROTTLE_WAIT_MS,
  isSecondaryRateLimit,
  retryAfterMs,
} from '../github/retry-after.ts';
import { Policy } from '../policy.ts';
import { type QueueError, WorkQueue } from '../queue/work-queue.ts';
import { decodeThreads, deliveryIdFor, type NotificationThread } from './thread.ts';

export class PollError extends Data.TaggedError('PollError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Event name stored on every delivery this producer writes. */
export const NOTIFICATION_EVENT = 'notification';

/** Notifications requested per page. */
const PAGE_SIZE = 50;

/** The fields of a pending repository invitation the acceptance pass reads. */
const RepositoryInvitation = Schema.Struct({
  id: Schema.Number,
  inviter: Schema.Struct({ login: Schema.String }),
  repository: Schema.Struct({ full_name: Schema.String }),
});
const decodeInvitations = Schema.decodeUnknown(Schema.Array(RepositoryInvitation));

/**
 * Pages fetched in one sweep.
 *
 * A backlog deeper than this is not lost: nothing is marked read past the point
 * the sweep stops, so GitHub keeps holding the remainder and the next poll picks
 * up where this one left off.
 */
const PAGE_CEILING = 10;

type PollOutcome = {
  /** How long to wait before the next sweep. */
  readonly waitMs: number;
  readonly stored: number;
  /** True when the sweep stopped early because the queue is full. */
  readonly deferred: boolean;
};

/**
 * Polls `GET /notifications` and commits what arrives to the durable inbox.
 *
 * The only transport. A repository webhook needs admin on the repository, so it
 * is scoped to whatever the operator can administer rather than to what the
 * account can reach — the opposite of the requirement. Notifications reach
 * exactly as far as her membership graph does.
 *
 * A thread is marked read only after its row is committed, which is what the
 * webhook route's 202 used to mean. It is a stronger guarantee than webhooks
 * gave: a crash before the mark leaves the item with GitHub, where the next
 * sweep finds it, whereas a webhook delivery that failed needed a manual
 * redelivery inside thirty days.
 */
export class NotificationPoller extends Effect.Service<NotificationPoller>()('NotificationPoller', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;
    const github = yield* GitHubClient;
    const queue = yield* WorkQueue;
    const policy = yield* Policy;
    const health = yield* CredentialHealth;

    /**
     * The daemon-wide credential latch. A 401 anywhere — this loop, a broker
     * call, a clone — suspends everything that talks to GitHub through one
     * shared ref instead of three private ones.
     */
    // ! One sweep at a time. The loop is sequential on its own, but `pollOnce`
    // ! is also callable directly, and two concurrent sweeps would each read
    // ! the same threads and race on marking them read.
    const gate = yield* Effect.makeSemaphore(1);

    const request = (page: number, lastModified: string | undefined) => {
      const base = HttpClientRequest.get('/notifications').pipe(
        HttpClientRequest.setUrlParams({ per_page: String(PAGE_SIZE), page: String(page) }),
      );
      // ! Page 1 only — see the 304 branch in `sweep`.
      return page === 1 && lastModified !== undefined
        ? HttpClientRequest.setHeader(base, 'if-modified-since', lastModified)
        : base;
    };

    const markRead = (thread: NotificationThread) =>
      Effect.gen(function* () {
        const client = yield* github.authenticated;
        const response = yield* client.execute(
          HttpClientRequest.patch(`/notifications/threads/${thread.id}`),
        );
        // ! Any non-error status is success. GitHub answers this endpoint with a
        // ! bare no-content code and the exact one is not worth encoding here; 304
        // ! means someone else already read the thread and 404 means it is gone.
        // ! None of the three is a reason to retry.
        if (response.status >= 400 && response.status !== 404) {
          return yield* new PollError({
            message: `Marking thread ${thread.id} read returned status ${response.status}`,
          });
        }
      }).pipe(
        // ! Never fatal, and never silent either. The row is already committed,
        // ! so a refused PATCH costs nothing durable — but it must be reported to
        // ! the caller, because a sweep that advances the cursor over a thread
        // ! still unread turns the next poll into a 304 and the thread is never
        // ! listed again until fresh activity lands on it.
        Effect.catchAll((error) =>
          Effect.logWarning('Could not mark a notification thread read')
            .pipe(Effect.annotateLogs({ thread: thread.id, reason: error.message }))
            .pipe(Effect.as(false)),
        ),
        Effect.map((marked) => marked !== false),
      );

    const sweep = Effect.gen(function* () {
      const floor = config.notificationPollMs;
      if (yield* health.isRejected) return { waitMs: floor, stored: 0, deferred: false };

      // ! Checked before anything is fetched, not after. The depth limit lives
      // ! in `enqueue`, which runs a stage later — by then the thread is stored
      // ! and marked read, and GitHub no longer holds the overflow. Refusing to
      // ! sweep leaves it exactly where it can wait.
      let room = policy.maxQueueDepth - (yield* queue.backlog);
      if (room <= 0) {
        yield* Effect.logWarning('Deferring the notification sweep: the queue is full').pipe(
          Effect.annotateLogs({ maxQueueDepth: policy.maxQueueDepth }),
        );
        return { waitMs: floor, stored: 0, deferred: true };
      }

      const lastModified = yield* queue.pollerCursor;
      const client = yield* github.authenticated;

      const threads = new Map<string, NotificationThread>();
      let nextLastModified: string | undefined;
      let requested: number | undefined;
      let backoffMs: number | undefined;
      let truncated = false;

      for (let page = 1; page <= PAGE_CEILING; page += 1) {
        const response = yield* client
          .execute(request(page, lastModified))
          .pipe(
            Effect.mapError(
              (cause) =>
                new PollError({ message: 'Could not reach GitHub to poll notifications', cause }),
            ),
          );

        if (page === 1) {
          const interval = Number(response.headers['x-poll-interval']);
          requested = Number.isFinite(interval) && interval > 0 ? interval * 1000 : undefined;
          nextLastModified = response.headers['last-modified'];
        }

        // ! End-of-list only on page 1. A later page answering 304 is
        // ! indistinguishable from an empty page, so reading it as "no changes"
        // ! would advance the cursor past threads nobody fetched — and every one
        // ! of them has `updated_at` at or below the `Last-Modified` we just
        // ! stored, so the next poll answers 304 and they never come back.
        if (response.status === 304) {
          if (page > 1) truncated = true;
          break;
        }

        if (response.status === 401) {
          yield* health.suspend;
          return { waitMs: floor, stored: 0, deferred: false };
        }

        if (response.status === 429 || response.status === 403 || response.status >= 500) {
          const now = yield* Clock.currentTimeMillis;
          const hinted = retryAfterMs(response.headers, now);
          const throttled =
            response.status === 429 ||
            hinted !== undefined ||
            response.headers['x-ratelimit-remaining'] === '0' ||
            isSecondaryRateLimit(yield* Effect.orElseSucceed(response.text, () => ''));
          backoffMs = throttled ? (hinted ?? DEFAULT_THROTTLE_WAIT_MS) : floor;
          // ! Truncated: pages after this one were never read, so the cursor
          // ! must not advance over them.
          truncated = true;
          yield* Effect.logWarning('Backing off from notification polling').pipe(
            Effect.annotateLogs({
              status: response.status,
              waitMs: Math.max(backoffMs, floor),
              ...(response.headers['x-ratelimit-remaining'] === undefined
                ? {}
                : { remainingQuota: response.headers['x-ratelimit-remaining'] }),
            }),
          );
          break;
        }

        if (response.status < 200 || response.status >= 300) {
          return yield* new PollError({
            message: `Polling notifications returned status ${response.status}${
              response.headers['x-ratelimit-remaining'] === undefined
                ? ''
                : `, remaining quota ${response.headers['x-ratelimit-remaining']}`
            }`,
          });
        }

        const page_ = yield* Effect.flatMap(response.json, decodeThreads).pipe(
          Effect.mapError(
            (cause) =>
              new PollError({ message: 'Notification list did not match its schema', cause }),
          ),
        );
        // ! Keyed by thread id across pages. A thread that gained activity
        // ! mid-sweep shifts position and can appear twice; without the map the
        // ! second copy would be stored under a second synthetic id.
        for (const thread of page_) threads.set(thread.id, thread);
        if (page_.length < PAGE_SIZE) break;
        if (page === PAGE_CEILING) truncated = true;
      }

      let stored = 0;
      let deferred = truncated;
      for (const thread of threads.values()) {
        // ! Every thread is stored, including reasons this phase does not act on.
        // ! Filtering on `reason` here would be cheaper, and is wrong: `reason`
        // ! describes the *thread*, not the activity that just landed on it, and
        // ! GitHub does not re-key an already-unread thread when a new kind of
        // ! activity arrives. A thread that went unread as `assign` and then
        // ! received a trusted mention still reports `assign` — skipping it here
        // ! would mark it read with nothing durable behind it, and the mention
        // ! would be gone from both sides. Qualification drops it a stage later
        // ! instead, where the row survives to say so.
        if (room <= 0) {
          deferred = true;
          yield* Effect.logWarning('Stopped the notification sweep: the queue filled up').pipe(
            Effect.annotateLogs({ remaining: threads.size - stored }),
          );
          break;
        }
        yield* queue.receiveDelivery({
          id: deliveryIdFor(thread),
          event: NOTIFICATION_EVENT,
          body: JSON.stringify(thread),
          source: 'notification',
        });
        // ! After the row is committed, never before. This is what the 202 used
        // ! to be: until the mark lands, GitHub still owns the item.
        if (!(yield* markRead(thread))) deferred = true;
        stored += 1;
        room -= 1;
      }

      // ! Only a sweep that consumed everything it saw may advance the cursor.
      // ! Advancing after an early stop turns the next poll into a 304 and the
      // ! threads we deliberately left unread are never fetched again.
      if (!deferred && nextLastModified !== undefined) {
        yield* queue.setPollerCursor(nextLastModified);
      }

      if (stored > 0) {
        yield* Effect.logInfo('Committed notifications').pipe(
          Effect.annotateLogs({ stored, deferred }),
        );
      }

      // ! A wait GitHub asked for beats both the header it advertises and the
      // ! configured floor. Retrying sooner than a throttle said is what turns a
      // ! throttle into a block.
      return {
        waitMs: Math.max(backoffMs ?? requested ?? floor, floor),
        stored,
        deferred,
      };
    });

    const pollOnce: Effect.Effect<PollOutcome, PollError | QueueError> = gate.withPermits(1)(sweep);

    /**
     * Accepts pending repository invitations from the allow-list, and leaves
     * everything else pending.
     *
     * Runs on this loop's cadence because it is the same account and the same
     * credential: a sweep's 401 suspends this pass too, through the shared
     * ref. A declined invitation is destroyed, so untrusted ones are only
     * ever logged — a pending invitation is a useful signal that somebody is
     * trying to add her somewhere. Joining a repository arms nothing: no
     * policy entry exists for it until an operator writes one.
     */
    const acceptInvitations = Effect.gen(function* () {
      if (config.autoAcceptInviters.length === 0) return;
      if (yield* health.isRejected) return;

      const client = yield* github.authenticated;
      // ponytail: one page of 100; a live backlog past that wants the sweep's paging loop
      const list = HttpClientRequest.get('/user/repository_invitations').pipe(
        HttpClientRequest.setUrlParams({ per_page: '100' }),
      );
      const response = yield* client.execute(list).pipe(
        Effect.mapError(
          (cause) =>
            new PollError({
              message: 'Could not reach GitHub for repository invitations',
              cause,
            }),
        ),
      );

      if (response.status === 401) {
        yield* health.suspend;
        return;
      }
      if (response.status < 200 || response.status >= 300) {
        yield* Effect.logWarning('Listing repository invitations failed').pipe(
          Effect.annotateLogs({ status: response.status }),
        );
        return;
      }

      const invitations = yield* Effect.flatMap(response.json, decodeInvitations).pipe(
        Effect.tapError((cause) =>
          Effect.logWarning('Repository invitation list did not match its schema', { cause }),
        ),
        Effect.catchAll(() => Effect.succeed([])),
      );

      for (const invitation of invitations) {
        const inviter = invitation.inviter.login.toLowerCase();
        if (!config.autoAcceptInviters.includes(inviter)) {
          yield* Effect.logDebug('Leaving a repository invitation pending').pipe(
            Effect.annotateLogs({ inviter, repository: invitation.repository.full_name }),
          );
          continue;
        }
        const accepted = yield* client
          .execute(HttpClientRequest.patch(`/user/repository_invitations/${invitation.id}`))
          .pipe(
            Effect.mapError(
              (cause) =>
                new PollError({ message: `Could not accept invitation ${invitation.id}`, cause }),
            ),
            Effect.either,
          );
        if (accepted._tag === 'Left') {
          yield* Effect.logWarning('Could not accept a repository invitation').pipe(
            Effect.annotateLogs({ id: invitation.id, reason: accepted.left.message }),
          );
          continue;
        }
        // ! Any non-error status is success. GitHub answers with a bare no-content
        // ! code; 404 means someone already declined or withdrew it, which is not
        // ! worth distinguishing here.
        if (accepted.right.status < 200 || accepted.right.status >= 300) {
          yield* Effect.logWarning('Accepting a repository invitation failed').pipe(
            Effect.annotateLogs({ id: invitation.id, status: accepted.right.status }),
          );
          continue;
        }
        yield* Effect.logInfo('Accepted a repository invitation').pipe(
          Effect.annotateLogs({ inviter, repository: invitation.repository.full_name }),
        );
      }
    });

    /**
     * ! `Effect.forever` runs the body before it ever sleeps, so the first
     * ! sweep happens at startup. Waiting one interval first would lose the
     * ! whole point of acknowledging fast — a mention would sit unanswered for
     * ! a minute after every restart.
     */
    const run = Effect.forever(
      Effect.gen(function* () {
        const outcome = yield* pollOnce.pipe(
          Effect.catchAll((error) =>
            Effect.logError('Notification poll failed')
              .pipe(Effect.annotateLogs({ reason: error.message }))
              .pipe(
                Effect.as({
                  waitMs: config.notificationPollMs,
                  stored: 0,
                  deferred: false,
                } satisfies PollOutcome),
              ),
          ),
        );
        // ! After the sleep, not before: a backoff the sweep just accepted must
        // ! cover this pass too — firing an unthrottled request into a declared
        // ! backoff is what turns a throttle into a block.
        yield* Effect.sleep(Duration.millis(outcome.waitMs));
        // ! Never fatal: a failed pass costs one cadence, killing the loop costs
        // ! polling itself.
        yield* acceptInvitations.pipe(
          Effect.catchAll((error) =>
            Effect.logWarning('Invitation acceptance failed').pipe(
              Effect.annotateLogs({ reason: error.message }),
            ),
          ),
        );
      }),
    );

    return { pollOnce, run, acceptInvitations };
  }),
  dependencies: [
    LictorConfig.Default,
    GitHubClient.Default,
    WorkQueue.Default,
    Policy.Default,
    CredentialHealth.Default,
  ],
}) {}
