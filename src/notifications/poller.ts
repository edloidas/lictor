import { HttpClientRequest } from '@effect/platform';
import { Clock, Data, Duration, Effect, Ref } from 'effect';
import { LictorConfig } from '../config.ts';
import { GitHubClient } from '../github/client.ts';
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

    /**
     * Set once a 401 is confirmed, and never cleared.
     *
     * A credential GitHub refuses does not heal without an operator changing
     * it, and repeating the same rejected request every minute forever buries
     * the one log line that says why nothing is happening.
     */
    const suspended = yield* Ref.make(false);
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
      if (yield* Ref.get(suspended)) return { waitMs: floor, stored: 0, deferred: false };

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
          yield* Ref.set(suspended, true);
          yield* Effect.logError(
            'Notification polling suspended: GitHub refused the configured credential. Replace LICTOR_GITHUB_TOKEN and restart the daemon.',
          );
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
            Effect.annotateLogs({ status: response.status, waitMs: Math.max(backoffMs, floor) }),
          );
          break;
        }

        if (response.status < 200 || response.status >= 300) {
          return yield* new PollError({
            message: `Polling notifications returned status ${response.status}`,
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
     * ! `Effect.forever` runs the body before it ever sleeps, so the first
     * ! sweep happens at startup. Waiting one interval first would lose the
     * ! whole point of acknowledging fast — a mention would sit unanswered for
     * ! a minute after every restart.
     */
    const run = Effect.forever(
      pollOnce.pipe(
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
        Effect.flatMap((outcome) => Effect.sleep(Duration.millis(outcome.waitMs))),
      ),
    );

    return { pollOnce, run };
  }),
  dependencies: [LictorConfig.Default, GitHubClient.Default, WorkQueue.Default, Policy.Default],
}) {}
