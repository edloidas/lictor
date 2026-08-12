import {
  HttpMiddleware,
  HttpRouter,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from '@effect/platform';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from './config.ts';
import { GitHubClient } from './github/client.ts';
import { registry } from './handlers/index.ts';
import { decodePayload } from './webhook/event.ts';
import { dispatch } from './webhook/router.ts';
import {
  DELIVERY_HEADER,
  EVENT_HEADER,
  SIGNATURE_HEADER,
  verifySignature,
} from './webhook/signature.ts';

/** Path GitHub delivers to. The App's webhook URL must end with it. */
export const WEBHOOK_PATH = '/webhooks/github';

/**
 * Receives one delivery.
 *
 * The response is deliberately uninformative: a caller that fails verification
 * learns only that it failed, never which check rejected it.
 */
const webhook = Effect.gen(function* () {
  const config = yield* LictorConfig;
  const request = yield* HttpServerRequest.HttpServerRequest;

  // ! Raw bytes, not `request.json`. The HMAC covers exactly what GitHub sent,
  // ! and re-serializing a parsed payload changes key order and whitespace.
  const body = yield* request.text;

  const verified = verifySignature({
    body,
    signature: request.headers[SIGNATURE_HEADER],
    secret: Redacted.value(config.webhookSecret),
  });

  if (!verified) {
    yield* Effect.logWarning('Rejected a delivery with a bad signature');
    return HttpServerResponse.empty({ status: 401 });
  }

  const event = request.headers[EVENT_HEADER];
  if (event === undefined) {
    return HttpServerResponse.empty({ status: 400 });
  }

  // ! `Effect.try`, not a bare `JSON.parse`. A throw inside `Effect.gen` is a
  // ! defect, which `catchAll` below does not see — the route would answer 500
  // ! on a body that is merely malformed.
  const json = yield* Effect.try(() => JSON.parse(body) as unknown);
  const payload = yield* decodePayload(json);
  const delivery = {
    event,
    id: request.headers[DELIVERY_HEADER] ?? '(unknown)',
    payload,
    raw: json,
  };

  // ! Detached on purpose. GitHub gives a webhook 10 seconds before it records
  // ! the delivery as failed, and handler work is not bounded by that. The
  // ! delivery is durable on GitHub's side and replayable by id, so acking
  // ! before the work finishes loses nothing we cannot re-request.
  yield* Effect.forkDaemon(dispatch(registry)(delivery));

  return HttpServerResponse.empty({ status: 202 });
}).pipe(
  Effect.catchAll((cause) =>
    Effect.logError('Malformed delivery', cause).pipe(
      Effect.as(HttpServerResponse.empty({ status: 400 })),
    ),
  ),
);

export const router = HttpRouter.empty.pipe(
  HttpRouter.get('/health', HttpServerResponse.text('ok')),
  HttpRouter.post(WEBHOOK_PATH, webhook),
);

/** The running server, minus its platform — `main.ts` supplies that. */
export const ServerLive = HttpServer.serve(router, HttpMiddleware.logger).pipe(
  HttpServer.withLogAddress,
  Layer.provide(GitHubClient.Default),
  Layer.provide(LictorConfig.Default),
);
