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
import { Policy } from './policy.ts';
import { WorkQueue } from './queue/work-queue.ts';
import { decodePayload } from './webhook/event.ts';
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

  const contentLength = Number(request.headers['content-length']);
  if (Number.isFinite(contentLength) && contentLength > config.webhookMaxBytes) {
    return HttpServerResponse.empty({ status: 413 });
  }

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

  if (new TextEncoder().encode(body).byteLength > config.webhookMaxBytes) {
    return HttpServerResponse.empty({ status: 413 });
  }

  const event = request.headers[EVENT_HEADER];
  const deliveryId = request.headers[DELIVERY_HEADER];
  if (event === undefined || deliveryId === undefined) {
    return HttpServerResponse.empty({ status: 400 });
  }

  // ! `Effect.try`, not a bare `JSON.parse`. A throw inside `Effect.gen` is a
  // ! defect, which `catchAll` below does not see — the route would answer 500
  // ! on a body that is merely malformed.
  const json = yield* Effect.try(() => JSON.parse(body) as unknown);
  const payload = yield* decodePayload(json);
  void payload;
  const queue = yield* WorkQueue;
  yield* queue.receiveDelivery({ id: deliveryId, event, body });

  return HttpServerResponse.empty({ status: 202 });
}).pipe(
  Effect.catchAll((cause) =>
    Effect.logError('Rejected delivery', cause).pipe(
      Effect.as(HttpServerResponse.empty({ status: cause._tag === 'QueueError' ? 503 : 400 })),
    ),
  ),
);

export const router = HttpRouter.empty.pipe(
  HttpRouter.get('/health', HttpServerResponse.text('ok')),
  HttpRouter.post(WEBHOOK_PATH, webhook),
);

export const Server = HttpServer.serve(router, HttpMiddleware.logger).pipe(
  HttpServer.withLogAddress,
);

/** Self-contained server layer for route tests and embedding without a worker. */
export const ServerLive = Server.pipe(
  Layer.provide(GitHubClient.Default),
  Layer.provide(WorkQueue.Default),
  Layer.provide(Policy.Default),
  Layer.provide(LictorConfig.Default),
);
