import { Schema } from 'effect';

/**
 * The fields every webhook payload shares. GitHub sends far more per event
 * type; `Schema.Struct` drops what it does not name, so a handler that needs
 * more decodes the payload again against its own schema.
 *
 * Everything is optional on purpose — `ping` carries no `action`, and
 * repository-less events (`installation`, `organization`) carry no `repository`.
 *
 * ! `nullable` matters as much as optional: GitHub sends an explicit
 * ! `"repository": null` on organization-scoped deliveries rather than omitting
 * ! the key, and a receiver that rejects those drops real events.
 */
export const WebhookPayload = Schema.Struct({
  action: Schema.optionalWith(Schema.String, { nullable: true }),
  installation: Schema.optionalWith(Schema.Struct({ id: Schema.Number }), { nullable: true }),
  repository: Schema.optionalWith(
    Schema.Struct({
      name: Schema.String,
      full_name: Schema.String,
    }),
    { nullable: true },
  ),
  sender: Schema.optionalWith(Schema.Struct({ login: Schema.String }), { nullable: true }),
});

export type WebhookPayload = Schema.Schema.Type<typeof WebhookPayload>;

export const decodePayload = Schema.decodeUnknown(WebhookPayload);

/** One verified webhook delivery: its headers, plus the decoded envelope. */
export type Delivery = {
  /** Value of `X-GitHub-Event` — the key handlers register under. */
  readonly event: string;
  /** Value of `X-GitHub-Delivery`, the UUID GitHub replays by. */
  readonly id: string;
  readonly payload: WebhookPayload;
};

/** `issues.opened`-style key, or the bare event name when there is no action. */
export const deliveryKey = (delivery: Delivery): string =>
  delivery.payload.action === undefined
    ? delivery.event
    : `${delivery.event}.${delivery.payload.action}`;
