<h1 align="center">Lictor</h1>

<p align="center">
A lictor for your agent: watches GitHub, hands work.
</p>

<p align="center">
  <a href="https://github.com/edloidas/lictor/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/edloidas/lictor/ci.yml?branch=master&label=CI" alt="CI status"></a>
  <a href="https://github.com/edloidas/lictor/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
  <a href="https://bun.sh/"><img src="https://img.shields.io/badge/bun-%E2%89%A5%201.3.14-fbf0df" alt="Bun >= 1.3.14"></a>
</p>

Lictor listens for GitHub App webhooks, verifies each delivery, and queues
trusted interactions for a local coding agent. It is a local process — you run
it on your own machine and expose only the webhook through a tunnel.

```
GitHub ──delivery──▶ tunnel ──▶ POST /webhooks/github
                                    │
                          verify HMAC over raw body
                                    │
                     qualify + enqueue ─┴─ 202 Accepted
                                    │
                       SQLite ──▶ Codex worker
```

## Contents

- [Requirements](#requirements) · [Setup](#setup) — [GitHub App](#create-a-github-app) · [Environment](#environment) · [Tunnel](#expose-it-to-github)
- [Running](#running) · [Endpoints](#endpoints)
- [Writing a handler](#writing-a-handler) · [Calling the GitHub API](#calling-the-github-api)
- [Design notes](#design-notes) — [Why 202](#why-202-and-not-200) · [Raw body](#why-the-raw-body)
- [Development](#development) · [Contributing](#contributing) · [License](#license)

## Requirements

[Bun](https://bun.sh) ≥ 1.3.14 (`.bun-version` pins the exact version) and a
tunnel — [cloudflared](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/)
is what `bun tunnel` expects, but ngrok or [smee](https://smee.io) work equally
well.

## Setup

### Create a GitHub App

[Settings → Developer settings → GitHub Apps → New GitHub App](https://github.com/settings/apps/new).

1. **Webhook URL** — leave a placeholder for now; you will paste the tunnel URL
   once it is running.
2. **Webhook secret** — generate one and keep it:

   ```bash
   openssl rand -hex 32
   ```

3. **Permissions** — grant only what your handlers need. Reading issues and
   pull requests is enough to start.
4. **Subscribe to events** — pick the events you intend to handle. Anything you
   subscribe to but do not register a handler for is logged and dropped.
5. Create the App, then **generate a private key** and download the `.pem`.
6. **Install the App** on the repositories it should watch.

### Environment

Copy `.env.example` to `.env` and fill it in. Bun loads `.env` on its own — no
dotenv package involved, and `.env` is gitignored.

| Variable                | Required | Description                                                   |
| ----------------------- | -------- | ------------------------------------------------------------- |
| `GITHUB_APP_ID`         | yes      | Numeric App ID from the App settings page                     |
| `GITHUB_WEBHOOK_SECRET` | yes      | The secret you set on the App                                 |
| `GITHUB_PRIVATE_KEY`    | yes      | Contents of the downloaded `.pem`                             |
| `GITHUB_TRUSTED_SENDERS` | yes     | Comma-separated users whose activity may create work          |
| `GITHUB_TARGET_USERS`   | yes      | Comma-separated users whose interactions count                |
| `PORT`                  | no       | Port to bind, default `3000`                                  |
| `LICTOR_DATABASE_PATH`  | no       | SQLite queue path, default `.lictor/lictor.sqlite`             |
| `LICTOR_EXECUTOR`       | no       | `codex` (default) or `disabled`                                |
| `LICTOR_CODEX_MODEL`    | no       | Codex model, default `gpt-5.6-luna`                            |
| `LICTOR_AGENT_WORKDIR`  | no       | Workspace Codex may modify, default current directory          |

> [!TIP]
> A PEM pasted into a single-line `.env` value must have its newlines escaped as
> `\n`, or the whole value wrapped in double quotes. Lictor un-escapes `\n`
> before handing the key to `node:crypto`, which rejects escaped newlines with
> an error that names neither the cause nor the fix.

### Expose it to GitHub

GitHub cannot reach `localhost`. Start a tunnel and point the App's webhook URL
at it:

```bash
bun tunnel   # cloudflared tunnel --url http://localhost:3000
```

Set the App's **Webhook URL** to `<tunnel-url>/webhooks/github`. GitHub sends a
`ping` immediately; the bundled handler logs `Webhook reachable`, which confirms
the tunnel, the secret, and the route all line up.

## Running

```bash
bun dev         # watch mode — restarts on save
bun run start   # one-shot
```

Install and authenticate the Codex CLI before enabling execution. Set
`LICTOR_EXECUTOR=disabled` to verify webhook ingestion and retain queued work
without claiming it. Re-enable the executor and restart Lictor to process the
pending queue.

Lictor logs every queue transition with its job and attempt number. On startup,
`Work queue ready` includes counts for pending, running, retry, interrupted,
completed, and failed work. The SQLite file contains normalized metadata and
agent output; protect and back it up like any other local application state.

## Qualified interactions

Lictor creates work only when the webhook sender appears in
`GITHUB_TRUSTED_SENDERS` and a configured target is:

- assigned to an issue or pull request;
- requested to review a pull request; or
- newly mentioned in an issue, pull request, review, or comment.

Edited bodies trigger work only for mentions introduced by that edit. Matching
is case-insensitive and exact, so `@adiutriel-bot` does not match `adiutriel`.
Raw bodies and comments are neither stored nor passed to Codex; the worker sends
bounded subject metadata and URLs, then lets Codex retrieve the current GitHub
context with the credentials available in its environment.

GitHub delivery IDs and a stable interaction identity make redelivery
idempotent. If local ingestion fails, redeliver the event from the GitHub App's
Advanced page. API polling and PAT-backed reconciliation are intentionally not
part of this release.

## Endpoints

| Method | Path               | Response                                                     |
| ------ | ------------------ | ------------------------------------------------------------ |
| `GET`  | `/health`          | `200 ok`                                                     |
| `POST` | `/webhooks/github` | `202` accepted · `401` bad or missing signature · `400` malformed |

Rejections are deliberately uninformative: a caller that fails verification
learns only that it failed, never which check rejected it.

## Writing a handler

A handler takes a `Delivery` and returns an `Effect` that cannot fail — it runs
detached from the request, so there is nothing left to report an error to.
Recover inside the handler and log what you swallowed.

```typescript
// src/handlers/issue-opened.ts
import { Effect } from 'effect';
import type { Handler } from '../webhook/router.ts';

export const handleIssueOpened: Handler = (delivery) =>
  Effect.logInfo('Issue opened').pipe(
    Effect.annotateLogs({
      repository: delivery.payload.repository?.full_name ?? '(none)',
    }),
  );
```

Register it in `src/handlers/index.ts`:

```typescript
export const registry: Registry = {
  ping: handlePing,
  'issues.opened': handleIssueOpened,
};
```

Keys are the `X-GitHub-Event` name, optionally narrowed with the payload action.
`issues.opened` wins over `issues` for the same delivery, and `issues` catches
every action you did not name.

The shared envelope — `action`, `installation`, `repository`, `sender` — is
already decoded. For anything event-specific, decode the payload again against
your own `Schema`.

## Calling the GitHub API

`GitHubClient.forInstallation` hands back an `HttpClient` already carrying an
installation token, the API base URL, and the version pin:

```typescript
import { Effect } from 'effect';
import { GitHubClient } from '../github/client.ts';
import type { Handler } from '../webhook/router.ts';

export const handleIssueOpened: Handler = (delivery) =>
  Effect.gen(function* () {
    const installation = delivery.payload.installation?.id;
    if (installation === undefined) return;

    const github = yield* GitHubClient;
    const client = yield* github.forInstallation(installation);

    yield* client.get(`/repos/${delivery.payload.repository?.full_name}/issues`);
  }).pipe(Effect.catchAll((error) => Effect.logError('Handler failed', error)));
```

Tokens are cached per installation and renewed five minutes before they expire,
so a busy repository does not spend a round trip per delivery.

## Design notes

### Why 202, and not 200

GitHub marks a delivery failed if the endpoint takes longer than 10 seconds, and
agent work is not bounded by that. The route verifies, decodes, forks the
ingestion handler with `Effect.forkDaemon`, and acks — so the ack means
*accepted*, not finished. Qualified work is persisted in SQLite before Codex
claims it. A crash between the acknowledgement and that insert is recovered by
manually replaying GitHub's retained delivery.

### Why the raw body

The HMAC covers the exact bytes GitHub sent. Parsing the JSON and
re-serializing it changes key order and whitespace, which changes the digest —
so verification reads `request.text` and always runs before parsing. A body that
is not even JSON is rejected as `401`, not `400`: an unsigned caller learns
nothing about how far it got.

## Development

```bash
bun install     # also installs the pre-commit hook
bun check:fix   # typecheck + biome check --write (lint, format, import sort)
bun test        # full suite — no network, every suite stubs GitHub
bun validate    # full gate: check + coverage
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for conventions and
[CLAUDE.md](CLAUDE.md) for the constraints that are not lintable.

## Contributing

Bug reports and pull requests are welcome — open an
[issue](https://github.com/edloidas/lictor/issues) first for anything larger
than a fix.

## License

[MIT](LICENSE) © Mikita Taukachou
