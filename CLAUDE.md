# lictor

GitHub automation daemon. TypeScript, Bun, Effect. Polls the notifications API,
qualifies what arrives, and hands durable work to an agent. It authenticates as a
real account with a classic personal access token, not as an App.

## Rules

`AGENTS.md` → `CLAUDE.md` is a symlink — edit `CLAUDE.md`, never replace the
symlink with a real file.

## Commands

```bash
with-secrets bun dev         # Watch-mode server on PORT (default 3000)
with-secrets bun run start   # One-shot server
bun check:fix                # Typecheck + biome --write (lint, format, imports)
bun test                     # Run tests — no network, every suite stubs GitHub
bun validate                 # Full gate: check + test:ci (coverage)
```

Only the two that talk to GitHub need `with-secrets`, and only where `.env` holds
references — run them bare otherwise, see [Secrets](#secrets). The test suite
stubs GitHub, so it needs no credential.

## Secrets

What `.env` holds decides how it must be treated. Copying `.env.example` produces
literal values — that is what a fresh checkout gets. The operator's machines keep
1Password references there instead:

```
LICTOR_GITHUB_TOKEN=op://<vault>/<item>/<field>
```

Read the value before concluding anything. One beginning `op://` is a reference;
anything else is a live credential. Whether `with-secrets` is on PATH proves
nothing either way — it resolves whatever the file holds and never converts it,
so a machine can have the wrapper and a literal-value `.env` at the same time.

`with-secrets` resolves them for the life of one command, injecting the results
as real environment variables — which Bun gives precedence over `.env`. Nothing
resolved is written anywhere.

The wrapper is a convenience on the operator's machine, not a dependency of this
project, and it is named for intent rather than backend so the authentication
under it can differ per machine. Assume it may be absent: without it the commands
run bare, against whatever `.env` already holds.

Where the references are in place and `bun dev` is run bare, the failure does not
look like a configuration error. Bun loads `.env`, `Config.redacted` takes the
literal `op://...` string without validating its shape, and the startup identity
probe fails with `GitHub rejected the configured credential with status 401` —
indistinguishable from a revoked token.

- A reference discloses nothing, and must never be replaced with a literal value
- A literal value *is* the credential: never print it, never paste it, never let
  it leave the machine
- Under either, never print a resolved value, to a log or a terminal

## Constraints

- Runtime: Bun — never npm, yarn, or pnpm. `bunfig.toml` pins `minimumReleaseAge`
  to 3 days, so `bun add` of a freshly published version silently resolves to an
  older one
- Effect throughout: services are `Effect.Service` classes, config is
  `Effect.Config`, payloads decode through `Effect.Schema`. No ad-hoc `async`
  functions in `src/` — an escape hatch there loses the error channel
- Runs locally, and needs no inbound reachability. `GET /notifications` is the
  only transport: a repository webhook requires admin on the repository, so it is
  scoped to the operator's rights instead of the account's own reach. The HTTP
  server exposes `GET /health` and nothing else
- **A notification thread is marked read only after its row is committed.** That
  is what the webhook 202 used to be. Crash before the mark and GitHub still
  holds the item; mark first and it is gone for good
- **The poller stores, the delivery worker qualifies.** A notification names a
  thread, never the sender or the body, so qualification has to fetch — and doing
  that inside the poll loop puts GitHub failures outside the durable retry budget.
  Enrichment failures are `NotificationError`, never `ParseError`, because
  `isTerminalFailure` treats the latter as permanent
- **A notification's `reason` is an exclusion list, never an allow list.** It
  describes the thread, not the activity that just landed on it, and GitHub does
  not re-key an already-unread thread — so a thread that went unread as `assign`
  and then received a mention still reports `assign`
- **Never mark read past the queue-depth limit.** GitHub is the overflow buffer;
  the limit is checked before the sweep, not in `enqueue`, which runs a stage
  later when the thread is already gone
- The eyes reaction is strictly best-effort and goes through `GitHubClient`, not
  `CapabilityBroker`. The broker refuses anything that is not a `running` job with
  a live lease, and a just-enqueued job is `pending`
- A throw inside `Effect.gen` is a defect, not a failure: `catchAll` never sees
  it, so the recovery branches in the delivery worker are all bypassed and the
  loop dies. Wrap anything that throws — `JSON.parse` above all — in `Effect.try`
- Secrets are `Config.redacted` and stay `Redacted` until the moment they are
  used, so a logged service or error trace cannot leak them
- `SOUL.md` beside the database is the operator's persona: prepended to every
  agent prompt as trusted prose, re-read per job, bounded at 32 KiB, and the only
  place operator text enters the prompt unescaped. A missing file is a supported
  configuration; a dangling symlink or an oversized file is logged, never fatal
- **In tests, provide `Service.DefaultWithoutDependencies`, not
  `Service.Default`.** `Default` bakes in `FetchHttpClient.layer`, which wins over
  any client provided from outside — a suite using `Default` silently calls the
  real api.github.com
- One decoder per `DeliverySource`, in `src/delivery-worker.ts`. Adding a producer
  means adding a member and the map forces its decoder into existence — nothing
  downstream assumes an envelope

## Comments

Default to none. A plain `//` comment only for a non-obvious invariant — why,
not what. `// !` is for critical things alone: data loss, security, silent
corruption — not every sharp edge.

## Ad-hoc scripts

For one-off checks: create the file with the `Write` tool, run `bun run <file>`,
delete it with `rm`. Never shell heredocs — braces, quotes, or `$` inside one
trip Claude Code's expansion-obfuscation guard and force an approval prompt.
Prefer promoting recurring checks to a real `*.test.ts`.

## Git & GitHub

Conventional commit style; PRs squash to one commit before merge, unless the PR
combines work from several tasks.

- **Commit**: `<type>: <description> #<issue>`, e.g. `feat: add issue handler #5`.
  Without an issue, drop the number. Body optional: past tense, one line per
  change, backticks for code refs.
- **Issue**: title `<type>: <description>`; `epic:` for issues that aggregate
  sub-issues, never used in commits. Body explains what and why and ends with a
  `Rationale` section, `####` headers for short issues and `###` at 3+. Assign
  to the current user unless told otherwise.
- **PR**: title matches the commit. Body factual, no emojis, sections separated
  by one blank line, `Closes #1, closes #23` last on its own line — GitHub
  applies the keyword only to the reference it precedes.
- Never append a generated-by footer, `---` rule, session link, `<sub>`
  attribution, or promotional line — including PRs opened from the web, where
  this file is the only source of truth.

### Issue Labels

One **main** label + 0–2 **supportive**.

- **Main** (exactly one): `bug`, `feature`, `improvement`, `epic`
- **Supportive**: `DX`, `AI`, `testing`, `performance`, `documentation`,
  `refactoring`, `critical`, `R&D`, `external`, `wontfix`, `duplicate`
