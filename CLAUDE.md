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
- **The depth budget counts work the daemon is behind on, so a job parked on its
  question and a job held for an operator approval are both outside it.** A row
  the claim skips still deferred the sweep while it counted — the parked row's
  own answer arrives through that sweep, and enough holds stop it for the whole
  queue. Both counts read one `COUNTED_JOBS_WHERE`, which shares
  `UNHELD_JOBS_WHERE` with `claimFor` so the two cannot drift; `backlog` is the
  wider by the deliveries term and must stay so, or `enqueue` refuses what the
  sweep already marked read. Neither population is bounded by a number any
  more — parked rows by `limits.answerExpiryHours`, held rows by
  `limits.approvalExpiryHours` against the rate that arms them, a rate against a
  window rather than a depth
- **A takeover kills the previous owner's recorded agent groups before it
  requeues their jobs, never after.** A `bun --watch` reload keeps the pid, wipes
  the heap milliseconds after SIGTERM, and so runs neither `ProcessRunner`'s
  release nor the ownership one — the detached agent survives, and `agent_processes`
  is the only thing that can still name it. Requeueing first is not a smaller
  version of this: it hands the work to a second agent sooner, while the first is
  most likely still alive. The record is signalled as a process *group*, so a
  recycled pid that leads no group of its own is `ESRCH` rather than a stranger
- **The daemon publishes no prose. It reacts.** The eyes acknowledgement and the
  terminal reaction that resolves it both go through `GitHubClient`, not
  `CapabilityBroker` — the broker refuses anything that is not a `running` job
  with a live lease, and a job is `pending` at one end and terminal at the other.
  The acknowledgement is best-effort; the resolution is durable
- **A terminal outcome owes its thread one `outbox` row, inserted in the same
  transaction that records the outcome.** The row stores raw fields and the
  reaction is chosen at send time, so nothing in that transaction can throw,
  roll the outcome back, lapse the lease, and rerun an agent whose side effects
  already landed. `fail` inserts nothing while it is scheduling a retry
- **The sender clears a stale reaction by posting it, never by searching for
  it.** A reaction POST is idempotent per user, content and target, so a repeat
  answers 200 with the reaction already there — which is how an id nothing
  recorded is learned. The listing is everyone's reactions, so finding this
  account in it is unbounded work on a popular target: a bounded scan either
  dead-letters the outcome or leaves a contradictory reaction standing, and
  re-reading the same prefix on retry converges to neither. Add before remove: a
  crash between them shows two reactions, and the other order shows none. Only
  the daemon's own vocabulary is ever posted, so a reaction the operator left by
  hand is never a candidate — and a terminal content is probed only past the
  first attempt, or an ordinary delivery would post a wrong outcome to look for
  one
- **`needs_input` publishes nothing and parks only where the agent already
  spoke.** `park` takes `askedAt` from the agent's own `create_comment` audit
  row, and a job that returned `needs_input` without posting one is terminal
  instead. A parked job consumes the next trusted reply on its thread as the
  answer — arming that for a question nobody was shown would swallow unrelated
  work
- **No string a terminal write holds reaches GitHub.** The agent's `summary`,
  an `ExecutorError` message, a `WorkspaceError` message, a policy refusal code:
  all of them stay in the row and the log, where `job.show` reads them
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
  real api.github.com. The bypassed stub is still constructed, so a log line in
  its constructor is not evidence it is in use; only what it received is.
  `test/service-wiring.test.ts` fails on any bare `.Default` of a service that
  declares `dependencies:`
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
