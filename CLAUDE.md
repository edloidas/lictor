# lictor

GitHub automation daemon. TypeScript, Bun, Effect. Polls the notifications API,
qualifies what arrives, and hands durable work to an agent. It authenticates as a
real account with a classic personal access token, not as an App.

## Rules

`AGENTS.md` → `CLAUDE.md` is a symlink — edit `CLAUDE.md`, never replace the
symlink with a real file.

## Commands

```bash
bun dev         # Watch-mode server on PORT (default 3000)
bun run start   # One-shot server
bun check:fix   # Typecheck + biome check --write (lint + format + import sort)
bun test        # Run tests — no network, every suite stubs GitHub
bun validate    # Full gate: check + test:ci (coverage)
```

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

Conventional Commits: `<type>: <description> #<issue>`

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`,
`build`, `ci`

- Imperative mood, under 72 chars, no period
- Include issue number when related: `feat: add issue handler #5`
- No promotional or generated-by lines
- Optional body: past tense, one line per change, backticks for code refs
- PRs should contain a single commit on merge; squash locally and force-push
  before merging unless the PR combines work from several tasks

### Issue Labels

Each issue gets one **main** label + 0–2 **supportive** labels.

- **Main** (exactly one): `bug`, `feature`, `improvement`, `epic`
- **Supportive** (optional): `DX`, `AI`, `testing`, `performance`,
  `documentation`, `refactoring`, `critical`, `R&D`, `external`, `wontfix`,
  `duplicate`

### Issues

- **Title**: `<type>: <description>`; `epic: <description>` for issues that
  aggregate sub-issues (never used in commits)
- Always assign the issue to the current user unless told otherwise
- **Body**: concisely explain what and why, end with a `Rationale` section;
  headers `####` for short issues (1–2 headers), `###` at 3+

### Pull Requests

- **Title**: `<type>: <description> #<number>`
- **Body**: concise, no emojis, separate all sections with one blank line
- Multiple issues go on one `Closes` line: `Closes #1 #23 #456`
- Never append a generated footer, `---` rule, session link, `<sub>`
  attribution, or promotional line. Applies to PRs created from the web too,
  where these instructions are the only source of truth.

  ```
  <summary of changes>

  Closes #<issue1> #<issue2>
  ```
