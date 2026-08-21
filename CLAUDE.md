# lictor

GitHub webhook server. TypeScript, Bun, Effect. Receives deliveries from a
GitHub webhook and dispatches them to handlers. It authenticates as a real
account with a classic personal access token, not as an App.

## Rules

`AGENTS.md` → `CLAUDE.md` is a symlink — edit `CLAUDE.md`, never replace the
symlink with a real file.

## Commands

```bash
bun dev         # Watch-mode server on PORT (default 3000)
bun run start   # One-shot server
bun tunnel      # cloudflared tunnel exposing localhost:3000 to GitHub
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
- Runs locally. GitHub cannot reach `localhost`, so a delivery only arrives
  through a tunnel whose URL is set as the repository webhook's payload URL
- **Signature verification reads the raw body, always before parsing.** The HMAC
  covers the exact bytes GitHub sent; re-serializing a parsed payload changes key
  order and whitespace, and every delivery then fails. `request.text`, never
  `request.json`
- **The webhook route acks with 202 and forks the handler with
  `Effect.forkDaemon`.** GitHub records a delivery as failed after 10 seconds, and
  handler work is not bounded by that. Deliveries are durable on GitHub's side and
  replayable by id, so acking early loses nothing
- Handlers are `Effect<void, never, GitHubClient>` — they cannot fail, because
  nothing is left to report a failure to once the response is sent. Recover
  inside the handler and log what you swallowed
- A throw inside `Effect.gen` is a defect, not a failure: `catchAll` never sees
  it and the route answers 500. Wrap anything that throws — `JSON.parse` above
  all — in `Effect.try`
- Secrets are `Config.redacted` and stay `Redacted` until the moment they are
  used, so a logged service or error trace cannot leak them
- **In tests, provide `Service.DefaultWithoutDependencies`, not
  `Service.Default`.** `Default` bakes in `FetchHttpClient.layer`, which wins over
  any client provided from outside — a suite using `Default` silently calls the
  real api.github.com
- New event handlers go in `src/handlers/` and are registered in
  `src/handlers/index.ts`. An event absent from that registry is logged and
  dropped, which is the normal case for a subscription you do not act on

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
