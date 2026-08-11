# Contributing

Thanks for helping out. Bug reports and pull requests are welcome — open an
[issue](https://github.com/edloidas/lictor/issues) first for anything larger
than a fix; the bug and feature forms collect what a report needs.

## Toolchain

**[Bun](https://bun.sh) only.** Do not use npm, yarn, or pnpm in this
repository. Bun is the runtime, the test runner (`bun test`), the script runner,
and the package manager whose lockfile (`bun.lock`) is committed. Installing
with another manager writes a competing lockfile and produces a different
dependency tree than CI resolves.

```bash
bun install     # also installs the pre-commit hook
bun check:fix   # typecheck + biome check --write (lint, format, import sort)
bun test        # full suite, under a second
bun validate    # full gate: check + coverage
```

Run `bun check:fix` and `bun test` while you work; run `bun validate` before
opening a pull request.

`bunfig.toml` pins `minimumReleaseAge` to three days. Nothing is exempt: a
fast-moving toolchain package is exactly the kind of dependency a compromised
publish would target. A `bun add` of a version published in the last three days
silently resolves to an older one — that is the guard working, not a bug.

## Pre-commit hook

`bun install` runs the `prepare` script, which points `core.hooksPath` at
`.githooks/`. The hook runs [nano-staged](https://github.com/usmanyunusov/nano-staged),
which applies `biome check --write` to staged `.ts` files and re-stages the
result. Formatting and lint fixes land automatically; anything Biome cannot fix
blocks the commit.

## Code conventions

Match the surrounding code. Style, naming, and type conventions are enforced by
`biome check` rather than written down. What cannot be linted lives in
[CLAUDE.md](CLAUDE.md); the constraints worth repeating here:

- **Effect all the way down.** Services are `Effect.Service` classes, config is
  `Effect.Config`, payloads decode through `Effect.Schema`. A bare `async`
  function in `src/` drops the error channel on the floor.
- **A throw inside `Effect.gen` is a defect, not a failure.** `catchAll` never
  sees it, and the route answers 500 on something that was merely malformed.
  Wrap anything that throws — `JSON.parse` above all — in `Effect.try`.
- **Verify signatures against the raw body, before parsing.** `request.text`,
  never `request.json`.
- **Handlers cannot fail.** `Handler` returns `Effect<void, never, GitHubClient>`
  because it runs detached from the request. Recover inside the handler and log
  what you swallowed.
- **Secrets stay `Redacted`** until the moment they are used.

## Tests

Bun's test runner, suites in `test/`. Two rules:

- **No network.** Every suite that touches GitHub stubs `HttpClient`. The full
  suite runs offline and finishes in under a second.
- **Provide `Service.DefaultWithoutDependencies`, not `Service.Default`.**
  `Default` bakes in `FetchHttpClient.layer`, and a baked-in layer wins over one
  provided from outside — a suite using `Default` silently calls the real
  api.github.com and passes or fails on GitHub's mood.

`test/server.test.ts` runs the whole server on an ephemeral port via
`BunHttpServer.layerTest`, with config from a `ConfigProvider.fromMap` so the
suite never depends on a `.env` being present. New routes belong there.

## Commits and pull requests

[Conventional Commits](https://www.conventionalcommits.org/), with the issue
number appended:

```
<type>: <description> #<issue>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`,
`build`, `ci`. Imperative mood, under 72 characters, no trailing period. The
optional body is past tense, one line per change, backticks around code
references.

A pull request should be a single commit when it merges — squash locally and
force-push rather than merging a chain of fixups. The pull request template
prefills the expected body shape.
