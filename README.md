<h1 align="center">Lictor</h1>

<p align="center">
A local GitHub automation daemon that gives Codex durable, policy-scoped work.
</p>

<p align="center">
  <a href="https://github.com/edloidas/lictor/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/edloidas/lictor/ci.yml?branch=master&label=CI" alt="CI status"></a>
  <a href="https://github.com/edloidas/lictor/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
  <a href="https://bun.sh/"><img src="https://img.shields.io/badge/bun-%E2%89%A5%201.3.14-fbf0df" alt="Bun >= 1.3.14"></a>
</p>

Lictor verifies signed GitHub webhooks, commits each delivery to SQLite before
acknowledging it, and runs qualifying work in an isolated repository worktree.
GitHub credentials stay in the daemon; agent GitHub operations pass through a
job-scoped, audited capability broker.

```text
GitHub -> tunnel -> webhook -> durable inbox -> policy -> queue
                                                       |
                                              isolated worktree
                                                       |
                                             Codex + broker tools
```

## Requirements

- Bun 1.3.14 or newer
- a classic `repo`-scope personal access token for the account the daemon acts as
- a webhook on each managed repository, delivering to the route below
- the Codex CLI, authenticated on the daemon machine
- a tunnel such as `cloudflared` for the public webhook route

Only `POST /webhooks/github` and the minimal `GET /health` liveness endpoint
are public. Readiness and management use an owner-only local Unix socket.

## Start safely

```bash
cp .env.example .env
mkdir -p ~/.lictor
cp policy.example.toml ~/.lictor/policy.toml
bun install
bun run start
```

Daemon state lives in `~/.lictor/` — database, control socket, policy, and the
agent's `CODEX_HOME` — deliberately outside any repository, since one of the
repositories lictor manages may be lictor itself. Override any of the three paths
with `LICTOR_DATABASE_PATH`, `LICTOR_POLICY_PATH`, or `LICTOR_SOCKET_PATH`; a
leading `~/` is expanded. One daemon per user owns that state: a second instance
sharing it loses the ownership lease and stops itself, so give a development
instance its own paths rather than pointing it at the same home.

If a `.lictor/` directory from an earlier version is still present in the working
directory, startup refuses rather than opening a fresh database beside it.

The example environment starts with `LICTOR_EXECUTOR=disabled`, empty trusted
principals, and no implicit GitHub mutation authority. This lets you verify token
authentication, webhook signatures, durable receipt, and policy loading before
Codex can claim work.

Create the token from the account's Developer settings, set it as
`LICTOR_GITHUB_TOKEN`, and put that account's login in `LICTOR_GITHUB_LOGIN` —
startup calls `GET /user` and refuses to run if the two disagree.

A classic token cannot be scoped to particular repositories: it reaches
everything the account reaches, which includes its own repositories and anything
granted through organization membership or a team. Use a dedicated account whose
entire access you control, not a personal one.

Add a webhook to each repository you intend to manage with the same secret, and
subscribe only to the issue, pull-request, review, and comment events you use.
Set its content type to `application/json` — the default urlencoded body passes
signature verification and then fails to parse. Point its payload URL at:

```text
https://<your-tunnel>/webhooks/github
```

Start the tunnel with `bun tunnel`. A GitHub `ping` confirms the route and
signature configuration.

## Activate repository policy

Edit `~/.lictor/policy.toml` before enabling execution. Absolute workspace roots
are mandatory; explicit repository paths must resolve beneath one of them.

```toml
[defaults]
execution = "approval"
clone = "denied"

[repositories]
allow = ["edloidas/*"]
deny = []
workspaceRoots = ["/srv/lictor/repositories"]

[repositories.overrides."edloidas/lictor"]
execution = "automatic"
clone = "allowed"
workspace = "/srv/lictor/repositories/edloidas/lictor"

[repositories.overrides."edloidas/lictor".capabilities]
read = true
comment = true
issues = true
branches = true
pullRequests = true
merge = false
```

When cloning is allowed, Lictor passes the token to Git as an `Authorization`
header injected per command, without storing it in the remote URL or credential
store.
Existing clones are accepted only when their canonical path stays inside an
allowed root and `origin` matches the delivery repository.

Run a policy check before activation:

```bash
bun cli policy.check edloidas/lictor
```

Authenticate the daemon-owned Codex home once. It lives beside the configured
SQLite database and is never taken from a managed repository:

```bash
CODEX_HOME=~/.lictor/codex codex login
```

Then set `GITHUB_TRUSTED_SENDERS`, `GITHUB_TARGET_USERS`, and
`LICTOR_EXECUTOR=codex` in `.env`, and restart the daemon.

## Operate the daemon

```bash
bun cli status
bun cli job.list
bun cli job.show 42
bun cli job.approve 42
bun cli job.retry 42
bun cli job.cancel 42
bun cli repository.list
bun cli repository.inspect edloidas/lictor
bun cli prune
bun cli backup /secure/backups/lictor-$(date +%F).sqlite
```

Add `--json` to any command for automation. The CLI communicates only through
the daemon socket; it never opens SQLite. Mutations are state-checked,
idempotent, and written to the audit log.

`status` reports queue counts, the daemon heartbeat, oldest active job,
executor mode, database size, and available disk. Jobs that exhaust their
attempt budget or contain corrupt durable data move to `dead_letter` instead of
blocking later work.

## Retention, backup, and recovery

Completed jobs are retained for 30 days and failed or dead-letter jobs for 90
days by default. Change these windows in the policy:

```toml
[retention]
completedDays = 30
failedDays = 90
```

`bun cli prune` applies the configured windows and checkpoints the WAL. The
`backup` command uses SQLite's online backup path after a full WAL checkpoint,
then creates the destination with mode `0600`.

To restore:

1. Stop Lictor and confirm no daemon owns the database.
2. Preserve the current database, `-wal`, and `-shm` files as one recovery set.
3. Copy the verified backup to `LICTOR_DATABASE_PATH` and set mode `0600`.
4. Start Lictor and run `bun cli status` before re-enabling execution.

Never restore by replacing only the live main database while the daemon is
running. SQLite state and its WAL must be treated as one consistency boundary.

## Failure guarantees and limits

- `202 Accepted` means the exact signed delivery is durably committed.
- Storage failure returns `503`. GitHub does not retry a failed delivery on its
  own, so recovery is a manual redelivery from the webhook's delivery log.
- Duplicate delivery and interaction identities are idempotent.
- One daemon owns the database; worker attempts use renewable fenced leases.
- Expired work is retried within its attempt budget, then dead-lettered.
- Queue depth, job age, per-repository attempts, and execution duration are
  policy limits, not prompt instructions.
- Codex receives an allowlisted environment without the access token, webhook
  secret, or ambient `gh` credentials. Every write goes through the broker.
- Logs use stable error codes and omit raw payloads, parse values, credentials,
  and agent stderr.

The public request body limit defaults to 1 MiB. Configure operational limits
in policy:

```toml
[limits]
maxQueueDepth = 10000
maxJobAgeMinutes = 1440

[limits.costs]
maxAttempts = 3
maxDurationMinutes = 30
```

Repository overrides may tighten `costs`. Global executor configuration remains
an upper bound, so a repository cannot expand machine-wide authority.

## Development

```bash
bun check:fix
bun test
bun validate
```

Tests run without network access and replace GitHub/process boundaries. The
validation gate enforces type checking, Biome, the complete test suite, and
80% line/function coverage thresholds.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution conventions and
[CLAUDE.md](CLAUDE.md) for architecture constraints.

## License

[MIT](LICENSE) © Mikita Taukachou
