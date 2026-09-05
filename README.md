<h1 align="center">Lictor</h1>

<p align="center">
A local GitHub automation daemon that gives Codex durable, policy-scoped work.
</p>

<p align="center">
  <a href="https://github.com/edloidas/lictor/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/edloidas/lictor/ci.yml?branch=master&label=CI" alt="CI status"></a>
  <a href="https://github.com/edloidas/lictor/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
  <a href="https://bun.sh/"><img src="https://img.shields.io/badge/bun-%E2%89%A5%201.3.14-fbf0df" alt="Bun >= 1.3.14"></a>
</p>

Lictor polls the GitHub notifications API, commits each thread to SQLite before
marking it read, acknowledges accepted work with an eyes reaction, and runs it in
a disposable clone of the repository. GitHub credentials stay in the daemon; agent
GitHub operations pass through a job-scoped, audited capability broker.

```text
GitHub notifications <- poll -- durable inbox -> qualify -> policy -> queue
                                      |                                 |
                              mark read after commit         disposable session
                                                                        |
                                                              Codex + broker tools
```

## Requirements

- Bun 1.3.14 or newer
- a classic `repo`-scope personal access token for the account the daemon acts as
- the Codex CLI, authenticated on the daemon machine

Nothing needs to reach the machine from outside. Only the minimal `GET /health`
liveness endpoint is public; readiness and management use an owner-only local
Unix socket.

The agent never shares that socket. Each job attempt gets its own short-lived
socket under `~/.lictor/agent/`, serving the agent's capability calls and nothing
else — no operator command is reachable from it, and it is removed when the
attempt ends. The job it speaks for is fixed by the daemon when the socket is
opened, so the agent has no way to name a different one.

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

### Persona

An optional `SOUL.md` in the same directory as the database (`~/.lictor/SOUL.md`
by default) is prepended to every agent prompt as your standing instructions. It
is re-read on every job, so edits are live without a restart — a symlink into a
repository you version separately is fine and preferred. `LICTOR_DATABASE_PATH`
moves it along with the rest of the state. Startup logs whether a persona was
found. A dangling symlink is reported as a warning and the agent runs on the
bare prompt rather than failing the job; a file over 32 KiB is truncated to its
first 32 KiB, still used, and reported as a warning.

The example environment starts with `LICTOR_EXECUTOR=disabled`, empty trusted
principals, and no implicit GitHub mutation authority. This lets you verify token
authentication, polling, durable receipt, and policy loading before Codex can
claim work.

Create the token from the account's Developer settings, set it as
`LICTOR_GITHUB_TOKEN`, and put that account's login in `LICTOR_GITHUB_LOGIN` —
startup calls `GET /user` and refuses to run if the two disagree. Startup also
checks the token's scopes and refuses a classic token whose `x-oauth-scopes`
lacks `repo`, naming what GitHub reported, so a mis-scoped credential fails
loudly before it can clone or comment with half its reach missing.

A classic token cannot be scoped to particular repositories: it reaches
everything the account reaches, which includes its own repositories and anything
granted through organization membership or a team. Use a dedicated account whose
entire access you control, not a personal one.

Nothing has to be configured per repository to make her *notice* — mentioning her
notifies her account, and notifications reach exactly as far as her membership
graph does. A repository webhook would instead need admin on that repository, so
it would be scoped to whatever the operator can administer rather than to what she
can reach.

Watch the log on the first mention. `Committed notifications` confirms polling and
credential scope; `Queued GitHub interaction` confirms qualification and policy.
Once execution is enabled, every claimed job runs to exactly one outcome line, and
`durationMs` on that line measures from the claim. `Claimed queued work` opens it.
A job the repository policy refuses ends there and then, at `Dropped queued work
denied by policy`. Otherwise `Starting agent process` reports the timeout budget
the child is given — the policy allowance capped by `LICTOR_EXECUTOR_TIMEOUT_MS`,
so neither value alone — and the outcome is one of `Completed queued work`,
`Queued work did not complete`, `Queued work will retry`, or `Queued work
failed`. No line carries Codex's stdout or stderr: what the agent reported is in
the database.

## Activate repository policy

Edit `~/.lictor/policy.toml` before enabling execution. Policy names
repositories, never filesystem paths: `allow` takes full `owner/name` entries and
rejects owner wildcards, so a repository is armed only when it is listed here.
Being invited to one grants reach, not permission.

```toml
[defaults]
execution = "approval"
clone = "denied"

[repositories]
allow = ["edloidas/lictor"]
deny = []

[repositories.overrides."edloidas/lictor"]
execution = "automatic"
clone = "allowed"

[repositories.overrides."edloidas/lictor".capabilities]
read = true
comment = true
issues = true
branches = true
pullRequests = true
merge = true
```

Repositories in `allow` are the ones you control: they earn the environment
sender list and the full capability set. Every other repository she can see is
accepted at a capped third-party tier — `read` and `comment` only, execution
under approval, cloning denied — unless an explicit override says otherwise, so
a stranger's prompt injection is bounded to "posts a comment after a human
approved it." A deny pattern still subtracts everywhere.

Sender trust is per repository: `[defaults].senders` replaces the environment
list everywhere, and `[repositories.overrides."repo"].senders` replaces it for
one repository. The environment list applies only to owned repositories.

Read and comment let her investigate and reply; `branches` and `pullRequests`
let her ship code changes — branch, commit, open a pull request, continue on a
branch she already started for that subject. Assignment and review-request
notifications are acted on when the assigner or requester is a trusted sender;
grant `issues` for closing, labelling, and editing, and `merge` where the
repository's policy allows it. `forcePush` and `deleteBranches` stay denied
unless a specific repository asks for them.

A trusted trigger opens its thread's live window (`[limits].livenessHours`,
default 24): while it is open — and the subject remains open — replies from any
participant continue the work as *continuation turns*, which never reach
`merge`, `forcePush`, or `deleteBranches` even where policy grants them.
Commits always carry the daemon account's identity: agent-supplied
`author`/`committer` fields are stripped, and every git subprocess invocation
is audited beside the broker calls, so the credential has no unaudited use.
Tool discovery is scoped the same way: a capability denied by policy is
invisible to the agent, not merely refused at call time.

When cloning is allowed, Lictor passes the token to Git as an `Authorization`
header injected per command, without storing it in the remote URL or credential
store. Every job gets its own disposable session: one full clone under
`sessions/` beside the configured SQLite database, on a path keyed by job id that
the daemon computes rather than one policy names. The session is deleted when the
job finishes; if it failed, the tree is kept under a `.failed-` name for
forensics instead, capped by count so a repeatedly failing repository cannot fill
the disk. Sessions left behind by a crash are swept by job id at startup and
hourly thereafter. Network git operations (`clone`, `fetch`) are bounded by
`LICTOR_GIT_TIMEOUT_MS`.

Upgrading from an earlier version: clones used to live under `workspaces/`
beside the configured SQLite database. That tree is no longer used and can be
removed by hand (`rm -rf <directory-of-LICTOR_DATABASE_PATH>/workspaces`).

Run a policy check before activation:

```bash
bun cli policy.check edloidas/lictor
```

Authenticate the daemon-owned Codex home once. It lives beside the configured
SQLite database and is never taken from a managed repository:

```bash
CODEX_HOME=~/.lictor/codex codex login
```

Then set `GITHUB_TRUSTED_SENDERS` and `LICTOR_EXECUTOR=codex` in `.env`, and
restart the daemon. There is no separate list of who may be mentioned: the account
the token belongs to is the only target, confirmed by `GET /user` at startup.

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

`bun cli prune` applies the configured windows and checkpoints the WAL; it does
not touch sessions, which are managed by the daemon itself — swept when their
job is gone, with failed sessions retained up to a count cap for forensics. The
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

- A notification thread is marked read only after its row is durably committed,
  so a crash mid-sweep leaves the item with GitHub rather than losing it.
- Storage failure leaves the thread unread and the sweep retries it. So does a
  full queue: the depth limit is checked before anything is marked read, which
  makes GitHub the overflow buffer.
- Duplicate delivery and interaction identities are idempotent. A replayed
  thread produces no second job and no second reaction.
- An eyes reaction on the triggering comment is best-effort acknowledgement, not
  a receipt: the job is committed whether or not the reaction lands.
- One daemon owns the database; worker attempts use renewable fenced leases.
- Expired work is retried within its attempt budget, then dead-lettered.
- Queue depth, job age, per-repository attempts, and execution duration are
  policy limits, not prompt instructions.
- Codex receives an allowlisted environment without the access token or ambient
  `gh` credentials. Every write goes through the broker.
- Logs use stable error codes and omit raw payloads, parse values, credentials,
  and agent stderr.

The stored delivery body limit defaults to 1 MiB. Configure operational limits
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
