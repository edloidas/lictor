# Security Policy

## Supported versions

Lictor is pre-1.0 and unversioned in practice — only `master` is supported.

## Reporting a vulnerability

Please do not open a public issue for security problems. Instead, use one of
the private channels:

- **GitHub**: [report a vulnerability](https://github.com/edloidas/lictor/security/advisories/new)
  via private vulnerability reporting — preferred.
- **Email**: [edloidas@gmail.com](mailto:edloidas@gmail.com) with
  `[lictor security]` in the subject if you cannot use GitHub.

Include the request or payload that triggers the problem, the observed
behavior, and the impact you believe it has. You should receive an initial
response within a week.

## Scope

Lictor accepts unauthenticated requests from the public internet by design, so
anything that reaches a handler without a valid signature is in scope — a
bypass of the HMAC check, a timing oracle in it, or a payload that escapes
decoding into handler code. So is any path that leaks `LICTOR_GITHUB_TOKEN` or
the webhook secret into a log line, an error trace, or a response body.

Out of scope: denial of service through delivery volume, since lictor is a
local process behind a tunnel and neither is expected to absorb load.

## Operational notes

- `.env` is gitignored. A token pushed to a fork is a token that must be
  revoked, not merely deleted.
- Rotate `GITHUB_WEBHOOK_SECRET` and `LICTOR_GITHUB_TOKEN` independently — the
  first only gates inbound deliveries, the second grants write access to every
  repository the account can reach.
- A classic token cannot be scoped per repository. Its reach is the account's
  entire access graph — its own repositories plus every collaborator invitation,
  organization membership, and team grant — so that graph, not a token setting,
  is the permission boundary. Use a dedicated account and keep its access to the
  repositories lictor manages; a personal account's token grants the agent
  everything its owner can reach.
- The daemon's own login must never appear in `GITHUB_TRUSTED_SENDERS`. Startup
  refuses it, because trusting your own account is a self-trigger loop.
- Treat `LICTOR_AGENT_WORKDIR` as a security boundary. Codex receives
  workspace-write access there; do not point it at a home directory or a folder
  containing unrelated credentials.
- Keep `GITHUB_TRUSTED_SENDERS` narrow. Target matching decides relevance, but
  the sender list decides whose GitHub prose may trigger local agent work.
- Protect `.lictor/lictor.sqlite`: it stores issue metadata, execution errors,
  and bounded agent output. It does not store raw webhook bodies or comments.
- `LICTOR_EXECUTOR=disabled` is the safe mode for validating a new webhook or
  allowlist. Work remains pending until execution is re-enabled.
