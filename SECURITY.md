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

Lictor exposes no inbound surface beyond `GET /health`, and all GitHub input is
pulled rather than pushed. In scope: anything that turns untrusted GitHub prose
into agent work without passing the trusted-sender check, any misattribution of
the triggering author, and any path that leaks `LICTOR_GITHUB_TOKEN` into a log
line, an error trace, or a response body.

Out of scope: denial of service through notification volume, since lictor is a
local process polling on an interval and is not expected to absorb load.

## Operational notes

- `.env` is gitignored. A token pushed to a fork is a token that must be
  revoked, not merely deleted.
- `LICTOR_GITHUB_TOKEN` is the only secret, and it is the whole boundary: it
  grants write access to every repository the account can reach and is also what
  marks her notifications read. Rotating it is the only revocation available.
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
- Keep `GITHUB_TRUSTED_SENDERS` narrow. A mention decides relevance, but the
  sender list decides whose GitHub prose may trigger local agent work. The
  *newest trusted* mention in an activity window is the trigger: an untrusted
  mention is ignored rather than allowed to suppress a trusted one, so no
  repository participant can mute her by mentioning her.
- Attribution is only as good as what REST reports. A comment, a review, and an
  issue body all carry an author, but none of them carries who last *edited* it,
  and a repository maintainer can rewrite someone else's comment. Every candidate
  is therefore keyed on when it was **created**, never when it was updated — that
  is what keeps the recorded sender authoritative. The cost is that a mention
  added by editing existing text is not acted on; a new comment is. GitHub's
  GraphQL schema exposes `editor` and `lastEditedAt`, which is what would let the
  edited case be attributed correctly.
- Quoted and displayed mentions are stripped before matching. GitHub's reply
  button quotes what it replies to and GitHub notifies on a mention inside a
  blockquote, so without this a reader agreeing with a request would re-issue it
  under their own name. The stripper is not a Markdown parser: a lazy blockquote
  continuation is not recognised.
- Protect `~/.lictor/lictor.sqlite`: it stores issue metadata, execution errors,
  and bounded agent output. It stores the notification thread envelope, which
  carries a subject title and urls, but never comment bodies.
- `LICTOR_EXECUTOR=disabled` is the safe mode for validating a new token or
  allowlist. Work remains pending until execution is re-enabled — but polling
  still marks threads read, so her inbox is consumed either way.
