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
decoding into handler code. So is any path that leaks `GITHUB_PRIVATE_KEY`, an
installation token, or the webhook secret into a log line, an error trace, or a
response body.

Out of scope: denial of service through delivery volume, since lictor is a
local process behind a tunnel and neither is expected to absorb load.

## Operational notes

- `.env` and `*.pem` are gitignored. A private key committed to a fork is a
  key that must be regenerated from the App's settings, not merely deleted.
- Rotate `GITHUB_WEBHOOK_SECRET` and the App private key independently — the
  first only gates inbound deliveries, the second grants API access to every
  installation.
- Grant the App the narrowest permission set the handlers actually need.
  Installation tokens inherit the App's permissions in full.
