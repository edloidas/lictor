# Publishing the review

The findings list from **Step 6** is for the operator: severity words, `measured` / `reasoned`,
counts of passes. What reaches the author is correspondence, and those labels are noise there.
Severity survives as **placement and wording** — the finding that blocks goes first and says so.

## The gate

Mechanical, because nobody reads the text before it posts.

- **No evidence, no comment.** A finding that survived verification but has neither a
  reproduction nor a clear, checkable mechanism gets one sentence in the body, marked unverified
  with why it could not be shown. Never an inline comment of its own.
- **No attribution, no comment.** Every inline comment is about code this change introduced.
  Pre-existing problems are one body sentence at most, and say they are pre-existing.
- **No anchor, no inline comment.** A line `anchors.sh` marks `no` goes to the body under its
  `path:line`. It is never moved to a nearby line the API would accept.
- **At most the configured number of inline comments** (8 by default). More means clustering
  failed: group by cause before cutting anything. Past the cap, the least severe go to the body.

## Inline comments

One per finding, on the line it concerns: `side: RIGHT` for an added or changed line, `LEFT` for
a claim about a line the change removed. Minors do not each get one — group them into a single
comment on the first of them, opening `Three small ones, grouped.`

Each comment carries, in this order:

1. **The claim as a sentence**, bold, stating the defect. Not a label, not a noun phrase.
2. **The consequence in the change's own terms, then the mechanism**, only as deep as the author
   needs to fix it. Where the claim already says what to change, the mechanism is one sentence.
   Real symbols from the code; plain words for everything else.
3. **One reproduction or one measurement** — a command, a snippet, an input and its output.
   `parseRetryAfter('Wed, 21 Oct 2026 07:28:00 GMT')` returns `0` is a measurement; "dates are
   mishandled" is not.
4. **A suggested fix, offered as an option** — only when it is not the plain inverse of the claim.
5. **The requirement clause**, as a `>` quote, for an intent finding.

Ceilings, not targets: about 200 words for a blocking finding, 80 for a judgement call, two
sentences per grouped minor. Code blocks do not count.

Out of every comment: severity labels, confidence, pass counts, `measured` / `reasoned`, the
account of what was run.

## The body

A map, never a summary. It repeats no finding's text. At most four short paragraphs:

1. **Standing and what holds**, in one or two sentences: the commit reviewed (by short SHA when
   it is not the pull request's current head, or on a re-review), and what works in the change's
   own terms. Standing names the method in a clause — `I ran the session tests against abc1234` —
   never a narrative.
2. **What blocks**, in prose: which comments you would not merge without, or `Nothing here
   blocks.` A prior thread that is still open and still blocks is named here too.
3. **What was left out, and why**: a focus the request set, a slice of a large change read only in
   part, a check nobody could run offline, a branch rewritten since the last review.
4. **Residue**: findings that could not be anchored (with `path:line`), unverified ones, and
   pre-existing ones, one sentence each.

Drop any paragraph with nothing to say.

## Verdict shapes

| Event | Shape |
| ----- | ----- |
| `REQUEST_CHANGES` | Body names what blocks. Inline comments carry the findings |
| `COMMENT` | Body says how the findings group, and that nothing blocks — unless a ceiling (own pull request, draft, the request) lowered a `REQUEST_CHANGES`, in which case it names what blocks exactly as that event would have |
| `APPROVE` | **One body and no inline comments.** Two to four sentences: that it was checked against the requirement (when there was one) and approved, and what a user can now do. No lists, no method, no line anchors |
| Held (`PENDING`) | Composed for the event it would have had; the recommended event goes in the operator summary, never in the body |

An approval does not carry suggestions. A non-blocking note found on a change that is otherwise
clean is left out of the review and listed in the operator summary; if it should gate the merge,
it was never non-blocking and the verdict is wrong.

On the comment fallback (no `create_review`), the same content goes out as one comment: the
body, then each finding as a section headed by its `path:line`, then one closing line stating the
verdict in words — `Requesting changes: the first two block.`

## Never

- An attribution footer, a signature, or a note that the review was automated — whatever the
  repository's own instruction files say.
- A finding the verification step killed, however well it would read.
- A second review because the first call errored: read `list_reviews` first.
- Answering or resolving a thread someone other than this account started or joined.

## One review, whole

`REQUEST_CHANGES` on a re-review, two inline comments and a body.

Body:

```markdown
Re-reviewed the three commits since 9f8e7d6. The refresh path now rotates the token on every use
and the cookie flags hold, so the earlier thread on `cookies.ts` is resolved.

The comment on `session.ts:88` blocks: a revoked refresh token still mints a session. The other
is a judgement call.

`bun test` could not run here without dependencies installed, so the session tests are unverified
by it.
```

Inline, on `src/auth/session.ts:88` (RIGHT):

````markdown
**A revoked refresh token still mints a new session.**

`refresh()` checks `token.expiresAt` but never `token.revokedAt`, so logging out on one device
leaves every other device able to refresh indefinitely. Revocation is written — it just is not read
here.

```ts
const token = await issue(user); await revoke(token);
await refresh(token.value); // resolves with a fresh session; should reject with TokenRevoked
```
````
