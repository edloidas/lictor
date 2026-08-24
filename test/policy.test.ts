import { describe, expect, test } from 'bun:test';
import { Effect } from 'effect';
import { PolicyError, parsePolicy } from '../src/policy.ts';

const parse = (source: string, senders: readonly string[] = []) =>
  Effect.runPromise(parsePolicy(source, senders));

describe('repository automation policy', () => {
  test('uses safe capability and retention defaults', async () => {
    const policy = await parse('');
    const repository = policy.forRepository('Edloidas/Lictor');

    // An unlisted repository is the third-party tier: accepted, but capped at
    // read and comment under approval execution, so joining one (#29) arms
    // nothing a human does not approve.
    expect(repository).toEqual({
      repository: 'edloidas/lictor',
      accepted: true,
      execution: 'approval',
      clone: 'denied',
      capabilities: {
        read: true,
        comment: true,
        issues: false,
        branches: false,
        pullRequests: false,
        merge: false,
        forcePush: false,
        deleteBranches: false,
        scripts: [],
      },
      trustedSenders: [],
      maxAttempts: 3,
      maxDurationMs: 30 * 60 * 1000,
    });
    expect(policy.completedRetentionDays).toBe(30);
    expect(policy.failedRetentionDays).toBe(90);
    expect(policy.maxQueueDepth).toBe(10_000);
    expect(policy.maxJobAgeMs).toBe(24 * 60 * 60 * 1000);
    expect(policy.livenessMs).toBe(24 * 60 * 60 * 1000);
  });

  test('reserves the environment sender list for owned repositories', async () => {
    const policy = await parse(
      `
[repositories]
allow = ["edloidas/lictor"]
`,
      ['operator'],
    );

    expect(policy.forRepository('edloidas/lictor').trustedSenders).toEqual(['operator']);
    expect(policy.forRepository('other/repository').trustedSenders).toEqual([]);
  });

  test('policy sender lists replace the environment list', async () => {
    const policy = await parse(
      `
[defaults]
senders = ["teammate"]

[repositories]
allow = ["edloidas/lictor"]

[repositories.overrides."edloidas/lictor"]
senders = ["maintainer"]
`,
      ['operator'],
    );

    expect(policy.forRepository('edloidas/lictor').trustedSenders).toEqual(['maintainer']);
    expect(policy.forRepository('edloidas/other').trustedSenders).toEqual(['teammate']);
  });

  test('an explicit override lifts the third-party cap', async () => {
    const policy = await parse(`
[repositories.overrides."other/repository"]
execution = "automatic"

[repositories.overrides."other/repository".capabilities]
branches = true
`);

    const repository = policy.forRepository('other/repository');
    expect(repository.accepted).toBe(true);
    expect(repository.execution).toBe('automatic');
    expect(repository.capabilities.branches).toBe(true);
  });

  test('ownership is exact and deny always wins', async () => {
    const policy = await parse(
      `
[repositories]
allow = ["Edloidas/Lictor", "team/platform-api", "team/archive-api"]
deny = ["team/archive-*"]
`,
      ['operator'],
    );

    // Unlisted repositories stay accepted at the third-party tier; what the
    // exact names decide is ownership, and with it the environment trust
    // default and the full capability set.
    expect(policy.forRepository('EDLOIDAS/LICTOR').trustedSenders).toEqual(['operator']);
    expect(policy.forRepository('team/platform-api').trustedSenders).toEqual(['operator']);
    expect(policy.forRepository('team/archive-api').accepted).toBe(false);
    expect(policy.forRepository('edloidas/lictor-docs').accepted).toBe(true);
    expect(policy.forRepository('other/repository').accepted).toBe(true);
  });

  test.each([
    ['edloidas/..'],
    ['edloidas/.'],
    ['../etc/passwd'],
    ['edloidas/lictor/extra'],
    ['edloidas/lic tor'],
    ['-edloidas/lictor'],
  ])('never accepts the hostile repository name %p', async (repository) => {
    const policy = await parse(`
[repositories]
allow = ["edloidas/lictor"]
`);

    expect(policy.forRepository(repository).accepted).toBe(false);
  });

  test('merges exact repository overrides over defaults', async () => {
    const policy = await parse(`
[defaults]
execution = "approval"
clone = "denied"

[defaults.capabilities]
comment = true
scripts = ["bun test"]

[repositories.overrides."edloidas/lictor"]
execution = "automatic"
clone = "allowed"

[repositories.overrides."edloidas/lictor".capabilities]
branches = true
merge = true
scripts = []
`);

    const repository = policy.forRepository('edloidas/lictor');
    expect(repository.execution).toBe('automatic');
    expect(repository.clone).toBe('allowed');
    expect(repository.capabilities.comment).toBe(true);
    expect(repository.capabilities.branches).toBe(true);
    expect(repository.capabilities.merge).toBe(true);
    expect(repository.capabilities.forcePush).toBe(false);
    expect(repository.capabilities.scripts).toEqual([]);
  });

  test('loads retention windows', async () => {
    const policy = await parse(`
[retention]
completedDays = 14
failedDays = 120
`);

    expect(policy.completedRetentionDays).toBe(14);
    expect(policy.failedRetentionDays).toBe(120);
  });

  test('does not let an exact override bypass a deny rule', async () => {
    const policy = await parse(`
[repositories]
deny = ["edloidas/lictor"]

[repositories.overrides."EDLOIDAS/LICTOR"]
execution = "automatic"
`);

    expect(policy.forRepository('edloidas/lictor').accepted).toBe(false);
  });

  test.each([
    ['invalid TOML', 'allow = ['],
    ['unknown execution mode', '[defaults]\nexecution = "sometimes"'],
    ['unknown security key', '[defaults]\nexecuton = "automatic"'],
    ['malformed repository name', '[repositories]\nallow = ["lictor"]'],
    // Reach is granted by an invitation; permission must be granted one
    // repository at a time, or being added anywhere arms everything there.
    ['an owner wildcard in allow', '[repositories]\nallow = ["edloidas/*"]'],
    ['a traversal segment in allow', '[repositories]\nallow = ["edloidas/.."]'],
    ['a removed workspace root key', '[repositories]\nworkspaceRoots = ["/srv/lictor"]'],
    [
      'a removed workspace override key',
      '[repositories.overrides."edloidas/lictor"]\nworkspace = "/srv/lictor/lictor"',
    ],
    ['fractional retention', '[retention]\ncompletedDays = 1.5'],
    ['excessive retention', '[retention]\nfailedDays = 3651'],
    ['invalid queue limit', '[limits]\nmaxQueueDepth = 0'],
    ['invalid repository cost', '[limits.costs]\nmaxDurationMinutes = 0'],
  ])('rejects %s', async (_name, source) => {
    const exit = await Effect.runPromiseExit(parsePolicy(source));
    expect(exit._tag).toBe('Failure');
    if (exit._tag === 'Failure') {
      expect(String(exit.cause)).toContain(PolicyError.name);
    }
  });

  test('reports the path and expectation for a schema error', async () => {
    const exit = await Effect.runPromiseExit(parsePolicy('[defaults]\nexecution = "sometimes"'));
    expect(String(exit)).toContain('[\\"defaults\\"]');
    expect(String(exit)).toContain('[\\"execution\\"]');
    expect(String(exit)).toContain('automatic');
  });
});
