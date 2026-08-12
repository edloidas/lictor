import { describe, expect, test } from 'bun:test';
import { Effect } from 'effect';
import { PolicyError, parsePolicy } from '../src/policy.ts';

const parse = (source: string) => Effect.runPromise(parsePolicy(source));

describe('repository automation policy', () => {
  test('uses safe capability and retention defaults', async () => {
    const policy = await parse('');
    const repository = policy.forRepository('Edloidas/Lictor');

    expect(repository).toEqual({
      repository: 'edloidas/lictor',
      accepted: true,
      execution: 'denied',
      clone: 'denied',
      capabilities: {
        read: true,
        comment: false,
        issues: false,
        branches: false,
        pullRequests: false,
        merge: false,
        forcePush: false,
        deleteBranches: false,
        scripts: [],
      },
      maxAttempts: 3,
      maxDurationMs: 30 * 60 * 1000,
    });
    expect(policy.completedRetentionDays).toBe(30);
    expect(policy.failedRetentionDays).toBe(90);
    expect(policy.maxQueueDepth).toBe(10_000);
    expect(policy.maxJobAgeMs).toBe(24 * 60 * 60 * 1000);
  });

  test('applies case-insensitive allow patterns with deny precedence', async () => {
    const policy = await parse(`
[repositories]
allow = ["Edloidas/*", "team/**"]
deny = ["edloidas/private-*", "team/archive-*"]
`);

    expect(policy.forRepository('EDLOIDAS/LICTOR').accepted).toBe(true);
    expect(policy.forRepository('edloidas/private-tools').accepted).toBe(false);
    expect(policy.forRepository('team/platform-api').accepted).toBe(true);
    expect(policy.forRepository('team/archive-api').accepted).toBe(false);
    expect(policy.forRepository('other/repository').accepted).toBe(false);
  });

  test('merges exact repository overrides over defaults', async () => {
    const policy = await parse(`
[defaults]
execution = "approval"
clone = "denied"

[defaults.capabilities]
comment = true
scripts = ["bun test"]

[repositories]
workspaceRoots = ["/srv/lictor/workspaces"]

[repositories.overrides."edloidas/lictor"]
execution = "automatic"
clone = "allowed"
workspace = "/srv/lictor/workspaces/lictor"

[repositories.overrides."edloidas/lictor".capabilities]
branches = true
merge = true
scripts = []
`);

    const repository = policy.forRepository('edloidas/lictor');
    expect(repository.execution).toBe('automatic');
    expect(repository.clone).toBe('allowed');
    expect(repository.workspace).toBe('/srv/lictor/workspaces/lictor');
    expect(repository.capabilities.comment).toBe(true);
    expect(repository.capabilities.branches).toBe(true);
    expect(repository.capabilities.merge).toBe(true);
    expect(repository.capabilities.forcePush).toBe(false);
    expect(repository.capabilities.scripts).toEqual([]);
  });

  test('loads workspace roots and retention windows', async () => {
    const policy = await parse(`
[repositories]
workspaceRoots = ["/srv/lictor", "/opt/checkouts"]

[retention]
completedDays = 14
failedDays = 120
`);

    expect(policy.workspaceRoots).toEqual(['/srv/lictor', '/opt/checkouts']);
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
    ['malformed repository pattern', '[repositories]\nallow = ["lictor"]'],
    ['relative workspace', '[repositories]\nworkspaceRoots = ["workspaces"]'],
    ['filesystem root', '[repositories]\nworkspaceRoots = ["/"]'],
    [
      'workspace outside configured roots',
      '[repositories]\nworkspaceRoots = ["/srv/lictor"]\n[repositories.overrides."edloidas/lictor"]\nworkspace = "/tmp/lictor"',
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
