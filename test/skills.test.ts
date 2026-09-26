import { afterAll, describe, expect, it } from 'bun:test';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Either } from 'effect';
import {
  bundledSkillsDir,
  installSkills,
  reviewSkillName,
  skillBody,
} from '../src/executor/skills.ts';

const dirs: string[] = [];
const tempDir = (prefix: string) => {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
};

afterAll(() => {
  for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
});

const skillDir = join(bundledSkillsDir, reviewSkillName);
const scripts = ['scope.sh', 'anchors.sh', 'checks.sh'];

describe('installSkills', () => {
  it('installs every bundled skill with its scripts still executable', async () => {
    const home = tempDir('lictor-codex-');

    const installed = await Effect.runPromise(installSkills(bundledSkillsDir, home));

    expect(installed).toContain(reviewSkillName);
    const target = join(home, 'skills', reviewSkillName);
    expect(readFileSync(join(target, 'SKILL.md'), 'utf8')).toBe(
      readFileSync(join(skillDir, 'SKILL.md'), 'utf8'),
    );
    for (const script of scripts) {
      expect(statSync(join(target, 'scripts', script)).mode & 0o111).not.toBe(0);
    }
  });

  it('drops files a previous copy had and this one does not', async () => {
    const home = tempDir('lictor-codex-');
    const target = join(home, 'skills', reviewSkillName);
    mkdirSync(target, { recursive: true });
    writeFileSync(join(target, 'SKILL.md'), 'stale');
    writeFileSync(join(target, 'left-behind.md'), 'stale');

    await Effect.runPromise(installSkills(bundledSkillsDir, home));

    expect(existsSync(join(target, 'left-behind.md'))).toBe(false);
    expect(readFileSync(join(target, 'SKILL.md'), 'utf8')).toBe(
      readFileSync(join(skillDir, 'SKILL.md'), 'utf8'),
    );
  });

  it('leaves skills it does not ship alone, and no staging behind', async () => {
    const home = tempDir('lictor-codex-');
    const operators = join(home, 'skills', 'operator-own');
    mkdirSync(operators, { recursive: true });
    writeFileSync(join(operators, 'SKILL.md'), 'mine');

    await Effect.runPromise(installSkills(bundledSkillsDir, home));

    expect(readFileSync(join(operators, 'SKILL.md'), 'utf8')).toBe('mine');
    expect(readdirSync(home).filter((name) => name.startsWith('.skill-staging-'))).toEqual([]);
  });

  it('fails as a SkillInstallError when the source is missing', async () => {
    const home = tempDir('lictor-codex-');

    const result = await Effect.runPromise(
      Effect.either(installSkills(join(home, 'no-such-dir'), home)),
    );

    expect(Either.isLeft(result) && result.left._tag).toBe('SkillInstallError');
  });
});

describe('skillBody', () => {
  it('strips the frontmatter a harness reads', () => {
    expect(skillBody('---\nname: x\ndescription: y\n---\n\n# Title\n\nText\n')).toBe(
      '# Title\n\nText',
    );
  });

  // A Markdown rule in the body must not be taken for the frontmatter's end.
  it('ends the frontmatter at its first closing fence', () => {
    expect(skillBody('---\nname: x\n---\n# Title\n\n---\n\nText\n')).toBe('# Title\n\n---\n\nText');
  });

  it('reads CRLF frontmatter', () => {
    expect(skillBody('---\r\nname: x\r\n---\r\n# Title\r\n')).toBe('# Title');
  });

  it('keeps a file without leading frontmatter whole', () => {
    expect(skillBody('# Title\n---\na\n---\nText')).toBe('# Title\n---\na\n---\nText');
  });
});

describe('the bundled review skill', () => {
  const skill = readFileSync(join(skillDir, 'SKILL.md'), 'utf8');

  it('declares its name and opts out of implicit invocation on both harness formats', () => {
    const frontmatter = Bun.YAML.parse(skill.split('---\n')[1] ?? '') as Record<string, unknown>;
    const openai = Bun.YAML.parse(
      readFileSync(join(skillDir, 'agents', 'openai.yaml'), 'utf8'),
    ) as { readonly policy?: { readonly allow_implicit_invocation?: unknown } };

    expect(frontmatter.name).toBe(reviewSkillName);
    expect(frontmatter['disable-model-invocation']).toBe(true);
    expect(openai.policy?.allow_implicit_invocation).toBe(false);
  });

  it('references only files it ships', () => {
    const referenced = new Set(skill.match(/(?:references|scripts)\/[a-z-]+\.(?:md|sh)/g));

    expect(referenced.size).toBeGreaterThan(0);
    for (const path of referenced) expect(existsSync(join(skillDir, path))).toBe(true);
  });

  // Every review job carries the body in its prompt.
  it('keeps its body small enough to inline', () => {
    expect(Buffer.byteLength(skillBody(skill))).toBeLessThan(24 * 1024);
  });
});

describe('review scripts', () => {
  const run = (cwd: string, script: string, args: readonly string[], stdin?: string) => {
    const result = Bun.spawnSync(['bash', join(skillDir, 'scripts', script), ...args], {
      cwd,
      stdin: stdin === undefined ? 'ignore' : Buffer.from(stdin),
      env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: cwd },
    });
    return { code: result.exitCode, out: result.stdout.toString(), err: result.stderr.toString() };
  };

  const git = (cwd: string, ...args: string[]) => {
    const result = Bun.spawnSync(['git', ...args], {
      cwd,
      env: {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        HOME: cwd,
        GIT_AUTHOR_NAME: 't',
        GIT_AUTHOR_EMAIL: 't@example.com',
        GIT_COMMITTER_NAME: 't',
        GIT_COMMITTER_EMAIL: 't@example.com',
      },
    });
    if (result.exitCode !== 0) throw new Error(result.stderr.toString());
    return result.stdout.toString().trim();
  };

  const lines = (count: number, replace: Record<number, string> = {}) =>
    `${Array.from({ length: count }, (_, i) => replace[i + 1] ?? String(i + 1)).join('\n')}\n`;

  /** A base with a 40-line file and a lock file, and a branch that changes both. */
  const repository = () => {
    const cwd = tempDir('lictor-review-repo-');
    git(cwd, 'init', '-q', '-b', 'main');
    writeFileSync(join(cwd, 'a.txt'), lines(40));
    writeFileSync(join(cwd, 'bun.lock'), 'one\n');
    git(cwd, 'add', '.');
    git(cwd, 'commit', '-qm', 'base');
    git(cwd, 'checkout', '-qb', 'feature');
    return cwd;
  };

  const values = (out: string) =>
    Object.fromEntries(
      out
        .split('\n')
        .filter((line) => line.includes('='))
        .map((line) => line.split('=', 2) as [string, string]),
    );

  it('scope.sh counts reviewable lines and keeps lock files out of them', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'bun.lock'), 'two\n');
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 20: 'twenty' }));
    git(cwd, 'commit', '-qam', 'change');

    const { code, out } = run(cwd, 'scope.sh', ['main']);

    expect(code).toBe(0);
    expect(values(out)).toMatchObject({
      from: git(cwd, 'rev-parse', 'main'),
      from_kind: 'merge-base',
      merge_base: git(cwd, 'rev-parse', 'main'),
      files: '1',
      lines: '2',
      skipped: '1',
      trivial: 'no',
      mode: 'quick',
    });
    expect(out.split('== files\n')[1]).toBe('1\t1\ta.txt\n');
  });

  // Whitespace is never trivial: a CRLF shebang or a Markdown hard break is behaviour.
  it('scope.sh calls a lock-file-only change trivial, and reviews a whitespace one', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'bun.lock'), 'two\n');
    git(cwd, 'commit', '-qam', 'lock');
    expect(values(run(cwd, 'scope.sh', ['main']).out).trivial).toBe('lockfiles');

    writeFileSync(join(cwd, 'a.txt'), lines(40, { 5: '5\r' }));
    git(cwd, 'commit', '-qam', 'crlf');
    expect(values(run(cwd, 'scope.sh', ['main']).out)).toMatchObject({ files: '1', trivial: 'no' });
  });

  // Indentation is syntax in Python, YAML and Make: moving `z()` into the block changes it.
  it('scope.sh reviews an indentation change', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.py'), 'if x:\n    y()\nz()\n');
    git(cwd, 'add', 'a.py');
    git(cwd, 'commit', '-qm', 'add');
    const from = git(cwd, 'rev-parse', 'HEAD');
    writeFileSync(join(cwd, 'a.py'), 'if x:\n    y()\n    z()\n');
    git(cwd, 'commit', '-qam', 'indent');

    expect(values(run(cwd, 'scope.sh', [from]).out).trivial).toBe('no');
  });

  it('scope.sh tells generated output apart from lock files, and reviews build/', () => {
    const cwd = repository();
    mkdirSync(join(cwd, 'dist'));
    writeFileSync(join(cwd, 'dist', 'app.js'), 'x\n');
    git(cwd, 'add', '.');
    git(cwd, 'commit', '-qm', 'dist');
    expect(values(run(cwd, 'scope.sh', ['main']).out)).toMatchObject({
      files: '0',
      trivial: 'generated',
    });

    mkdirSync(join(cwd, 'build'));
    writeFileSync(join(cwd, 'build', 'release.sh'), 'rm -rf /\n');
    git(cwd, 'add', '.');
    git(cwd, 'commit', '-qm', 'build');
    expect(values(run(cwd, 'scope.sh', ['main']).out)).toMatchObject({
      files: '1',
      trivial: 'no',
    });
  });

  it('scope.sh --since still reports the merge base anchors are checked against', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 5: 'five' }));
    git(cwd, 'commit', '-qam', 'first');
    const reviewed = git(cwd, 'rev-parse', 'HEAD');
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 5: 'five', 30: 'thirty' }));
    git(cwd, 'commit', '-qam', 'second');

    expect(values(run(cwd, 'scope.sh', ['--since', reviewed, 'main']).out)).toMatchObject({
      from: reviewed,
      merge_base: git(cwd, 'rev-parse', 'main'),
    });
  });

  it('scope.sh --since diffs from the earlier review and refuses a rewritten one', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 5: 'five' }));
    git(cwd, 'commit', '-qam', 'first');
    const reviewed = git(cwd, 'rev-parse', 'HEAD');
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 5: 'five', 30: 'thirty' }));
    git(cwd, 'commit', '-qam', 'second');

    const since = values(run(cwd, 'scope.sh', ['--since', reviewed, 'main']).out);
    expect(since).toMatchObject({ from: reviewed, from_kind: 'since', lines: '2' });

    git(cwd, 'checkout', '-q', 'main');
    git(cwd, 'checkout', '-qb', 'rewritten');
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 7: 'seven' }));
    git(cwd, 'commit', '-qam', 'other');
    const refused = run(cwd, 'scope.sh', ['--since', reviewed, 'main']);
    expect(refused.code).toBe(1);
    expect(refused.err).toContain('rewritten');
  });

  // Three lines inserted after line 20 make the sides differ: LEFT 18-23, RIGHT 18-26.
  it('anchors.sh accepts lines inside a hunk, on the side named, and nothing else', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 20: '20\nnew-a\nnew-b\nnew-c' }));
    git(cwd, 'commit', '-qam', 'insert');

    const { code, out } = run(
      cwd,
      'anchors.sh',
      ['main'],
      [
        'a.txt:21',
        'a.txt:26',
        'a.txt:23:LEFT',
        'a.txt:26:LEFT',
        'a.txt:17',
        'b.txt:1',
        'junk',
      ].join('\n'),
    );

    expect(code).toBe(1);
    expect(out.trim().split('\n')).toEqual([
      'ok a.txt:21',
      'ok a.txt:26',
      'ok a.txt:23:LEFT',
      'no a.txt:26:LEFT (hunks: 18-23)',
      'no a.txt:17 (hunks: 18-26)',
      'no b.txt:1 (no RIGHT hunks in this diff)',
      'no junk (not path:line)',
    ]);
  });

  // git prints the enclosing line after the closing @@; a `-1` there is source, not a range.
  it('anchors.sh ignores the function context in a hunk header', () => {
    const cwd = tempDir('lictor-review-repo-');
    git(cwd, 'init', '-q', '-b', 'main');
    const body = Array.from({ length: 40 }, (_, i) => `    line ${i + 2}`);
    writeFileSync(join(cwd, 'f.txt'), `total = count -1\n${body.join('\n')}\n`);
    git(cwd, 'add', '.');
    git(cwd, 'commit', '-qm', 'base');
    body[28] = '    changed';
    writeFileSync(join(cwd, 'f.txt'), `total = count -1\n${body.join('\n')}\n`);
    git(cwd, 'commit', '-qam', 'change');

    const { out } = run(cwd, 'anchors.sh', ['HEAD~1'], 'f.txt:1:LEFT\n');

    expect(out.trim()).toBe('no f.txt:1:LEFT (hunks: 27-33)');
  });

  it('anchors.sh answers every anchor when git refuses one path', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 20: 'twenty' }));
    git(cwd, 'commit', '-qam', 'change');

    const { code, out } = run(cwd, 'anchors.sh', ['main'], '../outside:1\na.txt:20\n');

    expect(code).toBe(1);
    expect(out.trim().split('\n')).toEqual([
      'no ../outside:1 (git cannot diff this path)',
      'ok a.txt:20',
    ]);
  });

  it('anchors.sh exits zero when every anchor holds', () => {
    const cwd = repository();
    writeFileSync(join(cwd, 'a.txt'), lines(40, { 20: 'twenty' }));
    git(cwd, 'commit', '-qam', 'change');

    expect(run(cwd, 'anchors.sh', ['main'], 'a.txt:20\n').code).toBe(0);
  });

  it('checks.sh marks a declared script blocked while dependencies are absent', () => {
    const cwd = tempDir('lictor-review-checks-');
    writeFileSync(join(cwd, 'package.json'), JSON.stringify({ scripts: { test: 'x', dev: 'y' } }));
    writeFileSync(join(cwd, 'bun.lock'), '');

    const blocked = run(cwd, 'checks.sh', []).out.trim();
    mkdirSync(join(cwd, 'node_modules'));
    const ready = run(cwd, 'checks.sh', []).out.trim();

    expect(blocked).toStartWith('blocked bun run test (node_modules absent');
    expect(ready).toBe('ready bun run test');
  });

  it.each([
    [
      'a declared script',
      '{ "scripts": { "test": "x" } }',
      'blocked bun run test (bun is not installed)',
    ],
    [
      'a check-named key outside scripts',
      '{\n  "scripts": { "build": "x" },\n  "jest": { "test": "y" }\n}',
      'none (no check command declared in a manifest this script reads)',
    ],
  ])('checks.sh with no JavaScript runtime on PATH reads %s', (_, manifest, expected) => {
    const cwd = tempDir('lictor-review-checks-');
    writeFileSync(join(cwd, 'package.json'), manifest);
    writeFileSync(join(cwd, 'bun.lock'), '');
    const bin = tempDir('lictor-review-bin-');
    for (const tool of ['bash', 'grep', 'sed', 'tr']) {
      const found = Bun.which(tool);
      if (found !== null) symlinkSync(found, join(bin, tool));
    }

    const result = Bun.spawnSync(['bash', join(skillDir, 'scripts', 'checks.sh')], {
      cwd,
      env: { PATH: bin, HOME: cwd },
    });

    expect(result.stdout.toString().trim()).toBe(expected);
  });
});
