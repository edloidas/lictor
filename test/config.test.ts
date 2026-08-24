import { describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { ConfigProvider, Effect, Layer } from 'effect';
import { LictorConfig, legacyStateConflict } from '../src/config.ts';

const required = new Map([
  ['LICTOR_GITHUB_TOKEN', 'test-token'],
  ['LICTOR_GITHUB_LOGIN', 'Adiutriel'],
]);

const load = (values: Map<string, string>) =>
  Effect.provide(
    LictorConfig,
    LictorConfig.Default.pipe(
      Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(values))),
    ),
  );

describe('LictorConfig', () => {
  test('enables autonomous execution when no executor is configured', async () => {
    expect((await Effect.runPromise(load(required))).executor).toBe('codex');
  });

  // State must not default into the working directory. Lictor serves many
  // repositories and one of them may be lictor itself, so a relative default
  // puts the live database inside a tree the agent is editing.
  test('defaults every state path under the home directory', async () => {
    const config = await Effect.runPromise(load(required));

    expect(config.databasePath).toBe(join(homedir(), '.lictor', 'lictor.sqlite'));
    expect(config.policyPath).toBe(join(homedir(), '.lictor', 'policy.toml'));
    expect(config.controlSocketPath).toBe(join(homedir(), '.lictor', 'lictor.sock'));
  });

  // The documented defaults are home paths, so an operator copying one into
  // `.env` writes a tilde. Unexpanded, it becomes a literal `~` directory and a
  // second, empty database — created silently, since paths are made on demand.
  test.each([
    ['~/elsewhere/lictor.sqlite', join(homedir(), 'elsewhere/lictor.sqlite')],
    ['~', homedir()],
  ])('expands a leading tilde in %p', async (value, expected) => {
    const config = await Effect.runPromise(
      load(new Map([...required, ['LICTOR_DATABASE_PATH', value]])),
    );

    expect(config.databasePath).toBe(expected);
  });

  test('leaves a path without a tilde exactly as given', async () => {
    const config = await Effect.runPromise(
      load(new Map([...required, ['LICTOR_DATABASE_PATH', '/var/lib/lictor/db.sqlite']])),
    );

    expect(config.databasePath).toBe('/var/lib/lictor/db.sqlite');
  });

  test('does not expand a tilde that is not leading', async () => {
    const config = await Effect.runPromise(
      load(new Map([...required, ['LICTOR_POLICY_PATH', '/srv/~backup/policy.toml']])),
    );

    expect(config.policyPath).toBe('/srv/~backup/policy.toml');
  });

  test('defaults codexHome to empty so the executor derives it from the database', async () => {
    expect((await Effect.runPromise(load(required))).codexHome).toBe('');
  });

  test('expands a leading tilde in LICTOR_CODEX_HOME', async () => {
    const config = await Effect.runPromise(
      load(new Map([...required, ['LICTOR_CODEX_HOME', '~/.codex']])),
    );

    expect(config.codexHome).toBe(join(homedir(), '.codex'));
  });

  test.each([
    ['LICTOR_WORKER_POLL_MS', '0'],
    ['LICTOR_WORKER_MAX_ATTEMPTS', '-1'],
    ['LICTOR_EXECUTOR_TIMEOUT_MS', '86400001'],
    ['LICTOR_EXECUTOR_OUTPUT_BYTES', '1.5'],
  ])('rejects invalid %s', async (name, value) => {
    const exit = await Effect.runPromiseExit(load(new Map([...required, [name, value]])));
    expect(exit._tag).toBe('Failure');
  });

  // Opening a fresh database beside the old one is the worst available
  // outcome: the daemon looks healthy while reading none of yesterday's policy
  // and processing none of yesterday's queue.
  describe('legacyStateConflict', () => {
    /** Both homes are temporary, so the verdict never depends on the real one. */
    const inScratch = <A>(run: (home: string, legacy: string) => A): A => {
      const root = mkdtempSync(join(tmpdir(), 'lictor-legacy-'));
      try {
        return run(join(root, 'home'), join(root, 'legacy'));
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    };
    const defaults = (home: string) => ({ databasePath: join(home, 'lictor.sqlite') });
    const writeLegacyDatabase = (legacy: string) => {
      mkdirSync(legacy, { recursive: true });
      writeFileSync(join(legacy, 'lictor.sqlite'), '');
    };

    test('reports a conflict when legacy state exists and the new home does not', () => {
      const message = inScratch((home, legacy) => {
        writeLegacyDatabase(legacy);
        return legacyStateConflict(defaults(home), home, legacy);
      });

      expect(message).toContain('LICTOR_DATABASE_PATH');
      // The guard fires when the target may not exist, so a bare `mv` glob into
      // it would abort having moved nothing and the operator would be stuck.
      expect(message).toContain('mkdir -p');
    });

    test('stays quiet when there is no legacy database', () => {
      expect(
        inScratch((home, legacy) => legacyStateConflict(defaults(home), home, legacy)),
      ).toBeUndefined();
    });

    // Once the new database exists it is the answer, and a leftover legacy file
    // must not block every subsequent start.
    test('stays quiet once the new database exists too', () => {
      const message = inScratch((home, legacy) => {
        writeLegacyDatabase(legacy);
        mkdirSync(home, { recursive: true });
        writeFileSync(join(home, 'lictor.sqlite'), '');
        return legacyStateConflict(defaults(home), home, legacy);
      });

      expect(message).toBeUndefined();
    });

    // The documented upgrade path is `mkdir -p ~/.lictor` plus a policy copy,
    // so testing the directory would let the instructions defeat the guard.
    test('still reports when the new home exists but holds no database', () => {
      const message = inScratch((home, legacy) => {
        writeLegacyDatabase(legacy);
        mkdirSync(home, { recursive: true });
        writeFileSync(join(home, 'policy.toml'), '');
        return legacyStateConflict(defaults(home), home, legacy);
      });

      expect(message).toContain('LICTOR_DATABASE_PATH');
    });

    test('stays quiet when the legacy directory holds no database', () => {
      const message = inScratch((home, legacy) => {
        mkdirSync(legacy, { recursive: true });
        return legacyStateConflict(defaults(home), home, legacy);
      });

      expect(message).toBeUndefined();
    });

    // An explicit path is a decision already made, and a legacy directory
    // beside it is then just a directory.
    test('stays quiet when the operator configured the database path', () => {
      const message = inScratch((home, legacy) => {
        writeLegacyDatabase(legacy);
        return legacyStateConflict({ databasePath: '/var/lib/lictor/db.sqlite' }, home, legacy);
      });

      expect(message).toBeUndefined();
    });

    // The worst case, and the one a both-paths-defaulted check misses. A
    // relocated policy fails loudly on a missing file; a relocated database
    // just opens an empty one, so the daemon looks healthy with no work.
    test('reports a conflict even when only the policy path was configured', () => {
      const message = inScratch((home, legacy) => {
        writeLegacyDatabase(legacy);
        // Typed wide on purpose: the signature ignores `policyPath`, and this is
        // what a real caller passes — the whole config.
        const config = {
          databasePath: join(home, 'lictor.sqlite'),
          policyPath: '/etc/lictor/policy.toml',
        };
        return legacyStateConflict(config, home, legacy);
      });

      expect(message).toContain('LICTOR_DATABASE_PATH');
    });
  });
});
