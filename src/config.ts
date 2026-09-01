import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { Config, Effect } from 'effect';

const loginList = (name: string) =>
  Config.string(name).pipe(
    Config.withDefault(''),
    Config.map((value) => [
      ...new Set(
        value
          .split(',')
          .map((login) => login.trim().toLowerCase())
          .filter(Boolean),
      ),
    ]),
  );

/**
 * Where daemon state lives when nothing says otherwise.
 *
 * Outside any repository on purpose. Lictor serves many repositories, and one of
 * them may be lictor itself — a database inside the working directory would then
 * sit in a tree the agent is editing.
 */
const HOME = join(homedir(), '.lictor');

/**
 * Path config with a home-relative default.
 *
 * A leading `~/` expands, because `.env.example` prints home paths and an
 * operator who copies one writes exactly that — unexpanded it silently opens a
 * second, empty database. Leading `~/` only: no path is rewritten beyond what
 * the operator typed.
 */
/** Where state used to live: relative to wherever the daemon happened to run. */
const LEGACY_HOME = '.lictor';

/**
 * Refuses to start beside state the daemon used to own.
 *
 * The default moved out of the working directory, and nothing migrates. Starting
 * anyway is the worst outcome available: a fresh empty database opens, the old
 * policy is not read, and queued work is neither processed nor reported missing —
 * so the daemon looks healthy while doing none of what it did yesterday. Failing
 * with both paths named turns that into a one-line fix.
 *
 * Only when the operator set nothing: an explicit path is a decision already
 * made, and a legacy directory beside it is then just a directory.
 */
export const legacyStateConflict = (
  config: { readonly databasePath: string },
  /** Injectable so the suite can assert this without depending on the real home. */
  home = HOME,
  legacy = LEGACY_HOME,
): string | undefined => {
  // ! The database alone decides: its relocation is the silent one — the queue
  // ! creates the file it cannot find, so the daemon comes up healthy holding
  // ! zero work. A relocated policy path fails loudly on its own.
  if (config.databasePath !== join(home, 'lictor.sqlite')) return undefined;
  // File test, not directory: the documented upgrade path (`mkdir -p ~/.lictor`)
  // leaves an empty legacy directory behind, and that strands nothing.
  if (existsSync(config.databasePath)) return undefined;
  if (!existsSync(join(legacy, 'lictor.sqlite'))) return undefined;
  // "mkdir -p" first — the guard fires when the target may not exist yet — and
  // "point ... at" rather than "set ...": `~/.lictor/lictor.sqlite` expands to
  // exactly the default, so telling them to set it would be no help at all.
  return `Found a daemon database at ${join(legacy, 'lictor.sqlite')} and none at ${config.databasePath}. Move it (mkdir -p ${home} && mv ${legacy}/* ${home}/) or point LICTOR_DATABASE_PATH, LICTOR_POLICY_PATH, and LICTOR_SOCKET_PATH at ${legacy} to keep using it.`;
};

const expandHome = (value: string): string => {
  if (value === '~') return homedir();
  if (value.startsWith('~/')) return join(homedir(), value.slice(2));
  return value;
};

const statePath = (name: string, fallback: string) =>
  Config.string(name).pipe(Config.withDefault(join(HOME, fallback)), Config.map(expandHome));

/**
 * Everything beside the database follows it, so relocating the database relocates
 * all state. Deliberately not its own setting: a second knob could disagree.
 */
export const stateDirOf = (databasePath: string): string => dirname(resolve(databasePath));

const positiveInteger = (name: string, fallback: number, maximum: number) =>
  Config.integer(name).pipe(
    Config.withDefault(fallback),
    Config.validate({
      message: `${name} must be between 1 and ${maximum}`,
      validation: (value) => value >= 1 && value <= maximum,
    }),
  );

/**
 * Environment-backed configuration. Bun loads `.env` automatically, so nothing
 * here needs a dotenv shim — see `.env.example` for the expected keys.
 *
 * The token is `Redacted`, which keeps it out of logs and error traces even when
 * a service is printed whole.
 */
export class LictorConfig extends Effect.Service<LictorConfig>()('LictorConfig', {
  effect: Effect.gen(function* () {
    const databasePath = yield* statePath('LICTOR_DATABASE_PATH', 'lictor.sqlite');
    return {
      /**
       * Personal access token for the account the daemon acts as.
       *
       * ! Deliberately not `GITHUB_TOKEN`: that name is exported by `gh`, GitHub
       * ! Actions, and direnv, and `.env` cannot override an exported variable —
       * ! a stray export would silently run the daemon as whoever owns it.
       */
      githubToken: yield* Config.redacted('LICTOR_GITHUB_TOKEN'),
      /**
       * Login the token is expected to belong to. Asserted against `GET /user`
       * at startup, so a swapped or revoked token fails loudly instead of
       * acting as the wrong account.
       */
      expectedLogin: yield* Config.string('LICTOR_GITHUB_LOGIN').pipe(
        Config.map((login) => login.trim().toLowerCase()),
        Config.validate({
          message: 'LICTOR_GITHUB_LOGIN must not be empty',
          validation: (login) => login.length > 0,
        }),
      ),
      /** GitHub users whose activity may create work. Empty trusts nobody. */
      trustedSenders: yield* loginList('GITHUB_TRUSTED_SENDERS'),
      /**
       * GitHub users whose repository invitations are accepted automatically.
       * Empty accepts nothing; nobody is ever declined.
       */
      autoAcceptInviters: yield* loginList('LICTOR_AUTO_ACCEPT_INVITERS'),
      /** Local SQLite file used for durable work. `stateDir` follows it. */
      databasePath,
      stateDir: stateDirOf(databasePath),
      policyPath: yield* statePath('LICTOR_POLICY_PATH', 'policy.toml'),
      controlSocketPath: yield* statePath('LICTOR_SOCKET_PATH', 'lictor.sock'),
      deliveryMaxBytes: yield* positiveInteger(
        'LICTOR_DELIVERY_MAX_BYTES',
        1024 * 1024,
        10 * 1024 * 1024,
      ),
      executor: yield* Config.literal(
        'codex',
        'disabled',
      )('LICTOR_EXECUTOR').pipe(Config.withDefault('codex' as const)),
      codexModel: yield* Config.string('LICTOR_CODEX_MODEL').pipe(
        Config.withDefault('gpt-5.6-luna'),
      ),
      /**
       * Explicit `CODEX_HOME` for the executor. Empty falls back to a `codex`
       * directory beside the database — see `src/executor/agent-executor.ts`.
       */
      codexHome: yield* Config.string('LICTOR_CODEX_HOME').pipe(
        Config.withDefault(''),
        Config.map(expandHome),
      ),
      agentWorkdir: yield* Config.string('LICTOR_AGENT_WORKDIR').pipe(
        Config.withDefault(process.cwd()),
      ),
      executorTimeoutMs: yield* positiveInteger(
        'LICTOR_EXECUTOR_TIMEOUT_MS',
        30 * 60 * 1000,
        24 * 60 * 60 * 1000,
      ),
      executorOutputBytes: yield* positiveInteger(
        'LICTOR_EXECUTOR_OUTPUT_BYTES',
        256 * 1024,
        10 * 1024 * 1024,
      ),
      /**
       * Ceiling on every git operation, network or local — including the
       * `checkout` that detaches onto a fetched ref, which materializes the
       * working-tree delta and can exceed any short budget on a cold cache.
       */
      gitTimeoutMs: yield* positiveInteger('LICTOR_GIT_TIMEOUT_MS', 180_000, 60 * 60 * 1000),
      workerPollMs: yield* positiveInteger('LICTOR_WORKER_POLL_MS', 1000, 60_000),
      workerMaxAttempts: yield* positiveInteger('LICTOR_WORKER_MAX_ATTEMPTS', 3, 100),
      workerRetryBaseMs: yield* positiveInteger(
        'LICTOR_WORKER_RETRY_BASE_MS',
        30_000,
        24 * 60 * 60 * 1000,
      ),
      /**
       * Floor for the gap between notification polls.
       *
       * ! Polling faster than GitHub's `X-Poll-Interval` says is what earns a
       * ! secondary rate limit; this only stops an absent or implausibly small
       * ! header from turning the loop into a spin.
       */
      notificationPollMs: yield* positiveInteger(
        'LICTOR_NOTIFICATION_POLL_MS',
        60_000,
        60 * 60 * 1000,
      ),
    };
  }),
}) {}

/** Port to bind. Read outside the service so the server layer can build with it. */
export const port = Config.integer('PORT').pipe(Config.withDefault(3000));
