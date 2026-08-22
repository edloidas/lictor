import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
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
 * A leading `~/` in a supplied value is expanded, because the defaults printed
 * in `.env.example` are home paths and an operator who copies one into `.env`
 * writes exactly that. Without expansion the daemon would create a literal `~`
 * directory beside the working directory and open a second, empty database
 * there — silently, since every path is created on demand.
 *
 * Only a leading `~/`. Not `~user`, not `$HOME`: one rule, stated once, and no
 * path is ever rewritten in a way the operator did not type.
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
  // ! The database alone decides. It is the only path whose relocation is
  // ! *silent* — the queue creates the file it cannot find, so the daemon comes
  // ! up healthy with zero work. A relocated policy path fails loudly on its own
  // ! when the file is missing, and requiring both to be defaults would miss the
  // ! operator who set only `LICTOR_POLICY_PATH`, which is the worst case: a
  // ! daemon that looks fine and quietly ignores yesterday's queue.
  if (config.databasePath !== join(home, 'lictor.sqlite')) return undefined;
  // ! Files, not directories. The setup instructions say to `mkdir -p ~/.lictor`
  // ! and copy a policy into it, so a directory test would be defeated by the
  // ! documented upgrade path itself — the new home exists, the old database is
  // ! still full of work, and the guard waves it through. An empty legacy
  // ! directory is likewise nothing to strand.
  if (existsSync(config.databasePath)) return undefined;
  if (!existsSync(join(legacy, 'lictor.sqlite'))) return undefined;
  // ! `mkdir -p` first, because the guard fires precisely when the target may not
  // ! exist yet, and `mv` with a glob into a missing directory aborts having moved
  // ! nothing. And "point ... at" rather than "set ...", because the operator may
  // ! already have set these — `~/.lictor/lictor.sqlite` expands to exactly the
  // ! default, so setting it explicitly is indistinguishable from setting nothing
  // ! and being told to set it would be no help at all.
  return `Found a daemon database at ${join(legacy, 'lictor.sqlite')} and none at ${config.databasePath}. Move it (mkdir -p ${home} && mv ${legacy}/* ${home}/) or point LICTOR_DATABASE_PATH, LICTOR_POLICY_PATH, and LICTOR_SOCKET_PATH at ${legacy} to keep using it.`;
};

const expandHome = (value: string): string => {
  if (value === '~') return homedir();
  if (value.startsWith('~/')) return join(homedir(), value.slice(2));
  return value;
};

const statePath = (name: string, fallback: string) =>
  Config.string(name).pipe(Config.withDefault(join(HOME, fallback)), Config.map(expandHome));

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
    return {
      /**
       * Personal access token for the account the daemon acts as.
       *
       * ! Deliberately not `GITHUB_TOKEN`. That name is exported by `gh`, GitHub
       * ! Actions, and direnv, and Bun does not let `.env` override a variable
       * ! already exported by the shell — a stray export would silently run the
       * ! daemon as whoever owns that token.
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
       * Local SQLite file used for durable work.
       *
       * `CODEX_HOME` is derived from this path's directory, so moving it moves
       * the agent's home with it — see `src/executor/agent-executor.ts`.
       */
      databasePath: yield* statePath('LICTOR_DATABASE_PATH', 'lictor.sqlite'),
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
       * ! A floor, not the interval. GitHub returns `X-Poll-Interval` and asks
       * ! callers to honour it; polling faster than it says is what earns a
       * ! secondary rate limit. This only stops a header that is absent or
       * ! implausibly small from turning the loop into a spin.
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
