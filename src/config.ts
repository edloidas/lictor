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
 * Both secrets are `Redacted`, which keeps them out of logs and error traces
 * even when a service is printed whole.
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
      /** Shared secret GitHub signs each delivery with. */
      webhookSecret: yield* Config.redacted('GITHUB_WEBHOOK_SECRET'),
      /** GitHub users whose activity may create work. Empty trusts nobody. */
      trustedSenders: yield* loginList('GITHUB_TRUSTED_SENDERS'),
      /** GitHub users whose assignments and mentions may create work. */
      targetUsers: yield* loginList('GITHUB_TARGET_USERS'),
      /** Local SQLite file used for durable work. */
      databasePath: yield* Config.string('LICTOR_DATABASE_PATH').pipe(
        Config.withDefault('.lictor/lictor.sqlite'),
      ),
      policyPath: yield* Config.string('LICTOR_POLICY_PATH').pipe(
        Config.withDefault('.lictor/policy.toml'),
      ),
      controlSocketPath: yield* Config.string('LICTOR_SOCKET_PATH').pipe(
        Config.withDefault('.lictor/lictor.sock'),
      ),
      webhookMaxBytes: yield* positiveInteger(
        'LICTOR_WEBHOOK_MAX_BYTES',
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
    };
  }),
}) {}

/** Port to bind. Read outside the service so the server layer can build with it. */
export const port = Config.integer('PORT').pipe(Config.withDefault(3000));
