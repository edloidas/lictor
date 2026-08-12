import { BunRuntime } from '@effect/platform-bun';
import { Data, Effect } from 'effect';
import { LictorConfig } from './config.ts';

class CliError extends Data.TaggedError('CliError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const command = process.argv[2];
const json = process.argv.includes('--json');
const args = process.argv.slice(3).filter((arg) => arg !== '--json');

const Main = Effect.gen(function* () {
  if (command === undefined)
    return yield* new CliError({
      message: 'Usage: bun cli <status|job.*|repository.*|policy.check|prune> [args] [--json]',
    });
  const config = yield* LictorConfig;
  const response = yield* Effect.async<string, CliError>((resume) => {
    let output = '';
    Bun.connect({
      unix: config.controlSocketPath,
      socket: {
        open(socket) {
          socket.write(`${JSON.stringify({ command, args })}\n`);
        },
        data(_socket, data) {
          output += Buffer.from(data).toString('utf8');
        },
        close() {
          resume(Effect.succeed(output.trim()));
        },
        error(_socket, cause) {
          resume(
            Effect.fail(new CliError({ message: 'Could not reach the Lictor daemon', cause })),
          );
        },
      },
    }).catch((cause) =>
      resume(Effect.fail(new CliError({ message: 'Could not open the control socket', cause }))),
    );
  });
  const decoded = yield* Effect.try({
    try: () =>
      JSON.parse(response) as {
        ok: boolean;
        result?: unknown;
        error?: { code: string; message: string };
      },
    catch: (cause) => new CliError({ message: 'Daemon returned an invalid response', cause }),
  });
  if (!decoded.ok)
    return yield* new CliError({
      message: `${decoded.error?.code ?? 'CONTROL_FAILED'}: ${decoded.error?.message ?? 'Command failed'}`,
    });
  const result = decoded.result;
  yield* Effect.sync(() => {
    if (json || typeof result !== 'object') console.log(JSON.stringify(result, null, json ? 2 : 0));
    else if (Array.isArray(result))
      for (const item of result)
        console.log(typeof item === 'string' ? item : JSON.stringify(item));
    else
      for (const [key, value] of Object.entries(result ?? {}))
        console.log(`${key}: ${typeof value === 'object' ? JSON.stringify(value) : String(value)}`);
  });
});

BunRuntime.runMain(Main.pipe(Effect.provide(LictorConfig.Default)));
