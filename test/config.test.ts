import { describe, expect, test } from 'bun:test';
import { ConfigProvider, Effect, Layer } from 'effect';
import { LictorConfig } from '../src/config.ts';

const required = new Map([
  ['GITHUB_APP_ID', '1'],
  ['GITHUB_PRIVATE_KEY', 'private'],
  ['GITHUB_WEBHOOK_SECRET', 'secret'],
]);

const load = (values: Map<string, string>) =>
  Effect.provide(
    LictorConfig,
    LictorConfig.Default.pipe(
      Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(values))),
    ),
  );

describe('LictorConfig', () => {
  test('disables execution when no executor is configured', async () => {
    expect((await Effect.runPromise(load(required))).executor).toBe('disabled');
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
});
