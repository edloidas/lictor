import { describe, expect, it } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { AgentListener, LONGEST_SOCKET_NAME } from '../src/control/agent-listener.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';

type SeenIdentity = {
  readonly jobId: number;
  readonly attemptNumber: number;
  readonly workerId: string;
};

const send = (path: string, line: string) =>
  Effect.async<string, Error>((resume) => {
    let output = '';
    Bun.connect({
      unix: path,
      socket: {
        open(socket) {
          socket.write(`${line}\n`);
        },
        data(_socket, data) {
          output += Buffer.from(data).toString('utf8');
        },
        close() {
          resume(Effect.succeed(output));
        },
        error(_socket, error) {
          resume(Effect.fail(error));
        },
      },
    }).catch((error) => resume(Effect.fail(error)));
  });

/**
 * Writes each chunk as its own socket write, then waits for the reply.
 *
 * Separate writes are not guaranteed to arrive as separate reads, so the gap is
 * what makes the split likely rather than certain.
 */
const sendChunks = (path: string, chunks: readonly (string | Buffer)[]) =>
  Effect.async<string, Error>((resume) => {
    let output = '';
    Bun.connect({
      unix: path,
      socket: {
        async open(socket) {
          for (const chunk of chunks) {
            socket.write(chunk);
            await Bun.sleep(40);
          }
        },
        data(_socket, data) {
          output += Buffer.from(data).toString('utf8');
        },
        close() {
          resume(Effect.succeed(output));
        },
        error(_socket, error) {
          resume(Effect.fail(error));
        },
      },
    }).catch((error) => resume(Effect.fail(error)));
  });

const config = (stateDir: string, executor: 'codex' | 'disabled' = 'codex') =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: [],
    autoAcceptInviters: [],
    databasePath: join(stateDir, 'lictor.sqlite'),
    stateDir,
    policyPath: 'unused',
    controlSocketPath: join(stateDir, 'lictor.sock'),
    deliveryMaxBytes: 1024,
    executor,
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  });

/**
 * The delay is load-bearing for the chunking cases: a real capability call
 * reaches GitHub, so the window between request and reply is wide in production
 * and a stub that answers instantly closes the socket before it can be probed.
 */
const recordingBroker = (seen: SeenIdentity[], delayMs = 0) =>
  Layer.succeed(
    CapabilityBroker,
    CapabilityBroker.make({
      callTool: () => Effect.die('unused'),
      listTools: [],
      handleMcp: ((
        jobId: number,
        attemptNumber: number,
        workerId: string,
        mcp: { readonly id: string | number },
      ) => {
        seen.push({ jobId, attemptNumber, workerId });
        return Effect.delay(
          Effect.succeed({
            jsonrpc: '2.0' as const,
            id: mcp.id,
            result: { jobId, attemptNumber, workerId },
          }),
          delayMs,
        );
      }) as unknown as InstanceType<typeof CapabilityBroker>['handleMcp'],
    }),
  );

describe('agent listener', () => {
  it('answers capability requests and binds identity to the listener, not the request', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const seen: SeenIdentity[] = [];
    const ListenerLive = AgentListener.DefaultWithoutDependencies.pipe(
      Layer.provide(
        Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker(seen)),
      ),
    );

    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const listener = yield* AgentListener;
            const { path } = yield* listener.open(7, 2, 'worker-a');
            // The forged identity is the point: the envelope has no field for it,
            // and anything smuggled alongside must not reach the broker.
            const answer = yield* send(
              path,
              JSON.stringify({
                jobId: 999,
                workerId: 'worker-intruder',
                mcp: { jsonrpc: '2.0', id: 1, method: 'tools/call' },
              }),
            );
            return { answer: JSON.parse(answer), mode: statSync(path).mode & 0o777, path };
          }).pipe(Effect.provide(ListenerLive)),
        ),
      );

      expect(seen).toEqual([{ jobId: 7, attemptNumber: 2, workerId: 'worker-a' }]);
      expect(result.answer).toMatchObject({
        ok: true,
        result: { result: { jobId: 7, attemptNumber: 2, workerId: 'worker-a' } },
      });
      expect(result.mode).toBe(0o600);
      expect(existsSync(result.path)).toBe(false);
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it('dispatches one request per line even when the write arrives in several chunks', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const seen: SeenIdentity[] = [];
    const ListenerLive = AgentListener.DefaultWithoutDependencies.pipe(
      Layer.provide(
        Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker(seen, 200)),
      ),
    );

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const listener = yield* AgentListener;
            const { path } = yield* listener.open(3, 1, 'worker-a');
            yield* sendChunks(path, [
              `${JSON.stringify({ mcp: { jsonrpc: '2.0', id: 1, method: 'tools/call' } })}\n`,
              'trailing',
            ]);
          }).pipe(Effect.provide(ListenerLive)),
        ),
      );

      expect(seen).toHaveLength(1);
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it('does not corrupt a multi-byte character split across chunks', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const received: string[] = [];
    const ListenerLive = AgentListener.DefaultWithoutDependencies.pipe(
      Layer.provide(
        Layer.merge(
          Layer.succeed(LictorConfig, config(stateDir)),
          Layer.succeed(
            CapabilityBroker,
            CapabilityBroker.make({
              callTool: () => Effect.die('unused'),
              listTools: [],
              handleMcp: ((
                _jobId: number,
                _attempt: number,
                _worker: string,
                mcp: { readonly params?: { readonly body?: string } },
              ) => {
                received.push(mcp.params?.body ?? '');
                return Effect.succeed({ jsonrpc: '2.0' as const, id: 1, result: {} });
              }) as unknown as InstanceType<typeof CapabilityBroker>['handleMcp'],
            }),
          ),
        ),
      ),
    );

    try {
      const body = 'héllo — 🚀 ünicode';
      const line = Buffer.from(
        `${JSON.stringify({ mcp: { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { body } } })}\n`,
        'utf8',
      );
      // Split mid-character: the rocket is four bytes, so cutting inside it is
      // what a real chunk boundary does to a large payload.
      const cut = line.indexOf(Buffer.from('🚀', 'utf8')) + 2;

      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const listener = yield* AgentListener;
            const { path } = yield* listener.open(4, 1, 'worker-a');
            yield* sendChunks(path, [line.subarray(0, cut), line.subarray(cut)]);
          }).pipe(Effect.provide(ListenerLive)),
        ),
      );

      expect(received).toEqual([body]);
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it('rejects a malformed line without reaching the broker', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const seen: SeenIdentity[] = [];
    const ListenerLive = AgentListener.DefaultWithoutDependencies.pipe(
      Layer.provide(
        Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker(seen)),
      ),
    );

    try {
      const answer = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const listener = yield* AgentListener;
            const { path } = yield* listener.open(1, 1, 'worker-a');
            return yield* send(path, 'not json');
          }).pipe(Effect.provide(ListenerLive)),
        ),
      );

      expect(JSON.parse(answer)).toMatchObject({
        ok: false,
        error: { code: 'AGENT_REQUEST_INVALID' },
      });
      expect(seen).toEqual([]);
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it('starts when the longest socket path is exactly at the limit', async () => {
    const base = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const longestName = LONGEST_SOCKET_NAME;
    const padding = 107 - (base.length + 1 + '/agent/'.length + longestName.length);
    expect(padding).toBeGreaterThan(0);
    const stateDir = join(base, 'x'.repeat(padding));
    // Asserted before the run: an arithmetic slip here would otherwise read as
    // the guard correctly refusing an over-long path.
    expect(join(stateDir, 'agent', longestName).length).toBe(107);

    try {
      const built = await Effect.runPromise(
        Effect.flatMap(AgentListener, () => Effect.succeed('built')).pipe(
          Effect.provide(
            AgentListener.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker([])),
              ),
            ),
          ),
        ),
      );

      expect(built).toBe('built');
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('clears stale sockets at startup without touching other entries or failing', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'lictor-agent-'));
    const directory = join(stateDir, 'agent');
    mkdirSync(join(directory, 'sub'), { recursive: true });
    writeFileSync(join(directory, 'notes.txt'), 'keep me');
    const stale = join(directory, 'job-1-1-deadbeefcafe.sock');
    const orphan = Bun.listen({ unix: stale, socket: { data() {} } });
    orphan.stop(true);

    try {
      const built = await Effect.runPromise(
        Effect.flatMap(AgentListener, () => Effect.succeed('built')).pipe(
          Effect.provide(
            AgentListener.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker([])),
              ),
            ),
          ),
        ),
      );

      expect(built).toBe('built');
      expect(existsSync(stale)).toBe(false);
      expect(existsSync(join(directory, 'notes.txt'))).toBe(true);
      expect(existsSync(join(directory, 'sub'))).toBe(true);
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it('starts on an over-long state directory when no agent ever runs', async () => {
    const stateDir = join(mkdtempSync(join(tmpdir(), 'lictor-agent-')), 'x'.repeat(120));

    const built = await Effect.runPromise(
      Effect.flatMap(AgentListener, () => Effect.succeed('built')).pipe(
        Effect.provide(
          AgentListener.DefaultWithoutDependencies.pipe(
            Layer.provide(
              Layer.merge(
                Layer.succeed(LictorConfig, config(stateDir, 'disabled')),
                recordingBroker([]),
              ),
            ),
          ),
        ),
      ),
    );

    expect(built).toBe('built');
  });

  it('refuses to start when the state directory cannot hold a socket path', async () => {
    const stateDir = join(mkdtempSync(join(tmpdir(), 'lictor-agent-')), 'x'.repeat(120));

    const failure = await Effect.runPromise(
      Effect.flip(
        Effect.flatMap(AgentListener, () => Effect.void).pipe(
          Effect.provide(
            AgentListener.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.merge(Layer.succeed(LictorConfig, config(stateDir)), recordingBroker([])),
              ),
            ),
          ),
        ),
      ),
    );

    expect(failure).toMatchObject({ code: 'AGENT_SOCKET_PATH_TOO_LONG' });
  });
});
