import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HttpClient, HttpClientRequest, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Redacted, type Scope } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { AgentListener } from '../src/control/agent-listener.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';
import { GitHubClient } from '../src/github/client.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
import { GitHubIdentity } from '../src/github/identity.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const work: WorkItem = {
  deliveryId: 'delivery-124',
  interactionId: 'interaction-124',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 13,
    title: 'Capability transport',
    url: 'https://github.com/edloidas/lictor/issues/13',
  },
};

const issueBody = { number: 13, title: 'Capability transport', state: 'open' };

const config = (stateDir: string) =>
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
    executor: 'codex',
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

const toolsCall = {
  jsonrpc: '2.0',
  id: 7,
  method: 'tools/call',
  params: { name: 'get_issue', arguments: { number: 13 } },
};

const envelope = JSON.stringify({ mcp: toolsCall });

/** Everything real but GitHub — a second double here and the file stops proving anything. */
const session = <A, E>(
  body: (context: {
    readonly stateDir: string;
    readonly requests: readonly string[];
  }) => Effect.Effect<A, E, AgentListener | WorkQueue | Scope.Scope>,
) => {
  const stateDir = mkdtempSync(join(tmpdir(), 'lictor-mcp-'));
  const requests: string[] = [];
  const ConfigLive = Layer.succeed(LictorConfig, config(stateDir));
  const client = HttpClient.make((request) => {
    requests.push(request.url);
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(JSON.stringify(issueBody), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );
  });
  const GitHubLive = Layer.succeed(
    GitHubClient,
    GitHubClient.make({
      authenticated: Effect.succeed(
        client.pipe(HttpClient.mapRequest(HttpClientRequest.prependUrl('https://api.github.test'))),
      ),
      addReaction: () => Effect.succeed(undefined),
    }),
  );
  const IdentityLive = Layer.succeed(
    GitHubIdentity,
    GitHubIdentity.make({
      verified: Effect.succeed({ login: 'adiutriel', tokenExpiresAt: undefined }),
    }),
  );
  const PolicyLive = Layer.effect(
    Policy,
    parsePolicy(
      '[defaults.capabilities]\nread = true\n[repositories]\nallow = ["edloidas/lictor"]',
    ).pipe(Effect.map(Policy.make)),
  );
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const BrokerLive = CapabilityBroker.DefaultWithoutDependencies.pipe(
    Layer.provide(
      Layer.mergeAll(GitHubLive, IdentityLive, PolicyLive, QueueLive, CredentialHealth.Default),
    ),
  );
  const ListenerLive = AgentListener.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.merge(ConfigLive, BrokerLive)),
  );
  return Effect.runPromise(
    Effect.scoped(
      body({ stateDir, requests }).pipe(Effect.provide(Layer.merge(ListenerLive, QueueLive))),
    ),
  ).finally(() => rmSync(stateDir, { recursive: true, force: true }));
};

const running = Effect.gen(function* () {
  const queue = yield* WorkQueue;
  const listener = yield* AgentListener;
  const enqueued = yield* queue.enqueue(work);
  const claimed = yield* Effect.fromNullable(yield* queue.claim);
  const { path } = yield* listener.open(enqueued.jobId, claimed.attempts, claimed.workerId);
  return { jobId: enqueued.jobId, path };
});

describe('capability.mcp over the agent transport', () => {
  it('answers a tools/call for a running job with a live lease', async () => {
    const result = await session(({ requests }) =>
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId, path } = yield* running;
        const answer = JSON.parse(yield* send(path, envelope)) as {
          readonly ok: boolean;
          readonly result: { readonly result?: { readonly content: { readonly text: string }[] } };
        };
        return { answer, requests: [...requests], audit: yield* queue.auditLog(jobId) };
      }),
    );

    expect(result.answer.ok).toBe(true);
    expect(result.answer.result).toMatchObject({ jsonrpc: '2.0', id: 7 });
    expect(JSON.parse(result.answer.result.result?.content[0]?.text ?? 'null')).toEqual(issueBody);
    expect(result.requests).toEqual(['https://api.github.test/repos/edloidas/lictor/issues/13']);
    expect(result.audit.map((row) => [row.capability, row.outcome])).toEqual([
      ['get_issue', 'started'],
      ['get_issue', 'ok'],
    ]);
  });

  it('refuses a tools/call once the job is no longer running', async () => {
    const result = await session(({ requests }) =>
      Effect.gen(function* () {
        const queue = yield* WorkQueue;
        const { jobId, path } = yield* running;
        yield* queue.cancel(jobId);
        return { answer: JSON.parse(yield* send(path, envelope)), requests: [...requests] };
      }),
    );

    expect(result.answer).toMatchObject({
      ok: true,
      result: { id: 7, error: { code: -32000, message: 'CAPABILITY_JOB_INACTIVE' } },
    });
    expect(result.requests).toEqual([]);
  });

  it('refuses a tools/call once the lease has expired', async () => {
    const result = await session(({ stateDir, requests }) =>
      Effect.gen(function* () {
        const { jobId, path } = yield* running;
        // Backdated rather than waited out: the lease is a fixed minute, and the
        // listener dispatches on the default runtime, where a test clock cannot reach.
        yield* Effect.sync(() => {
          const side = new Database(join(stateDir, 'lictor.sqlite'));
          side
            .query("UPDATE jobs SET lease_expires_at = ? WHERE id = ? AND status = 'running'")
            .run(Date.now() - 1, jobId);
          side.close();
        });
        return { answer: JSON.parse(yield* send(path, envelope)), requests: [...requests] };
      }),
    );

    expect(result.answer).toMatchObject({
      ok: true,
      result: { id: 7, error: { code: -32000, message: 'CAPABILITY_LEASE_EXPIRED' } },
    });
    expect(result.requests).toEqual([]);
  });
});

const bridge = join(import.meta.dir, '../src/github/mcp-client.ts');

const throughBridge = (path: string, lines: readonly string[]) =>
  Effect.gen(function* () {
    const child = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.spawn(['bun', bridge, path], { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' }),
      ),
      (spawned) => Effect.sync(() => spawned.kill()),
    );
    yield* Effect.sync(() => {
      for (const line of lines) child.stdin.write(`${line}\n`);
      child.stdin.end();
    });
    const stdout = yield* Effect.promise(() => new Response(child.stdout).text());
    const stderr = yield* Effect.promise(() => new Response(child.stderr).text());
    const exitCode = yield* Effect.promise(() => child.exited);
    return { lines: stdout.split('\n').filter((line) => line !== ''), stderr, exitCode };
  });

const notification = JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' });

describe('mcp-client bridge', () => {
  it('forwards a tools/call from agent stdio and writes the broker reply back', async () => {
    const result = await session(({ requests }) =>
      Effect.gen(function* () {
        const { path } = yield* running;
        const bridged = yield* throughBridge(path, [JSON.stringify(toolsCall)]);
        return { ...bridged, requests: [...requests] };
      }),
    );

    expect(result.exitCode).toBe(0);
    expect(result.stderr).toBe('');
    expect(result.lines).toHaveLength(1);
    const reply = JSON.parse(result.lines[0] ?? 'null') as {
      readonly jsonrpc: string;
      readonly id: number;
      readonly result: { readonly content: { readonly text: string }[] };
    };
    expect(reply).toMatchObject({ jsonrpc: '2.0', id: 7 });
    // Unwrapped to the payload: a bridge that answers with the envelope's shape
    // but drops what the broker returned satisfies every assertion above.
    expect(JSON.parse(reply.result.content[0]?.text ?? 'null')).toEqual(issueBody);
    expect(result.requests).toEqual(['https://api.github.test/repos/edloidas/lictor/issues/13']);
  });

  // Silence is the oracle: the listener answers everything it accepts, so a
  // forwarded notification would come back as a line rather than as nothing.
  it('drops a notification instead of forwarding it', async () => {
    const result = await session(({ requests }) =>
      Effect.gen(function* () {
        const { path } = yield* running;
        const bridged = yield* throughBridge(path, [notification]);
        return { ...bridged, requests: [...requests] };
      }),
    );

    expect(result.exitCode).toBe(0);
    expect(result.stderr).toBe('');
    expect(result.lines).toEqual([]);
    expect(result.requests).toEqual([]);
  });
});
