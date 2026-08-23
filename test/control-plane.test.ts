import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { ControlPlane, type ControlRequest, ControlServer } from '../src/control/control-plane.ts';
import { CapabilityBroker } from '../src/github/capability-broker.ts';
import { Policy, parsePolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import type { WorkItem } from '../src/work-item.ts';

const work: WorkItem = {
  deliveryId: 'control-delivery',
  interactionId: 'control-interaction',
  repository: 'edloidas/lictor',
  approvalRequired: true,
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['assigned'],
  subject: {
    kind: 'issue',
    number: 14,
    title: 'Control plane',
    url: 'https://github.com/edloidas/lictor/issues/14',
  },
};

const call = (path: string, request: ControlRequest) =>
  Effect.async<string, Error>((resume) => {
    let output = '';
    Bun.connect({
      unix: path,
      socket: {
        open(socket) {
          socket.write(`${JSON.stringify(request)}\n`);
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

describe('local control plane', () => {
  it('serves state-checked commands over an owner-only Unix socket', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-control-'));
    const socketPath = join(directory, 'lictor.sock');
    const databasePath = join(directory, 'lictor.sqlite');
    const ConfigLive = Layer.succeed(
      LictorConfig,
      LictorConfig.make({
        githubToken: Redacted.make('test-token'),
        expectedLogin: 'adiutriel',
        trustedSenders: [],
        databasePath,
        policyPath: 'unused',
        controlSocketPath: socketPath,
        deliveryMaxBytes: 1024,
        executor: 'disabled',
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
      }),
    );
    const PolicyLive = Layer.effect(
      Policy,
      parsePolicy('[defaults]\nexecution = "approval"').pipe(Effect.map(Policy.make)),
    );
    const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
    const BrokerLive = Layer.succeed(
      CapabilityBroker,
      CapabilityBroker.make({
        callTool: () => Effect.die('unused'),
        handleMcp: () => Effect.die('unused'),
        listTools: [],
      }),
    );
    const PlaneLive = ControlPlane.DefaultWithoutDependencies.pipe(
      Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, BrokerLive)),
    );
    const ServerLive = ControlServer.DefaultWithoutDependencies.pipe(
      Layer.provide(Layer.merge(ConfigLive, PlaneLive)),
    );
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const server = yield* ControlServer;
            const queue = yield* WorkQueue;
            const enqueued = yield* queue.enqueue(work);
            const approved = yield* call(server.path, {
              command: 'job.approve',
              args: [String(enqueued.jobId)],
            });
            const status = yield* call(server.path, { command: 'status' });
            return {
              approved: JSON.parse(approved),
              status: JSON.parse(status),
              claimed: yield* queue.claim,
              mode: statSync(server.path).mode & 0o777,
            };
          }).pipe(Effect.provide(Layer.merge(ServerLive, QueueLive))),
        ),
      );
      expect(result.approved).toMatchObject({ ok: true, result: { changed: true, jobId: 1 } });
      expect(result.status).toMatchObject({ ok: true, result: { executor: 'disabled' } });
      expect(result.claimed?.work.approvalRequired).toBe(false);
      expect(result.mode).toBe(0o600);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
