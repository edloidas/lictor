import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig, stateDirOf } from '../src/config.ts';
import { ControlPlane, type ControlRequest, ControlServer } from '../src/control/control-plane.ts';
import { CredentialHealth } from '../src/github/credential-health.ts';
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

const config = (controlSocketPath: string) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: [],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    stateDir: stateDirOf(':memory:'),
    policyPath: 'unused',
    controlSocketPath,
    deliveryMaxBytes: 1024,
    executor: 'disabled',
    codexModel: 'gpt-5.6-luna',
    codexHome: '',
    agentWorkdir: '.',
    executorTimeoutMs: 1000,
    executorOutputBytes: 1024,
    executorResultBytes: 1024,
    gitTimeoutMs: 180_000,
    workerPollMs: 10,
    workerMaxAttempts: 3,
    workerRetryBaseMs: 100,
    notificationPollMs: 60_000,
  });

/** One plane over its own in-memory queue, under the given policy document. */
const planeUnder = (policySource: string, socketPath: string) => {
  const ConfigLive = Layer.succeed(LictorConfig, config(socketPath));
  const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
  const PlaneLive = ControlPlane.DefaultWithoutDependencies.pipe(
    Layer.provide(
      Layer.mergeAll(
        ConfigLive,
        Layer.effect(Policy, parsePolicy(policySource).pipe(Effect.map(Policy.make))),
        QueueLive,
        CredentialHealth.Default,
      ),
    ),
  );
  return Layer.merge(PlaneLive, QueueLive);
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
        autoAcceptInviters: [],
        databasePath,
        stateDir: directory,
        policyPath: 'unused',
        controlSocketPath: socketPath,
        deliveryMaxBytes: 1024,
        executor: 'disabled',
        codexModel: 'gpt-5.6-luna',
        codexHome: '',
        agentWorkdir: '.',
        executorTimeoutMs: 1000,
        executorOutputBytes: 1024,
        executorResultBytes: 1024,
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
    const HealthLive = CredentialHealth.Default;
    // No broker is provided; ControlPlane no longer depends on one, so
    // `capability.mcp` below is expected to come back unrecognized.
    const PlaneLive = ControlPlane.DefaultWithoutDependencies.pipe(
      Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, HealthLive)),
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
            const health = yield* CredentialHealth;
            const enqueued = yield* queue.enqueue(work);
            yield* health.suspend;
            const approved = yield* call(server.path, {
              command: 'job.approve',
              args: [String(enqueued.jobId)],
            });
            const status = yield* call(server.path, { command: 'status' });
            const capability = yield* call(server.path, {
              command: 'capability.mcp',
              args: ['1', '1', 'worker-a', '{"jsonrpc":"2.0","id":1,"method":"tools/call"}'],
            });
            const claimed = yield* queue.claim;
            yield* queue.complete(enqueued.jobId, claimed?.attempts ?? 0, '{}', {
              repository: work.repository,
              subjectNumber: work.subject.number,
              outcome: 'completed',
              note: 'Opened the pull request.',
            });
            const shown = yield* call(server.path, {
              command: 'job.show',
              args: [String(enqueued.jobId)],
            });
            return {
              approved: JSON.parse(approved),
              status: JSON.parse(status),
              capability: JSON.parse(capability),
              shown: JSON.parse(shown),
              claimed,
              mode: statSync(server.path).mode & 0o777,
            };
          }).pipe(Effect.provide(Layer.mergeAll(ServerLive, QueueLive, HealthLive))),
        ),
      );
      expect(result.approved).toMatchObject({ ok: true, result: { changed: true, jobId: 1 } });
      expect(result.status).toMatchObject({
        ok: true,
        result: { executor: 'disabled', credentialRejected: true },
      });
      expect(result.capability).toMatchObject({
        ok: false,
        error: { code: 'CONTROL_COMMAND_UNKNOWN' },
      });
      expect(result.claimed?.work.approvalRequired).toBe(false);
      // Where policy withholds `comment` the outbox row is the only record of
      // what the agent said, so `job.show` has to carry it.
      expect(result.shown).toMatchObject({
        ok: true,
        result: {
          id: 1,
          outcome: 'completed',
          outbox: [{ outcome: 'completed', note: 'Opened the pull request.', status: 'pending' }],
        },
      });
      expect(result.mode).toBe(0o600);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('still reports a job whose payload no longer decodes', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'lictor-control-'));
    const path = join(directory, 'queue.sqlite');
    const ConfigLive = Layer.succeed(
      LictorConfig,
      LictorConfig.make({
        githubToken: Redacted.make('pat-value'),
        expectedLogin: 'adiutriel',
        trustedSenders: ['edloidas'],
        autoAcceptInviters: [],
        databasePath: path,
        stateDir: stateDirOf(path),
        policyPath: 'unused',
        controlSocketPath: join(directory, 'control.sock'),
        deliveryMaxBytes: 1024,
        executor: 'disabled',
        codexModel: 'gpt-5.6-luna',
        codexHome: '',
        agentWorkdir: '.',
        executorTimeoutMs: 1000,
        executorOutputBytes: 1024,
        executorResultBytes: 1024,
        gitTimeoutMs: 180_000,
        workerPollMs: 10,
        workerMaxAttempts: 3,
        workerRetryBaseMs: 100,
        notificationPollMs: 60_000,
      }),
    );
    const PolicyLive = Layer.effect(
      Policy,
      parsePolicy('[defaults]\nexecution = "automatic"').pipe(Effect.map(Policy.make)),
    );
    const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
    const PlaneLive = ControlPlane.DefaultWithoutDependencies.pipe(
      Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, CredentialHealth.Default)),
    );

    try {
      const enqueued = await Effect.runPromise(
        Effect.scoped(
          Effect.flatMap(WorkQueue, (queue) =>
            // Not approval-required: the claim skips those, and the claim is
            // what dead-letters an unreadable payload.
            queue.enqueue({ ...work, approvalRequired: false }),
          ).pipe(Effect.provide(QueueLive)),
        ),
      );

      const raw = new Database(path);
      const payload = JSON.parse(
        (
          raw.query('SELECT payload FROM jobs WHERE id = ?').get(enqueued.jobId) as {
            payload: string;
          }
        ).payload,
      ) as Record<string, unknown>;
      raw
        .query('UPDATE jobs SET payload = ? WHERE id = ?')
        .run(JSON.stringify({ ...payload, reasons: 'not-an-array' }), enqueued.jobId);
      raw.close();

      const shown = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const queue = yield* WorkQueue;
            // The claim dead-letters it and writes the message it still owes.
            yield* queue.claim;
            const plane = yield* ControlPlane;
            return yield* plane.execute({ command: 'job.show', args: [String(enqueued.jobId)] });
          }).pipe(Effect.provide(Layer.mergeAll(PlaneLive, QueueLive))),
        ),
      );

      // `queue.job` cannot decode this row, and it is exactly the row that owes
      // the thread a message — reporting nothing here loses the only record.
      expect(shown).toMatchObject({
        id: enqueued.jobId,
        undecodable: true,
        outbox: [{ outcome: 'failed', status: 'pending' }],
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  // ! The approval is the authorization decision. Minting at the first claim
  // ! instead let a daemon restart in between record the policy loaded
  // ! afterwards as the scope the operator released.
  it('records the approved scope when the operator approves, not at the first claim', async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const plane = yield* ControlPlane;
          const { jobId } = yield* queue.enqueue(work);
          const held = yield* queue.job(jobId);
          yield* plane.execute({ command: 'job.approve', args: [String(jobId)] });
          return { held, approved: yield* queue.job(jobId) };
        }).pipe(
          Effect.provide(
            planeUnder('[defaults]\nexecution = "approval"', '/tmp/lictor-approve.sock'),
          ),
        ),
      ),
    );

    expect(result.held?.grant).toBeUndefined();
    expect(result.approved?.grant?.decision).toBe('approved');
    // The third-party tier the repository falls into, recorded as it stood when
    // the operator released the hold.
    expect(result.approved?.grant?.capabilities.comment).toBe(true);
    expect(result.approved?.grant?.capabilities.merge).toBe(false);
    expect(result.approved?.work.approvalRequired).toBe(false);
  });

  // ! An empty ceiling is never overwritten once recorded, so approving into one
  // ! would leave the row unrunnable and no policy correction could release it.
  it('refuses an approval that would record a grant with no capability at all', async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const plane = yield* ControlPlane;
          const { jobId } = yield* queue.enqueue(work);
          const refused = yield* Effect.either(
            plane.execute({ command: 'job.approve', args: [String(jobId)] }),
          );
          // Same locked-down repository, but a job that was never held.
          const { jobId: automatic } = yield* queue.enqueue({
            ...work,
            deliveryId: 'control-automatic',
            interactionId: 'interaction-automatic',
            approvalRequired: false,
          });
          const notHeld = yield* plane.execute({
            command: 'job.approve',
            args: [String(automatic)],
          });
          return { refused, notHeld, job: yield* queue.job(jobId) };
        }).pipe(
          Effect.provide(
            planeUnder(
              '[defaults]\nexecution = "approval"\n\n[defaults.capabilities]\nread = false\n\n[repositories]\nallow = ["edloidas/lictor"]',
              '/tmp/lictor-nocap.sock',
            ),
          ),
        ),
      ),
    );

    // ! Gated on the state `approve` acts from, not on the verb. Approving a job
    // ! that is not held has always been an inert `no_change`, and a repository
    // ! locked down to nothing must not turn every such call into an error.
    expect(result.notHeld).toMatchObject({ changed: false });
    expect(result.refused._tag).toBe('Left');
    expect((result.refused as { left: { code: string } }).left.code).toBe(
      'CONTROL_APPROVAL_WITHOUT_CAPABILITY',
    );
    // Still held, and still approvable once the policy is corrected.
    expect(result.job?.grant).toBeUndefined();
    expect(result.job?.work.approvalRequired).toBe(true);
  });

  it('answers a parked question, and refuses a job that is not waiting on one', async () => {
    const ConfigLive = Layer.succeed(
      LictorConfig,
      LictorConfig.make({
        githubToken: Redacted.make('test-token'),
        expectedLogin: 'adiutriel',
        trustedSenders: [],
        autoAcceptInviters: [],
        databasePath: ':memory:',
        stateDir: stateDirOf(':memory:'),
        policyPath: 'unused',
        controlSocketPath: '/tmp/lictor-answer.sock',
        deliveryMaxBytes: 1024,
        executor: 'disabled',
        codexModel: 'gpt-5.6-luna',
        codexHome: '',
        agentWorkdir: '.',
        executorTimeoutMs: 1000,
        executorOutputBytes: 1024,
        executorResultBytes: 1024,
        gitTimeoutMs: 180_000,
        workerPollMs: 10,
        workerMaxAttempts: 3,
        workerRetryBaseMs: 100,
        notificationPollMs: 60_000,
      }),
    );
    const PolicyLive = Layer.effect(
      Policy,
      parsePolicy('[defaults]\nexecution = "automatic"').pipe(Effect.map(Policy.make)),
    );
    const QueueLive = WorkQueue.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive));
    const PlaneLive = ControlPlane.DefaultWithoutDependencies.pipe(
      Layer.provide(Layer.mergeAll(ConfigLive, PolicyLive, QueueLive, CredentialHealth.Default)),
    );
    const answerUrl = 'https://github.com/edloidas/lictor/issues/14#issuecomment-9';

    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* WorkQueue;
          const plane = yield* ControlPlane;
          const { jobId } = yield* queue.enqueue({ ...work, approvalRequired: false });
          const notWaiting = yield* Effect.either(
            plane.execute({ command: 'job.answer', args: [String(jobId), answerUrl] }),
          );
          const missing = yield* Effect.either(
            plane.execute({ command: 'job.answer', args: [String(jobId + 99), answerUrl] }),
          );
          const claimed = yield* queue.claim;
          yield* queue.park({
            jobId,
            attemptNumber: claimed?.attempts ?? 1,
            repository: work.repository,
            subjectNumber: work.subject.number,
            question: 'which branch?',
            // The socket is not bound by these; an operator holding it already
            // has `approve` and `cancel` on the same row.
            answerers: ['someone-else'],
            askedAt: Date.now(),
            expiresAt: Date.now() + 3_600_000,
          });
          const notAUrl = yield* Effect.either(
            plane.execute({ command: 'job.answer', args: [String(jobId), 'ftp://elsewhere'] }),
          );
          // The bound is what stops an answer nobody can read being written
          // into the payload the agent is handed.
          const tooLong = yield* Effect.either(
            plane.execute({
              command: 'job.answer',
              args: [String(jobId), `https://github.com/${'x'.repeat(2048)}`],
            }),
          );
          const answered = yield* plane.execute({
            command: 'job.answer',
            args: [String(jobId), answerUrl],
          });
          return {
            jobId,
            notWaiting,
            missing,
            notAUrl,
            tooLong,
            answered,
            job: yield* queue.job(jobId),
            audit: yield* queue.auditLog(jobId),
          };
        }).pipe(Effect.provide(Layer.mergeAll(PlaneLive, QueueLive))),
      ),
    );

    expect(result.notWaiting).toMatchObject({
      _tag: 'Left',
      left: { code: 'CONTROL_JOB_NOT_WAITING' },
    });
    expect(result.missing).toMatchObject({
      _tag: 'Left',
      left: { code: 'CONTROL_JOB_NOT_FOUND' },
    });
    expect(result.notAUrl).toMatchObject({
      _tag: 'Left',
      left: { code: 'CONTROL_ANSWER_URL_INVALID' },
    });
    expect(result.tooLong).toMatchObject({
      _tag: 'Left',
      left: { code: 'CONTROL_ANSWER_URL_INVALID' },
    });
    expect(result.answered).toEqual({ changed: true, jobId: result.jobId });
    expect(result.job?.questionId).toBeUndefined();
    expect(result.job?.work.answerUrl).toBe(answerUrl);
    expect(result.audit).toMatchObject([{ capability: 'control.answer', outcome: 'ok' }]);
  });
});
