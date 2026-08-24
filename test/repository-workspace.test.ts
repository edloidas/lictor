import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Cause, Effect, Exit, Fiber, Layer, Option, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { type ProcessRequest, ProcessRunner } from '../src/executor/process-runner.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import type { RepositoryPolicy } from '../src/policy.ts';
import { WorkQueue } from '../src/queue/work-queue.ts';
import { DiskStat, RepositoryWorkspace } from '../src/workspace/repository-workspace.ts';

const job = { id: 10, repository: 'edloidas/lictor' };

const policy = (clone: 'allowed' | 'denied' = 'denied'): RepositoryPolicy => ({
  repository: 'edloidas/lictor',
  accepted: true,
  execution: 'automatic',
  clone,
  maxAttempts: 3,
  trustedSenders: ['edloidas'],
  maxDurationMs: 30 * 60 * 1000,
  capabilities: {
    read: true,
    comment: false,
    issues: false,
    branches: false,
    pullRequests: false,
    merge: false,
    forcePush: false,
    deleteBranches: false,
    scripts: [],
  },
});

const config = (home: string) =>
  LictorConfig.make({
    githubToken: Redacted.make('test-token'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    autoAcceptInviters: [],
    databasePath: join(home, 'lictor.sqlite'),
    policyPath: join(home, 'policy.toml'),
    controlSocketPath: join(home, 'lictor.sock'),
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
  });

/** Where the daemon puts sessions, given its state directory. */
const sessions = (home: string) => join(home, 'sessions');

const sessionPath = (home: string, jobId: number) => join(sessions(home), `job-${jobId}`);

const service = (
  home: string,
  run: InstanceType<typeof ProcessRunner>['run'],
  statfs: InstanceType<typeof DiskStat>['statfs'] = () => ({
    bavail: 8 * 1024 * 1024,
    bsize: 512,
  }),
) =>
  RepositoryWorkspace.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(LictorConfig, config(home))),
    Layer.provide(Layer.succeed(ProcessRunner, ProcessRunner.make({ run }))),
    Layer.provide(Layer.succeed(DiskStat, DiskStat.make({ statfs }))),
    Layer.provide(
      Layer.succeed(
        GitHubCredential,
        GitHubCredential.make({
          token: Effect.succeed(Redacted.make('secret-token')),
          gitAuthHeader: Effect.succeed(Redacted.make('Basic c2VjcmV0')),
        }),
      ),
    ),
    // Real queue over the temp home's database: git invocations land in
    // `capability_audit`, asserted by reading the file after the run.
    Layer.provide(
      WorkQueue.DefaultWithoutDependencies.pipe(
        Layer.provide(Layer.succeed(LictorConfig, config(home))),
      ),
    ),
  );

const withHome = async <A>(body: (home: string) => Promise<A>): Promise<A> => {
  const home = mkdtempSync(join(tmpdir(), 'lictor-home-'));
  try {
    return await body(home);
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
};

const ok = () => ({ exitCode: 0, stdout: '', stderr: '', outputTruncated: false });

type Acquire = InstanceType<typeof RepositoryWorkspace>['acquire'];

/** Builds (not runs) an `acquire`, so each test decides how to interpret it. */
const acquiring = (
  home: string,
  run: InstanceType<typeof ProcessRunner>['run'],
  request: Parameters<Acquire>[0] = job,
  statfs?: InstanceType<typeof DiskStat>['statfs'],
) =>
  Effect.gen(function* () {
    const manager = yield* RepositoryWorkspace;
    return yield* manager.acquire(request, policy('allowed'));
  }).pipe(Effect.provide(service(home, run, statfs)));

describe('RepositoryWorkspace', () => {
  it('clones a full session at sessions/job-<id>', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      const result = await Effect.runPromise(
        acquiring(home, (request) =>
          Effect.sync(() => {
            calls.push(request);
            return ok();
          }),
        ),
      );

      expect(result.path).toBe(sessionPath(home, 10));
      const clone = calls.find((call) => call.command.includes('clone'));
      expect(clone?.command).toEqual([
        'git',
        'clone',
        'https://github.com/edloidas/lictor.git',
        sessionPath(home, 10),
      ]);
      // A full clone per job: no history surgery behind the agent's back.
      expect(calls.some((call) => call.command.includes('--depth'))).toBe(false);
      expect(calls.some((call) => call.command.includes('--filter'))).toBe(false);
      expect(calls.some((call) => call.command.includes('--single-branch'))).toBe(false);
    });
  });

  it('audits every git subprocess invocation with the command verbatim', async () => {
    await withHome(async (home) => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          yield* manager.acquire({ ...job, ref: 'refs/pull/9/head' }, policy('allowed'));
        }).pipe(Effect.provide(service(home, () => Effect.sync(() => ok())))),
      );

      const database = new Database(join(home, 'lictor.sqlite'), { readonly: true });
      try {
        const rows = database
          .query(
            `SELECT capability, outcome, input, actor FROM capability_audit
             WHERE job_id = ? ORDER BY id`,
          )
          .all(job.id) as
          | readonly { capability: string; outcome: string; input: string; actor: string }[]
          | [];
        expect([...rows].map((row) => row.capability)).toEqual([
          'git_clone',
          'git_fetch',
          'git_checkout',
        ]);
        for (const row of rows) {
          expect(row.outcome).toBe('ok');
          // The daemon's own credential is the actor; the token never appears
          // in the argv an audit row carries.
          expect(row.actor).toBe('adiutriel');
          const argv = JSON.parse(row.input) as string[];
          expect(argv[0]).toBe('git');
          expect(argv.some((part) => part.includes('secret-token'))).toBe(false);
        }
      } finally {
        database.close();
      }
    });
  });

  it('injects the Authorization header through git config env only', async () => {
    await withHome(async (home) => {
      let clone: ProcessRequest | undefined;
      await Effect.runPromise(
        acquiring(home, (request) =>
          Effect.sync(() => {
            if (request.command.includes('clone')) clone = request;
            return ok();
          }),
        ),
      );

      // The header rides in through config env, so it never lands in the
      // remote URL, a credential store, or anything inside the session.
      expect(clone?.env?.GIT_CONFIG_KEY_0).toBe('http.https://github.com/.extraHeader');
      expect(clone?.env?.GIT_CONFIG_VALUE_0).toBe('Authorization: Basic c2VjcmV0');
      expect(clone?.command.some((part) => part.includes('secret-token'))).toBe(false);
      expect(clone?.cwd).not.toContain('secret-token');
    });
  });

  it('fetches and detaches onto an explicit ref with the network timeout', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        acquiring(
          home,
          (request) => {
            calls.push(request);
            return Effect.succeed(ok());
          },
          { ...job, ref: 'issue-24' },
        ),
      );

      const fetch = calls.find((call) => call.command.includes('fetch'));
      expect(fetch?.command).toEqual(['git', 'fetch', 'origin', 'issue-24']);
      expect(fetch?.timeoutMs).toBe(180_000);
      expect(fetch?.env?.GIT_CONFIG_VALUE_0).toBe('Authorization: Basic c2VjcmV0');
      const checkout = calls.find((call) => call.command.includes('checkout'));
      expect(checkout?.command).toEqual(['git', 'checkout', '--detach', 'FETCH_HEAD']);
      expect(checkout?.timeoutMs).toBe(180_000);
      // Nothing pins the default branch on top of the requested ref.
      expect(calls.some((call) => call.command.join(' ').includes('origin/HEAD'))).toBe(false);
    });
  });

  it('fails a ref that cannot be fetched instead of landing on the default branch', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      const exit = await Effect.runPromiseExit(
        acquiring(
          home,
          (request) => {
            calls.push(request);
            return request.command.includes('fetch')
              ? Effect.succeed({
                  ...ok(),
                  exitCode: 1,
                  stderr: "fatal: couldn't find remote ref no-such-ref",
                })
              : Effect.succeed(ok());
          },
          { ...job, ref: 'no-such-ref' },
        ),
      );

      expect(String(exit)).toContain('WORKSPACE_REF_UNAVAILABLE');
      // No silent fallback: a failed fetch must not be followed by anything.
      expect(calls.some((call) => call.command.includes('checkout'))).toBe(false);
    });
  });

  it.each([['-x'], ['a b'], ['a..b'], ['a~b'], ['a^b'], ['a:b'], ['a?b'], ['a*b'], ['[x'], ['']])(
    'refuses the ref %p before any git runs',
    async (ref) => {
      await withHome(async (home) => {
        const exit = await Effect.runPromiseExit(
          acquiring(home, () => Effect.die('git must not run'), { ...job, ref }),
        );
        expect(String(exit)).toContain('WORKSPACE_REF_INVALID');
      });
    },
  );

  it('leaves the clone on its own checkout when no ref is given', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        acquiring(home, (request) => {
          calls.push(request);
          return Effect.succeed(ok());
        }),
      );

      // The fresh clone already sits on the remote default HEAD; probing
      // `origin/HEAD` and detaching would pin whatever that symbol names
      // rather than what the clone resolved.
      expect(calls.some((call) => call.command.includes('rev-parse'))).toBe(false);
      expect(calls.some((call) => call.command.includes('checkout'))).toBe(false);
      expect(calls.some((call) => call.command.includes('fetch'))).toBe(false);
    });
  });

  // A session is never reused: a leftover may be a killed attempt whose tree
  // nobody can vouch for. It moves aside for forensics and the clone restarts.
  it('moves a pre-existing session aside and clones afresh', async () => {
    await withHome(async (home) => {
      mkdirSync(sessionPath(home, 10), { recursive: true });
      writeFileSync(join(sessionPath(home, 10), 'half-written.txt'), '');
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        acquiring(home, (request) => {
          calls.push(request);
          return Effect.succeed(ok());
        }),
      );

      expect(existsSync(join(sessions(home), 'job-10.failed-1'))).toBe(true);
      expect(existsSync(join(sessions(home), 'job-10.failed-1', 'half-written.txt'))).toBe(true);
      expect(existsSync(sessionPath(home, 10))).toBe(false);
      expect(calls.some((call) => call.command.includes('clone'))).toBe(true);
    });
  });

  it('does not touch the filesystem when policy denies cloning', async () => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.acquire(job, policy());
        }).pipe(Effect.provide(service(home, () => Effect.die('git must not run')))),
      );

      expect(String(exit)).toContain('WORKSPACE_CLONE_DENIED');
      expect(existsSync(sessions(home))).toBe(false);
      // Terminal, or every attempt pays these checks again before dying the
      // same death — a policy decision cannot change between attempts.
      const failure = Option.getOrUndefined(
        Cause.failureOption(exit._tag === 'Failure' ? exit.cause : Cause.empty),
      );
      expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).toBe(false);
    });
  });

  // Repository names come from a GitHub payload and are joined into a path.
  // A traversal segment must never reach `join`, let alone `git`.
  it.each([['edloidas/..'], ['../../etc'], ['edloidas/lictor/extra']])(
    'refuses the repository name %p before git runs',
    async (repository) => {
      await withHome(async (home) => {
        const exit = await Effect.runPromiseExit(
          Effect.gen(function* () {
            const manager = yield* RepositoryWorkspace;
            return yield* manager.acquire({ ...job, repository }, policy('allowed'));
          }).pipe(Effect.provide(service(home, () => Effect.die('git must not run')))),
        );
        expect(String(exit)).toContain('WORKSPACE_REPOSITORY_INVALID');
      });
    },
  );

  it('deletes the session on release without retention', async () => {
    await withHome(async (home) => {
      mkdirSync(sessionPath(home, 10), { recursive: true });
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.release(10, { retain: false });
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(existsSync(sessionPath(home, 10))).toBe(false);
    });
  });

  it('keeps a retained session under a .failed- name', async () => {
    await withHome(async (home) => {
      mkdirSync(sessionPath(home, 10), { recursive: true });
      writeFileSync(join(sessionPath(home, 10), 'tree.txt'), '');
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.release(10, { retain: true });
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(existsSync(sessionPath(home, 10))).toBe(false);
      expect(existsSync(join(sessions(home), 'job-10.failed-1', 'tree.txt'))).toBe(true);
    });
  });

  // One repeatedly failing repository must not fill the disk one full clone
  // at a time; the cap keeps the newest forensics and drops the oldest.
  it('caps retained sessions by count, oldest mtime first', async () => {
    await withHome(async (home) => {
      mkdirSync(sessions(home), { recursive: true });
      for (let n = 1; n <= 9; n++) {
        const path = join(sessions(home), `job-1.failed-${n}`);
        mkdirSync(path, { recursive: true });
        utimesSync(path, new Date(n * 1000), new Date(n * 1000));
      }
      // A live session for another job joins the retained pool and pushes
      // ten past the cap of eight: the two oldest mtimes must go.
      mkdirSync(sessionPath(home, 2), { recursive: true });
      writeFileSync(join(sessionPath(home, 2), 'tree.txt'), '');
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.release(2, { retain: true });
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(existsSync(join(sessions(home), 'job-1.failed-1'))).toBe(false);
      expect(existsSync(join(sessions(home), 'job-1.failed-2'))).toBe(false);
      expect(existsSync(join(sessions(home), 'job-1.failed-3'))).toBe(true);
      expect(readdirSync(sessions(home)).length).toBe(8);
    });
  });

  // Regression: `slice(0, retained.length - LIMIT)` computes a negative end
  // under the cap, and `slice` reads that as an offset from the tail — so
  // retained forensics were destroyed precisely while the cap was respected.
  it('keeps every retained session while the count is under the cap', async () => {
    await withHome(async (home) => {
      mkdirSync(sessions(home), { recursive: true });
      for (let n = 1; n <= 6; n++) {
        const path = join(sessions(home), `job-1.failed-${n}`);
        mkdirSync(path, { recursive: true });
        utimesSync(path, new Date(n * 1000), new Date(n * 1000));
      }
      const release = Effect.gen(function* () {
        const manager = yield* RepositoryWorkspace;
        return yield* manager.release(2, { retain: true });
      }).pipe(Effect.provide(service(home, () => Effect.die('unused'))));
      await Effect.runPromise(release);
      for (let n = 1; n <= 6; n++) {
        expect(existsSync(join(sessions(home), `job-1.failed-${n}`))).toBe(true);
      }
      // The sweep path runs the same prune and must agree with release.
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.sweep(Effect.succeed(new Set<number>()));
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      for (let n = 1; n <= 6; n++) {
        expect(existsSync(join(sessions(home), `job-1.failed-${n}`))).toBe(true);
      }
    });
  });

  it('keeps every retained session at exactly the cap', async () => {
    await withHome(async (home) => {
      mkdirSync(sessions(home), { recursive: true });
      for (let n = 1; n <= 8; n++) {
        const path = join(sessions(home), `job-1.failed-${n}`);
        mkdirSync(path, { recursive: true });
        utimesSync(path, new Date(n * 1000), new Date(n * 1000));
      }
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          yield* manager.release(2, { retain: true });
          return yield* manager.sweep(Effect.succeed(new Set<number>()));
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(readdirSync(sessions(home)).length).toBe(8);
      for (let n = 1; n <= 8; n++) {
        expect(existsSync(join(sessions(home), `job-1.failed-${n}`))).toBe(true);
      }
    });
  });

  it('sweeps dead sessions, live ones staying put, and debris going away', async () => {
    await withHome(async (home) => {
      mkdirSync(join(sessions(home), 'job-7'), { recursive: true });
      mkdirSync(join(sessions(home), 'job-8'), { recursive: true });
      mkdirSync(join(sessions(home), 'garbage'), { recursive: true });
      mkdirSync(join(sessions(home), 'job-3.failed-1'), { recursive: true });
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.sweep(Effect.succeed(new Set([8])));
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(existsSync(join(sessions(home), 'job-7'))).toBe(false);
      expect(existsSync(join(sessions(home), 'job-8'))).toBe(true);
      expect(existsSync(join(sessions(home), 'garbage'))).toBe(false);
      // Forensic retention is swept by the cap, never by liveness.
      expect(existsSync(join(sessions(home), 'job-3.failed-1'))).toBe(true);
    });
  });

  it('succeeds when the sessions directory does not exist', async () => {
    await withHome(async (home) => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.sweep(Effect.succeed(new Set<number>()));
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );
      expect(result).toBeUndefined();
    });
  });

  // A refused credential never heals, and each retry pays for another clone.
  // git reports it only in prose on stderr, so the text is the only signal.
  it.each([
    ['remote: invalid credentials', 'WORKSPACE_CREDENTIAL_REJECTED'],
    [
      'fatal: Authentication failed for https://github.com/edloidas/lictor',
      'WORKSPACE_CREDENTIAL_REJECTED',
    ],
    ['remote: Write access to repository not granted.', 'WORKSPACE_ACCESS_DENIED'],
    // GitHub says "not found" for a private repository the credential cannot
    // see, so this must not be the terminal code that a real absence deserves.
    ['remote: Repository not found.', 'WORKSPACE_REPOSITORY_UNAVAILABLE'],
    ['You have exceeded a secondary rate limit', 'WORKSPACE_RATE_LIMITED'],
    [
      'fatal: unable to access https://github.com/edloidas/lictor: The requested URL returned error: 429',
      'WORKSPACE_RATE_LIMITED',
    ],
    ['error: something else entirely', 'WORKSPACE_CLONE_FAILED'],
  ])('classifies a failed clone reporting %p', async (stderr, code) => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        acquiring(home, () => Effect.succeed({ ...ok(), exitCode: 1, stderr })),
      );
      expect(String(exit)).toContain(code);
    });
  });

  // A refused credential heals when an operator rotates the token, so the job
  // must survive it — but not by retrying every 30 seconds and paying for a
  // clone each time.
  it('keeps a refused credential retryable but far out', async () => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        acquiring(home, () =>
          Effect.succeed({ ...ok(), exitCode: 1, stderr: 'remote: invalid credentials' }),
        ),
      );

      expect(exit._tag).toBe('Failure');
      if (exit._tag !== 'Failure') return;
      const failure = Option.getOrUndefined(Cause.failureOption(exit.cause));
      expect(failure?._tag).toBe('WorkspaceError');
      expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).not.toBe(false);
      expect(failure?._tag === 'WorkspaceError' ? failure.retryAfterMs : undefined).toBe(
        5 * 60 * 1000,
      );
    });
  });

  it('bounds every git command by the git timeout', async () => {
    await withHome(async (home) => {
      const calls: ProcessRequest[] = [];
      await Effect.runPromise(
        acquiring(
          home,
          (request) => {
            calls.push(request);
            return Effect.succeed(ok());
          },
          { ...job, ref: 'issue-24' },
        ),
      );

      // One output budget for everything, sized so TLS and credential
      // diagnostics survive truncation.
      for (const call of calls) {
        expect(call.outputLimitBytes).toBe(65_536);
      }
      // The checkout is not local bookkeeping: detaching onto `FETCH_HEAD`
      // materializes the working-tree delta, which on a large repository
      // with a cold cache routinely exceeds any short local budget — and
      // a deterministic timeout there burns the attempt budget on retries.
      expect(calls.map((call) => call.timeoutMs)).toEqual([180_000, 180_000, 180_000]);
    });
  });

  // Regression for the sweep ordering: liveness must resolve *after* the
  // listing. A session directory created while `liveJobIds` evaluates —
  // i.e. by a job that claimed and started cloning between the two — is
  // invisible to this pass even though its id is not live. Deleting it here
  // is exactly the mid-execution deletion the ordering forbids; only a
  // sweep that listed first can leave it standing.
  it('resolves liveness only after listing, so a session created mid-sweep survives', async () => {
    await withHome(async (home) => {
      mkdirSync(sessionPath(home, 7), { recursive: true });
      const liveJobIds = Effect.gen(function* () {
        yield* Effect.sync(() => mkdirSync(sessionPath(home, 99), { recursive: true }));
        return new Set<number>();
      });
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          return yield* manager.sweep(liveJobIds);
        }).pipe(Effect.provide(service(home, () => Effect.die('unused')))),
      );

      expect(existsSync(sessionPath(home, 99))).toBe(true);
      expect(existsSync(join(sessions(home), 'job-7'))).toBe(false);
    });
  });

  // Regression for the second sweep window: liveness is resolved once,
  // then entries are deleted sequentially. Between the two, an operator can
  // retry a dead job and its fresh clone lands on a directory this pass has
  // already judged deletable. The owned set is authoritative where the
  // queue's snapshot cannot be.
  it('sweep spares a session whose job the daemon currently owns', async () => {
    await withHome(async (home) => {
      mkdirSync(join(sessions(home), 'job-7'), { recursive: true });
      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          yield* manager.acquire(job, policy('allowed'));
          // Every id reports dead from the queue; only ownership saves 10.
          yield* manager.sweep(Effect.succeed(new Set<number>()));
          expect(existsSync(sessionPath(home, 10))).toBe(true);
          expect(existsSync(join(sessions(home), 'job-7'))).toBe(false);

          yield* manager.release(10, { retain: false });
          yield* manager.sweep(Effect.succeed(new Set<number>()));
          expect(existsSync(sessionPath(home, 10))).toBe(false);
        }).pipe(
          Effect.provide(
            service(home, (request) => {
              if (request.command.includes('clone')) {
                mkdirSync(sessionPath(home, 10), { recursive: true });
                writeFileSync(join(sessionPath(home, 10), 'tree.txt'), '');
              }
              return Effect.succeed(ok());
            }),
          ),
        ),
      );
    });
  });

  // The once-per-sweep variant above only pins the snapshot case: there,
  // the job is owned before `sweep` is even called. This one retries the
  // job *while* the sweep runs, after its liveness answer and ownership
  // snapshot are already behind it. The fork starts executing at the
  // sweep's first suspension — the `rm` of an earlier entry — and every
  // step of `acquire` up to the clone stub is synchronous, so by the time
  // the sweep reaches `job-10`, the fresh clone exists and is owned.
  // A snapshot of ownership taken before the loop misses it; a per-entry
  // read does not.
  it('spares a session whose job acquires mid-sweep', async () => {
    await withHome(async (home) => {
      mkdirSync(join(sessions(home), 'job-3'), { recursive: true });
      mkdirSync(join(sessions(home), 'job-5'), { recursive: true });
      // Stale leftover from a killed attempt.
      mkdirSync(sessionPath(home, 10), { recursive: true });
      writeFileSync(join(sessionPath(home, 10), 'stale.txt'), '');
      // The retry must land while the sweep still has entries ahead of
      // `job-10`. Directory order is filesystem-defined — hashed here —
      // so probe until some deletable entry precedes it.
      for (let n = 1; readdirSync(sessions(home))[0] === 'job-10'; n++) {
        mkdirSync(join(sessions(home), `debris-${n}`), { recursive: true });
      }

      // One service instance: the retry must claim ownership of the very
      // set this sweep consults.
      const workspace = service(home, (request) => {
        if (request.command.includes('clone')) {
          mkdirSync(sessionPath(home, 10), { recursive: true });
          writeFileSync(join(sessionPath(home, 10), 'tree.txt'), '');
        }
        return Effect.succeed(ok());
      });

      await Effect.runPromise(
        Effect.gen(function* () {
          const manager = yield* RepositoryWorkspace;
          // An operator retries the failed job while the sweep is running,
          // every id reporting dead from the queue. The fork begins
          // executing at the sweep's first suspension — the `rm` of an
          // entry ahead of `job-10` — so ownership lands mid-loop.
          const retry = yield* Effect.forkDaemon(manager.acquire(job, policy('allowed')));
          yield* manager.sweep(Effect.succeed(new Set<number>()));
          yield* Fiber.join(retry);
        }).pipe(Effect.provide(workspace)),
      );

      expect(existsSync(sessionPath(home, 10))).toBe(true);
      expect(existsSync(join(sessionPath(home, 10), 'tree.txt'))).toBe(true);
      // The stale attempt moved aside for forensics by the retry itself.
      expect(existsSync(join(sessions(home), 'job-10.failed-1'))).toBe(true);
    });
  });

  // A failure after cloning must retain forensics, not lose them:
  // `acquireUseRelease` runs no finalizer on a failed acquire, so without
  // quarantine inside `acquire` the next sweep deletes the evidence.
  it('quarantines the session when a ref fetch fails after cloning', async () => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        acquiring(
          home,
          (request) => {
            if (request.command.includes('clone')) {
              mkdirSync(sessionPath(home, 10), { recursive: true });
              writeFileSync(join(sessionPath(home, 10), 'tree.txt'), '');
              return Effect.succeed(ok());
            }
            if (request.command.includes('fetch')) {
              return Effect.succeed({
                ...ok(),
                exitCode: 1,
                stderr: "fatal: couldn't find remote ref no-such-ref",
              });
            }
            return Effect.succeed(ok());
          },
          { ...job, ref: 'no-such-ref' },
        ),
      );

      expect(String(exit)).toContain('WORKSPACE_REF_UNAVAILABLE');
      expect(existsSync(sessionPath(home, 10))).toBe(false);
      expect(existsSync(join(sessions(home), 'job-10.failed-1', 'tree.txt'))).toBe(true);
      expect(
        readdirSync(sessions(home)).filter((name) => name.startsWith('job-10.failed-')),
      ).toHaveLength(1);
    });
  });

  it('quarantines the session when acquire is interrupted mid-clone', async () => {
    await withHome(async (home) => {
      const exit = await Effect.runPromiseExit(
        acquiring(home, (request) => {
          if (request.command.includes('clone')) {
            mkdirSync(sessionPath(home, 10), { recursive: true });
            return Effect.interrupt;
          }
          return Effect.succeed(ok());
        }),
      );

      expect(Exit.isInterrupted(exit)).toBe(true);
      expect(existsSync(sessionPath(home, 10))).toBe(false);
      expect(
        readdirSync(sessions(home)).filter((name) => name.startsWith('job-10.failed-')),
      ).toHaveLength(1);
    });
  });

  describe('disk pressure', () => {
    it('prunes past the retention cap and re-probes before succeeding', async () => {
      await withHome(async (home) => {
        mkdirSync(sessions(home), { recursive: true });
        for (let n = 1; n <= 3; n++) {
          const path = join(sessions(home), `job-1.failed-${n}`);
          mkdirSync(path, { recursive: true });
          utimesSync(path, new Date(n * 1000), new Date(n * 1000));
        }
        let probes = 0;
        const calls: ProcessRequest[] = [];
        await Effect.runPromise(
          acquiring(
            home,
            (request) => {
              calls.push(request);
              return Effect.succeed(ok());
            },
            job,
            () => {
              probes += 1;
              // Under the floor once, comfortably above it after reclaiming.
              return { bavail: probes === 1 ? 100 : 8 * 1024 * 1024, bsize: 512 };
            },
          ),
        );

        expect(calls.some((call) => call.command.includes('clone'))).toBe(true);
        // Forensics are worth less than a daemon that runs: reclaim went
        // below the cap of eight, all the way to zero retained sessions.
        expect(readdirSync(sessions(home)).filter((name) => name.includes('.failed-'))).toEqual([]);
        expect(probes).toBe(2);
      });
    });

    // Regression: a large orphan named after a live job is unreachable by
    // `pruneRetained` (wrong name shape) and by the sweep (job is live). If
    // the disk probe ran before its own leftover was reclaimed, acquire
    // would fail WORKSPACE_DISK_EXHAUSTED on the orphan's dead weight and
    // wedge every job until an operator intervened.
    it('reclaims its own live-job leftover under the floor and proceeds', async () => {
      await withHome(async (home) => {
        mkdirSync(sessionPath(home, 10), { recursive: true });
        writeFileSync(join(sessionPath(home, 10), 'orphan.txt'), '');
        let probes = 0;
        const calls: ProcessRequest[] = [];
        await Effect.runPromise(
          acquiring(
            home,
            (request) => {
              calls.push(request);
              return Effect.succeed(ok());
            },
            job,
            () => {
              probes += 1;
              // Under the floor until the leftover's space is actually gone.
              return { bavail: probes === 1 ? 100 : 8 * 1024 * 1024, bsize: 512 };
            },
          ),
        );

        // Deleted by the under-floor prune, not renamed: a rename frees
        // nothing, which is what wedged the old ordering.
        expect(existsSync(join(sessions(home), 'job-10.failed-1'))).toBe(false);
        expect(existsSync(sessionPath(home, 10))).toBe(false);
        expect(calls.some((call) => call.command.includes('clone'))).toBe(true);
      });
    });

    it('fails slowly retryable when reclaiming still leaves the disk under the floor', async () => {
      await withHome(async (home) => {
        const exit = await Effect.runPromiseExit(
          acquiring(
            home,
            () => Effect.die('git must not run'),
            job,
            () => ({ bavail: 100, bsize: 512 }),
          ),
        );

        expect(String(exit)).toContain('WORKSPACE_DISK_EXHAUSTED');
        if (exit._tag !== 'Failure') return;
        const failure = Option.getOrUndefined(Cause.failureOption(exit.cause));
        expect(failure?._tag).toBe('WorkspaceError');
        expect(failure?._tag === 'WorkspaceError' ? failure.retryable : undefined).not.toBe(false);
        expect(failure?._tag === 'WorkspaceError' ? failure.retryAfterMs : undefined).toBe(
          15 * 60 * 1000,
        );
      });
    });

    it('reports a failed probe as its own error, not a full disk', async () => {
      await withHome(async (home) => {
        const exit = await Effect.runPromiseExit(
          acquiring(
            home,
            () => Effect.die('git must not run'),
            job,
            () => {
              throw new Error('ENOENT from statfs');
            },
          ),
        );

        expect(String(exit)).toContain('WORKSPACE_DISK_PROBE_FAILED');
        expect(String(exit)).not.toContain('WORKSPACE_DISK_EXHAUSTED');
      });
    });
  });
});
