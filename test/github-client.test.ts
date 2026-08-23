import { describe, expect, it } from 'bun:test';
import { HttpClient, HttpClientResponse } from '@effect/platform';
import { Effect, Layer, Redacted } from 'effect';
import { LictorConfig } from '../src/config.ts';
import { GitHubClient } from '../src/github/client.ts';
import { GitHubCredential } from '../src/github/credential.ts';
import type { ContextRef } from '../src/work-item.ts';

const ConfigLive = Layer.succeed(
  LictorConfig,
  LictorConfig.make({
    githubToken: Redacted.make('pat-value'),
    expectedLogin: 'adiutriel',
    trustedSenders: ['edloidas'],
    autoAcceptInviters: [],
    databasePath: ':memory:',
    policyPath: 'unused',
    controlSocketPath: '/tmp/lictor-client.sock',
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
    workerRetryBaseMs: 1,
    notificationPollMs: 60_000,
  }),
);

const react = (target: ContextRef, status = 201) => {
  const calls: string[] = [];
  const bodies: string[] = [];
  const stub = HttpClient.make((request) => {
    calls.push(`${request.method} ${request.url}`);
    const body = request.body as { readonly body?: unknown };
    bodies.push(body.body instanceof Uint8Array ? new TextDecoder().decode(body.body) : '');
    return Effect.succeed(
      HttpClientResponse.fromWeb(request, new Response(JSON.stringify({ id: 1 }), { status })),
    );
  });
  return Effect.runPromise(
    Effect.either(
      Effect.flatMap(GitHubClient, (github) =>
        github.addReaction('edloidas/lictor', target, 'eyes'),
      ).pipe(
        Effect.provide(
          GitHubClient.DefaultWithoutDependencies.pipe(
            Layer.provide(
              Layer.merge(
                Layer.succeed(HttpClient.HttpClient, stub),
                GitHubCredential.DefaultWithoutDependencies.pipe(Layer.provide(ConfigLive)),
              ),
            ),
          ),
        ),
      ),
    ),
  ).then((result) => ({ result, calls, bodies }));
};

describe('GitHubClient.addReaction', () => {
  // ! Three targets, three endpoints. A url built from the wrong one answers 404
  // ! and the acknowledgement silently never appears.
  it('posts an issue-comment reaction to the issues comment endpoint', async () => {
    const { result, calls, bodies } = await react({ kind: 'issue_comment', id: 99 });

    expect(result._tag).toBe('Right');
    expect(calls[0]).toBe(
      'POST https://api.github.com/repos/edloidas/lictor/issues/comments/99/reactions',
    );
    expect(bodies[0]).toBe('{"content":"eyes"}');
  });

  it('posts a review-comment reaction to the pulls comment endpoint', async () => {
    const { calls } = await react({ kind: 'review_comment', id: 501 });

    expect(calls[0]).toBe(
      'POST https://api.github.com/repos/edloidas/lictor/pulls/comments/501/reactions',
    );
  });

  it('posts a body reaction to the issue itself', async () => {
    const { calls } = await react({ kind: 'body', number: 17 });

    expect(calls[0]).toBe('POST https://api.github.com/repos/edloidas/lictor/issues/17/reactions');
  });

  // ! GitHub answers 200 rather than 201 when the reaction is already there,
  // ! which is the normal case for a replayed notification.
  it('treats an existing reaction as success', async () => {
    const { result } = await react({ kind: 'issue_comment', id: 99 }, 200);

    expect(result._tag).toBe('Right');
  });

  it('fails with GitHubRequestError on a refusal', async () => {
    const { result } = await react({ kind: 'issue_comment', id: 99 }, 403);

    expect(result._tag).toBe('Left');
    expect(String(result)).toContain('GitHubRequestError');
  });
});
