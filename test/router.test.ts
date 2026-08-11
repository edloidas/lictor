import { describe, expect, it } from 'bun:test';
import { Effect, Ref } from 'effect';
import { GitHubClient } from '../src/github/client.ts';
import type { Delivery } from '../src/webhook/event.ts';
import { dispatch, type Registry } from '../src/webhook/router.ts';

/** Handlers carry `GitHubClient` in their requirements; the router never calls it. */
const stubClient = GitHubClient.make({
  forInstallation: () => Effect.die('the router must not reach GitHub'),
});

const delivery = (event: string, action?: string): Delivery => ({
  event,
  id: 'd-1',
  payload: action === undefined ? {} : { action },
});

/** Runs a dispatch and reports which registry keys fired, in order. */
const run = (registry: (log: Ref.Ref<string[]>) => Registry, received: Delivery) =>
  Effect.runSync(
    Effect.gen(function* () {
      const log = yield* Ref.make<string[]>([]);
      yield* dispatch(registry(log))(received);
      return yield* Ref.get(log);
    }).pipe(Effect.provideService(GitHubClient, stubClient)),
  );

const record = (log: Ref.Ref<string[]>, label: string) => () =>
  Ref.update(log, (entries) => [...entries, label]);

describe('dispatch', () => {
  it('runs the handler registered for the bare event', () => {
    expect(run((log) => ({ ping: record(log, 'ping') }), delivery('ping'))).toEqual(['ping']);
  });

  it('prefers the event.action key over the bare event', () => {
    const registry = (log: Ref.Ref<string[]>) => ({
      issues: record(log, 'issues'),
      'issues.opened': record(log, 'issues.opened'),
    });

    expect(run(registry, delivery('issues', 'opened'))).toEqual(['issues.opened']);
  });

  it('falls back to the bare event when the action is not registered', () => {
    const registry = (log: Ref.Ref<string[]>) => ({ issues: record(log, 'issues') });

    expect(run(registry, delivery('issues', 'labeled'))).toEqual(['issues']);
  });

  it('drops an unregistered event without failing', () => {
    expect(run((log) => ({ ping: record(log, 'ping') }), delivery('push'))).toEqual([]);
  });

  it('runs nothing when the registry is empty', () => {
    expect(run(() => ({}), delivery('ping'))).toEqual([]);
  });
});
