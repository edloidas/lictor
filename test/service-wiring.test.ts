import { describe, expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = join(import.meta.dir, '..');

const sources = (dir: string): string[] =>
  readdirSync(join(root, dir), { recursive: true, encoding: 'utf8' })
    .filter((name) => name.endsWith('.ts'))
    .map((name) => join(dir, name));

const serviceClass = /export class (\w+) extends Effect\.Service<\1>\(\)\(/g;

// A service that declares `dependencies:` has them baked into `.Default`, and
// a stub provided from outside can never reach them. Only the services below
// carry that hazard; the leaves are fine either way.
const bakingServices = (): string[] =>
  sources('src').flatMap((file) => {
    const text = readFileSync(join(root, file), 'utf8');
    // Scoped to each service's own block, not the whole file: two services can
    // share a file, and attributing one's `dependencies:` to the other flags a
    // leaf whose only wiring form is `.Default`.
    const declarations = [...text.matchAll(serviceClass)];
    return declarations.flatMap((match, index) => {
      const name = match[1];
      const body = text.slice(match.index, declarations[index + 1]?.index ?? text.length);
      return name === undefined || !/^\s*dependencies:/m.test(body) ? [] : [name];
    });
  });

describe('service wiring in tests', () => {
  test('finds the services that bake dependencies', () => {
    expect(bakingServices()).toEqual(
      expect.arrayContaining(['DeliveryWorker', 'Policy', 'Worker']),
    );
    // Declared above `RepositoryWorkspace` in one file, and a leaf: proof the
    // scan reads service blocks rather than whole files.
    expect(bakingServices()).not.toContain('DiskStat');
  });

  test('no test provides a dependency-baking service as bare .Default', () => {
    const baking = bakingServices();
    const offenders = sources('test').flatMap((file) => {
      const text = readFileSync(join(root, file), 'utf8');
      return baking
        .filter((name) => new RegExp(`\\b${name}\\.Default\\b`).test(text))
        .map((name) => `${file}: ${name}.Default`);
    });
    expect(offenders).toEqual([]);
  });
});
