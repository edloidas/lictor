import { cpSync, mkdirSync, mkdtempSync, readdirSync, renameSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Data, Effect } from 'effect';

export class SkillInstallError extends Data.TaggedError('SkillInstallError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * Skills this daemon ships, read from its own source tree. Never from a job's
 * workspace: a skill there is whatever the pull request under review says.
 */
export const bundledSkillsDir = join(import.meta.dir, '../../skills');

/** The review procedure every review job carries, by the name it is installed under. */
export const reviewSkillName = 'lictor-review';

/**
 * Replaces each bundled skill under `<codexHome>/skills/<name>` with the copy
 * this daemon ships, so an edit or a manual change there lasts one restart.
 * Staged beside the target and renamed into place, which keeps a half-copied
 * tree from ever carrying the skill's name. A skill the operator installed
 * under another name is left alone.
 */
export const installSkills = (
  sourceDir: string,
  codexHome: string,
): Effect.Effect<readonly string[], SkillInstallError> =>
  Effect.try({
    try: () => {
      const target = join(codexHome, 'skills');
      mkdirSync(target, { recursive: true, mode: 0o700 });
      const installed: string[] = [];
      for (const entry of readdirSync(sourceDir, { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        const staging = mkdtempSync(join(codexHome, '.skill-staging-'));
        try {
          const staged = join(staging, entry.name);
          cpSync(join(sourceDir, entry.name), staged, { recursive: true });
          rmSync(join(target, entry.name), { recursive: true, force: true });
          renameSync(staged, join(target, entry.name));
        } finally {
          rmSync(staging, { recursive: true, force: true });
        }
        installed.push(entry.name);
      }
      return installed;
    },
    catch: (cause) =>
      new SkillInstallError({
        message: `Could not install the bundled skills into ${join(codexHome, 'skills')}`,
        cause,
      }),
  });

/** A skill's instructions without the frontmatter that only a harness reads. */
export const skillBody = (text: string): string => {
  const match = /^---\r?\n[\s\S]*?\r?\n---\r?\n/.exec(text);
  return (match === null ? text : text.slice(match[0].length)).trim();
};
