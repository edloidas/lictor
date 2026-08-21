import { Data, Effect, Redacted } from 'effect';
import { LictorConfig } from '../config.ts';

export class GitHubCredentialError extends Data.TaggedError('GitHubCredentialError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * The single place the daemon's GitHub credential is produced.
 *
 * Nothing downstream learns which kind of credential it holds: {@link GitHubClient}
 * asks for a token, {@link RepositoryWorkspace} asks for a finished `git` header.
 * Swapping a static token for one that has to be refreshed is a change to this
 * file alone.
 *
 * Both members are `Effect` rather than plain values, and both declare
 * {@link GitHubCredentialError} even though a static token cannot fail. A
 * credential that mints or refreshes on demand needs exactly that shape, and
 * widening an error channel later would ripple through every call site.
 */
export class GitHubCredential extends Effect.Service<GitHubCredential>()('GitHubCredential', {
  effect: Effect.gen(function* () {
    const config = yield* LictorConfig;

    const token: Effect.Effect<Redacted.Redacted<string>, GitHubCredentialError> = Effect.succeed(
      config.githubToken,
    );

    /**
     * `git` over HTTPS wants Basic auth. Bearer is the documented scheme for App
     * installation tokens only — with a token in the password field git never
     * even parses a Bearer value and reports `invalid credentials`, which reads
     * like a revoked token rather than a wrong scheme.
     *
     * The username is the literal `x-access-token` rather than the account
     * login. GitHub ignores it when the password is a token, so this is the one
     * form verified to work, and it keeps `git` auth from depending on a config
     * value whose real job is the startup identity assertion.
     */
    const gitAuthHeader: Effect.Effect<
      Redacted.Redacted<string>,
      GitHubCredentialError
    > = Effect.map(token, (value) =>
      Redacted.make(
        `Basic ${Buffer.from(`x-access-token:${Redacted.value(value)}`).toString('base64')}`,
      ),
    );

    return { token, gitAuthHeader };
  }),
  dependencies: [LictorConfig.Default],
}) {}
