import { describe, expect, it } from 'bun:test';
import { Schema } from 'effect';
import {
  describeGrantedTools,
  type GrantCapabilities,
  GrantSchema,
  grantCapabilities,
  grantedTools,
  grantNarrowing,
  intersectGrant,
  mintGrant,
  toolCapabilities,
} from '../src/github/grant.ts';
import type { Capabilities, RepositoryPolicy } from '../src/policy.ts';
import type { WorkItem } from '../src/work-item.ts';

const capabilities = (overrides: Partial<Capabilities> = {}): Capabilities => ({
  read: true,
  comment: false,
  issues: false,
  branches: false,
  pullRequests: false,
  merge: false,
  forcePush: false,
  deleteBranches: false,
  scripts: [],
  ...overrides,
});

const policy = (overrides: Partial<RepositoryPolicy> = {}): RepositoryPolicy => ({
  repository: 'edloidas/lictor',
  accepted: true,
  execution: 'automatic',
  clone: 'denied',
  capabilities: capabilities(),
  trustedSenders: ['edloidas'],
  maxAttempts: 3,
  maxDurationMs: 30 * 60 * 1000,
  ...overrides,
});

const work: WorkItem = {
  deliveryId: 'delivery-1',
  interactionId: 'interaction-1',
  repository: 'edloidas/lictor',
  sender: 'edloidas',
  targets: ['adiutriel'],
  reasons: ['mentioned'],
  subject: {
    kind: 'issue',
    number: 17,
    title: 'Something',
    url: 'https://github.com/edloidas/lictor/issues/17',
  },
};

const everything = capabilities({
  comment: true,
  issues: true,
  branches: true,
  pullRequests: true,
  merge: true,
  forcePush: true,
  deleteBranches: true,
});

describe('toolCapabilities', () => {
  // Pins what `ToolCapability` encodes: a filter comparing against these two
  // would have dead arms.
  it('maps no tool to force-push or branch deletion', () => {
    const spent = new Set(Object.values(toolCapabilities));

    expect(spent).not.toContain('forcePush');
    expect(spent).not.toContain('deleteBranches');
    expect(spent).toContain('merge');
  });
});

describe('grantCapabilities', () => {
  it('settles an absent policy capability as denied rather than undefined', () => {
    const settled = grantCapabilities({ scripts: [] } as unknown as Capabilities);

    expect(settled).toEqual({
      read: false,
      comment: false,
      issues: false,
      branches: false,
      pullRequests: false,
      merge: false,
      forcePush: false,
      deleteBranches: false,
    });
  });
});

describe('mintGrant', () => {
  it('records the repository policy as the authority the job was admitted under', () => {
    const grant = mintGrant(policy({ capabilities: everything }), work, 1_700_000_000_000);

    expect(grant.version).toBe(1);
    expect(grant.repository).toBe('edloidas/lictor');
    expect(grant.interactionId).toBe('interaction-1');
    expect(grant.decision).toBe('automatic');
    expect(grant.capabilities.merge).toBe(true);
    expect(grant.maxAttempts).toBe(3);
    expect(grant.mintedAt).toBe(1_700_000_000_000);
    expect(Schema.is(GrantSchema)(grant)).toBe(true);
  });

  // Only `approve` writes the literal `false`; an automatic repository leaves the
  // field absent, so reading it as approved would credit an operator who never acted.
  it('calls the decision approved only where a hold was actually released', () => {
    expect(mintGrant(policy(), work, 0).decision).toBe('automatic');
    expect(mintGrant(policy(), { ...work, approvalRequired: true }, 0).decision).toBe('automatic');
    expect(mintGrant(policy(), { ...work, approvalRequired: false }, 0).decision).toBe('approved');
  });

  it('moves the fingerprint on an authority change', () => {
    const before = mintGrant(policy(), work, 0).fingerprint;

    expect(mintGrant(policy({ capabilities: everything }), work, 0).fingerprint).not.toBe(before);
    expect(mintGrant(policy({ maxAttempts: 4 }), work, 0).fingerprint).not.toBe(before);
    expect(mintGrant(policy({ execution: 'approval' }), work, 0).fingerprint).not.toBe(before);
  });

  // A fingerprint that moves on a field the grant does not bound reports a
  // change no reader can act on.
  it('ignores policy that bounds neither the tools nor the budgets', () => {
    const before = mintGrant(policy(), work, 0).fingerprint;

    expect(mintGrant(policy({ trustedSenders: ['someone-else'] }), work, 0).fingerprint).toBe(
      before,
    );
    expect(
      mintGrant(policy({ capabilities: capabilities({ scripts: ['bun test'] }) }), work, 0)
        .fingerprint,
    ).toBe(before);
  });

  it('does not move the fingerprint with the mint time', () => {
    expect(mintGrant(policy(), work, 0).fingerprint).toBe(
      mintGrant(policy(), work, 99).fingerprint,
    );
  });
});

describe('intersectGrant', () => {
  const wide = mintGrant(policy({ capabilities: everything, maxAttempts: 5 }), work, 0);
  const narrow = mintGrant(policy({ maxAttempts: 2 }), work, 0);

  // ! The whole point of storing a grant: policy edited after the mint may take
  // ! authority away and may never add any.
  it('never grants what the stored record withheld', () => {
    const effective = intersectGrant(narrow, wide);

    expect(effective.capabilities.merge).toBe(false);
    expect(effective.capabilities.comment).toBe(false);
    expect(effective.maxAttempts).toBe(2);
  });

  it('applies a tightening that landed after the mint', () => {
    const effective = intersectGrant(wide, narrow);

    expect(effective.capabilities.merge).toBe(false);
    expect(effective.capabilities.read).toBe(true);
    expect(effective.maxAttempts).toBe(2);
  });

  // The record says what was authorized; the intersection says what may run now.
  // Overwriting the identity would leave nothing able to answer the first.
  it('keeps the identity of the record it was stored under', () => {
    const effective = intersectGrant(
      { ...narrow, decision: 'approved', mintedAt: 42 },
      { ...wide, mintedAt: 99 },
    );

    expect(effective.decision).toBe('approved');
    expect(effective.mintedAt).toBe(42);
    expect(effective.fingerprint).toBe(narrow.fingerprint);
  });
});

describe('grantNarrowing', () => {
  const wide = mintGrant(policy({ capabilities: everything, maxAttempts: 5 }), work, 0);
  const narrow = mintGrant(policy({ maxAttempts: 2 }), work, 0);

  it('names every capability current policy took, and the budgets it lowered', () => {
    const taken = grantNarrowing(wide, narrow);

    expect(taken?.withheld).toEqual([
      'comment',
      'issues',
      'branches',
      'pullRequests',
      'merge',
      'forcePush',
      'deleteBranches',
    ]);
    expect(taken?.grantFingerprint).toBe(wide.fingerprint);
    expect(taken?.policyFingerprint).toBe(narrow.fingerprint);
    expect(taken?.maxAttempts).toBe(2);
  });

  // A fingerprint that merely differs is not a narrowing: policy may have
  // widened, and recording that would say the job ran narrower than it did.
  it('reports nothing where policy widened after the mint', () => {
    expect(grantNarrowing(narrow, wide)).toBeUndefined();
  });

  it('reports nothing where policy has not moved', () => {
    expect(grantNarrowing(wide, wide)).toBeUndefined();
  });

  // The capability set can be untouched while the run budget is not.
  it('reports a lowered duration on its own', () => {
    const shorter = mintGrant(
      policy({ capabilities: everything, maxAttempts: 5, maxDurationMs: 60_000 }),
      work,
      0,
    );
    const taken = grantNarrowing(wide, shorter);

    expect(taken?.withheld).toEqual([]);
    expect(taken?.maxDurationMs).toBe(60_000);
    expect(taken?.maxAttempts).toBeUndefined();
  });
});

describe('grantedTools', () => {
  const granted = (overrides: Partial<GrantCapabilities>, narrowed = false) =>
    grantedTools({ ...grantCapabilities(everything), ...overrides }, narrowed);

  it('offers only what the capabilities cover', () => {
    const tools = grantedTools(grantCapabilities(capabilities()), false);

    expect(tools).toContain('get_issue');
    expect(tools).not.toContain('create_comment');
    expect(tools).not.toContain('merge_pull_request');
  });

  it('withholds the merge escalation from a continuation and keeps the rest', () => {
    expect(granted({}, true)).not.toContain('merge_pull_request');
    expect(granted({}, false)).toContain('merge_pull_request');
    expect(granted({}, true)).toContain('update_branch');
    expect(granted({}, true)).toContain('create_comment');
  });
});

describe('describeGrantedTools', () => {
  // The shipped example policy grants `branches` and denies `forcePush`; a bare
  // name there advertises a call that answers `CAPABILITY_DENIED`.
  it('qualifies update_branch where force pushing is denied', () => {
    const described = describeGrantedTools(
      grantCapabilities(capabilities({ branches: true })),
      false,
    );

    expect(described).toContain('update_branch (never with force)');
    expect(described).not.toContain('update_branch');
  });

  it('leaves it unqualified where force pushing is granted', () => {
    const described = describeGrantedTools(grantCapabilities(everything), false);

    expect(described).toContain('update_branch');
    expect(described).not.toContain('update_branch (never with force)');
  });

  // The broker refuses a forced move on a continuation however generous policy
  // is, so the capability alone would advertise a call that answers
  // `CAPABILITY_DENIED`.
  it('qualifies it on a continuation even where policy grants force pushing', () => {
    const described = describeGrantedTools(grantCapabilities(everything), true);

    expect(described).toContain('update_branch (never with force)');
    expect(described).not.toContain('update_branch');
  });
});
