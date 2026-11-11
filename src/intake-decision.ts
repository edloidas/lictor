import { Schema } from 'effect';

/** Why intake turned a notification into no work. Fixed codes, never thread text. */
export const IGNORE_REASONS = [
  'activity_time_unreadable',
  'machine_traffic',
  'no_subject_number',
  'untrusted_sender',
  'no_trigger',
  'policy_denied',
] as const;
export type IgnoreReason = (typeof IGNORE_REASONS)[number];

/** Something intake noticed on its way to the decision, without it being the decision. */
export const INTAKE_NOTES = ['subject_closed'] as const;
export type IntakeNote = (typeof INTAKE_NOTES)[number];

/**
 * What one delivery decided, recorded on its row when it finishes.
 *
 * An answer is its own field rather than a decision: the window that carried
 * it can also carry a new trigger, and then the delivery did both.
 */
export type IntakeDecision = {
  readonly decision: 'queued' | 'duplicate' | 'answered' | 'ignored';
  readonly reason?: IgnoreReason;
  readonly notes?: readonly IntakeNote[];
  readonly jobId?: number;
  readonly answered?: { readonly jobId: number; readonly resumed: boolean };
};

export const IntakeDecisionSchema: Schema.Schema<IntakeDecision> = Schema.Struct({
  decision: Schema.Literal('queued', 'duplicate', 'answered', 'ignored'),
  reason: Schema.optionalWith(Schema.Literal(...IGNORE_REASONS), { exact: true }),
  notes: Schema.optionalWith(Schema.Array(Schema.Literal(...INTAKE_NOTES)), { exact: true }),
  jobId: Schema.optionalWith(Schema.Number, { exact: true }),
  answered: Schema.optionalWith(Schema.Struct({ jobId: Schema.Number, resumed: Schema.Boolean }), {
    exact: true,
  }),
});
