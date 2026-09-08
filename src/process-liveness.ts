/**
 * Whether a pid names a process that is still running.
 *
 * ! `EPERM` is a live process this user may not signal, and every other error
 * ! means the question went unanswered — reading either as dead is what lets
 * ! one daemon delete another's live state. Absence has to be proven, never
 * ! inferred from a failure to ask.
 *
 * A pid can be recycled, so a true answer means "some process holds this
 * number", never "that process". Callers that act destructively on a false
 * answer need a second reason to believe the number still refers to the owner
 * they recorded.
 */
export const processAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code !== 'ESRCH';
  }
};
