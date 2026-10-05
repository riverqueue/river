import type { JobState } from "./job.js";
import { JOB_STATE } from "./job.js";

const JOB_STATE_BIT_POSITIONS: Record<JobState, number> = {
  [JOB_STATE.available]: 7,
  [JOB_STATE.cancelled]: 6,
  [JOB_STATE.completed]: 5,
  [JOB_STATE.discarded]: 4,
  [JOB_STATE.pending]: 3,
  [JOB_STATE.retryable]: 2,
  [JOB_STATE.running]: 1,
  [JOB_STATE.scheduled]: 0,
};

/** Convert an array of job states to an 8-bit bitmask string. */
export function uniqueBitmaskFromStates(states: readonly JobState[]): string {
  let val = 0;
  for (const state of states) {
    const bitIndex = JOB_STATE_BIT_POSITIONS[state];
    const bitPosition = 7 - (bitIndex % 8);
    val |= 1 << bitPosition;
  }
  return val.toString(2).padStart(8, "0");
}

/** Convert a bitmask integer to an array of job states. */
export function uniqueBitmaskToStates(mask: number): JobState[] {
  const states: JobState[] = [];
  for (const [state, bitIndex] of Object.entries(JOB_STATE_BIT_POSITIONS)) {
    const bitPosition = 7 - (bitIndex % 8);
    if ((mask & (1 << bitPosition)) !== 0) {
      states.push(state as JobState);
    }
  }
  return states.sort();
}
