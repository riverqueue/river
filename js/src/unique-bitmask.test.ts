import { describe, it, expect } from "vitest";
import {
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
} from "./unique-bitmask.js";
import type { JobState } from "./job.js";
import { JOB_STATE } from "./job.js";

describe("uniqueBitmaskFromStates", () => {
  it("produces correct bitmask for individual states", () => {
    // Bit positions (in the 8-char string, left to right):
    //   0=scheduled, 1=running, 2=retryable, 3=pending,
    //   4=discarded, 5=completed, 6=cancelled, 7=available
    expect(uniqueBitmaskFromStates([JOB_STATE.available])).toBe("00000001");
    expect(uniqueBitmaskFromStates([JOB_STATE.cancelled])).toBe("00000010");
    expect(uniqueBitmaskFromStates([JOB_STATE.completed])).toBe("00000100");
    expect(uniqueBitmaskFromStates([JOB_STATE.discarded])).toBe("00001000");
    expect(uniqueBitmaskFromStates([JOB_STATE.pending])).toBe("00010000");
    expect(uniqueBitmaskFromStates([JOB_STATE.retryable])).toBe("00100000");
    expect(uniqueBitmaskFromStates([JOB_STATE.running])).toBe("01000000");
    expect(uniqueBitmaskFromStates([JOB_STATE.scheduled])).toBe("10000000");
  });

  it("combines multiple states", () => {
    expect(
      uniqueBitmaskFromStates([JOB_STATE.available, JOB_STATE.scheduled])
    ).toBe("10000001");

    expect(
      uniqueBitmaskFromStates([
        JOB_STATE.available,
        JOB_STATE.running,
        JOB_STATE.scheduled,
      ])
    ).toBe("11000001");
  });

  it("produces correct bitmask for default unique states", () => {
    // Default: available, completed, pending, retryable, running, scheduled
    const defaults: JobState[] = [
      JOB_STATE.available,
      JOB_STATE.completed,
      JOB_STATE.pending,
      JOB_STATE.retryable,
      JOB_STATE.running,
      JOB_STATE.scheduled,
    ];
    expect(uniqueBitmaskFromStates(defaults)).toBe("11110101");
  });

  it("produces correct bitmask for all states", () => {
    const all: JobState[] = [
      JOB_STATE.available,
      JOB_STATE.cancelled,
      JOB_STATE.completed,
      JOB_STATE.discarded,
      JOB_STATE.pending,
      JOB_STATE.retryable,
      JOB_STATE.running,
      JOB_STATE.scheduled,
    ];
    expect(uniqueBitmaskFromStates(all)).toBe("11111111");
  });

  it("returns all zeros for empty array", () => {
    expect(uniqueBitmaskFromStates([])).toBe("00000000");
  });
});

describe("uniqueBitmaskToStates", () => {
  it("decodes individual bits", () => {
    expect(uniqueBitmaskToStates(0b00000001)).toEqual([JOB_STATE.available]);
    expect(uniqueBitmaskToStates(0b10000000)).toEqual([JOB_STATE.scheduled]);
  });

  it("decodes combined bitmask", () => {
    // available + scheduled
    expect(uniqueBitmaskToStates(0b10000001)).toEqual(
      [JOB_STATE.available, JOB_STATE.scheduled].sort()
    );
  });

  it("returns empty array for zero", () => {
    expect(uniqueBitmaskToStates(0)).toEqual([]);
  });

  it("returns sorted states", () => {
    const states = uniqueBitmaskToStates(0b11111111);
    const sorted = [...states].sort();
    expect(states).toEqual(sorted);
  });
});

describe("round-trip", () => {
  it("fromStates then toStates returns original states sorted", () => {
    const states: JobState[] = [
      JOB_STATE.running,
      JOB_STATE.available,
      JOB_STATE.pending,
    ];
    const bitmask = uniqueBitmaskFromStates(states);
    const result = uniqueBitmaskToStates(parseInt(bitmask, 2));
    expect(result).toEqual([...states].sort());
  });

  it("round-trips all states", () => {
    const all: JobState[] = [
      JOB_STATE.available,
      JOB_STATE.cancelled,
      JOB_STATE.completed,
      JOB_STATE.discarded,
      JOB_STATE.pending,
      JOB_STATE.retryable,
      JOB_STATE.running,
      JOB_STATE.scheduled,
    ];
    const bitmask = uniqueBitmaskFromStates(all);
    const result = uniqueBitmaskToStates(parseInt(bitmask, 2));
    expect(result).toEqual([...all].sort());
  });
});
