import { describe, it, expect } from "vitest";
import {
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
} from "./unique-bitmask.js";
import type { JobState } from "./job.js";
import {
  JOB_STATE_AVAILABLE,
  JOB_STATE_CANCELLED,
  JOB_STATE_COMPLETED,
  JOB_STATE_DISCARDED,
  JOB_STATE_PENDING,
  JOB_STATE_RETRYABLE,
  JOB_STATE_RUNNING,
  JOB_STATE_SCHEDULED,
} from "./job.js";

describe("uniqueBitmaskFromStates", () => {
  it("produces correct bitmask for individual states", () => {
    // Bit positions (in the 8-char string, left to right):
    //   0=scheduled, 1=running, 2=retryable, 3=pending,
    //   4=discarded, 5=completed, 6=cancelled, 7=available
    expect(uniqueBitmaskFromStates([JOB_STATE_AVAILABLE])).toBe("00000001");
    expect(uniqueBitmaskFromStates([JOB_STATE_CANCELLED])).toBe("00000010");
    expect(uniqueBitmaskFromStates([JOB_STATE_COMPLETED])).toBe("00000100");
    expect(uniqueBitmaskFromStates([JOB_STATE_DISCARDED])).toBe("00001000");
    expect(uniqueBitmaskFromStates([JOB_STATE_PENDING])).toBe("00010000");
    expect(uniqueBitmaskFromStates([JOB_STATE_RETRYABLE])).toBe("00100000");
    expect(uniqueBitmaskFromStates([JOB_STATE_RUNNING])).toBe("01000000");
    expect(uniqueBitmaskFromStates([JOB_STATE_SCHEDULED])).toBe("10000000");
  });

  it("combines multiple states", () => {
    expect(
      uniqueBitmaskFromStates([JOB_STATE_AVAILABLE, JOB_STATE_SCHEDULED])
    ).toBe("10000001");

    expect(
      uniqueBitmaskFromStates([
        JOB_STATE_AVAILABLE,
        JOB_STATE_RUNNING,
        JOB_STATE_SCHEDULED,
      ])
    ).toBe("11000001");
  });

  it("produces correct bitmask for default unique states", () => {
    // Default: available, completed, pending, retryable, running, scheduled
    const defaults: JobState[] = [
      JOB_STATE_AVAILABLE,
      JOB_STATE_COMPLETED,
      JOB_STATE_PENDING,
      JOB_STATE_RETRYABLE,
      JOB_STATE_RUNNING,
      JOB_STATE_SCHEDULED,
    ];
    expect(uniqueBitmaskFromStates(defaults)).toBe("11110101");
  });

  it("produces correct bitmask for all states", () => {
    const all: JobState[] = [
      JOB_STATE_AVAILABLE,
      JOB_STATE_CANCELLED,
      JOB_STATE_COMPLETED,
      JOB_STATE_DISCARDED,
      JOB_STATE_PENDING,
      JOB_STATE_RETRYABLE,
      JOB_STATE_RUNNING,
      JOB_STATE_SCHEDULED,
    ];
    expect(uniqueBitmaskFromStates(all)).toBe("11111111");
  });

  it("returns all zeros for empty array", () => {
    expect(uniqueBitmaskFromStates([])).toBe("00000000");
  });
});

describe("uniqueBitmaskToStates", () => {
  it("decodes individual bits", () => {
    expect(uniqueBitmaskToStates(0b00000001)).toEqual([JOB_STATE_AVAILABLE]);
    expect(uniqueBitmaskToStates(0b10000000)).toEqual([JOB_STATE_SCHEDULED]);
  });

  it("decodes combined bitmask", () => {
    // available + scheduled
    expect(uniqueBitmaskToStates(0b10000001)).toEqual(
      [JOB_STATE_AVAILABLE, JOB_STATE_SCHEDULED].sort()
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
      JOB_STATE_RUNNING,
      JOB_STATE_AVAILABLE,
      JOB_STATE_PENDING,
    ];
    const bitmask = uniqueBitmaskFromStates(states);
    const result = uniqueBitmaskToStates(parseInt(bitmask, 2));
    expect(result).toEqual([...states].sort());
  });

  it("round-trips all states", () => {
    const all: JobState[] = [
      JOB_STATE_AVAILABLE,
      JOB_STATE_CANCELLED,
      JOB_STATE_COMPLETED,
      JOB_STATE_DISCARDED,
      JOB_STATE_PENDING,
      JOB_STATE_RETRYABLE,
      JOB_STATE_RUNNING,
      JOB_STATE_SCHEDULED,
    ];
    const bitmask = uniqueBitmaskFromStates(all);
    const result = uniqueBitmaskToStates(parseInt(bitmask, 2));
    expect(result).toEqual([...all].sort());
  });
});
