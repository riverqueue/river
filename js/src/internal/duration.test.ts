import { describe, expect, it } from "vitest";

import { ValidationError } from "../errors.js";
import {
  millisecondsToDuration,
  toDuration,
  toMilliseconds,
  toNullableMilliseconds,
} from "./duration.js";

describe("durations", () => {
  it("converts duration-likes to whole milliseconds, rounding up", () => {
    expect(toMilliseconds("x", { seconds: 5 })).toBe(5_000);
    expect(toMilliseconds("x", { days: 1 })).toBe(86_400_000);
    expect(toMilliseconds("x", { microseconds: 1 })).toBe(1);
    expect(toMilliseconds("x", Temporal.Duration.from("PT1M30S"))).toBe(90_000);
    expect(toMilliseconds("x", { seconds: 0 }, { allowZero: true })).toBe(0);
    expect(toNullableMilliseconds("x", null)).toBeNull();
    expect(millisecondsToDuration(90_061_001).toString()).toBe("PT25H1M1.001S");
  });

  it("rejects numbers, calendar units, and out-of-range values", () => {
    expect(() => toDuration("x", 5 as never)).toThrow("not a bare number");
    expect(() => toDuration("x", { months: 1 })).toThrow("calendar units");
    expect(() => toDuration("x", { seconds: -1 })).toThrow("negative");
    expect(() => toDuration("x", { seconds: 0 })).toThrow("positive");
    expect(() => toDuration("x", { hours: 3_000_000 })).toThrow("maximum");
    expect(() => toDuration("x", {}, { error: ValidationError })).toThrow(
      ValidationError
    );
  });
});
