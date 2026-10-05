import { describe, expect, it } from "vitest";

import { parseGoDuration } from "./go-duration.js";

const NANOSECOND = 1n;
const MICROSECOND = 1_000n;
const MILLISECOND = 1_000_000n;
const SECOND = 1_000_000_000n;
const MINUTE = 60n * SECOND;
const HOUR = 60n * MINUTE;
const MAX_INT64 = (1n << 63n) - 1n;
const MIN_INT64 = -(1n << 63n);

describe("parseGoDuration", () => {
  // Go's `parseDurationTests` from time/time_test.go.
  it.each<[string, bigint]>([
    // simple
    ["0", 0n],
    ["5s", 5n * SECOND],
    ["30s", 30n * SECOND],
    ["1478s", 1478n * SECOND],
    // sign
    ["-5s", -5n * SECOND],
    ["+5s", 5n * SECOND],
    ["-0", 0n],
    ["+0", 0n],
    // decimal
    ["5.0s", 5n * SECOND],
    ["5.6s", 5n * SECOND + 600n * MILLISECOND],
    ["5.s", 5n * SECOND],
    [".5s", 500n * MILLISECOND],
    ["1.0s", SECOND],
    ["1.00s", SECOND],
    ["1.004s", SECOND + 4n * MILLISECOND],
    ["1.0040s", SECOND + 4n * MILLISECOND],
    ["100.00100s", 100n * SECOND + MILLISECOND],
    // different units
    ["10ns", 10n * NANOSECOND],
    ["11us", 11n * MICROSECOND],
    ["12\u00b5s", 12n * MICROSECOND],
    ["12\u03bcs", 12n * MICROSECOND],
    ["13ms", 13n * MILLISECOND],
    ["14s", 14n * SECOND],
    ["15m", 15n * MINUTE],
    ["16h", 16n * HOUR],
    // composite durations
    ["3h30m", 3n * HOUR + 30n * MINUTE],
    ["10.5s4m", 4n * MINUTE + 10n * SECOND + 500n * MILLISECOND],
    ["-2m3.4s", -(2n * MINUTE + 3n * SECOND + 400n * MILLISECOND)],
    [
      "1h2m3s4ms5us6ns",
      HOUR +
        2n * MINUTE +
        3n * SECOND +
        4n * MILLISECOND +
        5n * MICROSECOND +
        6n,
    ],
    [
      "39h9m14.425s",
      39n * HOUR + 9n * MINUTE + 14n * SECOND + 425n * MILLISECOND,
    ],
    // large value
    ["52763797000ns", 52_763_797_000n],
    // more than 9 digits after the decimal point
    ["0.3333333333333333333h", 20n * MINUTE],
    // 2^53 + 1 cannot be stored precisely in a float64
    ["9007199254740993ns", (1n << 53n) + 1n],
    // largest duration an int64 of nanoseconds represents
    ["9223372036854775807ns", MAX_INT64],
    ["9223372036854775.807us", MAX_INT64],
    ["9223372036s854ms775us807ns", MAX_INT64],
    ["-9223372036854775808ns", MIN_INT64],
    ["-9223372036854775.808us", MIN_INT64],
    ["-9223372036s854ms775us808ns", MIN_INT64],
    // largest negative round trip value
    ["-2562047h47m16.854775808s", MIN_INT64],
    // huge fraction
    ["0.100000000000000000000h", 6n * MINUTE],
    // the first overflow check in leadingFraction
    ["0.830103483285477580700h", 49n * MINUTE + 48n * SECOND + 372_539_827n],
  ])("parses %j", (text, expected) => {
    expect(parseGoDuration(text)).toBe(expected);
  });

  // Go's `parseDurationErrorTests` from time/time_test.go.
  it.each([
    "",
    "3",
    "-",
    "s",
    ".",
    "-.",
    ".s",
    "+.s",
    "1d",
    "\u0085\u0085",
    "\ufffd",
    "\ufffd hello \ufffd world",
    // overflow
    "9223372036854775810ns",
    "9223372036854775808ns",
    "-9223372036854775809ns",
    "9223372036854776us",
    "3000000h",
    "9223372036854775.808us",
    "9223372036854ms775us808ns",
  ])("rejects %j", (text) => {
    // Like Go's test, require only that the error quotes the input.
    expect(() => parseGoDuration(text)).toThrow(JSON.stringify(text));
  });

  it("reports missing and unknown units like Go", () => {
    expect(() => parseGoDuration("1h5")).toThrow(
      'time: missing unit in duration "1h5"'
    );
    expect(() => parseGoDuration("5x")).toThrow(
      'time: unknown unit "x" in duration "5x"'
    );
    expect(() => parseGoDuration("1h ")).toThrow(
      'time: unknown unit "h " in duration "1h "'
    );
  });

  it("wraps a uint64 sum of exactly 2^64 to zero as Go does", () => {
    expect(parseGoDuration("9223372036854775808ns9223372036854775808ns")).toBe(
      0n
    );
  });
});
