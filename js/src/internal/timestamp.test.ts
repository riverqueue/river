import { describe, expect, it } from "vitest";

import { postgresTimestamp } from "./timestamp.js";

describe("postgresTimestamp", () => {
  it.each([
    ["2026-01-01T00:00:00.0000019Z", "2026-01-01T00:00:00.000001Z"],
    ["2026-01-01T00:00:00.0000015Z", "2026-01-01T00:00:00.000001Z"],
    ["2026-01-01T00:00:00.123456Z", "2026-01-01T00:00:00.123456Z"],
    ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
    // Before the epoch, pgx still truncates toward the past.
    ["1969-12-31T23:59:59.9999999Z", "1969-12-31T23:59:59.999999Z"],
  ])("truncates %s to microseconds like Go's pgx", (input, expected) => {
    expect(postgresTimestamp(Temporal.Instant.from(input))).toBe(expected);
  });
});
