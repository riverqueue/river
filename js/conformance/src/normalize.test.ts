import { exactJsonNumber, type JobRow } from "riverqueue";
import { describe, expect, it } from "vitest";

import { exactJsonTokens, normalizeJob } from "./normalize.js";

describe("normalizeJob", () => {
  it("retains exact IDs and removes the private uniqueness nonce", () => {
    const job: JobRow = {
      args: {
        decimal: exactJsonNumber("0.12345678901234567890123456789"),
        integer: exactJsonNumber("9223372036854775807"),
        message: "hello",
      },
      attempt: 1,
      attemptedAt: Temporal.Instant.from("2026-01-02T03:04:05.123456Z"),
      attemptedBy: ["javascript"],
      createdAt: Temporal.Instant.from("2026-01-02T03:04:05.123456Z"),
      errors: [],
      finalizedAt: null,
      id: 9_007_199_254_740_993n,
      kind: "conformance_echo",
      maxAttempts: 25,
      metadata: {
        beyond_float: exactJsonNumber("1e400"),
        big_integer: exactJsonNumber("123456789012345678901234567890"),
        long_decimal: exactJsonNumber("0.1000000000000000055511151231257827"),
        negative: exactJsonNumber("-9223372036854775808"),
        owner: "application",
        "river:unique_nonce": "internal",
      },
      priority: 1,
      queue: "default",
      scheduledAt: Temporal.Instant.from("2026-01-02T03:04:05.123456Z"),
      state: "running",
      tags: [],
      uniqueKey: Uint8Array.from([0, 1, 254, 255]),
      uniqueStates: ["running", "available"],
    };
    expect(normalizeJob(job)).toMatchObject({
      attempted_at: "2026-01-02T03:04:05.123456Z",
      id: 9_007_199_254_740_993n,
      metadata: { owner: "application" },
      unique_key: "0001feff",
      unique_states: ["available", "running"],
    });
    expect(exactJsonTokens(job)).toEqual({
      beyond_float: "1e400",
      big_integer: "123456789012345678901234567890",
      decimal: "0.12345678901234567890123456789",
      integer: "9223372036854775807",
      long_decimal: "0.1000000000000000055511151231257827",
      negative: "-9223372036854775808",
    });
  });
});
