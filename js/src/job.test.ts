import { describe, expect, it } from "vitest";

import { JOB_STATE, jobFromJsonValue, jobToJsonValue } from "./job.js";
import type { JobRow } from "./job.js";

describe("job JSON helpers", () => {
  it("round trips exact IDs, nanosecond instants, bytes, and JSON", () => {
    const job: JobRow = {
      args: { message: "hello" },
      attempt: 1,
      attemptedAt: Temporal.Instant.from("2026-08-30T12:34:56.123456789Z"),
      attemptedBy: ["client-a"],
      createdAt: Temporal.Instant.from("2026-08-30T12:00:00.000001Z"),
      errors: [
        {
          at: Temporal.Instant.from("2026-08-30T12:30:00.000002Z"),
          attempt: 1,
          error: "failed",
          trace: "trace",
        },
      ],
      finalizedAt: null,
      id: 9_223_372_036_854_775_807n,
      kind: "email",
      maxAttempts: 25,
      metadata: { nested: { value: true } },
      priority: 1,
      queue: "default",
      scheduledAt: Temporal.Instant.from("2026-08-30T12:00:00.000003Z"),
      state: JOB_STATE.available,
      tags: ["tag-one"],
      uniqueKey: Uint8Array.of(0, 15, 255),
      uniqueStates: null,
    };

    const json = jobToJsonValue(job);
    expect(json.id).toBe("9223372036854775807");
    expect(json.attemptedAt).toBe("2026-08-30T12:34:56.123456789Z");
    expect(json.uniqueKey).toBe("000fff");
    expect(json.uniqueStates).toBeNull();
    expect(() => JSON.stringify(json)).not.toThrow();

    const decoded = jobFromJsonValue(JSON.parse(JSON.stringify(json)));
    expect(decoded).toEqual(job);
    expect(decoded.id).toBe(9_223_372_036_854_775_807n);
    expect(decoded.createdAt.epochNanoseconds).toBe(
      job.createdAt.epochNanoseconds
    );
  });

  it("rejects lossy or unknown wire values", () => {
    const valid = jobToJsonValue({
      args: {},
      attempt: 0,
      attemptedAt: null,
      attemptedBy: [],
      createdAt: Temporal.Instant.from("2026-01-01T00:00:00Z"),
      errors: [],
      finalizedAt: null,
      id: 1n,
      kind: "test",
      maxAttempts: 1,
      metadata: {},
      priority: 1,
      queue: "default",
      scheduledAt: Temporal.Instant.from("2026-01-01T00:00:00Z"),
      state: JOB_STATE.available,
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    });

    // Go's SQLite driver accepts rows with zero max attempts; so does River
    // JS, so their JSON form must round-trip too.
    expect(jobFromJsonValue({ ...valid, maxAttempts: 0 }).maxAttempts).toBe(0);
    expect(() => jobFromJsonValue({ ...valid, maxAttempts: -1 })).toThrow();
    expect(() => jobFromJsonValue({ ...valid, id: 1 })).toThrow(
      "job.id: must be a string"
    );
    expect(() => jobFromJsonValue({ ...valid, state: "future_state" })).toThrow(
      "unknown River job state"
    );
    expect(() => jobFromJsonValue({ ...valid, uniqueKey: "ABC" })).toThrow(
      "lowercase hexadecimal"
    );
  });
});
