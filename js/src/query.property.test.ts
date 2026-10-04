import { Buffer } from "node:buffer";

import fc from "fast-check";
import { describe, expect, it } from "vitest";

import type { JobListCursorValue, JobListOrderBy } from "./driver.js";
import { ValidationError } from "./errors.js";
import type { JobRow, JobState } from "./job.js";
import {
  encodeJobListCursor,
  encodeJobListCursorValue,
  encodeQueueCursor,
  normalizeJobListOptions,
  normalizeQueueListOptions,
} from "./query.js";

const INT8_MAX = 9_223_372_036_854_775_807n;
const rawJson = (JSON as unknown as { rawJSON(text: string): unknown }).rawJSON;

// Years 0000 through 9999, which Go's time JSON supports, except Go's zero
// time: a cursor carries that for no time.
const GO_ZERO_TIME_NS = -62_135_596_800n * 10n ** 9n;
const instantArbitrary = fc
  .bigInt({
    max: 253_402_300_799_999_999_999n,
    min: -62_167_219_200n * 10n ** 9n,
  })
  .filter((nanoseconds) => nanoseconds !== GO_ZERO_TIME_NS)
  .map((nanoseconds) => Temporal.Instant.fromEpochNanoseconds(nanoseconds));

const idArbitrary = fc.oneof(
  fc.bigInt({ max: INT8_MAX, min: 1n }),
  fc.bigInt({ max: 4096n, min: 0n }).map((offset) => INT8_MAX - offset),
  fc
    .bigInt({ max: 4096n, min: -4096n })
    .map((offset) => BigInt(Number.MAX_SAFE_INTEGER) + offset)
);

const nameArbitrary = fc.oneof(
  fc.string({ minLength: 1, maxLength: 12 }),
  fc.string({ minLength: 1, maxLength: 6, unit: "binary" })
);

const cursorValueArbitrary: fc.Arbitrary<JobListCursorValue> = fc.oneof(
  fc.record({
    id: idArbitrary,
    kind: nameArbitrary,
    queue: nameArbitrary,
    sortField: fc.constant("id" as const),
    time: fc.constant(null),
  }),
  fc.record({
    id: idArbitrary,
    kind: nameArbitrary,
    queue: nameArbitrary,
    sortField: fc.constantFrom<JobListOrderBy>(
      "finalizedAt",
      "scheduledAt",
      "time"
    ),
    time: instantArbitrary,
  })
);

const stateArbitrary = fc.constantFrom<JobState>(
  "available",
  "cancelled",
  "completed",
  "discarded",
  "pending",
  "retryable",
  "running",
  "scheduled"
);

function decode(after: string, orderBy: JobListOrderBy): JobListCursorValue {
  const { after: decoded } = normalizeJobListOptions({
    after,
    orderBy,
    // finalizedAt ordering is only valid over finalized states.
    ...(orderBy === "finalizedAt"
      ? { states: ["cancelled", "completed", "discarded"] }
      : {}),
  });
  if (decoded === null) throw new Error("cursor decoded to null");
  return decoded;
}

/** Only a rejected cursor may throw, and only as a River validation error. */
function decodeOrReject(after: string): JobListCursorValue | ValidationError {
  for (const orderBy of ["finalizedAt", "id", "scheduledAt", "time"] as const) {
    try {
      return decode(after, orderBy);
    } catch (error: unknown) {
      if (!(error instanceof ValidationError)) throw error;
      if (!/cursor/.test(error.message)) throw error;
    }
  }
  return new ValidationError("invalid job list cursor");
}

describe("job list cursor properties", () => {
  it("round-trips every exact cursor value", () => {
    fc.assert(
      fc.property(cursorValueArbitrary, (value) => {
        const cursor = encodeJobListCursorValue(value);
        // Go's padded URL-safe base64.
        expect(cursor).toMatch(/^[A-Za-z0-9_-]+={0,2}$/);
        expect(cursor.length % 4).toBe(0);
        expect(decode(cursor, value.sortField)).toEqual(value);
        for (const other of ["finalizedAt", "id", "scheduledAt", "time"]) {
          if (other === value.sortField) continue;
          expect(() => decode(cursor, other as JobListOrderBy)).toThrow(
            ValidationError
          );
        }
      }),
      { numRuns: 500 }
    );
  });

  it("encodes the time field each ordering sorts by, for every listed state", () => {
    const finalizedStates: readonly JobState[] = [
      "cancelled",
      "completed",
      "discarded",
    ];
    fc.assert(
      fc.property(
        idArbitrary,
        stateArbitrary,
        fc.uniqueArray(stateArbitrary),
        fc.option(instantArbitrary, { nil: null }),
        instantArbitrary,
        instantArbitrary,
        (id, state, states, attemptedAt, scheduledAt, finalizedAt) => {
          const finalized = finalizedStates.includes(state);
          const job = {
            attemptedAt,
            finalizedAt: finalized ? finalizedAt : null,
            id,
            kind: "kind",
            queue: "queue",
            scheduledAt,
            state,
          } as unknown as JobRow;
          // Like Go, `time` orders every listed job by the first listed
          // state's field, whatever the job's own state.
          const first = states[0];
          const timeField =
            first === "running"
              ? "attemptedAt"
              : first !== undefined && finalizedStates.includes(first)
                ? "finalizedAt"
                : "scheduledAt";
          const onlyFinalized =
            states.length > 0 &&
            states.every((listed) => finalizedStates.includes(listed));
          for (const orderBy of [
            "finalizedAt",
            "id",
            "scheduledAt",
            "time",
          ] as const) {
            const field = orderBy === "time" ? timeField : orderBy;
            const params = { sortField: orderBy, states };
            if (field === "id") {
              expect(decode(encodeJobListCursor(job, params), orderBy)).toEqual(
                {
                  id,
                  kind: "kind",
                  queue: "queue",
                  sortField: "id",
                  time: null,
                }
              );
              continue;
            }
            const time = job[field];
            const nullable =
              field === "attemptedAt" ||
              (field === "finalizedAt" && !onlyFinalized);
            if (time === null && !nullable) {
              expect(() => encodeJobListCursor(job, params)).toThrow(
                ValidationError
              );
              continue;
            }
            const cursor = encodeJobListCursor(job, params);
            if (orderBy === "finalizedAt" && !onlyFinalized) continue;
            expect(decode(cursor, orderBy)).toEqual({
              id,
              kind: "kind",
              queue: "queue",
              sortField: orderBy,
              time,
            });
          }
        }
      ),
      { numRuns: 300 }
    );
  });

  it("rejects arbitrary input only with a cursor validation error", () => {
    const payloadArbitrary = fc.oneof(
      fc.string(),
      fc.string({ unit: "binary" }),
      fc.json(),
      fc
        .record(
          {
            id: fc.oneof(
              fc.bigInt().map((value) => rawJson(value.toString())),
              fc.bigInt().map(String),
              fc.string(),
              fc.integer(),
              fc.double(),
              fc.constant(null)
            ),
            kind: fc.oneof(fc.string(), fc.integer()),
            queue: fc.oneof(fc.string(), fc.constant(null)),
            sort_field: fc.oneof(
              fc.constantFrom("finalized_at", "id", "scheduled_at", "time"),
              fc.string()
            ),
            time: fc.oneof(
              fc.constant(null),
              instantArbitrary.map(String),
              fc.constant("0001-01-01T00:00:00Z"),
              fc.string()
            ),
            unknown: fc.json(),
          },
          { requiredKeys: [] }
        )
        .map((value) => JSON.stringify(value))
    );
    const cursorArbitrary = fc.oneof(
      fc.string({ maxLength: 40 }),
      payloadArbitrary.map((payload) =>
        Buffer.from(payload).toString("base64url")
      ),
      payloadArbitrary.map((payload) => Buffer.from(payload).toString("base64"))
    );
    fc.assert(
      fc.property(cursorArbitrary, (cursor) => {
        const result = decodeOrReject(cursor);
        if (result instanceof ValidationError) return;
        // Anything accepted is a well-formed cursor value.
        expect(
          decode(encodeJobListCursorValue(result), result.sortField)
        ).toEqual(result);
      }),
      { numRuns: 1_000 }
    );
  });

  it("accepts an edited cursor only when it still encodes a valid value", () => {
    fc.assert(
      fc.property(
        cursorValueArbitrary,
        fc.nat(),
        fc.string({ maxLength: 2 }),
        (value, position, insertion) => {
          const cursor = encodeJobListCursorValue(value);
          const at = position % (cursor.length + 1);
          const edited = `${cursor.slice(0, at)}${insertion}${cursor.slice(at + 1)}`;
          const result = decodeOrReject(edited);
          if (result instanceof ValidationError) return;
          // An accepted edit is at most a different spelling of a valid
          // cursor (for example an escaped character), never a lossy one.
          expect(() =>
            new TextDecoder("utf-8", { fatal: true }).decode(
              Buffer.from(edited, "base64url")
            )
          ).not.toThrow();
          expect(
            decode(encodeJobListCursorValue(result), result.sortField)
          ).toEqual(result);
        }
      ),
      { numRuns: 500 }
    );
  });

  it("round-trips queue cursors and rejects other payloads", () => {
    fc.assert(
      fc.property(nameArbitrary, fc.string(), (name, garbage) => {
        const cursor = encodeQueueCursor({ name } as never);
        expect(normalizeQueueListOptions({ after: cursor }).nameAfter).toBe(
          name
        );
        try {
          const decoded = normalizeQueueListOptions({ after: garbage });
          expect(encodeQueueCursor({ name: decoded.nameAfter } as never)).toBe(
            garbage
          );
        } catch (error: unknown) {
          expect(error).toBeInstanceOf(ValidationError);
        }
      }),
      { numRuns: 300 }
    );
  });
});
