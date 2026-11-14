import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import type { JobRow } from "../job.js";
import {
  isExactJsonNumber,
  jsonNumberToBigInt,
  parseJson,
  type ExactJsonNumber,
  type JsonObject,
  type JsonValue,
} from "../json.js";
import { snooze } from "../worker.js";
import { completionCommand, defaultNextRetry } from "./completion-command.js";

/**
 * A retry delay case from River Go's protocol goldens: the bounds, in
 * nanoseconds, of the delay its default retry policy schedules after
 * `error_count` errors, jitter included.
 */
interface RetryCase {
  readonly error_count: number;
  readonly job_id: ExactJsonNumber | number;
  readonly max_delay_ns: ExactJsonNumber | number;
  readonly min_delay_ns: ExactJsonNumber | number;
  readonly now: string;
  readonly seed: ExactJsonNumber | number;
}

interface SnoozeCounterCase {
  readonly expected_snoozes: JsonValue;
  readonly metadata: JsonObject;
  readonly name: string;
}

/** River Go's protocol goldens, including its retry delay bounds. */
const PROTOCOL_GOLDENS = new URL(
  "../../../conformance/testdata/protocol_values.json",
  import.meta.url
);

/** River Go's snooze counter goldens, generated from its executor. */
const GOLDENS = new URL(
  "../../../conformance/testdata/snooze_counters.json",
  import.meta.url
);

/**
 * Reads a fixture that `make generate/fixtures` writes from River's Go
 * implementation. A missing fixture fails the test rather than skipping it.
 */
async function readFixture(url: URL): Promise<string> {
  try {
    return await readFile(url, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `missing conformance fixture ${fileURLToPath(url)}; run \`make generate/fixtures\` from the repository root`,
        { cause: error }
      );
    }
    throw error;
  }
}

describe("completionCommand", () => {
  it("counts snoozes like River for Go's executor", async () => {
    // Parse with River's exact JSON so integers beyond 2^53 stay exact.
    const golden = parseJson(await readFixture(GOLDENS)) as unknown as {
      readonly snooze_counters: readonly SnoozeCounterCase[];
    };
    expect(golden.snooze_counters.length).toBeGreaterThan(0);
    const results = golden.snooze_counters.map(({ metadata, name }) => [
      name,
      numberText(snoozeCommand(metadata).metadata?.snoozes),
    ]);

    expect(results).toEqual(
      golden.snooze_counters.map(({ expected_snoozes, name }) => [
        name,
        numberText(expected_snoozes),
      ])
    );
  });

  it.each([
    "2.9",
    "-2",
    "1e400",
    "9223372036854775807",
    "9223372036854775808",
    '"4"',
    '"4.5"',
    '"1e3"',
    '" 5"',
    '"abc"',
    "true",
    "false",
    "null",
    "[3]",
    '{"count":3}',
  ])("recovers safely from invalid snooze counter %s", (raw) => {
    const metadata = parseJson(`{"snoozes":${raw}}`) as JsonObject;
    const command = snoozeCommand(metadata);

    expect(command.kind).toBe("snooze");
    expect(command.scheduledAt).not.toBeNull();
    expect(BigInt(numberText(command.metadata?.snoozes))).toBeGreaterThan(0n);
  });
});

describe("defaultNextRetry", () => {
  // `conformance.test.ts` checks every Go retry case stays within its
  // bounds across the jitter range. This also checks that no jitter gives
  // exactly Go's lower bound, so the base delay matches Go's and isn't
  // merely close to it.
  it("schedules Go's minimum delay without jitter", async () => {
    // Parse with River's exact JSON so nanosecond bounds beyond 2^53 stay
    // exact.
    const golden = parseJson(
      await readFixture(PROTOCOL_GOLDENS)
    ) as unknown as { readonly retry_cases: readonly RetryCase[] };
    expect(golden.retry_cases.length).toBeGreaterThan(0);

    const delays = golden.retry_cases.map((retryCase) => {
      const now = Temporal.Instant.from(retryCase.now);
      const errors = Array.from(
        { length: retryCase.error_count - 1 },
        (_, i) => ({
          at: now,
          attempt: i + 1,
          error: "failed",
          trace: "",
        })
      );
      const retryJob = {
        ...job({}),
        attempt: retryCase.error_count,
        errors,
        id: jsonNumberToBigInt(retryCase.job_id),
        maxAttempts: retryCase.error_count + 1,
      };
      return [
        retryCase.error_count,
        defaultNextRetry(retryJob, now, () => 0).epochNanoseconds -
          now.epochNanoseconds,
      ];
    });

    expect(delays).toEqual(
      golden.retry_cases.map(({ error_count, min_delay_ns }) => [
        error_count,
        jsonNumberToBigInt(min_delay_ns),
      ])
    );
  });
});

function job(metadata: JsonObject): JobRow {
  const at = Temporal.Instant.from("2026-09-01T00:00:00Z");
  return {
    args: {},
    attempt: 1,
    attemptedAt: at,
    attemptedBy: ["client"],
    createdAt: at,
    errors: [],
    finalizedAt: null,
    id: 1n,
    kind: "snooze",
    maxAttempts: 25,
    metadata,
    priority: 1,
    queue: "default",
    scheduledAt: at,
    state: "running",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

/** A JSON number's exact text, so ordinary and exact numbers compare. */
function numberText(value: JsonValue | undefined): string {
  if (typeof value === "number") return String(value);
  if (isExactJsonNumber(value)) return value.rawJSON;
  throw new Error(`not a JSON number: ${JSON.stringify(value)}`);
}

function snoozeCommand(metadata: JsonObject) {
  const now = Temporal.Instant.from("2026-09-01T00:00:00Z");
  return completionCommand(
    job(metadata),
    "client",
    { outcome: snooze({ seconds: 30 }), status: "succeeded" },
    now,
    now,
    now,
    5_000
  );
}
