import { Buffer } from "node:buffer";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it, onTestFinished, vi } from "vitest";

import { testSqliteMemory } from "../driver/sqlite/src/driver.js";
import { createMigrator } from "../migrate/src/index.js";
import { buildUniqueKey, type Client } from "./client.js";
import { decodeAttemptError, decodeJobState } from "./driver-codecs.js";
import { ValidationError } from "./errors.js";
import type { UniqueOptions } from "./insert-options.js";
import { UNIQUE_INSERT_NONCE_KEY } from "./internal/postgres-capabilities.js";
import { defineJob } from "./job-definition.js";
import {
  JOB_STATE,
  jobToJsonValue,
  type JobRow,
  type JobState,
} from "./job.js";
import {
  jsonNumberToBigInt,
  parseJson,
  stringifyJson,
  type ExactJsonNumber,
  type JsonObject,
} from "./json.js";
import { buildPeriodicInsert, periodicJob } from "./periodic.js";
import { Resumable } from "./resumable.js";
import { defaultNextRetry } from "./runtime/completion-command.js";
import {
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
} from "./unique-bitmask.js";

interface UniqueKeyCase {
  readonly args: JsonObject;
  readonly expected_error?: string;
  readonly expected_sha256?: string;
  readonly expected_state_mask: number;
  readonly kind: string;
  readonly name: string;
  readonly now: string;
  readonly options: {
    readonly by_args: boolean;
    readonly by_period_nanos: number;
    readonly by_queue: boolean;
    readonly by_state?: readonly JobState[];
    readonly exclude_kind: boolean;
  };
  readonly queue: string;
  readonly scheduled_at: string | null;
  readonly selected_unique_components?: readonly (readonly string[])[];
}

/** Keep large argument integers and retry bounds exact when reading Go JSON. */
async function readFixture(name: string) {
  const url = new URL(
    `../../conformance/testdata/${name}.json`,
    import.meta.url
  );
  try {
    return parseJson(await readFile(url, "utf8"));
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

const uniqueKeys = (await readFixture("unique_keys")) as unknown as {
  readonly cases: readonly UniqueKeyCase[];
  readonly typed_only_cases: readonly UniqueKeyCase[];
};
const protocol = (await readFixture("protocol_values")) as unknown as {
  readonly attempt_error: JsonObject;
  readonly job_states: readonly { state: string; unique_bit: number }[];
  readonly metadata_keys: {
    readonly output: string;
    readonly periodic_job_id: string;
    readonly rescue_count: string;
    readonly resumable_cursor: string;
    readonly resumable_step: string;
    readonly unique_nonce: string;
  };
  readonly retry_cases: readonly {
    error_count: number;
    job_id: ExactJsonNumber | number;
    max_delay_ns: ExactJsonNumber | number;
    min_delay_ns: ExactJsonNumber | number;
    now: string;
  }[];
};

describe("Go unique-key fixtures", () => {
  it.each(uniqueKeys.cases)("$name", (fixture) => {
    const now = Temporal.Instant.from(fixture.now);
    const options: UniqueOptions = {
      ...(fixture.options.by_args
        ? {
            byArgs: fixture.selected_unique_components?.length
              ? fixture.selected_unique_components.map((segments) =>
                  segments
                    .map((segment) => segment.replace(/[.\\]/g, "\\$&"))
                    .join(".")
                )
              : true,
          }
        : {}),
      ...(fixture.options.by_period_nanos > 0
        ? { byPeriod: { nanoseconds: fixture.options.by_period_nanos } }
        : {}),
      ...(fixture.options.by_state === undefined
        ? {}
        : { byState: fixture.options.by_state }),
      byQueue: fixture.options.by_queue,
      excludeKind: fixture.options.exclude_kind,
    };
    const build = () =>
      buildUniqueKey(
        {
          args: fixture.args,
          kind: fixture.kind,
          queue: fixture.queue,
          // Client insertion resolves an absent scheduled time to now.
          scheduledAt:
            fixture.scheduled_at === null
              ? now
              : Temporal.Instant.from(fixture.scheduled_at),
        },
        options
      );

    if (fixture.expected_error !== undefined) {
      expect(fixture.expected_error).toBe("rejected");
      expect(build).toThrow(ValidationError);
    } else {
      const [key, states] = build();
      expect(Buffer.from(key).toString("hex")).toBe(fixture.expected_sha256);
      expect(Number.parseInt(uniqueBitmaskFromStates(states), 2)).toBe(
        fixture.expected_state_mask
      );
    }
  });

  it("keeps the unsupported raw JSON cases explicit", () => {
    // JavaScript objects cannot preserve duplicate keys or insertion order
    // for integer-like keys. Rust can test these via its raw JSON API.
    expect(uniqueKeys.typed_only_cases.map(({ name }) => name).sort()).toEqual([
      "typed_duplicate_top_level_keys",
      "typed_integer_like_map_keys",
    ]);
  });
});

describe("Go protocol fixtures", () => {
  it("decodes every state and preserves its uniqueness bit in both directions", () => {
    expect(protocol.job_states.map(({ state }) => state).sort()).toEqual(
      Object.values(JOB_STATE).sort()
    );
    for (const { state, unique_bit: bit } of protocol.job_states) {
      const decoded = decodeJobState(state);
      expect(Number.parseInt(uniqueBitmaskFromStates([decoded]), 2)).toBe(bit);
      expect(uniqueBitmaskToStates(bit)).toEqual([decoded]);
    }
  });

  it("reads and writes Go's attempt error without losing fields or timestamp precision", () => {
    const error = decodeAttemptError(stringifyJson(protocol.attempt_error));
    // Fixed semantic expectations also catch a renamed field being silently ignored.
    expect(error).toEqual({
      at: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
      attempt: 3,
      error: 'worker failed: escaped "detail"',
      trace: "frame one\nframe two",
    });
    expect(jobToJsonValue({ ...job(), errors: [error] }).errors).toEqual([
      protocol.attempt_error,
    ]);
  });

  // Go and JS have different random generators. Check the shared bounds,
  // including both jitter extremes and the maximum time.Duration cap.
  describe.each(protocol.retry_cases)(
    "retry with $error_count errors",
    (fixture) => {
      it.each([0, 0.5, 1 - Number.EPSILON])(
        "jitter %s stays inside Go's bounds",
        (random) => {
          const now = Temporal.Instant.from(fixture.now);
          const row = {
            ...job(),
            id: jsonNumberToBigInt(fixture.job_id),
            errors: Array.from({ length: fixture.error_count - 1 }, () =>
              decodeAttemptError(stringifyJson(protocol.attempt_error))
            ),
          };
          const delay =
            defaultNextRetry(row, now, () => random).epochNanoseconds -
            now.epochNanoseconds;
          expect(delay).toBeGreaterThanOrEqual(
            jsonNumberToBigInt(fixture.min_delay_ns)
          );
          expect(delay).toBeLessThanOrEqual(
            jsonNumberToBigInt(fixture.max_delay_ns)
          );
        }
      );
    }
  );

  it("uses Go's periodic job ID and unique insert nonce keys", async () => {
    const periodic = periodicJob({
      args: {},
      every: { hours: 1 },
      id: "conformance_periodic",
      job: defineJob({ kind: "periodic" }),
    });
    const insert = await buildPeriodicInsert({
      job: periodic,
      scheduledAt: job().scheduledAt,
    });
    expect(insert?.options?.metadata).toEqual({
      periodic: true,
      [protocol.metadata_keys.periodic_job_id]: "conformance_periodic",
    });
    expect(UNIQUE_INSERT_NONCE_KEY).toBe(protocol.metadata_keys.unique_nonce);
  });

  it("resumes from and writes Go's resumable step and cursor keys", async () => {
    const keys = protocol.metadata_keys;
    const resumable = new Resumable({} as Client, {
      ...job(),
      metadata: {
        [keys.resumable_cursor]: { process: { offset: 2 } },
        [keys.resumable_step]: "process",
      },
    });
    const before = vi.fn();
    await resumable.step("before", before);
    expect(before).not.toHaveBeenCalled();
    await expect(
      resumable.stepWithCursor("process", (cursor) => {
        expect(cursor).toEqual({ offset: 2 });
        resumable.setCursor({ offset: 3 });
        throw new Error("retry");
      })
    ).rejects.toThrow("failed");
    expect(resumable.finish(true).metadata).toEqual({
      [keys.resumable_cursor]: { process: { offset: 3 } },
      [keys.resumable_step]: "process",
    });
  });

  it("persists output and increments Go's rescue counter", async () => {
    const driver = testSqliteMemory();
    const database = driver.connect();
    onTestFinished(() => {
      database.close();
      driver.close();
    });
    await createMigrator({ database }).migrateUp();
    const now = Temporal.Now.instant();
    const inserted = await driver.jobInsert({
      args: {},
      kind: "conformance",
      metadata: { [protocol.metadata_keys.rescue_count]: 2 },
      scheduledAt: now,
    });
    expect(inserted.job.metadata[protocol.metadata_keys.unique_nonce]).toEqual(
      expect.any(String)
    );
    await driver.jobClaim({
      attemptedBy: "client",
      kinds: [],
      queues: [{ limit: 1, name: "default" }],
    });
    const rescued = await driver.jobRescueMany(
      [
        {
          error: decodeAttemptError(stringifyJson(protocol.attempt_error)),
          id: inserted.job.id,
          scheduledAt: now,
          state: "retryable",
        },
      ],
      Temporal.Now.instant().add({ seconds: 1 })
    );
    expect(rescued[0]?.metadata).toMatchObject({
      [protocol.metadata_keys.rescue_count]: 3,
    });
    await driver.jobSchedule({ limit: 1, now });
    const claimed = await driver.jobClaim({
      attemptedBy: "client",
      kinds: [],
      queues: [{ limit: 1, name: "default" }],
    });
    const completed = await driver.jobCompleteMany([
      {
        attempt: claimed.jobs[0]!.attempt,
        attemptedBy: "client",
        error: null,
        finalizedAt: now,
        id: inserted.job.id,
        kind: "complete",
        output: { ok: true },
        outputSet: true,
        scheduledAt: null,
      },
    ]);
    expect(completed[0]?.job?.metadata).toMatchObject({
      [protocol.metadata_keys.output]: { ok: true },
      [protocol.metadata_keys.rescue_count]: 3,
    });
  });
});

function job(): JobRow {
  const now = Temporal.Instant.from("2026-01-02T03:04:05.6789Z");
  return {
    args: {},
    attempt: 1,
    attemptedAt: now,
    attemptedBy: ["client"],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id: 42n,
    kind: "conformance",
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: now,
    state: "running",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}
