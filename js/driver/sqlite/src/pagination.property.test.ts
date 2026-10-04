import type { DatabaseSync } from "node:sqlite";

import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { Client, defineJob } from "riverqueue";
import type { JobRow, JobState } from "riverqueue";
import type { JobListOrderBy, SortDirection } from "riverqueue/unstable-driver";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteMemory,
} from "./driver.js";
import type { SqliteDriverOptions } from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

const pageJob = defineJob({
  kind: "pagination_property",
  decode: (value) => value,
});

// A handful of scheduled times shared by many jobs, so that nearly every
// page boundary falls inside a run of equal sort values.
const SCHEDULE_OFFSETS_MS = [-7_200_000, -3_600_000, -1, 3_600_000, 7_200_000];

interface GeneratedJob {
  readonly cancel: boolean;
  readonly pending: boolean;
  readonly priority: number;
  readonly scheduleOffset: number;
}

const jobsArbitrary = fc.array(
  fc.record<GeneratedJob>({
    cancel: fc.boolean(),
    pending: fc.boolean(),
    priority: fc.integer({ max: 4, min: 1 }),
    scheduleOffset: fc.constantFrom(...SCHEDULE_OFFSETS_MS),
  }),
  { maxLength: 30, minLength: 1 }
);

const FINALIZED = ["cancelled", "completed", "discarded"];

/**
 * The time River sorts by, or `null` where that column is null. Like Go,
 * `time` picks one column for the whole query from the first requested
 * state (`available` when none are given).
 */
function sortTime(
  job: JobRow,
  orderBy: JobListOrderBy,
  firstState: JobState
): bigint | null {
  const column =
    orderBy !== "time"
      ? orderBy
      : FINALIZED.includes(firstState)
        ? "finalizedAt"
        : firstState === "running"
          ? "attemptedAt"
          : "scheduledAt";
  return column === "id" ? 0n : (job[column]?.epochNanoseconds ?? null);
}

/**
 * River's keyset order: the sort time, then the ID, in one direction. Null
 * times sort after every time, so last ascending and first descending.
 */
function compareJobs(
  orderBy: JobListOrderBy,
  direction: SortDirection,
  firstState: JobState
): (left: JobRow, right: JobRow) => number {
  const sign = direction === "asc" ? 1 : -1;
  return (left, right) => {
    const leftTime = sortTime(left, orderBy, firstState);
    const rightTime = sortTime(right, orderBy, firstState);
    if (leftTime !== rightTime) {
      if (leftTime === null) return sign;
      if (rightTime === null) return -sign;
      return leftTime < rightTime ? -sign : sign;
    }
    return left.id < right.id ? -sign : left.id > right.id ? sign : 0;
  };
}

async function setup(): Promise<{
  client: Client;
  driver: SqliteRuntime;
}> {
  const driver = testSqliteMemory(STRICT);
  const database = driver.database;
  const moduleUrl = new URL("../../../migrate/dist/index.js", import.meta.url);
  const { createMigrator } = (await import(moduleUrl.href)) as {
    createMigrator(target: { database: DatabaseSync }): {
      migrateUp(): Promise<unknown>;
    };
  };
  await createMigrator({ database }).migrateUp();
  return { client: new Client(driver), driver };
}

describe("SQLite job list pagination properties", () => {
  it("visits every job exactly once in keyset order across ties", async () => {
    await fc.assert(
      fc.asyncProperty(
        jobsArbitrary,
        fc.constantFrom<JobListOrderBy>(
          "finalizedAt",
          "id",
          "scheduledAt",
          "time"
        ),
        fc.constantFrom<SortDirection>("asc", "desc"),
        fc.integer({ max: 7, min: 1 }),
        fc.constantFrom<readonly JobState[]>(
          ["available", "pending", "scheduled"],
          ["scheduled", "available"],
          ["cancelled"],
          // Mixed states sort every job by the first state's column, which
          // is null for the unfinalized jobs of a finalized-first filter.
          ["available", "cancelled", "pending", "scheduled"],
          ["cancelled", "available", "pending", "scheduled"],
          []
        ),
        async (generated, orderBy, sortDirection, limit, timeStates) => {
          const { client, driver } = await setup();
          try {
            const base = Temporal.Now.instant().round("millisecond");
            const { length } = await client.insertMany(
              generated.map((job) => ({
                args: {},
                job: pageJob,
                options: {
                  pending: job.pending,
                  priority: job.priority,
                  scheduledAt: base.add({ milliseconds: job.scheduleOffset }),
                },
              }))
            );
            expect(length).toBe(generated.length);
            // Cancel in a burst so that finalization times collide too.
            const all = await client.jobs.list({ limit: 1_000 });
            await Promise.all(
              all.jobs
                .filter((_, index) => generated[index]?.cancel === true)
                .map((job) => client.jobs.cancel(job.id))
            );

            const states =
              orderBy === "finalizedAt"
                ? (["cancelled"] as const)
                : orderBy === "time"
                  ? timeStates
                  : undefined;
            const filter = {
              orderBy,
              sortDirection,
              ...(states === undefined ? {} : { states }),
            };
            const full = await client.jobs.list({ ...filter, limit: 1_000 });
            const expected = [...full.jobs].sort(
              compareJobs(orderBy, sortDirection, states?.[0] ?? "available")
            );
            expect(full.jobs.map((job) => job.id)).toEqual(
              expected.map((job) => job.id)
            );
            expect(full.jobs.length).toBe(
              states === undefined || states.length === 0
                ? generated.length
                : all.jobs.filter((_, index) => {
                    const job = generated[index];
                    const state = job?.cancel
                      ? "cancelled"
                      : job?.pending
                        ? "pending"
                        : (job?.scheduleOffset ?? 0) > 0
                          ? "scheduled"
                          : "available";
                    return states.includes(state);
                  }).length
            );

            const paged: bigint[] = [];
            let after: string | undefined;
            for (let page = 0; page <= generated.length + 1; page++) {
              const result = await client.jobs.list({
                ...filter,
                limit,
                ...(after === undefined ? {} : { after }),
              });
              expect(result.jobs.length).toBeLessThanOrEqual(limit);
              paged.push(...result.jobs.map((job) => job.id));
              if (result.nextCursor === null || result.jobs.length === 0) {
                break;
              }
              after = result.nextCursor;
            }
            expect(paged).toEqual(full.jobs.map((job) => job.id));
          } finally {
            driver.close();
          }
        }
      ),
      { numRuns: 60 }
    );
  });
});
