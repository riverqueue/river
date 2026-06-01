import { describe, it, expect, beforeEach, vi } from "vitest";
import { PgDriver } from "./driver.js";
import type { JobInsertParams } from "riverqueue";

// Simulates what pg returns for a river_job row.
function fakePgRow(overrides: Record<string, unknown> = {}) {
  return {
    id: "42", // pg returns bigint as string
    args: { strings: ["a", "b"] },
    attempt: 0,
    attempted_at: null,
    attempted_by: null,
    created_at: new Date("2024-06-01T00:00:00Z"),
    errors: null,
    finalized_at: null,
    kind: "sort",
    max_attempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduled_at: new Date("2024-06-01T00:00:00Z"),
    state: "available",
    tags: ["tag1", "tag2"],
    unique_key: null,
    unique_states: null,
    unique_skipped_as_duplicate: false,
    ...overrides,
  };
}

function fakeInsertParams(
  overrides: Partial<JobInsertParams> = {}
): JobInsertParams {
  return {
    encodedArgs: '{"strings":["a","b"]}',
    kind: "sort",
    maxAttempts: 25,
    priority: 1,
    queue: "default",
    scheduledAt: new Date("2024-06-01T00:00:00Z"),
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
    ...overrides,
  };
}

// Minimal mock matching the pg Pool/PoolClient query interface.
function mockPgClient() {
  return {
    capturedSql: "" as string,
    capturedValues: [] as unknown[],
    rowsToReturn: [] as Record<string, unknown>[],
    query: vi.fn(async function (
      this: {
        capturedSql: string;
        capturedValues: unknown[];
        rowsToReturn: Record<string, unknown>[];
      },
      sql: string,
      values: unknown[]
    ) {
      this.capturedSql = sql;
      this.capturedValues = values;
      return { rows: this.rowsToReturn, rowCount: this.rowsToReturn.length };
    }),
  };
}

describe("PgDriver", () => {
  let pgClient: ReturnType<typeof mockPgClient>;
  let driver: PgDriver;

  beforeEach(() => {
    pgClient = mockPgClient();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    driver = new PgDriver(pgClient as any);
  });

  describe("jobInsertMany", () => {
    it("returns empty array for empty params", async () => {
      const results = await driver.jobInsertMany([]);
      expect(results).toEqual([]);
      expect(pgClient.query).not.toHaveBeenCalled();
    });

    it("constructs correct SQL and parameters for single insert", async () => {
      const scheduledAt = new Date("2024-06-01T12:00:00Z");
      pgClient.rowsToReturn = [fakePgRow()];

      await driver.jobInsertMany([
        fakeInsertParams({ scheduledAt, tags: ["urgent"] }),
      ]);

      expect(pgClient.query).toHaveBeenCalledOnce();
      const sql = pgClient.query.mock.calls[0]![0] as string;
      const values = pgClient.query.mock.calls[0]![1] as unknown[];

      expect(sql).toContain("INSERT INTO river_job");
      expect(sql).toContain("ON CONFLICT (unique_key)");
      expect(sql).toContain("river_job_state_in_bitmask");
      expect(sql).toContain("RETURNING");
      expect(sql).toContain("unique_skipped_as_duplicate");

      // 10 params per row
      expect(values).toHaveLength(10);
      expect(values[0]).toBe('{"strings":["a","b"]}'); // encodedArgs
      expect(values[1]).toBe("sort"); // kind
      expect(values[2]).toBe(25); // maxAttempts
      expect(values[3]).toBe(1); // priority
      expect(values[4]).toBe("default"); // queue
      expect(values[5]).toBe(scheduledAt); // scheduledAt
      expect(values[6]).toBe("available"); // state
      expect(values[7]).toEqual(["urgent"]); // tags
      expect(values[8]).toBeNull(); // uniqueKey
      expect(values[9]).toBeNull(); // uniqueStates
    });

    it("constructs correct parameters for batch insert", async () => {
      pgClient.rowsToReturn = [fakePgRow(), fakePgRow({ id: "43" })];

      await driver.jobInsertMany([
        fakeInsertParams({ kind: "job_a" }),
        fakeInsertParams({ kind: "job_b" }),
      ]);

      const sql = pgClient.query.mock.calls[0]![0] as string;
      const values = pgClient.query.mock.calls[0]![1] as unknown[];

      // Should have two VALUE clauses
      expect(sql).toContain("$1::jsonb");
      expect(sql).toContain("$11::jsonb");
      expect(values).toHaveLength(20);
      expect(values[1]).toBe("job_a");
      expect(values[11]).toBe("job_b");
    });

    it("converts unique key to Buffer", async () => {
      const uniqueKey = new Uint8Array([1, 2, 3, 4]);
      pgClient.rowsToReturn = [fakePgRow()];

      await driver.jobInsertMany([
        fakeInsertParams({ uniqueKey, uniqueStates: "11110101" }),
      ]);

      const values = pgClient.query.mock.calls[0]![1] as unknown[];
      expect(Buffer.isBuffer(values[8])).toBe(true);
      expect(values[9]).toBe("11110101");
    });

    it("uses schema prefix in SQL when provided", async () => {
      pgClient.rowsToReturn = [fakePgRow()];

      await driver.jobInsertMany([fakeInsertParams()], {
        schemaPrefix: '"custom".',
      });

      const sql = pgClient.query.mock.calls[0]![0] as string;
      expect(sql).toContain('INSERT INTO "custom".river_job');
      expect(sql).toContain('"custom".river_job_state_in_bitmask');
    });

    it("omits schema prefix when empty", async () => {
      pgClient.rowsToReturn = [fakePgRow()];

      await driver.jobInsertMany([fakeInsertParams()], {
        schemaPrefix: "",
      });

      const sql = pgClient.query.mock.calls[0]![0] as string;
      expect(sql).toContain("INSERT INTO river_job");
      expect(sql).not.toContain('".');
    });
  });

  describe("jobInsert", () => {
    it("delegates to jobInsertMany", async () => {
      pgClient.rowsToReturn = [fakePgRow()];

      const [job, skipped] = await driver.jobInsert(fakeInsertParams());

      expect(pgClient.query).toHaveBeenCalledOnce();
      expect(job.kind).toBe("sort");
      expect(skipped).toBe(false);
    });
  });

  describe("row mapping", () => {
    it("maps basic columns correctly", async () => {
      pgClient.rowsToReturn = [fakePgRow()];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.id).toBe(42);
      expect(typeof job.id).toBe("number");
      expect(job.args).toEqual({ strings: ["a", "b"] });
      expect(job.attempt).toBe(0);
      expect(job.kind).toBe("sort");
      expect(job.maxAttempts).toBe(25);
      expect(job.metadata).toEqual({});
      expect(job.priority).toBe(1);
      expect(job.queue).toBe("default");
      expect(job.state).toBe("available");
      expect(job.tags).toEqual(["tag1", "tag2"]);
      expect(job.createdAt).toEqual(new Date("2024-06-01T00:00:00Z"));
      expect(job.scheduledAt).toEqual(new Date("2024-06-01T00:00:00Z"));
    });

    it("maps null columns", async () => {
      pgClient.rowsToReturn = [fakePgRow()];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.attemptedAt).toBeNull();
      expect(job.attemptedBy).toBeNull();
      expect(job.errors).toBeNull();
      expect(job.finalizedAt).toBeNull();
      expect(job.uniqueKey).toBeNull();
      expect(job.uniqueStates).toBeNull();
    });

    it("maps non-null optional columns", async () => {
      pgClient.rowsToReturn = [
        fakePgRow({
          attempted_at: new Date("2024-06-01T01:00:00Z"),
          attempted_by: ["worker-1"],
          finalized_at: new Date("2024-06-01T02:00:00Z"),
        }),
      ];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.attemptedAt).toEqual(new Date("2024-06-01T01:00:00Z"));
      expect(job.attemptedBy).toEqual(["worker-1"]);
      expect(job.finalizedAt).toEqual(new Date("2024-06-01T02:00:00Z"));
    });

    it("maps errors from jsonb array", async () => {
      pgClient.rowsToReturn = [
        fakePgRow({
          errors: [
            {
              at: "2024-06-01T01:00:00Z",
              attempt: 1,
              error: "something broke",
              trace: "stack trace here",
            },
          ],
        }),
      ];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.errors).toHaveLength(1);
      expect(job.errors![0]!.at).toEqual(new Date("2024-06-01T01:00:00Z"));
      expect(job.errors![0]!.attempt).toBe(1);
      expect(job.errors![0]!.error).toBe("something broke");
      expect(job.errors![0]!.trace).toBe("stack trace here");
    });

    it("maps unique key from Buffer", async () => {
      const buf = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
      pgClient.rowsToReturn = [fakePgRow({ unique_key: buf })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.uniqueKey).toBeInstanceOf(Uint8Array);
      expect(job.uniqueKey).toEqual(new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
    });

    it("maps unique states from bit string", async () => {
      // "10000001" = available + scheduled
      pgClient.rowsToReturn = [fakePgRow({ unique_states: "10000001" })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.uniqueStates).toEqual(["available", "scheduled"]);
    });

    it("reports unique_skipped_as_duplicate", async () => {
      pgClient.rowsToReturn = [
        fakePgRow({ unique_skipped_as_duplicate: true }),
      ];

      const [, skipped] = await driver.jobInsert(fakeInsertParams());

      expect(skipped).toBe(true);
    });

    it("defaults tags to empty array when null", async () => {
      pgClient.rowsToReturn = [fakePgRow({ tags: null })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.tags).toEqual([]);
    });
  });
});
