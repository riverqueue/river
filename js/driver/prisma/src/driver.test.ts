import { describe, it, expect, beforeEach, vi } from "vitest";
import { PrismaDriver } from "./driver.js";
import type { PrismaClientLike } from "./driver.js";
import type { JobInsertParams } from "riverqueue";

// Simulates what Prisma returns for a river_job row.
// Key differences from pg: BigInt for id, Number for smallint.
function fakePrismaRow(overrides: Record<string, unknown> = {}) {
  return {
    id: BigInt(42), // Prisma returns BigInt for bigint columns
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

function mockPrismaClient() {
  const rowsToReturn: Record<string, unknown>[] = [];

  const mock = {
    rowsToReturn,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    $queryRawUnsafe: vi.fn(async (_sql: string, ..._values: unknown[]) => {
      return mock.rowsToReturn;
    }),
  };
  return mock as typeof mock & PrismaClientLike;
}

describe("PrismaDriver", () => {
  let prisma: ReturnType<typeof mockPrismaClient>;
  let driver: PrismaDriver;

  beforeEach(() => {
    prisma = mockPrismaClient();
    driver = new PrismaDriver(prisma);
  });

  describe("jobInsertMany", () => {
    it("returns empty array for empty params", async () => {
      const results = await driver.jobInsertMany([]);
      expect(results).toEqual([]);
      expect(prisma.$queryRawUnsafe).not.toHaveBeenCalled();
    });

    it("constructs correct SQL and parameters", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      await driver.jobInsertMany([fakeInsertParams({ tags: ["urgent"] })]);

      expect(prisma.$queryRawUnsafe).toHaveBeenCalledOnce();
      const sql = (prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>).mock
        .calls[0]![0] as string;
      const values = (
        prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>
      ).mock.calls[0]!.slice(1) as unknown[];

      expect(sql).toContain("INSERT INTO river_job");
      expect(sql).toContain("ON CONFLICT (unique_key)");
      expect(sql).toContain("RETURNING");

      expect(values).toHaveLength(10);
      expect(values[0]).toBe('{"strings":["a","b"]}');
      expect(values[1]).toBe("sort");
      expect(values[7]).toEqual(["urgent"]);
    });

    it("constructs correct parameters for batch insert", async () => {
      prisma.rowsToReturn = [
        fakePrismaRow(),
        fakePrismaRow({ id: BigInt(43) }),
      ];

      await driver.jobInsertMany([
        fakeInsertParams({ kind: "job_a" }),
        fakeInsertParams({ kind: "job_b" }),
      ]);

      const values = (
        prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>
      ).mock.calls[0]!.slice(1) as unknown[];

      expect(values).toHaveLength(20);
      expect(values[1]).toBe("job_a");
      expect(values[11]).toBe("job_b");
    });

    it("converts unique key to Buffer", async () => {
      const uniqueKey = new Uint8Array([1, 2, 3, 4]);
      prisma.rowsToReturn = [fakePrismaRow()];

      await driver.jobInsertMany([
        fakeInsertParams({ uniqueKey, uniqueStates: "11110101" }),
      ]);

      const values = (
        prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>
      ).mock.calls[0]!.slice(1) as unknown[];

      expect(Buffer.isBuffer(values[8])).toBe(true);
      expect(values[9]).toBe("11110101");
    });

    it("uses schema prefix in SQL when provided", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      await driver.jobInsertMany([fakeInsertParams()], {
        schemaPrefix: '"custom".',
      });

      const sql = (prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>).mock
        .calls[0]![0] as string;
      expect(sql).toContain('INSERT INTO "custom".river_job');
      expect(sql).toContain('"custom".river_job_state_in_bitmask');
    });

    it("schema-qualifies the river_job_state cast", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      await driver.jobInsertMany([fakeInsertParams()], {
        schemaPrefix: '"custom".',
      });

      const sql = (prisma.$queryRawUnsafe as ReturnType<typeof vi.fn>).mock
        .calls[0]![0] as string;

      // The state parameter cast must be schema-qualified. Without it,
      // Prisma inserts fail with 'type "river_job_state" does not exist'
      // when the schema is not on search_path.
      expect(sql).toContain('::"custom".river_job_state,');
      expect(sql).not.toMatch(/::river_job_state[^_]/);
    });
  });

  describe("jobInsert", () => {
    it("delegates to jobInsertMany", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      const [job, skipped] = await driver.jobInsert(fakeInsertParams());

      expect(prisma.$queryRawUnsafe).toHaveBeenCalledOnce();
      expect(job.kind).toBe("sort");
      expect(skipped).toBe(false);
    });
  });

  describe("row mapping", () => {
    it("converts BigInt id to number", async () => {
      prisma.rowsToReturn = [fakePrismaRow({ id: BigInt(999) })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.id).toBe(999);
      expect(typeof job.id).toBe("number");
    });

    it("maps basic columns correctly", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.id).toBe(42);
      expect(job.args).toEqual({ strings: ["a", "b"] });
      expect(job.attempt).toBe(0);
      expect(job.kind).toBe("sort");
      expect(job.maxAttempts).toBe(25);
      expect(job.priority).toBe(1);
      expect(job.queue).toBe("default");
      expect(job.state).toBe("available");
      expect(job.tags).toEqual(["tag1", "tag2"]);
    });

    it("maps null columns", async () => {
      prisma.rowsToReturn = [fakePrismaRow()];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.attemptedAt).toBeNull();
      expect(job.attemptedBy).toBeNull();
      expect(job.errors).toBeNull();
      expect(job.finalizedAt).toBeNull();
      expect(job.uniqueKey).toBeNull();
      expect(job.uniqueStates).toBeNull();
    });

    it("maps non-null optional columns", async () => {
      prisma.rowsToReturn = [
        fakePrismaRow({
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
      prisma.rowsToReturn = [
        fakePrismaRow({
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
      const buf = Buffer.from([0xca, 0xfe]);
      prisma.rowsToReturn = [fakePrismaRow({ unique_key: buf })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.uniqueKey).toBeInstanceOf(Uint8Array);
      expect(job.uniqueKey).toEqual(new Uint8Array([0xca, 0xfe]));
    });

    it("maps unique states from bit string", async () => {
      prisma.rowsToReturn = [fakePrismaRow({ unique_states: "11110101" })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      // 11110101 = available, completed, pending, retryable, running, scheduled
      expect(job.uniqueStates).toEqual([
        "available",
        "completed",
        "pending",
        "retryable",
        "running",
        "scheduled",
      ]);
    });

    it("reports unique_skipped_as_duplicate", async () => {
      prisma.rowsToReturn = [
        fakePrismaRow({ unique_skipped_as_duplicate: true }),
      ];

      const [, skipped] = await driver.jobInsert(fakeInsertParams());

      expect(skipped).toBe(true);
    });

    it("defaults tags to empty array when null", async () => {
      prisma.rowsToReturn = [fakePrismaRow({ tags: null })];

      const [job] = await driver.jobInsert(fakeInsertParams());

      expect(job.tags).toEqual([]);
    });
  });
});
