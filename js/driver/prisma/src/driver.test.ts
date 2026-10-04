import { beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import {
  Client,
  defineJob,
  type InsertClient,
  isExactJsonNumber,
} from "riverqueue";
import type { JobInsertParams } from "riverqueue/unstable-driver";
import { PrismaDriver, testPrismaDriver } from "./driver.js";
import type { PrismaClientLike, PrismaInserter } from "./driver.js";

const CREATED_AT = "2024-06-01T00:00:00.123456Z";

function fakePrismaRow(overrides: Record<string, unknown> = {}) {
  return {
    args: '{"strings": ["a", "b"]}',
    attempt: 0,
    attempted_at: null,
    attempted_by: null,
    created_at: CREATED_AT,
    errors: null,
    finalized_at: null,
    id: "9007199254740993",
    kind: "sort",
    max_attempts: 25,
    metadata: "{}",
    priority: 1,
    queue: "default",
    scheduled_at: CREATED_AT,
    state: "available",
    tags: ["tag1", "tag2"],
    unique_key: null,
    unique_skipped_as_duplicate: false,
    unique_states: null,
    ...overrides,
  };
}

function fakeInsertParams(
  overrides: Partial<JobInsertParams> = {}
): JobInsertParams {
  const args = { strings: ["a", "b"] };
  return {
    args,
    encodedArgs: JSON.stringify(args),
    kind: "sort",
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: Temporal.Instant.from("2024-06-01T00:00:00.123456Z"),
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
    ...overrides,
  };
}

/**
 * A Prisma client that records each statement in `statements`, apart from
 * the driver's one-time server detection, which it answers as `product`.
 */
function mockPrismaClient(product = "PostgreSQL 17.4") {
  const mock = {
    rowsToReturn: [] as Record<string, unknown>[],
    statements: vi.fn((query: string, ...values: unknown[]) => {
      void query;
      void values;
    }),
    $queryRawUnsafe: async (query: string, ...values: unknown[]) => {
      if (query.includes("yb_listen_notify_enabled")) {
        return [
          {
            product,
            version_num: 170_004,
            yb_listen_notify_enabled: false,
          },
        ];
      }
      mock.statements(query, ...values);
      return mock.rowsToReturn;
    },
  };
  return mock as typeof mock & PrismaClientLike;
}

describe("PrismaDriver", () => {
  let prisma: ReturnType<typeof mockPrismaClient>;
  let driver: PrismaInserter;

  beforeEach(() => {
    prisma = mockPrismaClient();
    driver = testPrismaDriver(prisma);
  });

  it("is typed as an insert-only client", () => {
    const client = new Client(driver);

    expectTypeOf(client).toEqualTypeOf<InsertClient<PrismaClientLike>>();
    // @ts-expect-error -- Prisma cannot run workers.
    expect(() => void client.start).not.toThrow();
    // @ts-expect-error -- Prisma cannot query jobs.
    expect(() => void client.jobs).not.toThrow();
  });

  it("keeps exact IDs and PostgreSQL timestamp precision", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.job.id).toBe(9_007_199_254_740_993n);
    expect(result.job.createdAt.toString()).toBe("2024-06-01T00:00:00.123456Z");
    expect(result.status).toBe("inserted");
  });

  it("normalizes nullable collection columns", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.job.attemptedBy).toEqual([]);
    expect(result.job.errors).toEqual([]);
    expect(result.job.uniqueStates).toBeNull();
  });

  it("decodes optional fields and semantic unique states", async () => {
    prisma.rowsToReturn = [
      fakePrismaRow({
        attempted_at: "2024-06-01T01:00:00.000001Z",
        attempted_by: ["worker-1"],
        errors: [
          JSON.stringify({
            at: "2024-06-01T01:00:00.000002Z",
            attempt: 1,
            error: "something broke",
            trace: "trace",
          }),
        ],
        finalized_at: "2024-06-01T02:00:00.000003Z",
        unique_key: "cafe",
        unique_states: "11110101",
      }),
    ];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.job.attemptedAt?.toString()).toBe(
      "2024-06-01T01:00:00.000001Z"
    );
    expect(result.job.errors[0]?.at.toString()).toBe(
      "2024-06-01T01:00:00.000002Z"
    );
    expect(result.job.finalizedAt?.toString()).toBe(
      "2024-06-01T02:00:00.000003Z"
    );
    expect(result.job.uniqueKey).toEqual(Uint8Array.from([0xca, 0xfe]));
    expect(result.job.uniqueStates).toEqual([
      "available",
      "completed",
      "pending",
      "retryable",
      "running",
      "scheduled",
    ]);
  });

  it("encodes the complete insert contract", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];

    await driver.jobInsertMany([
      fakeInsertParams({
        metadata: { source: "test" },
        tags: ["urgent"],
        uniqueKey: Uint8Array.from([1, 2, 3]),
        uniqueStates: ["available", "running"],
      }),
    ]);

    const call = prisma.statements.mock.calls[0];
    const sql = call?.[0] as string;
    const values = call?.slice(1) as unknown[];
    expect(sql).toContain('INSERT INTO "river_job"');
    expect(sql).toContain("ORDER BY prepared_job_data.input_order");
    expect(sql).toContain("FROM unnest(");
    expect(values).toHaveLength(13);
    expect(values[12]).toEqual([null]);
    expect(values[3]).toEqual(['{"source":"test"}']);
    expect(values[8]).toEqual(['["urgent"]']);
    expect(values[9]).toEqual(["010203"]);
    expect(values[10]).toEqual(["01000001"]);
  });

  it("decodes JSON beyond JavaScript's safe integers exactly", async () => {
    prisma.rowsToReturn = [
      fakePrismaRow({
        args: '{"user_id": 9007199254740993}',
        errors: [
          '{"at": "2024-06-01T01:00:00Z", "attempt": 1, "error": "x", "trace": "", "code": 9007199254740995}',
        ],
        metadata: '{"tenant": 9007199254740994}',
      }),
    ];

    const { job } = await driver.jobInsert(fakeInsertParams());

    // A lossy decode would round these, and rejecting them would throw
    // after the insert already committed.
    expect(isExactJsonNumber(job.args.user_id)).toBe(true);
    expect(JSON.stringify(job.args)).toBe('{"user_id":9007199254740993}');
    expect(JSON.stringify(job.metadata)).toBe('{"tenant":9007199254740994}');
    expect(job.errors[0]?.error).toBe("x");
  });

  it("notifies producers only when the client asks it to", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];

    await driver.jobInsert(fakeInsertParams());
    await driver.notifyInsert(["default", "other"]);
    await driver.notifyInsert([]);

    expect(prisma.statements).toHaveBeenCalledTimes(2);
    const insertSql = prisma.statements.mock.calls[0]?.[0] as string;
    expect(insertSql).not.toContain("pg_notify(");
    const [notifySql, ...values] = prisma.statements.mock.calls[1]!;
    expect(notifySql).toContain("pg_notify(");
    expect(notifySql).toContain("'river_insert'");
    expect(values).toEqual([null, ["default", "other"]]);
  });

  it("marks unique insertions with nonces and sends no notifications on YugabyteDB", async () => {
    prisma = mockPrismaClient("PostgreSQL 15.12-YB-2025.2.1.0-b1");
    driver = testPrismaDriver(prisma);
    prisma.rowsToReturn = [
      fakePrismaRow(),
      fakePrismaRow({ metadata: '{"river:unique_nonce": "0000000000000000"}' }),
    ];

    const results = await driver.jobInsertMany([
      fakeInsertParams(),
      fakeInsertParams(),
    ]);
    await driver.notifyInsert(["default"]);

    expect(prisma.statements).toHaveBeenCalledOnce();
    const [sql, , , , metadata] = prisma.statements.mock.calls[0]!;
    expect(sql).toContain("RETURNING *, false AS conflicted");
    const nonces = (metadata as string[]).map(
      (text) =>
        (JSON.parse(text) as Record<string, string>)["river:unique_nonce"]
    );
    expect(nonces).toEqual([
      expect.stringMatching(/^[0-9a-f]{16}$/),
      expect.stringMatching(/^[0-9a-f]{16}$/),
    ]);
    // Neither returned row carries its insertion's nonce: both existed.
    expect(results.map(({ status }) => status)).toEqual([
      "duplicate",
      "duplicate",
    ]);
  });

  it("uses a constructor-owned portable schema", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];
    driver = testPrismaDriver(prisma, { schema: "custom_schema" });

    await driver.jobInsert(fakeInsertParams());

    const sql = prisma.statements.mock.calls[0]?.[0] as string;
    expect(sql).toContain('"custom_schema"."river_job"');
    expect(sql).toContain('::"custom_schema"."river_job_state"');
    await driver.notifyInsert(["default"]);
    expect(prisma.statements.mock.calls[1]?.[1]).toBe("custom_schema");
  });

  it("uses the exact caller-owned transaction", async () => {
    const tx = mockPrismaClient();
    tx.rowsToReturn = [fakePrismaRow()];

    await driver.jobInsert(fakeInsertParams(), { tx });

    expect(tx.statements).toHaveBeenCalledOnce();
    expect(prisma.statements).not.toHaveBeenCalled();
  });

  it("inserts without a transaction in a Prisma interactive transaction", async () => {
    const tx = mockPrismaClient();
    tx.rowsToReturn = [fakePrismaRow()];
    const failure = new Error("after next");
    const transactions: string[] = [];
    const root = Object.assign(mockPrismaClient(), {
      $transaction: async <R>(
        callback: (transaction: PrismaClientLike) => Promise<R>
      ): Promise<R> => {
        try {
          const result = await callback(tx);
          transactions.push("commit");
          return result;
        } catch (error: unknown) {
          transactions.push("rollback");
          throw error;
        }
      },
    });
    const client = new Client(new PrismaDriver(root), {
      insertMiddleware: [
        async (_context, next) => {
          await next();
          throw failure;
        },
      ],
    });

    await expect(client.insert(defineJob({ kind: "sort" }), {})).rejects.toBe(
      failure
    );

    expect(transactions).toEqual(["rollback"]);
    // The insertion and its insert notification, both rolled back.
    expect(tx.statements).toHaveBeenCalledTimes(2);
    expect(root.statements).not.toHaveBeenCalled();
  });

  it("passes its transaction options to Prisma's interactive transaction", async () => {
    const tx = mockPrismaClient();
    tx.rowsToReturn = [fakePrismaRow()];
    const passed: unknown[] = [];
    const root = Object.assign(mockPrismaClient(), {
      $transaction: <R>(
        callback: (transaction: PrismaClientLike) => Promise<R>,
        options?: unknown
      ): Promise<R> => {
        passed.push(options);
        return callback(tx);
      },
    });

    await new Client(new PrismaDriver(root)).insert(
      defineJob({ kind: "sort" }),
      {}
    );
    await new Client(
      new PrismaDriver(root, {
        transactionOptions: {
          maxWait: { seconds: 3 },
          timeout: { seconds: 9 },
        },
      })
    ).insert(defineJob({ kind: "sort" }), {});

    expect(passed).toEqual([undefined, { maxWait: 3_000, timeout: 9_000 }]);
  });

  it("requires $transaction to insert without a transaction", async () => {
    const client = new Client(driver);

    await expect(
      client.insert(defineJob({ kind: "sort" }), {})
    ).rejects.toMatchObject({
      code: "configuration",
      message: expect.stringContaining("pass { tx }"),
    });
    expect(prisma.statements).not.toHaveBeenCalled();
  });

  it("returns no rows without querying for an empty batch", async () => {
    await expect(driver.jobInsertMany([])).resolves.toEqual([]);
    expect(prisma.statements).not.toHaveBeenCalled();
  });

  it("rejects incomplete batch results", async () => {
    prisma.rowsToReturn = [fakePrismaRow()];

    await expect(
      driver.jobInsertMany([fakeInsertParams(), fakeInsertParams()])
    ).rejects.toThrow("1 rows for 2");
  });

  it("rejects invalid schema names before issuing SQL", () => {
    expect(() => new PrismaDriver(prisma, { schema: "" })).toThrow(
      "must start"
    );
    expect(() => new PrismaDriver(prisma, { schema: "bad\0schema" })).toThrow(
      "must start"
    );
    expect(() => new PrismaDriver(prisma, { schema: 'odd"schema' })).toThrow(
      "must start"
    );
    expect(() => new PrismaDriver(prisma, { schema: "a".repeat(47) })).toThrow(
      "46 bytes"
    );
    expect(
      () => new PrismaDriver(prisma, { schema: "a".repeat(46) })
    ).not.toThrow();
  });
});
