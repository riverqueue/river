import { Buffer } from "node:buffer";
import { EventEmitter } from "node:events";
import type { Client, ClientBase, Pool, PoolClient, QueryConfig } from "pg";
import { types as globalPgTypes } from "pg";
import { beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import {
  Client as RiverClient,
  ConfigurationError,
  defineJob,
  isExactJsonNumber,
  type Client as RiverClientType,
  Workers,
} from "riverqueue";
import type { JobInsertParams } from "riverqueue/unstable-driver";
import { PgDriver, type PgRuntime, testPgDriver } from "./driver.js";
import { PG_EXACT_TYPES } from "./exact-types.js";
import { databaseError } from "./errors.js";

const CREATED_AT = Temporal.Instant.from("2026-08-30T12:00:00.123456Z");
const SCHEDULED_AT = Temporal.Instant.from("2026-08-30T13:00:00.654321Z");

function fakePgRow(overrides: Record<string, unknown> = {}) {
  return {
    args: { strings: ["a", "b"] },
    attempt: 0,
    attempted_at: null,
    attempted_by: null,
    created_at: CREATED_AT,
    errors: null,
    finalized_at: null,
    id: 9_007_199_254_740_993n,
    kind: "sort",
    max_attempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduled_at: SCHEDULED_AT,
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
    args: { strings: ["a", "b"] },
    encodedArgs: '{"strings":["a","b"]}',
    kind: "sort",
    maxAttempts: 25,
    metadata: { source: "test" },
    priority: 1,
    queue: "default",
    scheduledAt: SCHEDULED_AT,
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
    ...overrides,
  };
}

function mockPgClient() {
  const state = {
    configs: [] as QueryConfig<unknown[]>[],
    rowCount: null as number | null,
    rows: [] as Record<string, unknown>[],
  };
  const query = vi.fn(async (config: QueryConfig<unknown[]>) => {
    // Answer the driver's one-time server detection as PostgreSQL 17,
    // outside the statements each test inspects.
    // LISTEN and ping queries are plain SQL strings, not query configs.
    if (
      (config.text as string | undefined)?.includes("yb_listen_notify_enabled")
    ) {
      return {
        command: "SELECT",
        fields: [],
        oid: 0,
        rowCount: 1,
        rows: [
          {
            date_style: "ISO, MDY",
            product: "PostgreSQL 17.4",
            version_num: 170_004,
            yb_listen_notify_enabled: false,
          },
        ],
      };
    }
    state.configs.push(config);
    return {
      command: "SELECT",
      fields: [],
      oid: 0,
      rowCount: state.rowCount ?? state.rows.length,
      rows: state.rows,
    };
  });
  return { query, state };
}

function mockLeasedPgClient() {
  const base = mockPgClient();
  const events = new EventEmitter();
  return {
    ...base,
    emit: events.emit.bind(events),
    listenerCount: events.listenerCount.bind(events),
    off: events.off.bind(events),
    on: events.on.bind(events),
    once: events.once.bind(events),
    release: vi.fn(),
  };
}

function asPool(client: ReturnType<typeof mockPgClient>): Pool {
  return client as unknown as Pool;
}

function asPoolClient(client: ReturnType<typeof mockPgClient>): PoolClient {
  return client as unknown as PoolClient;
}

describe("PgDriver client typing", () => {
  it("infers a full client that accepts any node-postgres client as tx", () => {
    const pool = {} as Pool;
    const typed = (): void => {
      const client = new RiverClient(new PgDriver(pool));
      expectTypeOf(client).toEqualTypeOf<RiverClientType<ClientBase>>();
      const pooled = {} as PoolClient;
      const standalone = {} as Client;
      const job = defineJob({ kind: "typed" });
      void client.insert(job, {}, { tx: pooled });
      void client.insert(job, {}, { tx: standalone });
      // @ts-expect-error -- transactions must be node-postgres clients.
      void client.insert(job, {}, { tx: 12_345 });
      void client.jobs.get(1n, { tx: standalone });
      new Workers().add(job, async ({ client, completeTx }) => {
        await completeTx(pooled);
        await client.insert(job, {}, { tx: standalone });
        // @ts-expect-error -- not a transaction of any installed driver.
        await completeTx(12_345);
      });
    };
    expect(typed).toBeTypeOf("function");
  });
});

describe("PgDriver surface", () => {
  it("exposes nothing but its construction", () => {
    const driver = new PgDriver(asPool(mockPgClient()));

    expect(Reflect.ownKeys(driver)).toEqual([]);
    expect(Reflect.ownKeys(PgDriver.prototype)).toEqual(["constructor"]);
    expect(new RiverClient(driver)).toBeInstanceOf(RiverClient);
  });
});

describe("PostgreSQL exact type parsing", () => {
  it("preserves exact JSON and JSONB numbers", () => {
    const parseJson = PG_EXACT_TYPES.getTypeParser(114, "text");

    const object = parseJson(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807}'
    ) as Record<string, unknown>;
    expect(isExactJsonNumber(object.decimal)).toBe(true);
    expect(isExactJsonNumber(object.integer)).toBe(true);
    expect(JSON.stringify(object)).toBe(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807}'
    );
  });

  it("leaves JSONB array elements as their text", () => {
    const parseJsonbArray = PG_EXACT_TYPES.getTypeParser(
      3807 as Parameters<typeof PG_EXACT_TYPES.getTypeParser>[0],
      "text"
    );

    expect(
      parseJsonbArray(
        '{"{\\"attempt\\": 1, \\"value\\": 9223372036854775807}",NULL}'
      )
    ).toEqual(['{"attempt": 1, "value": 9223372036854775807}', null]);
  });

  it("decodes text int8 values without Number coercion", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(20, "text");

    expect(parser("9223372036854775807")).toBe(9_223_372_036_854_775_807n);
    expect(globalPgTypes.getTypeParser(20, "text")("9223372036854775807")).toBe(
      "9223372036854775807"
    );
  });

  it("decodes binary int8 values", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(20, "binary");
    const encoded = Buffer.alloc(8);
    encoded.writeBigInt64BE(-9_223_372_036_854_775_808n);

    expect(parser(encoded)).toBe(-9_223_372_036_854_775_808n);
  });

  it("preserves timestamptz microseconds and offsets from text", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(1184, "text");

    expect(parser("2026-08-30 07:00:00.123456-05").toString()).toBe(
      "2026-08-30T12:00:00.123456Z"
    );
  });

  it("decodes binary timestamptz values at PostgreSQL's epoch", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(1184, "binary");
    const encoded = Buffer.alloc(8);
    encoded.writeBigInt64BE(1n);

    expect(parser(encoded).toString()).toBe("2000-01-01T00:00:00.000001Z");
  });

  it("reads timestamptz offsets with seconds", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(1184, "text");

    // A session time zone's historical local mean time, like Asia/Kolkata's.
    expect(parser("1900-01-01 05:53:28+05:53:28").toString()).toBe(
      "1900-01-01T00:00:00Z"
    );
    expect(parser("2026-08-30 07:00:00-0530").toString()).toBe(
      "2026-08-30T12:30:00Z"
    );
  });

  it("owns the parsers for every built-in type River reads", () => {
    const parser = (oid: number) =>
      PG_EXACT_TYPES.getTypeParser(oid, "text") as (value: string) => unknown;
    const overridden = [16, 17, 19, 21, 23, 25, 1009, 1015, 1043, 1560, 1562];
    const originals = overridden.map(
      (oid) => [oid, globalPgTypes.getTypeParser(oid, "text")] as const
    );
    // An application may replace node-postgres's global parsers.
    for (const oid of overridden) {
      globalPgTypes.setTypeParser(oid, "text", () => "application parser");
    }
    try {
      expect(parser(16)("t")).toBe(true);
      expect(parser(16)("f")).toBe(false);
      expect(parser(17)("\\x00ff41")).toEqual(Buffer.from([0, 255, 65]));
      // `bytea_output = escape`.
      expect(parser(17)("\\000\\377A\\\\")).toEqual(
        Buffer.from([0, 255, 65, 92])
      );
      expect(parser(19)("river_job")).toBe("river_job");
      expect(parser(21)("-4")).toBe(-4);
      expect(parser(23)("2147483647")).toBe(2_147_483_647);
      expect(parser(25)("text")).toBe("text");
      expect(parser(1009)('{a,"b,c",NULL}')).toEqual(["a", "b,c", null]);
      expect(parser(1015)("{x}")).toEqual(["x"]);
      expect(parser(1043)("varchar")).toBe("varchar");
      expect(parser(1560)("00000101")).toBe("00000101");
      expect(parser(1562)("101")).toBe("101");
    } finally {
      for (const [oid, original] of originals) {
        globalPgTypes.setTypeParser(oid, "text", original);
      }
    }
  });

  it("rejects PostgreSQL timestamp infinity", () => {
    const parser = PG_EXACT_TYPES.getTypeParser(1184, "text");

    expect(() => parser("infinity")).toThrow(/infinite timestamps/);
  });
});

describe("PgDriver", () => {
  let pgClient: ReturnType<typeof mockPgClient>;
  let driver: PgRuntime;

  beforeEach(() => {
    pgClient = mockPgClient();
    driver = testPgDriver(asPool(pgClient));
  });

  it("classifies only explicit transient database failures as retryable", () => {
    const failure = (message: string, code?: string) =>
      code === undefined
        ? new Error(message)
        : Object.assign(new Error(message), { code });
    const transient: readonly [string, Error][] = [
      ["connection exception", failure("connection failure", "08006")],
      ["too many connections", failure("too many clients", "53300")],
      ["out of memory", failure("out of memory", "53200")],
      ["disk full", failure("could not extend file", "53100")],
      ["serialization failure", failure("could not serialize", "40001")],
      ["deadlock", failure("deadlock detected", "40P01")],
      ["lock timeout", failure("could not obtain lock", "55P03")],
      ["statement timeout", failure("canceling statement", "57014")],
      ["administrator shutdown", failure("terminating", "57P01")],
      ["crash shutdown", failure("terminating", "57P02")],
      ["cannot connect now", failure("starting up", "57P03")],
      ["idle session timeout", failure("idle session", "57P05")],
      ["idle transaction timeout", failure("idle transaction", "25P03")],
      ["connection reset", failure("read ECONNRESET", "ECONNRESET")],
      ["DNS failure", failure("getaddrinfo ENOTFOUND db", "ENOTFOUND")],
      ["DNS retry", failure("getaddrinfo EAI_AGAIN db", "EAI_AGAIN")],
      ["ended connection", failure("Connection terminated unexpectedly")],
      [
        "pool connection timeout",
        failure("timeout exceeded when trying to connect"),
      ],
      [
        "nested cause",
        new Error("wrapped", { cause: failure("timeout", "57014") }),
      ],
    ];
    for (const [label, cause] of transient) {
      expect(databaseError("jobClaim", "failed", cause).retryable, label).toBe(
        true
      );
    }

    const permanent: readonly [string, Error][] = [
      ["unique violation", failure("constraint", "23505")],
      ["syntax error", failure("syntax", "42601")],
      ["undefined table", failure("missing relation", "42P01")],
      ["invalid input", failure("invalid input syntax", "22P02")],
      ["no code", failure("something else")],
    ];
    for (const [label, cause] of permanent) {
      expect(databaseError("jobClaim", "failed", cause).retryable, label).toBe(
        false
      );
    }
    expect(databaseError("jobClaim", "failed").retryable).toBe(false);
  });

  it("does not issue a query for an empty insert batch", async () => {
    await expect(driver.jobInsertMany([])).resolves.toEqual([]);
    expect(pgClient.query).not.toHaveBeenCalled();
  });

  it("inserts exact values and uses query-scoped parsers", async () => {
    pgClient.state.rows = [fakePgRow()];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.status).toBe("inserted");
    expect(result.job.id).toBe(9_007_199_254_740_993n);
    expect(result.job.createdAt.toString()).toBe("2026-08-30T12:00:00.123456Z");
    const config = pgClient.state.configs[0]!;
    expect(config.types).toBe(PG_EXACT_TYPES);
    expect(config.text).toContain('INSERT INTO "river_job"');
    expect(config.text).toContain(
      "inserted_jobs.conflicted AS unique_skipped_as_duplicate"
    );
    expect(config.text).toContain("FROM unnest(");
    // Insertion notifies nobody; the client notifies producers afterward.
    expect(config.text).not.toContain("pg_notify");
    expect(config.values).toHaveLength(13);
    expect(config.values![3]).toEqual(['{"source":"test"}']);
    expect(config.values![6]).toEqual(["2026-08-30T13:00:00.654321Z"]);
    expect(config.values![11]).toBe('"river_job_id_seq"');
    // No creation time: the database's current time is used.
    expect(config.values![12]).toEqual([null]);
  });

  it("preserves batch input order and duplicate status", async () => {
    pgClient.state.rows = [
      fakePgRow({ id: 101n, kind: "first" }),
      fakePgRow({
        id: 101n,
        kind: "first",
        unique_skipped_as_duplicate: true,
      }),
      fakePgRow({ id: 102n, kind: "third" }),
    ];

    const results = await driver.jobInsertMany([
      fakeInsertParams({ kind: "first" }),
      fakeInsertParams({
        createdAt: Temporal.Instant.from("2026-08-01T00:00:00.123456Z"),
        kind: "second",
      }),
      fakeInsertParams({ kind: "third" }),
    ]);

    expect(results.map(({ job }) => job.id)).toEqual([101n, 101n, 102n]);
    expect(results.map(({ status }) => status)).toEqual([
      "inserted",
      "duplicate",
      "inserted",
    ]);
    const values = pgClient.state.configs[0]!.values!;
    expect(values).toHaveLength(13);
    expect(values[1]).toEqual(["first", "second", "third"]);
    expect(values[12]).toEqual([null, "2026-08-01T00:00:00.123456Z", null]);
  });

  it("maps nullable database arrays to empty arrays", async () => {
    pgClient.state.rows = [
      fakePgRow({ attempted_by: null, errors: null, tags: null }),
    ];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.job.attemptedBy).toEqual([]);
    expect(result.job.errors).toEqual([]);
    expect(result.job.tags).toEqual([]);
    expect(result.job.uniqueStates).toBeNull();
  });

  it("maps exact errors, unique keys, and unique states", async () => {
    pgClient.state.rows = [
      fakePgRow({
        errors: [
          '{"at": "2026-08-30T12:30:00.000002Z", "attempt": 1, "error": "something broke", "trace": "stack trace"}',
        ],
        unique_key: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
        unique_states: "10000001",
      }),
    ];

    const result = await driver.jobInsert(fakeInsertParams());

    expect(result.job.errors[0]!.at.toString()).toBe(
      "2026-08-30T12:30:00.000002Z"
    );
    expect(result.job.uniqueKey).toEqual(
      new Uint8Array([0xde, 0xad, 0xbe, 0xef])
    );
    expect(result.job.uniqueStates).toEqual(["available", "scheduled"]);
  });

  it("uses a portable schema and parameterizes the sequence name", async () => {
    const schema = "river_custom";
    driver = testPgDriver(asPool(pgClient), { schema });
    pgClient.state.rows = [fakePgRow()];

    await driver.jobInsert(fakeInsertParams());

    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain('"river_custom"."river_job"');
    expect(config.text).not.toContain("river_job_id_seq'::regclass");
    expect(config.values![11]).toBe('"river_custom"."river_job_id_seq"');
  });

  it("rejects invalid schema identifiers before querying", () => {
    expect(() => testPgDriver(asPool(pgClient), { schema: "" })).toThrow(
      ConfigurationError
    );
    expect(() =>
      testPgDriver(asPool(pgClient), { schema: "bad\0schema" })
    ).toThrow(ConfigurationError);
    expect(() =>
      testPgDriver(asPool(pgClient), { schema: "a".repeat(47) })
    ).toThrow(/46 bytes/);
    expect(() =>
      testPgDriver(asPool(pgClient), { schema: `odd"schema` })
    ).toThrow(ConfigurationError);
    expect(() =>
      testPgDriver(asPool(pgClient), { schema: "a".repeat(46) })
    ).not.toThrow();
    expect(pgClient.query).not.toHaveBeenCalled();
  });

  it("uses the exact transaction client for an operation", async () => {
    const transaction = mockPgClient();
    transaction.state.rows = [fakePgRow()];

    await driver.jobGet(42n, { tx: asPoolClient(transaction) });

    expect(transaction.query).toHaveBeenCalledOnce();
    expect(pgClient.query).not.toHaveBeenCalled();
  });

  it("requires a Pool for runtime startup without taking ownership", async () => {
    const poolClient = mockPgClient();
    poolClient.state.rows = [fakePgRow()];
    const release = vi.fn();
    const clientDriver = testPgDriver({
      ...poolClient,
      release,
    } as unknown as PoolClient);
    expect(() => clientDriver.runtimeStartPreflight()).toThrow(
      /requires PgDriver to be constructed with a Pool/
    );
    await clientDriver.jobGet(1n);
    expect(release).not.toHaveBeenCalled();

    const plainClient = mockPgClient();
    plainClient.state.rows = [fakePgRow()];
    const endClient = vi.fn();
    const plainClientDriver = testPgDriver({
      ...plainClient,
      end: endClient,
    } as unknown as Client);
    expect(() => plainClientDriver.runtimeStartPreflight()).toThrow(
      /requires PgDriver to be constructed with a Pool/
    );
    await plainClientDriver.jobGet(1n);
    expect(endClient).not.toHaveBeenCalled();

    const fullPool = mockPgClient();
    fullPool.state.rows = [fakePgRow()];
    const endPool = vi.fn();
    const poolDriver = testPgDriver({
      ...fullPool,
      connect: vi.fn(),
      end: endPool,
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool);
    expect(() => poolDriver.runtimeStartPreflight()).not.toThrow();
    await poolDriver.jobGet(1n);
    expect(endPool).not.toHaveBeenCalled();

    const oneConnectionPool = testPgDriver({
      ...fullPool,
      connect: vi.fn(),
      idleCount: 1,
      options: { max: 1 },
      totalCount: 1,
    } as unknown as Pool);
    expect(() => oneConnectionPool.runtimeStartPreflight()).toThrow(
      /Pool max to be at least 4/
    );
    expect(() =>
      oneConnectionPool.runtimeStartPreflight({
        maintenance: false,
        notifications: false,
        reindex: false,
      })
    ).not.toThrow();
  });

  it("stops waiting for a connection to claim when aborted, but never abandons a started claim", async () => {
    const leased = mockLeasedPgClient();
    let connected!: (client: PoolClient) => void;
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(
        () =>
          new Promise<PoolClient>((resolve) => {
            connected = resolve;
          })
      ),
      idleCount: 0,
      options: { max: 1 },
      totalCount: 1,
    };
    const pgDriver = testPgDriver(pool as unknown as Pool);
    const params = {
      attemptedBy: "worker",
      kinds: [],
      queues: [{ limit: 1, name: "default" }],
    };
    const controller = new AbortController();

    const claim = pgDriver.jobClaim(params, { signal: controller.signal });
    controller.abort(new Error("stopping"));
    await expect(claim).rejects.toThrow("stopping");
    // A connection that arrives after the stop goes back to the pool unused.
    connected(leased as unknown as PoolClient);
    await vi.waitFor(() => expect(leased.release).toHaveBeenCalledTimes(1));
    expect(leased.query).not.toHaveBeenCalled();

    // Once a connection is leased the claim runs even if the stop arrives.
    const running = new AbortController();
    const started = pgDriver.jobClaim(params, { signal: running.signal });
    connected(leased as unknown as PoolClient);
    await vi.waitFor(() => expect(leased.query).toHaveBeenCalledTimes(1));
    running.abort(new Error("stopping"));
    await expect(started).resolves.toEqual({ jobs: [] });
  });

  it("stops waiting for a maintenance connection when its batch aborts", async () => {
    const leased = mockLeasedPgClient();
    let connected!: (client: PoolClient) => void;
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(
        () =>
          new Promise<PoolClient>((resolve) => {
            connected = resolve;
          })
      ),
      idleCount: 0,
      options: { max: 1 },
      totalCount: 1,
    };
    const pgDriver = testPgDriver(pool as unknown as Pool);
    const controller = new AbortController();
    const now = Temporal.Now.instant();

    const cleaning = pgDriver.maintenanceCleanQueues(
      { electedAt: now, expiresAt: now, leaderId: "leader" },
      now,
      10,
      { signal: controller.signal, timeoutMs: 1_000 }
    );
    controller.abort(new Error("stopping"));
    await expect(cleaning).rejects.toThrow("stopping");
    connected(leased as unknown as PoolClient);
    await vi.waitFor(() => expect(leased.release).toHaveBeenCalledTimes(1));
    expect(leased.query).not.toHaveBeenCalled();
  });

  it("rolls back an insertion's leased transaction when middleware throws", async () => {
    const leased = mockLeasedPgClient();
    leased.state.rows = [fakePgRow()];
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(async () => leased as unknown as PoolClient),
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    };
    const failure = new Error("after next");
    const client = new RiverClient(testPgDriver(pool as unknown as Pool), {
      insertMiddleware: [
        async (_context, next) => {
          await next();
          throw failure;
        },
      ],
    });

    await expect(
      client.insert(defineJob({ kind: "sort" }), { strings: [] })
    ).rejects.toBe(failure);

    const statements = leased.query.mock.calls.map(([config]) =>
      typeof config === "string" ? config : config.text.trim().split(/\s/)[0]
    );
    expect(statements[0]).toBe("BEGIN");
    expect(statements.at(-1)).toBe("ROLLBACK");
    expect(statements).not.toContain("COMMIT");
    expect(pool.query).not.toHaveBeenCalled();
    expect(leased.release).toHaveBeenCalledOnce();
  });

  it("inserts without a transaction only when it can lease a connection", async () => {
    const plainClient = mockPgClient();
    plainClient.state.rows = [fakePgRow()];
    const client = new RiverClient(
      testPgDriver(plainClient as unknown as Client)
    );
    const job = defineJob({ kind: "pool_less" });

    await expect(client.insert(job, {})).rejects.toMatchObject({
      code: "configuration",
      message: expect.stringContaining("pass { tx }"),
    });
    await expect(client.insertMany([{ args: {}, job }])).rejects.toBeInstanceOf(
      ConfigurationError
    );
    expect(plainClient.query).not.toHaveBeenCalled();

    const transaction = mockPgClient();
    transaction.state.rows = [fakePgRow()];
    await client.insert(job, {}, { tx: asPoolClient(transaction) });
    // The insertion, then its queue's insert notification.
    expect(transaction.state.configs).toHaveLength(2);
    expect(transaction.state.configs[1]?.text).toContain("pg_notify");
    expect(transaction.state.configs[1]?.values).toEqual([
      null,
      "river_insert",
      ['{"queue": "default"}'],
    ]);
  });

  it("supervises leased transaction failures and removes normal listeners", async () => {
    const normal = mockLeasedPgClient();
    normal.state.rows = [fakePgRow()];
    const failed = mockLeasedPgClient();
    const connect = vi
      .fn<() => Promise<PoolClient>>()
      .mockResolvedValueOnce(normal as unknown as PoolClient)
      .mockResolvedValueOnce(failed as unknown as PoolClient);
    const pool = {
      ...mockPgClient(),
      connect,
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const client = new RiverClient(testPgDriver(pool));
    const job = defineJob({ kind: "leased" });

    await client.insert(job, {});
    expect(normal.listenerCount("error")).toBe(0);
    expect(normal.release).toHaveBeenCalledOnce();
    expect(normal.release).toHaveBeenCalledWith();

    let releaseQuery!: () => void;
    failed.query
      .mockResolvedValueOnce({
        command: "BEGIN",
        fields: [],
        oid: 0,
        rowCount: 0,
        rows: [],
      })
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            releaseQuery = () =>
              resolve({
                command: "INSERT",
                fields: [],
                oid: 0,
                rowCount: 0,
                rows: [],
              });
          })
      );
    const failure = new Error("leased connection terminated");
    const operation = client.insert(job, {});
    await vi.waitFor(() => expect(releaseQuery).toBeTypeOf("function"));
    failed.emit("error", failure);

    await expect(operation).rejects.toBe(failure);
    expect(failed.release).toHaveBeenCalledWith(true);
    expect(failed.listenerCount("error")).toBe(1);
    failed.emit("end");
    expect(failed.listenerCount("error")).toBe(0);
    releaseQuery();
  });

  it("supervises abortable completion query lease failures", async () => {
    const leased = mockLeasedPgClient();
    let releaseQuery!: () => void;
    leased.query.mockImplementation(
      () =>
        new Promise((resolve) => {
          releaseQuery = () =>
            resolve({
              command: "UPDATE",
              fields: [],
              oid: 0,
              rowCount: 0,
              rows: [],
            });
        })
    );
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(async () => leased as unknown as PoolClient),
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const leaseDriver = testPgDriver(pool);
    const failure = new Error("completion connection terminated");
    const operation = leaseDriver.jobCompleteMany(
      [
        {
          attempt: 1,
          attemptedBy: "worker",
          error: null,
          finalizedAt: CREATED_AT,
          id: 1n,
          kind: "complete",
          metadata: {},
          output: null,
          outputSet: false,
          scheduledAt: null,
        },
      ],
      { signal: new AbortController().signal }
    );
    await vi.waitFor(() => expect(releaseQuery).toBeTypeOf("function"));
    leased.emit("error", failure);

    await expect(operation).rejects.toBe(failure);
    expect(leased.release).toHaveBeenCalledWith(true);
    expect(leased.listenerCount("error")).toBe(1);
    leased.emit("end");
    expect(leased.listenerCount("error")).toBe(0);
    releaseQuery();
  });

  it("cancels an aborted completion statement on another connection", async () => {
    const leased = Object.assign(mockLeasedPgClient(), { processID: 4242 });
    leased.query.mockImplementation(() => new Promise(() => undefined));
    const canceller = mockLeasedPgClient();
    const connect = vi
      .fn<() => Promise<PoolClient>>()
      .mockResolvedValueOnce(leased as unknown as PoolClient)
      .mockResolvedValueOnce(canceller as unknown as PoolClient);
    const pool = {
      ...mockPgClient(),
      connect,
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const leaseDriver = testPgDriver(pool);
    const controller = new AbortController();
    const reason = new Error("completion timed out");
    const completion = leaseDriver.jobCompleteMany(
      [
        {
          attempt: 1,
          attemptedBy: "worker",
          error: null,
          finalizedAt: CREATED_AT,
          id: 1n,
          kind: "complete",
          metadata: {},
          output: null,
          outputSet: false,
          scheduledAt: null,
        },
      ],
      { signal: controller.signal }
    );
    await vi.waitFor(() => expect(leased.query).toHaveBeenCalledOnce());

    controller.abort(reason);

    await expect(completion).rejects.toBe(reason);
    expect(leased.release).toHaveBeenCalledWith(true);
    await vi.waitFor(() => expect(canceller.release).toHaveBeenCalledWith());
    const [cancel] = canceller.state.configs;
    expect(cancel?.text).toContain("pg_cancel_backend(pid)");
    expect(cancel?.values).toEqual([4242, "/* river:jobCompleteMany */"]);
    expect(leased.query.mock.calls[0]?.[0]).toMatchObject({
      text: expect.stringMatching(/^\/\* river:jobCompleteMany \*\/\n/),
    });
  });

  it("pings an idle listener and fails when a ping never answers", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const emptyResult = {
        command: "SELECT",
        fields: [],
        oid: 0,
        rowCount: 0,
        rows: [],
      };
      const halfOpen = mockLeasedPgClient();
      const queries: string[] = [];
      // LISTEN and ping queries are plain SQL strings, not query configs.
      halfOpen.query.mockImplementation(((text: string) => {
        queries.push(text);
        return queries.length > 1
          ? new Promise(() => undefined)
          : Promise.resolve(emptyResult);
      }) as never);
      const connect = vi
        .fn<() => Promise<PoolClient>>()
        .mockResolvedValueOnce(halfOpen as unknown as PoolClient);
      const pool = {
        ...mockPgClient(),
        connect,
        idleCount: 1,
        options: { max: 4 },
        totalCount: 1,
      } as unknown as Pool;
      const listenDriver = testPgDriver(pool, { schema: "river" });
      const abort = new AbortController();
      let readies = 0;
      const iterator = listenDriver.listen(
        ["river_insert"],
        abort.signal,
        () => {
          readies++;
        },
        { pingIntervalMs: 1_000 }
      );
      const next = iterator.next();
      const failed = next.catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(0);
      expect(readies).toBe(1);
      expect(queries).toEqual(['LISTEN "river.river_insert"']);

      await vi.advanceTimersByTimeAsync(1_000);
      // The ping re-issues the idempotent LISTEN so the session still shows
      // as a listener in pg_stat_activity.
      expect(queries).toEqual([
        'LISTEN "river.river_insert"',
        'LISTEN "river.river_insert"',
      ]);
      expect(halfOpen.release).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1_000);
      // The caller resubscribes; the listener doesn't reconnect on its own.
      await expect(failed).resolves.toMatchObject({
        message: "PostgreSQL LISTEN connection did not answer a ping",
        name: "DatabaseOperationError",
      });
      expect(halfOpen.release).toHaveBeenCalledWith(true);
      expect(connect).toHaveBeenCalledOnce();
      expect(readies).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds a listener connection whose setup never answers", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      // A pooled connection with a half-open socket accepts the LISTEN but
      // never answers it.
      const halfOpen = mockLeasedPgClient();
      halfOpen.query.mockImplementation(() => new Promise(() => undefined));
      const connect = vi
        .fn<() => Promise<PoolClient>>()
        .mockResolvedValueOnce(halfOpen as unknown as PoolClient);
      const pool = {
        ...mockPgClient(),
        connect,
        idleCount: 1,
        options: { max: 4 },
        totalCount: 1,
      } as unknown as Pool;
      const listenDriver = testPgDriver(pool, { schema: "river" });
      const abort = new AbortController();
      let readies = 0;
      const iterator = listenDriver.listen(
        ["river_insert"],
        abort.signal,
        () => {
          readies++;
        },
        { setupTimeoutMs: 1_000 }
      );
      const failed = iterator.next().catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(999);
      expect(readies).toBe(0);
      expect(halfOpen.release).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      await expect(failed).resolves.toMatchObject({
        message:
          "PostgreSQL LISTEN connection setup did not finish within 1000 ms",
        name: "DatabaseOperationError",
      });
      expect(halfOpen.release).toHaveBeenCalledWith(true);
      expect(connect).toHaveBeenCalledOnce();
      expect(readies).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops a listener as soon as it aborts while connecting", async () => {
    const connection = Promise.withResolvers<PoolClient>();
    const late = mockLeasedPgClient();
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(() => connection.promise),
      idleCount: 0,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const listenDriver = testPgDriver(pool, { schema: "river" });
    const abort = new AbortController();
    const iterator = listenDriver.listen(["river_insert"], abort.signal);
    const next = iterator.next();
    await vi.waitFor(() => expect(pool.connect).toHaveBeenCalledOnce());

    abort.abort();
    await expect(next).resolves.toEqual({ done: true, value: undefined });

    // A connection the pool hands out after the listener gave up is discarded.
    connection.resolve(late as unknown as PoolClient);
    await vi.waitFor(() => expect(late.release).toHaveBeenCalledWith(true));
  });

  it("supervises forcibly terminated leader-fenced maintenance leases", async () => {
    const leased = mockLeasedPgClient();
    let releaseFence!: () => void;
    leased.query
      .mockResolvedValueOnce({
        command: "BEGIN",
        fields: [],
        oid: 0,
        rowCount: 0,
        rows: [],
      })
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            releaseFence = () =>
              resolve({
                command: "SELECT",
                fields: [],
                oid: 0,
                rowCount: 1,
                rows: [{ held: true }],
              });
          })
      );
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(async () => leased as unknown as PoolClient),
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const leaseDriver = testPgDriver(pool);
    const failure = new Error("maintenance connection terminated");
    const maintenance = leaseDriver.maintenanceSchedule(
      {
        electedAt: CREATED_AT,
        expiresAt: CREATED_AT.add({ minutes: 1 }),
        leaderId: "leader",
      },
      {
        allowInsertNotifications: allowEveryQueue,
        limit: 10,
        notificationHorizon: CREATED_AT,
        now: CREATED_AT,
        scheduledAtHorizon: CREATED_AT,
      }
    );
    await vi.waitFor(() => expect(releaseFence).toBeTypeOf("function"));
    leased.emit("error", failure);

    await expect(maintenance).rejects.toBe(failure);
    expect(leased.release).toHaveBeenCalledWith(true);
    expect(leased.listenerCount("error")).toBe(1);
    leased.emit("end");
    expect(leased.listenerCount("error")).toBe(0);
    releaseFence();
  });

  it("destroys a leader-fenced maintenance lease when its batch aborts", async () => {
    const leased = mockLeasedPgClient();
    // The connection went half-open after BEGIN: nothing else answers.
    leased.query
      .mockResolvedValueOnce({
        command: "BEGIN",
        fields: [],
        oid: 0,
        rowCount: 0,
        rows: [],
      })
      .mockImplementation(() => new Promise(() => undefined));
    const pool = {
      ...mockPgClient(),
      connect: vi.fn(async () => leased as unknown as PoolClient),
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const leaseDriver = testPgDriver(pool);
    const abort = new AbortController();
    const maintenance = leaseDriver.maintenanceSchedule(
      {
        electedAt: CREATED_AT,
        expiresAt: CREATED_AT.add({ minutes: 1 }),
        leaderId: "leader",
      },
      {
        allowInsertNotifications: allowEveryQueue,
        limit: 10,
        notificationHorizon: CREATED_AT,
        now: CREATED_AT,
        scheduledAtHorizon: CREATED_AT,
      },
      { signal: abort.signal, timeoutMs: null }
    );
    await vi.waitFor(() => expect(leased.query).toHaveBeenCalledTimes(2));
    const reason = new Error("client is stopping");
    abort.abort(reason);

    await expect(maintenance).rejects.toBe(reason);
    expect(leased.release).toHaveBeenCalledWith(true);
    expect(leased.query).toHaveBeenCalledTimes(2);
  });

  it("supervises forcibly terminated reindex leases", async () => {
    const base = mockPgClient();
    base.query.mockImplementation(async (config: QueryConfig<unknown[]>) => {
      const rows = config.text.includes("AS index_name")
        ? [{ exists: true, index_name: "river_job_kind" }]
        : config.text.includes("AS artifact_name")
          ? []
          : [{ held: true }];
      return {
        command: "SELECT",
        fields: [],
        oid: 0,
        rowCount: rows.length,
        rows,
      };
    });
    const leased = mockLeasedPgClient();
    let releaseReindex!: () => void;
    leased.query.mockImplementation((config: QueryConfig<unknown[]>) => {
      const text = typeof config === "string" ? config : config.text;
      if (text.includes("set_config")) {
        return Promise.resolve({
          command: "SELECT",
          fields: [],
          oid: 0,
          rowCount: 1,
          rows: [],
        });
      }
      return new Promise((resolve) => {
        releaseReindex = () =>
          resolve({
            command: "REINDEX",
            fields: [],
            oid: 0,
            rowCount: 0,
            rows: [],
          });
      });
    });
    const pool = {
      ...base,
      connect: vi.fn(async () => leased as unknown as PoolClient),
      idleCount: 1,
      options: { max: 4 },
      totalCount: 1,
    } as unknown as Pool;
    const leaseDriver = testPgDriver(pool);
    const failure = new Error("reindex connection terminated");
    const reindex = leaseDriver.maintenanceReindex(
      {
        electedAt: CREATED_AT,
        expiresAt: CREATED_AT.add({ minutes: 1 }),
        leaderId: "leader",
      },
      ["river_job_kind"],
      60_000,
      new AbortController().signal
    );
    await vi.waitFor(() => expect(releaseReindex).toBeTypeOf("function"));
    leased.emit("error", failure);

    await expect(reindex).rejects.toBe(failure);
    expect(leased.release).toHaveBeenCalledWith(true);
    leased.emit("end");
    expect(leased.listenerCount("error")).toBe(0);
    releaseReindex();
  });

  it("rejects a mismatched transaction before querying", async () => {
    const invalidTransaction = {} as PoolClient;

    await expect(
      driver.jobGet(42n, { tx: invalidTransaction })
    ).rejects.toMatchObject({
      backend: "postgres",
      code: "backend_mismatch",
      details: { backend: "postgres", operation: "jobGet" },
    });
    expect(pgClient.query).not.toHaveBeenCalled();
  });

  it("gets jobs by decimal bigint and returns null for absence", async () => {
    pgClient.state.rows = [fakePgRow()];

    const job = await driver.jobGet(9_223_372_036_854_775_807n);

    expect(job!.id).toBe(9_007_199_254_740_993n);
    expect(pgClient.state.configs[0]!.values).toEqual(["9223372036854775807"]);

    pgClient.state.rows = [];
    await expect(driver.jobGet(1n)).resolves.toBeNull();
  });

  it("atomically merges metadata and replaces metadata output", async () => {
    pgClient.state.rows = [
      fakePgRow({
        metadata: { keep: true, output: null, tenant: "acme" },
      }),
    ];

    await driver.jobUpdate(42n, {
      metadata: { tenant: "acme" },
      output: null,
    });

    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain("jsonb_set");
    expect(config.values).toEqual([
      "42",
      true,
      '{"tenant":"acme"}',
      true,
      "null",
    ]);
  });

  it("cancels with exact metadata, schema, notification, and now", async () => {
    driver = testPgDriver(asPool(pgClient), { schema: "river_custom" });
    pgClient.state.rows = [fakePgRow({ state: "cancelled" })];
    const cancelAttemptedAt = Temporal.Instant.from(
      "2026-08-30T12:00:00.123456789Z"
    );
    const now = Temporal.Instant.from("2026-08-30T12:01:00.000001Z");

    await driver.jobCancelWithOptions({
      cancelAttemptedAt,
      controlTopic: "river_control",
      id: 42n,
      now,
    });

    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain('UPDATE "river_custom"."river_job"');
    expect(config.text).toContain("pg_notify");
    expect(config.values).toEqual([
      "42",
      "river_control",
      '"2026-08-30T12:00:00.123456789Z"',
      "river_custom",
      "2026-08-30T12:01:00.000001Z",
      true,
    ]);
  });

  it("returns discriminated delete results", async () => {
    pgClient.state.rows = [fakePgRow({ was_deleted: true })];
    await expect(driver.jobDelete(1n)).resolves.toMatchObject({
      status: "deleted",
    });

    pgClient.state.rows = [fakePgRow({ state: "running", was_deleted: false })];
    await expect(driver.jobDelete(1n)).resolves.toMatchObject({
      status: "running",
    });

    pgClient.state.rows = [];
    await expect(driver.jobDelete(1n)).resolves.toEqual({
      status: "not_found",
    });
  });

  it("bulk deletes only an explicitly authorized filtered set on the supplied transaction", async () => {
    const transaction = mockPgClient();
    transaction.state.rows = [fakePgRow({ id: 5n })];

    const deleted = await driver.jobDeleteMany(
      {
        all: false,
        ids: [5n],
        kinds: [],
        limit: 100,
        priorities: [],
        queues: [],
        states: [],
      },
      { tx: asPoolClient(transaction) }
    );

    expect(deleted.map(({ id }) => id)).toEqual([5n]);
    expect(pgClient.query).not.toHaveBeenCalled();
    const config = transaction.state.configs[0]!;
    expect(config.text).toContain("FOR UPDATE SKIP LOCKED");
    expect(config.text).toContain("state != 'running'");
    expect(config.text).toContain("ORDER BY id ASC");
    expect(config.values).toEqual([["5"], [], [], [], [], 100]);

    await expect(
      driver.jobDeleteMany({
        all: false,
        ids: [],
        kinds: [],
        limit: 100,
        priorities: [],
        queues: [],
        states: [],
      })
    ).rejects.toThrow(/requires a filter or all=true/);
    await expect(
      driver.jobDeleteMany({
        all: true,
        ids: [5n],
        kinds: [],
        limit: 100,
        priorities: [],
        queues: [],
        states: [],
      })
    ).rejects.toThrow(/cannot be combined with filters/);
  });

  it("retries with an exact optional now value", async () => {
    pgClient.state.rows = [fakePgRow()];
    const now = Temporal.Instant.from("2026-08-30T12:01:00.000001Z");

    await driver.jobRetryWithOptions({ id: 42n, now });

    expect(pgClient.state.configs[0]!.values).toEqual([
      "42",
      "2026-08-30T12:01:00.000001Z",
    ]);
  });

  it("gets, lists, pauses, resumes, and updates queues", async () => {
    const queueRow = {
      created_at: CREATED_AT,
      metadata: { tenant: "acme" },
      metadata_text: '{"tenant": "acme"}',
      name: "email",
      paused_at: null,
      updated_at: SCHEDULED_AT,
    };
    pgClient.state.rows = [queueRow];

    await expect(driver.queueGet("email")).resolves.toMatchObject({
      name: "email",
    });
    await expect(
      driver.queueList({ limit: 10, nameAfter: null })
    ).resolves.toHaveLength(1);

    const pauseAt = Temporal.Instant.from("2026-08-30T14:00:00.000001Z");
    pgClient.state.rows = [{ ...queueRow, paused_at: pauseAt }];
    await expect(driver.queuePause("email")).resolves.toMatchObject({
      name: "email",
      pausedAt: pauseAt,
    });

    pgClient.state.rows = [
      { ...queueRow, paused_at: pauseAt },
      { ...queueRow, name: "reports", paused_at: pauseAt },
    ];
    await expect(
      driver.queuePauseWithOptions({
        name: "*",
        now: pauseAt,
      })
    ).resolves.toBe(2);
    // With no matching queue, the notification row still anchors the join.
    pgClient.state.rows = [
      Object.fromEntries(Object.keys(queueRow).map((key) => [key, null])),
    ];
    await expect(driver.queueResumeWithOptions({ name: "*" })).resolves.toBe(0);

    pgClient.state.rows = [queueRow];
    pgClient.state.rowCount = 1;
    await expect(
      driver.queueUpdate("email", { metadata: { owner: "workers" } })
    ).resolves.toMatchObject({ name: "email" });

    const configs = pgClient.state.configs;
    expect(configs[0]!.text).toContain("WHERE name = $1::text");
    expect(configs[1]!.text).toContain("ORDER BY name ASC");
    expect(configs[2]!.text).toContain("RETURNING *");
    // One aggregated notification: never a row per notified queue.
    expect(configs[2]!.text).toContain(
      "count(CASE WHEN $6::boolean THEN pg_notify("
    );
    expect(configs[2]!.text).toContain("FROM notification");
    expect(configs[2]!.text).toContain("LEFT JOIN updated ON true");
    expect(configs[2]!.values).toEqual([
      null,
      "email",
      null,
      "river_control",
      true,
      true,
    ]);
    expect(configs[3]!.values).toEqual([
      "2026-08-30T14:00:00.000001Z",
      "*",
      null,
      "river_control",
      true,
      true,
    ]);
    expect(configs[4]!.values).toEqual([
      null,
      "*",
      null,
      "river_control",
      false,
      true,
    ]);
    expect(configs[5]!.values).toEqual([
      true,
      '{"owner":"workers"}',
      "email",
      null,
      "river_control",
      '{"action":"metadata_changed","metadata":{"owner":"workers"},"queue":"email"}',
      true,
    ]);
  });

  it("claims per-queue capacities and records exact attempt ownership", async () => {
    pgClient.state.rows = [
      fakePgRow({
        attempt: 1,
        attempted_by: ["worker-a"],
        state: "running",
      }),
    ];

    const jobs = (
      await driver.jobClaim({
        attemptedBy: "worker-a",
        kinds: ["sort"],
        queues: [
          { limit: 2, name: "default" },
          { limit: 1, name: "priority" },
        ],
      })
    ).jobs;

    expect(jobs[0]).toMatchObject({
      attempt: 1,
      attemptedBy: ["worker-a"],
      state: "running",
    });
    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain("FOR UPDATE SKIP LOCKED");
    expect(config.text).toContain("CROSS JOIN LATERAL");
    expect(config.values).toEqual([
      ["default", "priority"],
      [2, 1],
      ["sort"],
      "worker-a",
    ]);
  });

  it("guards completions by both attempt number and attempt owner", async () => {
    pgClient.state.rows = [
      fakePgRow({
        attempt: 2,
        attempted_by: ["worker-a"],
        id: 41n,
        state: "completed",
        transition_applied: true,
      }),
      fakePgRow({
        attempt: 3,
        attempted_by: ["worker-b"],
        id: 42n,
        state: "running",
        transition_applied: false,
      }),
    ];
    const errorAt = Temporal.Instant.from("2026-08-30T14:00:00.000001Z");
    const finalizedAt = Temporal.Instant.from("2026-08-30T14:00:01.123456789Z");

    const results = await driver.jobCompleteMany([
      {
        attempt: 2,
        attemptedBy: "worker-a",
        error: null,
        id: 41n,
        kind: "complete",
        finalizedAt,
        metadata: { checkpoint: "finished" },
        output: { value: 1 },
        outputSet: true,
        scheduledAt: null,
      },
      {
        attempt: 2,
        attemptedBy: "worker-a",
        error: { at: errorAt, error: "failed", trace: "trace" },
        id: 42n,
        kind: "retry",
        finalizedAt: null,
        metadata: { checkpoint: "retrying" },
        output: null,
        outputSet: false,
        scheduledAt: SCHEDULED_AT,
      },
      {
        attempt: 1,
        attemptedBy: "worker-a",
        error: null,
        id: 43n,
        kind: "discard",
        finalizedAt,
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);

    expect(results.map(({ key, status }) => ({ key, status }))).toEqual([
      { key: "41:2:worker-a", status: "applied" },
      { key: "42:2:worker-a", status: "stale" },
      { key: "43:1:worker-a", status: "stale" },
    ]);
    expect(results[2]!.job).toBeNull();
    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain(
      "river_job.attempt = job_input.expected_attempt"
    );
    expect(config.text).toContain("array_length(river_job.attempted_by, 1)");
    expect(config.text).toContain("river_job.state <> 'running'");
    expect(config.text).toContain(
      "SET metadata = river_job.metadata || job_input.metadata_updates"
    );
    expect(config.values![4]).toEqual([
      null,
      JSON.stringify({
        at: errorAt.toString(),
        attempt: 2,
        error: "failed",
        trace: "trace",
      }),
      null,
    ]);
    // Truncated to PostgreSQL's microseconds like Go's pgx.
    expect(config.values![5]).toEqual([
      "2026-08-30T14:00:01.123456Z",
      null,
      "2026-08-30T14:00:01.123456Z",
    ]);
    expect(config.values![6]).toEqual([
      JSON.stringify({ checkpoint: "finished", output: { value: 1 } }),
      JSON.stringify({ checkpoint: "retrying" }),
      "{}",
    ]);
  });

  it("stops awaiting an in-flight completion when aborted", async () => {
    let releaseQuery!: () => void;
    pgClient.query.mockImplementationOnce(async () => {
      await new Promise<void>((resolve) => {
        releaseQuery = resolve;
      });
      return {
        command: "SELECT",
        fields: [],
        oid: 0,
        rowCount: 0,
        rows: [],
      };
    });
    const controller = new AbortController();
    const reason = new Error("runtime stopping");

    const completion = driver.jobCompleteMany(
      [
        {
          attempt: 1,
          attemptedBy: "worker-a",
          error: null,
          id: 44n,
          kind: "complete",
          finalizedAt: Temporal.Now.instant(),
          output: null,
          outputSet: false,
          scheduledAt: null,
        },
      ],
      { signal: controller.signal }
    );
    controller.abort(reason);

    await expect(completion).rejects.toBe(reason);
    releaseQuery();
  });

  it("decrements the attempt only for an owned snooze transition", async () => {
    pgClient.state.rows = [
      fakePgRow({
        attempt: 0,
        attempted_by: ["worker-a"],
        id: 44n,
        state: "scheduled",
        transition_applied: true,
      }),
    ];

    const result = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "worker-a",
        error: null,
        id: 44n,
        kind: "snooze",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt: SCHEDULED_AT,
      },
    ]);

    expect(result[0]).toMatchObject({
      job: { attempt: 0, state: "scheduled" },
      status: "applied",
    });
    const sql = pgClient.state.configs[0]!.text;
    expect(sql).toContain("greatest(river_job.attempt - 1, 0)");
    expect(sql).toContain("NOT (river_job.metadata ? 'cancel_attempted_at')");
  });

  it("returns interrupted work immediately without recording an error", async () => {
    pgClient.state.rows = [
      fakePgRow({
        attempt: 0,
        attempted_by: ["worker-a"],
        id: 45n,
        state: "available",
        transition_applied: true,
      }),
    ];

    const result = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "worker-a",
        error: null,
        id: 45n,
        kind: "interrupt",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt: SCHEDULED_AT,
      },
    ]);

    expect(result[0]).toMatchObject({
      job: { attempt: 0, errors: [], state: "available" },
      status: "applied",
    });
    expect(pgClient.state.configs[0]!.values![3]).toEqual(["available"]);
    expect(pgClient.state.configs[0]!.values![4]).toEqual([null]);
  });

  it("uses safe stable list ordering and keyset parameters", async () => {
    pgClient.state.rows = [fakePgRow()];

    const listParams = {
      after: {
        id: 40n,
        kind: "sort",
        queue: "default",
        sortField: "scheduledAt",
        time: SCHEDULED_AT,
      },
      ids: [],
      kinds: ["sort"],
      limit: 20,
      metadata: { tenant: "acme" },
      priorities: [1],
      queues: ["default"],
      sortDirection: "desc",
      sortField: "scheduledAt",
      states: ["available"],
      tagsAll: ["alpha"],
      tagsAny: ["beta"],
    } as const;
    await driver.jobList(listParams);

    const config = pgClient.state.configs[0]!;
    expect(config.text).toContain("scheduled_at DESC, id DESC");
    expect(config.text).toContain("metadata @> $8::jsonb");
    expect(config.text).toContain("scheduled_at < $9::timestamptz");
    expect(config.values![7]).toBe('{"tenant":"acme"}');
    expect(config.values![8]).toBe(SCHEDULED_AT.toString());
    expect(config.values![9]).toBe("40");
  });

  it("filters one state by equality and finalized lists by the partial index", async () => {
    const base = {
      after: null,
      ids: [],
      kinds: [],
      limit: 20,
      metadata: null,
      priorities: [],
      queues: [],
      sortDirection: "desc",
      tagsAll: [],
      tagsAny: [],
    } as const;

    for (const state of ["cancelled", "completed", "discarded"] as const) {
      await driver.jobList({ ...base, sortField: "time", states: [state] });
    }
    await driver.jobList({ ...base, sortField: "time", states: ["running"] });
    await driver.jobList({ ...base, sortField: "id", states: ["completed"] });
    await driver.jobList({
      ...base,
      sortField: "time",
      states: ["completed", "discarded"],
    });
    await driver.jobList({ ...base, sortField: "id", states: [] });

    const [cancelled, completed, discarded, running, byId, many, none] =
      pgClient.state.configs;
    for (const [config, state] of [
      [cancelled, "cancelled"],
      [completed, "completed"],
      [discarded, "discarded"],
    ] as const) {
      expect(config!.text).toContain(
        'state = $4::"river_job_state" AND finalized_at IS NOT NULL'
      );
      expect(config!.text).toContain("ORDER BY finalized_at DESC, id DESC");
      expect(config!.values![3]).toBe(state);
    }
    expect(running!.text).toContain('state = $4::"river_job_state"\n');
    expect(running!.text).not.toContain("finalized_at IS NOT NULL");
    expect(byId!.text).toContain('state = $4::"river_job_state"\n');
    expect(byId!.text).not.toContain("finalized_at IS NOT NULL");
    for (const config of [many, none]) {
      expect(config!.text).toContain(
        'state = ANY($4::text[]::"river_job_state"[])'
      );
    }
    expect(many!.values![3]).toEqual(["completed", "discarded"]);
    expect(none!.values![3]).toEqual([]);
  });

  it("chooses time ordering from the first requested state", async () => {
    await driver.jobList({
      after: null,
      ids: [],
      kinds: [],
      limit: 20,
      metadata: null,
      priorities: [],
      queues: [],
      sortDirection: "asc",
      sortField: "time",
      states: ["running", "completed"],
      tagsAll: [],
      tagsAny: [],
    });

    // `attempted_at` may be null, so nulls explicitly sort last like Go.
    expect(pgClient.state.configs[0]!.text).toContain(
      "ORDER BY attempted_at ASC NULLS LAST, id ASC"
    );
  });

  it("uses exact leader terms for lease renewal", async () => {
    pgClient.state.rows = [
      {
        elected_at: CREATED_AT,
        expires_at: SCHEDULED_AT,
        leader_id: "leader-a",
      },
    ];

    const leader = await driver.leaderReelect({
      electedAt: CREATED_AT,
      leaderId: "leader-a",
      now: CREATED_AT,
      ttlSeconds: 30,
    });

    expect(leader).toEqual({
      electedAt: CREATED_AT,
      expiresAt: SCHEDULED_AT,
      leaderId: "leader-a",
    });
    expect(pgClient.state.configs[0]!.values).toEqual([
      CREATED_AT.toString(),
      30,
      CREATED_AT.toString(),
      "leader-a",
    ]);
  });

  it("wraps database failures without exposing SQL", async () => {
    pgClient.query.mockRejectedValueOnce(new Error("password=secret"));

    const promise = driver.jobGet(1n);

    await expect(promise).rejects.toMatchObject({
      backend: "postgres",
      code: "database",
      message: "PostgreSQL operation jobGet failed",
      operation: "jobGet",
    });
    await expect(promise).rejects.not.toHaveProperty("sql");
  });
});

/** Let every queue's insert notification through, with no limiter. */
function allowEveryQueue(queues: readonly string[]): readonly string[] {
  return [...new Set(queues)];
}
