import { createHash } from "node:crypto";

import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";

import { buildUniqueKey, Client, encodeUniqueArgs } from "./client.js";
import type {
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
} from "./driver.js";
import {
  driverMigrationTarget,
  registerDriver,
} from "./internal/driver-registry.js";
import { defineJob } from "./job-definition.js";
import { ConfigurationError, ValidationError } from "./errors.js";
import {
  JOB_STATE,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
} from "./job.js";
import type { JobRow } from "./job.js";
import type { JsonObject } from "./json.js";
import { createJobArgsTransformPlugin } from "./job-args-transform.js";
import type { JobArgsInsertTransformInput } from "./job-args-transform.js";
import { createJobInsertMetadataTransformPlugin } from "./job-insert-metadata-transform.js";
import { buildPeriodicInsert, periodicJob } from "./periodic.js";

type SortInput = { strings: string[] };

const sortJob = defineJob<SortInput>()({ kind: "sort" });

class FakeDriver implements InsertDriver<{ readonly transaction: true }> {
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: { readonly transaction: true };
  };

  constructor() {
    registerDriver(this, {
      backend: "fake",
      capability: "insert",
      operations: this,
    });
  }

  insertedParams: JobInsertParams[] = [];
  lastOptions: InsertDriverOptions<{ readonly transaction: true }> | undefined;
  nextStatus: DriverInsertResult["status"] = "inserted";
  readonly notifications: {
    readonly options:
      InsertDriverOptions<{ readonly transaction: true }> | undefined;
    readonly queues: readonly string[];
  }[] = [];

  async jobInsert(
    params: JobInsertParams,
    options?: InsertDriverOptions<{ readonly transaction: true }>
  ): Promise<DriverInsertResult> {
    this.insertedParams.push(params);
    this.lastOptions = options;
    return { job: fakeJobRow(params), status: this.nextStatus };
  }

  async jobInsertMany(
    params: readonly JobInsertParams[],
    options?: InsertDriverOptions<{ readonly transaction: true }>
  ): Promise<readonly DriverInsertResult[]> {
    this.insertedParams.push(...params);
    this.lastOptions = options;
    return params.map((item) => ({
      job: fakeJobRow(item),
      status: this.nextStatus,
    }));
  }

  async notifyInsert(
    queues: readonly string[],
    options?: InsertDriverOptions<{ readonly transaction: true }>
  ): Promise<void> {
    this.notifications.push({ options, queues });
  }
}

function fakeJobRow(params: JobInsertParams): JobRow {
  return {
    args: params.args,
    attempt: 0,
    attemptedAt: null,
    attemptedBy: [],
    createdAt: Temporal.Now.instant(),
    errors: [],
    finalizedAt: null,
    id: 9_007_199_254_740_993n,
    kind: params.kind,
    maxAttempts: params.maxAttempts,
    metadata: params.metadata,
    priority: params.priority,
    queue: params.queue,
    scheduledAt: params.scheduledAt ?? Temporal.Now.instant(),
    state: params.state,
    tags: params.tags,
    uniqueKey: params.uniqueKey,
    uniqueStates: [],
  };
}

describe("Client", () => {
  let client: Client<{ readonly transaction: true }>;
  let driver: FakeDriver;

  beforeEach(() => {
    driver = new FakeDriver();
    // Typed as a full client, as untyped JavaScript may use it, so tests can
    // check that runtime operations fail on an insert-only driver.
    client = new Client(driver) as unknown as Client<{
      readonly transaction: true;
    }>;
  });

  it("rejects a value that isn't a registered River driver", () => {
    const pool = { connect: () => undefined, query: () => undefined };
    // An object with a driver's methods that no driver registered, such as
    // a driver from a second installed copy of riverqueue.
    const unregistered = {
      jobInsert: () => undefined,
      jobInsertMany: () => undefined,
    };
    for (const value of [pool, unregistered, null, "postgres://x/river"]) {
      expect(
        () =>
          // @ts-expect-error -- none of these is a River driver
          new Client(value)
      ).toThrow(ConfigurationError);
    }
  });

  it("records a driver's migration target only when well formed", () => {
    const operations = {
      jobInsert: () => undefined,
      jobInsertMany: () => undefined,
    } as unknown as InsertDriver;
    const pool = {};
    const migrating = {};
    registerDriver(migrating, {
      backend: "fake",
      capability: "insert",
      migration: { pool, schema: "river" },
      operations,
    });

    expect(driverMigrationTarget(migrating)).toEqual({ pool, schema: "river" });
    expect(driverMigrationTarget(driver)).toBeUndefined();
    expect(driverMigrationTarget({ pool, schema: "river" })).toBeUndefined();
    for (const migration of [{}, { pool: null }, { pool, schema: 1 }]) {
      expect(() =>
        registerDriver(
          {},
          {
            backend: "fake",
            capability: "insert",
            migration: migration as never,
            operations,
          }
        )
      ).toThrow(ValidationError);
    }
  });

  it("inserts a definition and preserves exact result types", async () => {
    const result = await client.insert(sortJob, { strings: ["b", "a"] });

    expectTypeOf(result.job.id).toEqualTypeOf<bigint>();
    expectTypeOf(result.job.args.strings).toEqualTypeOf<string[]>();
    expect(result).toMatchObject({ status: "inserted" });
    expect(result.job.id).toBe(9_007_199_254_740_993n);

    const params = driver.insertedParams[0]!;
    expect(params).toMatchObject({
      encodedArgs: '{"strings":["b","a"]}',
      kind: "sort",
      maxAttempts: MAX_ATTEMPTS_DEFAULT,
      priority: PRIORITY_DEFAULT,
      queue: QUEUE_DEFAULT,
      state: JOB_STATE.available,
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    });
    // River's own inserts leave the creation and scheduled times of an
    // unscheduled job to the database.
    expect(params.scheduledAt).toBeUndefined();
    expect(params.createdAt).toBeUndefined();
  });

  it("accepts Date scheduling and relative delays", async () => {
    const date = new Date(Date.UTC(2030, 0, 2, 3, 4, 5, 678));
    await client.insert(sortJob, { strings: [] }, { scheduledAt: date });
    expect(driver.insertedParams[0]).toMatchObject({
      scheduledAt: Temporal.Instant.from("2030-01-02T03:04:05.678Z"),
      state: "scheduled",
    });

    const before = Temporal.Now.instant();
    await client.insert(sortJob, { strings: [] }, { delay: { minutes: 5 } });
    const after = Temporal.Now.instant();
    const delayed = driver.insertedParams[1]!;
    expect(delayed.state).toBe("scheduled");
    expect(
      Temporal.Instant.compare(delayed.scheduledAt!, before.add({ minutes: 5 }))
    ).toBeGreaterThanOrEqual(0);
    expect(
      Temporal.Instant.compare(delayed.scheduledAt!, after.add({ minutes: 5 }))
    ).toBeLessThanOrEqual(0);

    const delayedDefinition = defineJob({
      defaults: { delay: { hours: 1 } },
      kind: "delayed_default",
    });
    await client.insert(delayedDefinition, {}, { scheduledAt: date });
    expect(driver.insertedParams[2]!.scheduledAt).toEqual(
      Temporal.Instant.from("2030-01-02T03:04:05.678Z")
    );

    await expect(
      client.insert(
        sortJob,
        { strings: [] },
        { delay: { seconds: 1 }, scheduledAt: date }
      )
    ).rejects.toThrow("mutually exclusive");
    await expect(
      client.insert(sortJob, { strings: [] }, { delay: { months: 1 } })
    ).rejects.toThrow("calendar units");
    await expect(
      client.insert(sortJob, { strings: [] }, { delay: { seconds: -1 } })
    ).rejects.toThrow("must not be negative");
    await expect(
      client.insert(
        sortJob,
        { strings: [] },
        { scheduledAt: new Date(Number.NaN) }
      )
    ).rejects.toThrow("valid Date");
  });

  it("uses explicit precedence without truthiness fallbacks", async () => {
    const definition = defineJob<SortInput>()({
      defaults: { maxAttempts: 7, priority: 2, queue: "job_default" },
      kind: "sort_with_defaults",
    });
    const clientWithDefaults = new Client(driver, {
      defaultInsertOptions: {
        maxAttempts: 9,
        priority: 3,
        queue: "client_default",
      },
    });

    await clientWithDefaults.insert(
      definition,
      { strings: [] },
      {
        maxAttempts: 4,
        priority: 1,
        queue: "call_site",
        tags: [],
      }
    );

    expect(driver.insertedParams[0]).toMatchObject({
      maxAttempts: 4,
      priority: 1,
      queue: "call_site",
      tags: [],
    });

    await expect(
      clientWithDefaults.insert(definition, { strings: [] }, { maxAttempts: 0 })
    ).rejects.toThrow("maxAttempts must be a safe integer between 1 and 32767");
    await expect(
      clientWithDefaults.insert(definition, { strings: [] }, { queue: "" })
    ).rejects.toThrow("queue name must not be empty");
  });

  it("rejects non-finite args and metadata with Go's text", async () => {
    const client = new Client(new FakeDriver());
    const definition = defineJob<{ ratio: number }>()({ kind: "ratio" });

    await expect(
      client.insert(definition, { ratio: Number.NaN })
    ).rejects.toThrow("unsupported value: NaN");
    await expect(
      client.insert(
        definition,
        { ratio: 1 },
        { metadata: { weight: Number.NEGATIVE_INFINITY } }
      )
    ).rejects.toThrow("unsupported value: -Inf");
  });

  it("keeps explicit scheduled and pending states distinct", async () => {
    const scheduledAt = Temporal.Instant.from("2026-08-30T12:34:56.123456789Z");
    await client.insert(sortJob, { strings: [] }, { scheduledAt });
    await client.insert(
      sortJob,
      { strings: [] },
      { pending: true, scheduledAt }
    );

    expect(driver.insertedParams[0]).toMatchObject({
      scheduledAt,
      state: JOB_STATE.scheduled,
    });
    expect(driver.insertedParams[1]).toMatchObject({
      scheduledAt,
      state: JOB_STATE.pending,
    });
  });

  it("inserts periodic occurrences available at their occurrence time", async () => {
    // Go's periodic enqueuer sets the occurrence time after resolving the
    // insert state, so a job due slightly in the future is still available.
    const scheduledAt = Temporal.Now.instant().add({ milliseconds: 50 });
    const periodic = await buildPeriodicInsert({
      job: periodicJob({
        args: { strings: [] },
        every: { hours: 1 },
        job: sortJob,
      }),
      scheduledAt,
    });
    if (periodic === null) throw new Error("expected a periodic insert");
    await client.insertMany([periodic]);
    // A caller's own schedule still makes the job scheduled.
    await client.insertMany([
      { args: { strings: [] }, job: sortJob, options: { scheduledAt } },
    ]);

    expect(driver.insertedParams[0]).toMatchObject({
      scheduledAt,
      state: JOB_STATE.available,
    });
    expect(driver.insertedParams[1]).toMatchObject({
      scheduledAt,
      state: JOB_STATE.scheduled,
    });
  });

  it("validates Standard Schema input but persists the untransformed JSON", async () => {
    const emailJob = defineJob({
      kind: "email",
      schema: {
        "~standard": {
          types: undefined as unknown as {
            input: { email: string };
            output: { email: string; normalized: true };
          },
          validate(value: unknown) {
            const input = value as { email?: unknown };
            if (typeof input.email !== "string") {
              return { issues: [{ message: "email must be a string" }] };
            }
            return {
              value: {
                email: input.email.toLowerCase(),
                normalized: true as const,
              },
            };
          },
          vendor: "river-test",
          version: 1 as const,
        },
      },
    });

    await client.insert(emailJob, { email: "Person@Example.COM" });

    expect(driver.insertedParams[0]!.encodedArgs).toBe(
      '{"email":"Person@Example.COM"}'
    );
    await expect(
      client.insert(emailJob, { email: 42 } as never)
    ).rejects.toThrow("invalid payload for job kind");
  });

  it("rejects a batch repeating a unique key before writing, like Go", async () => {
    await expect(
      client.insertMany([
        {
          args: { strings: ["same"] },
          job: sortJob,
          options: { unique: { byArgs: true } },
        },
        { args: { strings: ["different"] }, job: sortJob },
        {
          args: { strings: ["same"] },
          job: sortJob,
          options: { unique: { byArgs: true } },
        },
      ])
    ).rejects.toThrow(
      new ValidationError("unique key appears more than once in batch")
    );
    expect(driver.insertedParams).toHaveLength(0);
  });

  it("preserves each definition input across heterogeneous batches", async () => {
    const countJob = defineJob<{ count: number }>()({ kind: "count" });
    const results = await client.insertMany([
      { args: { strings: ["typed"] }, job: sortJob },
      { args: { count: 42 }, job: countJob },
    ]);

    expectTypeOf(results[0].job.args).toEqualTypeOf<SortInput>();
    expectTypeOf(results[1].job.args).toEqualTypeOf<{ count: number }>();

    const typecheckOnly = (): boolean => false;
    if (typecheckOnly()) {
      // @ts-expect-error args must correspond to the definition in this item.
      await client.insertMany([{ args: { count: 42 }, job: sortJob }]);
    }
  });

  it("preserves nested JSON field order in unique hashes like Go", async () => {
    const objectJob = defineJob()({ kind: "object" });
    await client.insert(
      objectJob,
      { nested: { a: 1, b: 2 } },
      { unique: { byArgs: true } }
    );
    await client.insert(
      objectJob,
      { nested: { b: 2, a: 1 } },
      { unique: { byArgs: true } }
    );

    expect(driver.insertedParams[0]!.uniqueKey).not.toEqual(
      driver.insertedParams[1]!.uniqueKey
    );
  });

  it.each([
    {
      args: {},
      hash: "23aa86692d9807ab10e433e378f1c0804573f5e345818461b919322dd381b4c3",
    },
    {
      args: {
        account: { id: "acct", region: "west", ignored: "ignored" },
        "path/key": "slash",
      },
      hash: "7d62e81ac25cfa2dec69ad5a41e0b78188ee1b299bed329b453da6b3abca70bd",
    },
  ])(
    "hashes selected paths through actual insertion: $hash",
    async ({ args, hash }) => {
      const definition = defineJob()({
        kind: "conformance_selected_args",
      });
      await client.insert(definition, args, {
        unique: {
          byArgs: ["path/key", "label", "account.region", "account.id"],
        },
      });
      expect(
        Buffer.from(driver.insertedParams[0]!.uniqueKey!).toString("hex")
      ).toBe(hash);
    }
  );

  it("assembles selected unique paths in sorted path order like Go", async () => {
    const definition = defineJob()({ kind: "object" });
    await client.insert(
      definition,
      { "<k>": 3, a: { b: 1 }, "a-c": 2 },
      { unique: { byArgs: ["a.b", "a-c", "<k>"] } }
    );

    // "a-c" sorts before "a.b" bytewise, so River Go writes it first even
    // though "a" sorts before "a-c" as a key, and writes "<k>" verbatim.
    const expected = createHash("sha256")
      .update('&kind=object&args={"<k>":3,"a-c":2,"a":{"b":1}}')
      .digest("hex");
    expect(
      Buffer.from(driver.insertedParams[0]!.uniqueKey!).toString("hex")
    ).toBe(expected);
  });

  it("hashes literal keys and accepts escaped selected paths", async () => {
    const definition = defineJob()({ kind: "object" });
    for (const path of ["a*", "a.b?", "a|b", "#", "a@b", "a\\b", ":a"]) {
      expect(() =>
        defineJob({
          defaults: { unique: { byArgs: [path] } },
          kind: "unique_paths",
        })
      ).not.toThrow();
    }
    for (const path of ["", "a..b", "a\\"]) {
      expect(() =>
        defineJob({
          defaults: { unique: { byArgs: [path] } },
          kind: "unique_paths",
        })
      ).toThrow(ValidationError);
    }

    await client.insert(
      definition,
      { "a#b": 1, "": 2 },
      { unique: { byArgs: true } }
    );
    await client.insert(
      definition,
      { "a#b": 2, "": 2 },
      { unique: { byArgs: true } }
    );
    expect(driver.insertedParams[0]!.uniqueKey).not.toEqual(
      driver.insertedParams[1]!.uniqueKey
    );
    // Nested keys are hashed verbatim, and keys only matter when hashed.
    await client.insert(
      definition,
      { nested: { "a#b": 1 } },
      { unique: { byArgs: true } }
    );
    await client.insert(definition, { "a#b": 1 });
    expect(driver.insertedParams).toHaveLength(4);
  });

  it("rejects selected path segments Go reads as array indices", async () => {
    // River Go's sjson builds a JSON array, not an object, for these
    // segments, so a JavaScript key could never match Go's.
    for (const path of [
      "0",
      "-1",
      "items.0",
      "items.10",
      "a.-1",
      "a.007.b",
      "a.\\0",
      "\\-1",
    ]) {
      expect(() =>
        defineJob({
          defaults: { unique: { byArgs: [path] } },
          kind: "unique_paths",
        })
      ).toThrow(/array index/);
      expect(() =>
        encodeUniqueArgs({ items: [1], a: { "-1": 1 } }, [path])
      ).toThrow(ValidationError);
    }
    const definition = defineJob()({ kind: "object" });
    await expect(
      client.insert(
        definition,
        { items: [1] },
        { unique: { byArgs: ["items.0"] } }
      )
    ).rejects.toThrow(ValidationError);
    expect(driver.insertedParams).toHaveLength(0);

    // Other segments that merely contain digits or a minus sign are fields.
    for (const path of ["a0", "0a", "-2", "-1a", "a.+1", "a.1x", ":0"]) {
      expect(() =>
        defineJob({
          defaults: { unique: { byArgs: [path] } },
          kind: "unique_paths",
        })
      ).not.toThrow();
    }
  });

  it("accepts overlapping unique paths without mutating arguments", async () => {
    const definition = defineJob()({ kind: "object" });
    const args = Object.freeze({ nested: Object.freeze({ z: 1, a: 2 }) });
    await client.insert(definition, args, {
      unique: { byArgs: ["nested", "nested.a"] },
    });
    await client.insert(definition, args, { unique: { byArgs: ["nested"] } });
    expect(driver.insertedParams[0]!.uniqueKey).toEqual(
      driver.insertedParams[1]!.uniqueKey
    );
    expect(args).toEqual({ nested: { z: 1, a: 2 } });
  });

  it("matches Go year-one period truncation fixtures", async () => {
    const definition = defineJob<{ id: number }>()({
      kind: "conformance_simple",
    });
    await client.insert(
      definition,
      { id: 42 },
      {
        scheduledAt: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
        unique: { byPeriod: { minutes: 90 } },
      }
    );

    expect(
      Array.from(driver.insertedParams[0]!.uniqueKey!, (byte) =>
        byte.toString(16).padStart(2, "0")
      ).join("")
    ).toBe("5396f06a082abd7a929915135ebd363a9a47d800176b03ce7736f93a5ba9e22e");
  });

  it("hashes by-period uniqueness from the effective scheduled time", async () => {
    const definition = defineJob<{ id: number }>()({
      defaults: { delay: { hours: 2 } },
      kind: "conformance_simple",
    });
    const unique = { byPeriod: { hours: 1 } };
    await client.insert(definition, { id: 42 }, { unique });
    const delayed = driver.insertedParams[0]!;
    await client.insert(
      definition,
      { id: 42 },
      { scheduledAt: delayed.scheduledAt!, unique }
    );
    await client.insert(
      definition,
      { id: 42 },
      {
        scheduledAt: Temporal.Instant.from("2026-01-02T10:51:05.6789+05:30"),
        unique,
      }
    );

    await client.insert(
      definition,
      { id: 42 },
      { scheduledAt: Temporal.Now.instant(), unique }
    );

    expect(delayed.state).toBe("scheduled");
    expect(delayed.uniqueKey).toEqual(driver.insertedParams[1]!.uniqueKey);
    expect(delayed.uniqueKey).not.toEqual(driver.insertedParams[3]!.uniqueKey);
    expect(
      Buffer.from(driver.insertedParams[2]!.uniqueKey!).toString("hex")
    ).toBe("b7f3c49952996b760b8b3ff6cf48f426e03a6ef0f004fb6faa51725365cf309a");
  });

  it("leaves an unscheduled job's time to the database, like Go", async () => {
    const definition = defineJob<{ id: number }>()({
      kind: "conformance_simple",
    });
    const unique = { byArgs: true, byPeriod: { minutes: 1 } } as const;

    const before = Temporal.Now.instant();
    await client.insert(definition, { id: 42 }, { unique });
    const after = Temporal.Now.instant();

    const params = driver.insertedParams[0]!;
    expect("scheduledAt" in params).toBe(false);
    expect("createdAt" in params).toBe(false);
    expect(params.state).toBe("available");
    // Like Go, the period key uses the current time instead.
    const keys = [before, after].map((scheduledAt) =>
      Buffer.from(
        buildUniqueKey({ ...params, scheduledAt }, unique)[0]
      ).toString("hex")
    );
    expect(keys).toContain(Buffer.from(params.uniqueKey!).toString("hex"));
  });

  it("supports exact subsecond Temporal uniqueness periods", async () => {
    const definition = defineJob<{ id: number }>()({
      kind: "conformance_simple",
    });
    await client.insert(
      definition,
      { id: 42 },
      {
        scheduledAt: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
        unique: {
          byPeriod: Temporal.Duration.from({ milliseconds: 1_500 }),
        },
      }
    );

    expect(
      Array.from(driver.insertedParams[0]!.uniqueKey!, (byte) =>
        byte.toString(16).padStart(2, "0")
      ).join("")
    ).toBe("2c1a88adffe46598d28e4ca05f5e7a93a77a36face4345341262c77d26524398");

    await expect(
      client.insert(
        definition,
        { id: 42 },
        {
          unique: {
            byPeriod: Temporal.Duration.from({ weeks: 1 }),
          },
        }
      )
    ).rejects.toThrow("must not contain calendar units");
    await expect(
      client.insert(
        definition,
        { id: 42 },
        {
          unique: {
            byPeriod: Temporal.Duration.from({ milliseconds: 999 }),
          },
        }
      )
    ).rejects.toThrow("must be at least one second");
    await client.insert(
      definition,
      { id: 42 },
      {
        scheduledAt: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
        unique: { byPeriod: { days: 1 } },
      }
    );
    await client.insert(
      definition,
      { id: 42 },
      {
        scheduledAt: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
        unique: { byPeriod: { hours: 24 } },
      }
    );
    expect(driver.insertedParams.at(-1)!.uniqueKey).toEqual(
      driver.insertedParams.at(-2)!.uniqueKey
    );
  });

  it("matches Go empty-state and exclude-kind uniqueness edges", async () => {
    await client.insert(
      sortJob,
      { strings: ["empty states"] },
      { unique: { byState: [] } }
    );
    // Like Go, excluding the kind needs another dimension, and then keys
    // match across kinds.
    await expect(
      client.insert(
        sortJob,
        { strings: ["exclude kind only"] },
        { unique: { excludeKind: true } }
      )
    ).rejects.toThrow(
      new ValidationError(
        "unique.excludeKind requires byArgs, byQueue, or byPeriod"
      )
    );
    const otherKind = defineJob<SortInput>()({ kind: "other_sort" });
    for (const definition of [sortJob, otherKind]) {
      await client.insert(
        definition,
        { strings: ["exclude kind"] },
        { unique: { byArgs: true, excludeKind: true } }
      );
    }

    expect(driver.insertedParams[0]).toMatchObject({
      uniqueStates: [
        "available",
        "completed",
        "pending",
        "retryable",
        "running",
        "scheduled",
      ],
    });
    expect(driver.insertedParams[0]?.uniqueKey).not.toBeNull();
    expect(driver.insertedParams).toHaveLength(3);
    expect(driver.insertedParams[1]?.uniqueKey).not.toBeNull();
    expect(driver.insertedParams[2]?.uniqueKey).toEqual(
      driver.insertedParams[1]?.uniqueKey
    );
  });

  it("validates every unique option through one path", async () => {
    for (const unique of [
      { byArgs: false },
      { byQueue: "yes" },
      { excludeKind: 1 },
      { excludeKind: true },
      { byQueue: false, excludeKind: true },
    ]) {
      await expect(
        client.insert(sortJob, { strings: [] }, { unique } as never)
      ).rejects.toThrow("unique.");
      expect(
        () => new Client(driver, { defaultInsertOptions: { unique } as never })
      ).toThrow("unique.");
      expect(() =>
        defineJob({ defaults: { unique } as never, kind: "invalid_unique" })
      ).toThrow("unique.");
    }
  });

  it("composes exact argument transforms around every insertion path", async () => {
    const order: string[] = [];
    const first = createJobArgsTransformPlugin({
      name: "first",
      onInsert: (input) => {
        order.push("first:insert");
        expect(Object.isFrozen(input)).toBe(true);
        expect(Object.isFrozen(input.args)).toBe(true);
        expect(Object.isFrozen(input.args.strings)).toBe(true);
        const args = { first: input.args };
        return { args, encodedArgs: JSON.stringify(args) };
      },
      onRead: ({ args }) => {
        order.push("first:read");
        return args.first as JsonObject;
      },
    });
    const second = createJobArgsTransformPlugin({
      name: "second",
      onInsert: (input) => {
        order.push("second:insert");
        const args = { second: input.args };
        return { args, encodedArgs: JSON.stringify(args) };
      },
      onRead: ({ args }) => {
        order.push("second:read");
        return args.second as JsonObject;
      },
    });
    const operations: string[] = [];
    let beforeArgs: JsonObject | undefined;
    let afterArgs: JsonObject | undefined;
    const transformedClient = new Client(driver, {
      hooks: {
        afterInsert: (context, results) => {
          operations.push(`after:${context.operation}`);
          afterArgs = results[0]?.job.args;
        },
        beforeInsert: (context) => {
          operations.push(`before:${context.operation}`);
          beforeArgs = context.requests[0]?.args;
          expect(() => {
            (context.requests[0]?.args as { mutated?: boolean }).mutated = true;
          }).toThrow();
        },
      },
      plugins: [first, second],
    });

    const inserted = await transformedClient.insert(sortJob, {
      strings: ["one"],
    });
    await transformedClient.insertMany([
      { args: { strings: ["many"] }, job: sortJob },
    ]);
    await transformedClient.insertMany([
      { args: { strings: ["fast"] }, job: sortJob },
    ]);

    expect(driver.insertedParams[0]?.args).toEqual({
      second: { first: { strings: ["one"] } },
    });
    expect(driver.insertedParams[0]?.encodedArgs).toBe(
      '{"second":{"first":{"strings":["one"]}}}'
    );
    expect(beforeArgs).toEqual({
      second: { first: { strings: ["fast"] } },
    });
    expect(afterArgs).toEqual({
      second: { first: { strings: ["fast"] } },
    });
    expect(inserted.job.args).toEqual({ strings: ["one"] });
    expect(order).toEqual([
      "first:insert",
      "second:insert",
      "second:read",
      "first:read",
      "first:insert",
      "second:insert",
      "second:read",
      "first:read",
      "first:insert",
      "second:insert",
      "second:read",
      "first:read",
    ]);
    expect(operations).toEqual([
      "before:insert",
      "after:insert",
      "before:insertMany",
      "after:insertMany",
      "before:insertMany",
      "after:insertMany",
    ]);
  });

  it("derives unique keys before transforming persisted arguments", async () => {
    const plainDriver = new FakeDriver();
    const transformedDriver = new FakeDriver();
    const plainClient = new Client(plainDriver);
    const transformedClient = new Client(transformedDriver, {
      plugins: [
        createJobArgsTransformPlugin({
          name: "wrapper",
          onInsert: ({ args }) => {
            const wrapped = { envelope: args };
            return { args: wrapped, encodedArgs: JSON.stringify(wrapped) };
          },
          onRead: ({ args }) => args.envelope as JsonObject,
        }),
      ],
    });

    await plainClient.insert(
      sortJob,
      { strings: ["same"] },
      { unique: { byArgs: true } }
    );
    await transformedClient.insert(
      sortJob,
      { strings: ["same"] },
      { unique: { byArgs: true } }
    );

    expect(transformedDriver.insertedParams[0]?.uniqueKey).toEqual(
      plainDriver.insertedParams[0]?.uniqueKey
    );
  });

  it("transforms insert metadata from plaintext for every insertion path", async () => {
    const seenDefinitions: unknown[] = [];
    const transformedClient = new Client(driver, {
      hooks: {
        beforeInsert: ({ requests }) => {
          seenDefinitions.push(...requests.map(({ definition }) => definition));
        },
      },
      plugins: [
        createJobInsertMetadataTransformPlugin({
          name: "routing",
          onInsert: (input) => {
            expect(Object.isFrozen(input)).toBe(true);
            expect(Object.isFrozen(input.args)).toBe(true);
            expect(Object.isFrozen(input.metadata)).toBe(true);
            seenDefinitions.push(input.definition);
            const strings = input.args.strings as readonly string[];
            return {
              metadata: {
                ...input.metadata,
                route: `${input.queue}:${input.kind}:${String(strings[0])}`,
              },
              ...(strings[0] === "fast" ? { pending: true as const } : {}),
            };
          },
        }),
        createJobArgsTransformPlugin({
          name: "envelope",
          onInsert: ({ args, definition }) => {
            seenDefinitions.push(definition);
            const wrapped = { envelope: args };
            return {
              args: wrapped,
              encodedArgs: JSON.stringify(wrapped),
            };
          },
          onRead: ({ args }) => args.envelope as JsonObject,
        }),
      ],
    });

    await transformedClient.insert(
      sortJob,
      { strings: ["one"] },
      { metadata: { caller: true }, queue: "critical" }
    );
    await transformedClient.insertMany([
      {
        args: { strings: ["many"] },
        job: sortJob,
        options: { queue: "bulk" },
      },
    ]);
    await transformedClient.insertMany([
      {
        args: { strings: ["fast"] },
        job: sortJob,
        options: { queue: "fast" },
      },
    ]);

    expect(driver.insertedParams.map(({ metadata }) => metadata)).toEqual([
      { caller: true, route: "critical:sort:one" },
      { route: "bulk:sort:many" },
      { route: "fast:sort:fast" },
    ]);
    // A transformer can make an insertion pending on any path.
    expect(driver.insertedParams.map(({ state }) => state)).toEqual([
      "available",
      "available",
      "pending",
    ]);
    expect(driver.insertedParams[0]?.args).toEqual({
      envelope: { strings: ["one"] },
    });
    // Metadata transform, args transform, and insert hook each see the
    // caller's definition object on every insertion path.
    expect(seenDefinitions).toHaveLength(9);
    expect(seenDefinitions.every((definition) => definition === sortJob)).toBe(
      true
    );
  });

  it("rejects invalid metadata transformer results", async () => {
    for (const result of [
      { metadata: [] },
      { metadata: {}, pending: false },
      null,
    ]) {
      const transformedClient = new Client(driver, {
        plugins: [
          createJobInsertMetadataTransformPlugin({
            name: "invalid",
            onInsert: () => result as never,
          }),
        ],
      });
      await expect(
        transformedClient.insert(sortJob, { strings: [] })
      ).rejects.toThrow(/transformer "invalid"/);
    }
    expect(driver.insertedParams).toEqual([]);
  });

  it("rejects inconsistent transformed encodings before hooks or storage", async () => {
    let beforeInsertCalls = 0;
    const transformedClient = new Client(driver, {
      hooks: {
        beforeInsert: () => {
          beforeInsertCalls += 1;
        },
      },
      plugins: [
        createJobArgsTransformPlugin({
          name: "invalid",
          onInsert: () => ({
            args: { value: "left" },
            encodedArgs: '{"value":"right"}',
          }),
          onRead: ({ args }) => args,
        }),
      ],
    });

    await expect(
      transformedClient.insert(sortJob, { strings: [] })
    ).rejects.toThrow("mismatched args and encodedArgs");
    expect(beforeInsertCalls).toBe(0);
    expect(driver.insertedParams).toHaveLength(0);
  });

  it("rejects insert-only argument transforms that cannot decode results", () => {
    expect(() =>
      createJobArgsTransformPlugin({
        name: "insert-only",
        onInsert: (input: JobArgsInsertTransformInput) => ({
          args: input.args,
          encodedArgs: input.encodedArgs,
        }),
      } as never)
    ).toThrow("onRead must be a function");
  });

  it("passes a caller-owned transaction through unchanged", async () => {
    const tx = { transaction: true as const };
    await client.insert(sortJob, { strings: [] }, { tx });

    expect(driver.lastOptions).toEqual({ tx });
  });
});

describe("Client insert notifications", () => {
  const setup = (fetchCooldown: Temporal.DurationLike = { seconds: 60 }) => {
    const driver = new FakeDriver();
    const client = new Client(driver, { fetchCooldown });
    return { client, driver };
  };

  it("notifies each queue of available jobs once per fetch cooldown", async () => {
    const { client, driver } = setup();

    await client.insertMany([
      { args: { strings: [] }, job: sortJob },
      { args: { strings: [] }, job: sortJob, options: { queue: "other" } },
      { args: { strings: ["again"] }, job: sortJob },
    ]);
    await client.insert(sortJob, { strings: ["later"] });
    await client.insert(sortJob, { strings: [] }, { queue: "third" });

    expect(driver.notifications).toEqual([
      { options: undefined, queues: [QUEUE_DEFAULT, "other"] },
      { options: undefined, queues: ["third"] },
    ]);
  });

  it("notifies a queue again once the fetch cooldown passes", async () => {
    const { client, driver } = setup({ milliseconds: 1 });

    await client.insert(sortJob, { strings: [] });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await client.insert(sortJob, { strings: [] });

    expect(driver.notifications.map(({ queues }) => queues)).toEqual([
      [QUEUE_DEFAULT],
      [QUEUE_DEFAULT],
    ]);
  });

  it("notifies nobody of jobs that aren't available", async () => {
    const { client, driver } = setup();

    await client.insert(
      sortJob,
      { strings: [] },
      { scheduledAt: Temporal.Now.instant().add({ hours: 1 }) }
    );
    await client.insert(sortJob, { strings: [] }, { pending: true });

    expect(driver.notifications).toEqual([]);
  });

  it("notifies a unique duplicate's queue like River for Go", async () => {
    const { client, driver } = setup();
    driver.nextStatus = "duplicate";

    await client.insert(sortJob, { strings: [] });

    expect(driver.notifications.map(({ queues }) => queues)).toEqual([
      [QUEUE_DEFAULT],
    ]);
  });

  it("notifies in the insertion's transaction", async () => {
    const { client, driver } = setup();
    const tx = { transaction: true as const };

    await client.insert(sortJob, { strings: [] }, { tx });

    expect(driver.notifications).toEqual([
      { options: { tx }, queues: [QUEUE_DEFAULT] },
    ]);
  });

  it("keeps a limiter per client", async () => {
    const driver = new FakeDriver();
    const first = new Client(driver);
    const second = new Client(driver);

    await first.insert(sortJob, { strings: [] });
    await second.insert(sortJob, { strings: [] });
    await first.insert(sortJob, { strings: [] });

    expect(driver.notifications.map(({ queues }) => queues)).toEqual([
      [QUEUE_DEFAULT],
      [QUEUE_DEFAULT],
    ]);
  });

  it("validates the fetch cooldown and gives it to queues without their own", () => {
    const driver = new FakeDriver();

    expect(
      () => new Client(driver, { fetchCooldown: { milliseconds: 0 } })
    ).toThrow("fetchCooldown must be positive");
    expect(
      () => new Client(driver, { fetchCooldown: { seconds: -1 } })
    ).toThrow("fetchCooldown must not be negative");
    expect(
      () =>
        new Client(driver, {
          fetchCooldown: { seconds: 2 },
          queues: { default: { maxWorkers: 1 } },
        })
    ).toThrow("queue pollInterval cannot be shorter than fetchCooldown");
    expect(
      () =>
        new Client(driver, {
          fetchCooldown: { seconds: 2 },
          queues: {
            default: { maxWorkers: 1, pollInterval: { seconds: 5 } },
            fast: {
              fetchCooldown: { milliseconds: 10 },
              maxWorkers: 1,
            },
          },
        })
    ).not.toThrow();
  });
});

describe("Client operation scopes", () => {
  type Transaction = { readonly name: string };

  /** Records a scope's events and discards its inserts when it rejects. */
  class ScopedDriver implements InsertDriver<Transaction> {
    declare readonly "~river"?: {
      readonly capability: "insert";
      readonly transaction: Transaction;
    };

    constructor() {
      registerDriver(this, {
        backend: "fake",
        capability: "insert",
        operations: this,
      });
    }

    readonly committed: string[] = [];
    readonly events: string[] = [];
    #pending: string[] = [];

    jobInsert(
      params: JobInsertParams,
      options?: InsertDriverOptions<Transaction>
    ): Promise<DriverInsertResult> {
      return this.jobInsertMany([params], options).then(
        ([result]) => result as DriverInsertResult
      );
    }

    jobInsertMany(
      params: readonly JobInsertParams[],
      options?: InsertDriverOptions<Transaction>
    ): Promise<readonly DriverInsertResult[]> {
      this.events.push(`write:${options?.tx?.name ?? "none"}`);
      this.#pending.push(...params.map(({ kind }) => kind));
      return Promise.resolve(
        params.map((item) => ({ job: fakeJobRow(item), status: "inserted" }))
      );
    }

    async operationScope<T>(
      tx: Transaction | undefined,
      callback: (tx: Transaction) => Promise<T>
    ): Promise<T> {
      if (tx !== undefined) {
        this.events.push(`join:${tx.name}`);
        return callback(tx);
      }
      this.events.push("begin");
      this.#pending = [];
      try {
        const result = await callback({ name: "scope" });
        this.committed.push(...this.#pending);
        this.events.push("commit");
        return result;
      } catch (error: unknown) {
        this.events.push("rollback");
        throw error;
      }
    }
  }

  const scopedJob = defineJob({
    decode(value) {
      events?.push("validate");
      return value;
    },
    kind: "scoped",
  });
  let events: string[] | undefined;

  const setup = (
    options: {
      afterInsert?: () => void;
      afterNext?: () => void;
    } = {}
  ) => {
    const driver = new ScopedDriver();
    events = driver.events;
    const client = new Client(driver, {
      hooks: {
        afterInsert: () => {
          driver.events.push("afterInsert");
          options.afterInsert?.();
        },
        beforeInsert: () => {
          driver.events.push("beforeInsert");
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          driver.events.push("middleware:before");
          const results = await next();
          driver.events.push("middleware:after");
          options.afterNext?.();
          return results;
        },
      ],
    });
    return { client, driver };
  };

  it("runs validation, middleware, hooks, and the write in one scope", async () => {
    const { client, driver } = setup();

    await client.insert(scopedJob, {});

    expect(driver.events).toEqual([
      "begin",
      "validate",
      "middleware:before",
      "beforeInsert",
      "write:scope",
      "afterInsert",
      "middleware:after",
      "commit",
    ]);
    expect(driver.committed).toEqual(["scoped"]);
  });

  it("rolls back when middleware throws after next()", async () => {
    const failure = new Error("after next");
    const { client, driver } = setup({
      afterNext: () => {
        throw failure;
      },
    });

    await expect(client.insert(scopedJob, {})).rejects.toBe(failure);
    await expect(
      client.insertMany([{ args: {}, job: scopedJob }])
    ).rejects.toBe(failure);

    expect(driver.events.filter((event) => event === "rollback")).toHaveLength(
      2
    );
    expect(driver.committed).toEqual([]);
  });

  it("rolls back when an afterInsert hook throws", async () => {
    const failure = new Error("after insert");
    const { client, driver } = setup({
      afterInsert: () => {
        throw failure;
      },
    });

    await expect(client.insert(scopedJob, {})).rejects.toBe(failure);

    expect(driver.events.at(-1)).toBe("rollback");
    expect(driver.committed).toEqual([]);
  });

  it("joins a caller-owned transaction instead of opening one", async () => {
    const { client, driver } = setup();

    await client.insertMany([{ args: {}, job: scopedJob }], {
      tx: { name: "caller" },
    });

    expect(driver.events[0]).toBe("join:caller");
    expect(driver.events).toContain("write:caller");
    expect(driver.events).not.toContain("begin");
  });
});
