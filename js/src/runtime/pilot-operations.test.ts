import { describe, expect, it } from "vitest";

import type {
  DriverInsertResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobInsertParams,
  RuntimeDriver,
  RuntimeLeader,
} from "../driver.js";
import { ExtensionError, ValidationError } from "../errors.js";
import type { JobRow } from "../job.js";
import type {
  Pilot,
  PilotDatabase,
  PilotInterceptors,
  PreparedInsertParams,
} from "../pilot.js";
import { PilotOperations, validatePreparedParams } from "./pilot-operations.js";

describe("PilotOperations", () => {
  type Transaction = string;

  interface TestBundle {
    readonly database: PilotDatabase<Transaction>;
    readonly driver: RuntimeDriver<Transaction>;
    /** Transaction boundaries and standard operations, in order. */
    readonly log: string[];
  }

  const setup = (): TestBundle => {
    const log: string[] = [];
    let transactions = 0;
    const database: PilotDatabase<Transaction> = {
      backend: "test",
      connection: (callback) => Promise.resolve(callback("connection")),
      deleteFinalizedJobs: () => Promise.resolve(0),
      loadClaimed: () => Promise.resolve({ jobs: [] }),
      notify: () => Promise.resolve(),
      schema: null,
      async transaction(callback, options = {}) {
        // Like River's drivers, run directly in a caller's transaction,
        // without a savepoint.
        if (options.tx !== undefined) return callback(options.tx);
        const tx = `tx${++transactions}`;
        log.push(`begin ${tx}`);
        try {
          const result = await callback(tx);
          log.push(`commit ${tx}`);
          return result;
        } catch (error: unknown) {
          log.push(`rollback ${tx}`);
          throw error;
        }
      },
    };
    const standard = async <T>(name: string, value: T): Promise<T> => {
      log.push(`start ${name}`);
      // A macrotask, so an interceptor that doesn't await `next` settles
      // first.
      await new Promise((resolve) => setTimeout(resolve, 1));
      log.push(`end ${name}`);
      return value;
    };
    const driver = {
      jobCancel: (id: bigint, options?: { readonly tx?: Transaction }) =>
        standard(`cancel in ${options?.tx}`, jobRow(id, "cancelled")),
      jobCompleteMany: (
        commands: readonly JobCompletionCommand[],
        options?: { readonly tx?: Transaction }
      ) =>
        standard(
          `complete in ${options?.tx}`,
          commands.map((command): JobCompletionResult => ({
            job: jobRow(command.id, "completed"),
            key: `${command.id}:${command.attempt}:${command.attemptedBy}`,
            status: "applied",
          }))
        ),
      jobRetry: (id: bigint, options?: { readonly tx?: Transaction }) =>
        standard(`retry in ${options?.tx}`, jobRow(id, "available")),
      maintenanceGetStuck: () =>
        standard("getStuck", [jobRow(5n, "running"), jobRow(6n, "running")]),
      maintenanceRescue: (
        _leader: RuntimeLeader,
        _before: Temporal.Instant,
        jobs: readonly unknown[],
        options?: { readonly tx?: Transaction }
      ) => standard(`rescue in ${options?.tx}`, jobs.length),
    } as unknown as RuntimeDriver<Transaction>;
    return { database, driver, log };
  };

  const operations = (
    bundle: TestBundle,
    intercept: PilotInterceptors<Transaction>
  ): PilotOperations<Transaction> =>
    new PilotOperations(
      { intercept } satisfies Pilot<Transaction>,
      bundle.database
    );

  const insertStandard =
    (bundle: TestBundle) =>
    async (
      rows: readonly JobInsertParams[],
      tx: Transaction | undefined
    ): Promise<readonly DriverInsertResult[]> => {
      bundle.log.push(
        `insert ${rows.map(({ kind }) => kind).join(",")} in ${tx}`
      );
      await Promise.resolve();
      return rows.map((row, index) => ({
        job: { ...jobRow(BigInt(index + 1), "available"), kind: row.kind },
        status: "inserted" as const,
      }));
    };

  it("runs the standard operation directly without an interceptor", async () => {
    const bundle = setup();
    const ops = operations(bundle, {});

    await expect(
      ops.cancel(bundle.driver, 1n, undefined)
    ).resolves.toMatchObject({ id: 1n });
    await expect(
      ops.insert("insert", [params("a")], "caller", insertStandard(bundle))
    ).resolves.toHaveLength(1);

    expect(bundle.log).toEqual([
      "start cancel in undefined",
      "end cancel in undefined",
      "insert a in caller",
    ]);
  });

  it("binds next to the operation's transaction", async () => {
    const bundle = setup();
    const seen: string[] = [];
    const ops = operations(bundle, {
      async insert(context, next) {
        seen.push(`${context.operation} ${context.tx}`);
        const results = await next();
        seen.push(`after ${results.length}`);
        return results;
      },
    });

    const results = await ops.insert(
      "insertMany",
      [params("a"), params("b")],
      "caller",
      insertStandard(bundle)
    );

    expect(results).toHaveLength(2);
    expect(Object.isFrozen(results)).toBe(true);
    expect(seen).toEqual(["insertMany caller", "after 2"]);
    expect(bundle.log).toEqual(["insert a,b in caller"]);
  });

  it("inserts in the caller's transaction and leaves its commit or rollback to the caller", async () => {
    for (const fail of [false, true]) {
      const bundle = setup();
      const seen: string[] = [];
      const ops = operations(bundle, {
        async insert(context, next) {
          seen.push(`interceptor in ${context.tx}`);
          const results = await next();
          if (fail) throw new Error("interceptor failed");
          return results;
        },
      });

      const inserted = ops.insert(
        "insertMany",
        [params("a"), params("b")],
        "caller",
        insertStandard(bundle)
      );

      if (fail) {
        await expect(inserted).rejects.toThrow("interceptor failed");
      } else {
        await expect(inserted).resolves.toHaveLength(2);
      }
      expect(seen).toEqual(["interceptor in caller"]);
      // No transaction of River's: no begin, commit, or rollback.
      expect(bundle.log).toEqual(["insert a,b in caller"]);
    }
  });

  it("passes the interceptor each row's arguments from before the argument transforms", async () => {
    const bundle = setup();
    const seen: (readonly string[])[] = [];
    const ops = operations(bundle, {
      insert(context, next) {
        seen.push(context.originalEncodedArgs);
        return next();
      },
    });

    await ops.insert(
      "insertMany",
      [params("a"), params("b")],
      "caller",
      insertStandard(bundle),
      new AbortController().signal,
      ['{"a":1}', '{"b":2}']
    );
    // Without them, the prepared rows' own arguments.
    await ops.insert("insert", [params("c")], "caller", insertStandard(bundle));

    expect(seen).toEqual([['{"a":1}', '{"b":2}'], ["{}"]]);
    expect(Object.isFrozen(seen[0])).toBe(true);
  });

  it("inserts replacement rows one for one", async () => {
    const bundle = setup();
    const ops = operations(bundle, {
      insert: (context, next) =>
        next({
          params: context.params.map((row) => ({
            ...row,
            kind: `${row.kind}2`,
          })),
        }),
    });

    await ops.insert(
      "insertMany",
      [params("a"), params("b")],
      "caller",
      insertStandard(bundle)
    );

    expect(bundle.log).toContain("insert a2,b2 in caller");
  });

  it("rejects replacement rows that don't match the prepared rows", async () => {
    for (const replacement of [
      { params: [params("a")] },
      { params: [params("a"), { ...params("b"), priority: 9 }] },
      {},
    ]) {
      const bundle = setup();
      const ops = operations(bundle, {
        insert: (_context, next) =>
          next(replacement as { readonly params: readonly JobInsertParams[] }),
      });

      await expect(
        ops.insert(
          "insertMany",
          [params("a"), params("b")],
          "caller",
          insertStandard(bundle)
        )
      ).rejects.toBeInstanceOf(ExtensionError);
      expect(bundle.log).toEqual([]);
    }
  });

  it("requires insert, complete, cancel, and retry to call next", async () => {
    const bundle = setup();
    const skip = () => Promise.resolve(null as never);
    const ops = operations(bundle, {
      cancel: skip,
      complete: skip,
      insert: skip,
      retry: skip,
    });

    await expect(ops.cancel(bundle.driver, 1n, undefined)).rejects.toThrow(
      "cancel interceptor must call next() exactly once"
    );
    await expect(ops.retry(bundle.driver, 1n, "caller")).rejects.toThrow(
      "retry interceptor must call next() exactly once"
    );
    await expect(
      ops.complete(bundle.driver, [command(1n)], {})
    ).rejects.toThrow("complete interceptor must call next() exactly once");
    await expect(
      ops.insert("insert", [params("a")], "caller", insertStandard(bundle))
    ).rejects.toThrow("insert interceptor must call next() exactly once");
    expect(bundle.log).toEqual([
      "begin tx1",
      "rollback tx1",
      "begin tx2",
      "rollback tx2",
    ]);
  });

  it("rejects a second call of next without running it", async () => {
    const bundle = setup();
    let second: unknown;
    const ops = operations(bundle, {
      async cancel(_context, next) {
        const job = await next();
        second = await next().catch((error: unknown) => error);
        return job;
      },
    });

    await expect(
      ops.cancel(bundle.driver, 1n, undefined)
    ).resolves.toMatchObject({ id: 1n });

    expect(second).toBeInstanceOf(ExtensionError);
    expect((second as Error).message).toBe(
      "cancel interceptor called next() more than once"
    );
    expect(bundle.log.filter((line) => line.startsWith("start"))).toHaveLength(
      1
    );
  });

  it("rejects next called after the interceptor settled", async () => {
    const bundle = setup();
    let captured: (() => Promise<readonly JobRow[]>) | undefined;
    const ops = operations(bundle, {
      getStuck(_context, next) {
        captured = next;
        return Promise.resolve([]);
      },
    });

    await expect(
      ops.getStuck(
        bundle.driver,
        leader(),
        Temporal.Now.instant(),
        0n,
        10,
        batch()
      )
    ).resolves.toEqual([]);

    await expect(captured?.()).rejects.toThrow(
      "getStuck interceptor called next() after it settled"
    );
    expect(bundle.log).toEqual([]);
  });

  it("awaits an unawaited next before failing, and never commits early", async () => {
    const bundle = setup();
    const ops = operations(bundle, {
      complete(_context, next) {
        void next();
        return Promise.resolve([]);
      },
    });

    await expect(
      ops.complete(bundle.driver, [command(1n)], {})
    ).rejects.toThrow(
      "complete interceptor settled before its next() continuation did; await it"
    );

    expect(bundle.log).toEqual([
      "begin tx1",
      "start complete in tx1",
      "end complete in tx1",
      "rollback tx1",
    ]);
  });

  it("rejects a result other than the continuation's own", async () => {
    const bundle = setup();
    const ops = operations(bundle, {
      async complete(_context, next) {
        const results = await next();
        return results.map((result) => ({ ...result }));
      },
    });

    await expect(
      ops.complete(bundle.driver, [command(1n)], {
        signal: new AbortController().signal,
      })
    ).rejects.toThrow(
      "complete interceptor must resolve with the result of next()"
    );
    expect(bundle.log.at(-1)).toBe("rollback tx1");
  });

  it("rejects a continuation result the interceptor changed", async () => {
    const bundle = setup();
    const ops = operations(bundle, {
      async retry(_context, next) {
        const job = await next();
        if (job !== null) (job as { state: string }).state = "running";
        return job;
      },
      async complete(_context, next) {
        const results = await next();
        (results[0] as { status: string }).status = "stale";
        return results;
      },
    });

    await expect(ops.retry(bundle.driver, 1n, undefined)).rejects.toThrow(
      "retry interceptor changed the result of next()"
    );
    await expect(
      ops.complete(bundle.driver, [command(1n)], {})
    ).rejects.toThrow("complete interceptor changed the result of next()");
    expect(
      bundle.log.filter((line) => line.startsWith("rollback"))
    ).toHaveLength(2);
  });

  it("fails with River's error when an interceptor swallows it", async () => {
    const bundle = setup();
    const failure = new Error("standard failed");
    const ops = operations(bundle, {
      async cancel(_context, next) {
        await next().catch(() => undefined);
        return null;
      },
    });
    const driver = {
      ...bundle.driver,
      jobCancel: () => Promise.reject(failure),
    } as RuntimeDriver<Transaction>;

    await expect(ops.cancel(driver, 1n, undefined)).rejects.toBe(failure);
    expect(bundle.log).toEqual(["begin tx1", "rollback tx1"]);
  });

  it("lets getStuck and rescue replace River's operation", async () => {
    const bundle = setup();
    const replaced = [jobRow(7n, "running"), jobRow(9n, "running")];
    const ops = operations(bundle, {
      getStuck: () => Promise.resolve(replaced),
      rescue: (context) => Promise.resolve(context.jobs.length - 1),
    });

    await expect(
      ops.getStuck(
        bundle.driver,
        leader(),
        Temporal.Now.instant(),
        6n,
        2,
        batch()
      )
    ).resolves.toEqual(replaced);
    await expect(
      ops.rescue(
        bundle.driver,
        leader(),
        Temporal.Now.instant(),
        [rescue(7n), rescue(9n)],
        new AbortController().signal
      )
    ).resolves.toBe(1);
    expect(bundle.log).toEqual(["begin tx1", "commit tx1"]);
  });

  it("validates rows and counts that replace River's operation", async () => {
    for (const rows of [
      [jobRow(7n, "running"), jobRow(7n, "running")],
      [jobRow(5n, "running")],
      [jobRow(7n, "available")],
      [jobRow(7n, "running"), jobRow(8n, "running"), jobRow(9n, "running")],
    ]) {
      const bundle = setup();
      const ops = operations(bundle, { getStuck: () => Promise.resolve(rows) });
      await expect(
        ops.getStuck(
          bundle.driver,
          leader(),
          Temporal.Now.instant(),
          6n,
          2,
          batch()
        )
      ).rejects.toBeInstanceOf(ExtensionError);
    }
    for (const count of [-1, 3, 1.5, Number.NaN]) {
      const bundle = setup();
      const ops = operations(bundle, { rescue: () => Promise.resolve(count) });
      await expect(
        ops.rescue(
          bundle.driver,
          leader(),
          Temporal.Now.instant(),
          [rescue(7n), rescue(9n)],
          new AbortController().signal
        )
      ).rejects.toBeInstanceOf(ExtensionError);
      expect(bundle.log).toEqual(["begin tx1", "rollback tx1"]);
    }
  });

  it("returns what River's rescue returned when the interceptor calls next", async () => {
    const bundle = setup();
    const ops = operations(bundle, {
      rescue: (_context, next) => next(),
    });

    await expect(
      ops.rescue(
        bundle.driver,
        leader(),
        Temporal.Now.instant(),
        [rescue(7n)],
        new AbortController().signal
      )
    ).resolves.toBe(1);
    expect(bundle.log).toEqual([
      "begin tx1",
      "start rescue in tx1",
      "end rescue in tx1",
      "commit tx1",
    ]);
  });

  it("calls interceptors with intercept as this", async () => {
    const bundle = setup();
    const intercept: PilotInterceptors<Transaction> & { seen?: unknown } = {
      cancel(this: unknown, _context, next) {
        intercept.seen = this;
        return next();
      },
    };
    const ops = operations(bundle, intercept);

    await ops.cancel(bundle.driver, 1n, undefined);

    expect(intercept.seen).toBe(intercept);
  });
});

describe("validatePreparedParams", () => {
  it("accepts prepared rows and snapshots the list", () => {
    const rows = [stored("a"), { ...stored("b"), encodedArgs: "[1,null]" }];
    const validated = validatePreparedParams(rows);

    expect(validated).toEqual(rows);
    expect(validated).not.toBe(rows);
    expect(Object.isFrozen(validated)).toBe(true);
  });

  it("rejects rows River can't insert", () => {
    for (const change of [
      // Arguments come from `encodedArgs` alone.
      { args: {} },
      { encodedArgs: 1 },
      { encodedArgs: "{not json" },
      { kind: "" },
      { queue: "" },
      { maxAttempts: 0 },
      { priority: 5 },
      { metadata: null },
      { scheduledAt: "2026-01-01T00:00:00Z" },
      { createdAt: null },
      { state: "done" },
      { tags: [1] },
      { uniqueKey: "key" },
      { uniqueStates: ["done"] },
    ]) {
      expect(() =>
        validatePreparedParams([{ ...stored("a"), ...change }])
      ).toThrow(ValidationError);
    }
  });
});

/** A reinserted job's stored fields. */
function stored(kind: string): PreparedInsertParams {
  const { args, ...fields } = params(kind);
  void args;
  return fields;
}

function params(kind: string): JobInsertParams {
  return {
    args: {},
    encodedArgs: "{}",
    kind,
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: Temporal.Now.instant(),
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

function command(id: bigint): JobCompletionCommand {
  return {
    attempt: 1,
    attemptedBy: "client",
    error: null,
    finalizedAt: Temporal.Now.instant(),
    id,
    kind: "complete",
    output: null,
    outputSet: false,
    scheduledAt: null,
  };
}

function jobRow(id: bigint, state: JobRow["state"]): JobRow {
  const now = Temporal.Now.instant();
  return {
    args: {},
    attempt: 1,
    attemptedAt: now,
    attemptedBy: ["client"],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id,
    kind: "test",
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: now,
    state,
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

function leader(): RuntimeLeader {
  const now = Temporal.Now.instant();
  return { electedAt: now, expiresAt: now.add({ seconds: 30 }), leaderId: "l" };
}

function batch() {
  return { signal: new AbortController().signal, timeoutMs: null };
}

function rescue(id: bigint) {
  return {
    error: {
      at: Temporal.Now.instant(),
      attempt: 1,
      error: "stuck",
      trace: "",
    },
    finalizedAt: null,
    id,
    scheduledAt: Temporal.Now.instant(),
    state: "retryable" as const,
  };
}
