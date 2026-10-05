import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Client, defineJob, type ClientOptions } from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteDriver,
  testSqliteMemory,
} from "./driver.js";
import { transaction } from "./scope.js";
import type { SqliteDriverOptions } from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

const otherJob = defineJob({ kind: "other" });
const scopedJob = defineJob({ kind: "scoped" });

describe("SqliteDriver insertion transactions", () => {
  test("rolls an insertion back when middleware or a hook throws after the write", async () => {
    const failure = new Error("fails after the write");
    let failIn: "afterInsert" | "middleware" | null = null;
    const { client, count } = await setup({
      hooks: {
        afterInsert: () => {
          if (failIn === "afterInsert") throw failure;
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          if (failIn === "middleware") throw failure;
          return results;
        },
      ],
    });

    for (const where of ["middleware", "afterInsert"] as const) {
      failIn = where;
      await expect(client.insert(scopedJob, {})).rejects.toBe(failure);
      await expect(
        client.insertMany([{ args: {}, job: scopedJob }])
      ).rejects.toBe(failure);
    }
    expect(count("river_job")).toBe(0);
    expect(count("river_notification")).toBe(0);

    failIn = null;
    await client.insert(scopedJob, {});
    expect(count("river_job")).toBe(1);
  });

  test("takes the write lock only at the insertion's first statement", async () => {
    let entered!: () => void;
    const inMiddleware = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, count, database } = await setup({
      insertMiddleware: [
        async (_context, next) => {
          entered();
          // Stands in for I/O such as a remote key service call.
          await gate;
          return next();
        },
      ],
    });
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");

    const insertion = client.insert(scopedJob, {});
    await inMiddleware;
    // No lock is held yet, so another connection writes without waiting.
    database.prepare("INSERT INTO application_row (id) VALUES (?)").run("a");
    release();
    await insertion;

    expect(count("application_row")).toBe(1);
    expect(count("river_job")).toBe(1);
  });

  test("keeps application statements on other handles out of its transaction", async () => {
    const outcomes: string[] = [];
    const { client, count, database } = await setup((application) => ({
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          outcomes.push(
            `read:${String(
              application
                .prepare("SELECT count(*) AS count FROM river_job")
                .get()?.count
            )}`
          );
          try {
            application
              .prepare("INSERT INTO application_row (id) VALUES (?)")
              .run("joined");
          } catch (error: unknown) {
            outcomes.push(
              `write:${String((error as { errcode?: number }).errcode)}`
            );
          }
          return results;
        },
      ],
    }));
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");

    await client.insert(scopedJob, {});

    // The application saw committed state only and couldn't write while
    // River held the lock; River's insertion still committed on its own.
    expect(outcomes).toEqual(["read:0", "write:5"]);
    expect(count("application_row")).toBe(0);
    expect(count("river_job")).toBe(1);
  });

  test("fails River calls from middleware and hooks that would wait for its transaction", async () => {
    let call: (() => Promise<unknown>) | undefined;
    const { client, count, database } = await setup({
      hooks: {
        afterInsert: async () => {
          await call?.();
        },
      },
    });
    const reentrant = { code: "transaction_scope", reason: "reentrant" };

    call = () => client.jobs.get(1n);
    await expect(client.insert(scopedJob, {})).rejects.toMatchObject(reentrant);
    call = () => client.insert(scopedJob, {});
    await expect(client.insert(scopedJob, {})).rejects.toMatchObject(reentrant);
    call = () => transaction(database, () => undefined);
    await expect(client.insert(scopedJob, {})).rejects.toMatchObject(reentrant);
    expect(count("river_job")).toBe(0);
  });

  test("lets unrelated work that inherited its async context run before the write", async () => {
    // A shared batcher created lazily inside insert middleware inherits the
    // insertion's async context, so its later flushes run "inside" River's
    // transaction while the middleware still awaits I/O before next().
    const bus = new EventEmitter();
    let batcher: NodeJS.Timeout | undefined;
    onTestFinished(() => clearInterval(batcher));
    const { client, count } = await setup({
      insertMiddleware: [
        async (context, next) => {
          if (
            batcher === undefined &&
            context.requests[0]?.args.wire === true
          ) {
            batcher = setInterval(() => bus.emit("flush"), 5);
            await new Promise((resolve) => setTimeout(resolve, 50));
          }
          return next();
        },
      ],
    });
    const flushed: Promise<unknown>[] = [];
    bus.on("flush", () => {
      flushed.push(client.insert(scopedJob, {}));
    });

    await client.insert(scopedJob, { wire: true });
    clearInterval(batcher);
    const results = await Promise.allSettled(flushed);

    expect(results.length).toBeGreaterThan(2);
    expect(results.filter(({ status }) => status === "rejected")).toEqual([]);
    expect(count("river_job")).toBe(results.length + 1);
  });

  test("lets middleware and beforeInsert hooks call River before the write", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-sqlite-other-"));
    const otherDatabase = new DatabaseSync(join(directory, "other.db"));
    onTestFinished(() => {
      otherDatabase.close();
      rmSync(directory, { force: true, recursive: true });
    });
    const { client, count } = await setup((application) => ({
      hooks: {
        beforeInsert: async (context) => {
          if (context.requests[0]?.kind !== scopedJob.kind) return;
          await client.jobs.list();
          await transaction(otherDatabase, () => undefined);
          await transaction(application, () => undefined);
        },
      },
      insertMiddleware: [
        async (context, next) => {
          if (context.requests[0]?.kind === scopedJob.kind) {
            await client.insert(otherJob, {});
          }
          return next();
        },
      ],
    }));

    await client.insert(scopedJob, {});

    expect(count("river_job")).toBe(2);
  });

  test("inserts in an application transaction on another handle", async () => {
    const { client, count, database, driver } = await setup();
    const application = driver.connect();
    onTestFinished(() => application.close());
    application.exec("CREATE TABLE application_row (id text PRIMARY KEY)");
    const rollback = new Error("roll back");

    await expect(
      transaction(application, async (tx) => {
        tx.prepare("INSERT INTO application_row (id) VALUES (?)").run("a");
        await client.insert(scopedJob, {}, { tx });
        // Readers outside the transaction don't see the job yet.
        expect(
          database.prepare("SELECT count(*) AS count FROM river_job").get()
        ).toEqual({ count: 0 });
        throw rollback;
      })
    ).rejects.toBe(rollback);
    expect(count("river_job")).toBe(0);

    await transaction(application, async (tx) => {
      tx.prepare("INSERT INTO application_row (id) VALUES (?)").run("b");
      await client.insertMany([{ args: {}, job: scopedJob }], { tx });
    });
    expect(count("application_row")).toBe(1);
    expect(count("river_job")).toBe(1);
  });

  test("fails an insertion that awaits I/O while River holds the write lock", async () => {
    const io: Record<string, () => Promise<unknown>> = {
      file: () => readFile(import.meta.filename),
      timer: () => new Promise((resolve) => setTimeout(resolve, 5)),
    };
    let wait: (() => Promise<unknown>) | null = null;
    let where: "afterInsert" | "middleware" = "middleware";
    const { client, count } = await setup({
      hooks: {
        afterInsert: async () => {
          if (where === "afterInsert") await wait?.();
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          if (where === "middleware") await wait?.();
          return results;
        },
      ],
    });

    for (const [name, awaitIo] of Object.entries(io)) {
      for (const hook of ["middleware", "afterInsert"] as const) {
        wait = awaitIo;
        where = hook;
        await expect(
          client.insert(scopedJob, {}),
          `${name} in ${hook}`
        ).rejects.toMatchObject({
          code: "transaction_scope",
          message: expect.stringContaining("after next()"),
          reason: "event_loop_turn",
          retryable: false,
        });
      }
    }
    expect(count("river_job")).toBe(0);

    // I/O before next() holds no lock and is fine.
    wait = null;
    await client.insert(scopedJob, {});
    expect(count("river_job")).toBe(1);
  });

  test("releases the write lock as soon as the event loop turns", async () => {
    let afterNext!: () => void;
    const reachedAfterNext = new Promise<void>((resolve) => {
      afterNext = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, count, database, driver } = await setup({
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          afterNext();
          await gate;
          return results;
        },
      ],
    });
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");

    const insertion = client.insert(scopedJob, {});
    void insertion.catch(() => undefined);
    await reachedAfterNext;
    await new Promise((resolve) => setImmediate(resolve));

    // The middleware still awaits, but the lock is already released for
    // application writers and River's other work alike.
    database.prepare("INSERT INTO application_row (id) VALUES (?)").run("a");
    await driver.jobInsert({ args: {}, kind: "during_window" });
    release();

    await expect(insertion).rejects.toMatchObject({
      reason: "event_loop_turn",
    });
    expect(count("application_row")).toBe(1);
    expect(
      database
        .prepare("SELECT kind FROM river_job")
        .all()
        .map(({ kind }) => kind)
    ).toEqual(["during_window"]);
  });

  test("fails fast local I/O after next() deterministically in strict mode", async () => {
    // The probe misses I/O that completes before the event loop's check
    // phase, which is common for an insertion started from a timer or
    // setImmediate callback. River's own tests run in strict mode, which
    // counts every macrotask callback instead.
    const io: Record<string, () => Promise<unknown>> = {
      digest: () => crypto.subtle.digest("SHA-256", new Uint8Array(16)),
      stat: () => stat(import.meta.filename),
    };
    let wait: () => Promise<unknown> = () => Promise.resolve();
    const { client, count } = await setup({
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          await wait();
          return results;
        },
      ],
    });
    const origins: Record<string, (run: () => void) => void> = {
      immediate: (run) => setImmediate(run),
      timer: (run) => setTimeout(run, 0),
    };

    for (const [origin, schedule] of Object.entries(origins)) {
      for (const [name, awaitIo] of Object.entries(io)) {
        wait = awaitIo;
        for (let index = 0; index < 25; index++) {
          const outcome = await new Promise<unknown>((resolve) => {
            schedule(() => {
              client
                .insert(scopedJob, {})
                .then(
                  () => "committed",
                  (error: unknown) => (error as { reason?: string }).reason
                )
                .then(resolve, resolve);
            });
          });
          expect(outcome, `${name} from ${origin}`).toBe("event_loop_turn");
        }
      }
    }
    expect(count("river_job")).toBe(0);
  });

  test("allows microtask-only waits after next() in strict mode", async () => {
    const { client, count } = await setup({
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          for (let index = 0; index < 20; index++) await Promise.resolve();
          await new Promise<void>((resolve) => {
            queueMicrotask(resolve);
          });
          await new Promise<void>((resolve) => {
            process.nextTick(resolve);
          });
          return results;
        },
      ],
    });
    const origins: ((run: () => void) => void)[] = [
      (run) => setImmediate(run),
      (run) => setTimeout(run, 0),
      (run) => {
        void stat(import.meta.filename).then(run);
      },
      (run) => {
        run();
      },
    ];

    for (const schedule of origins) {
      for (let index = 0; index < 10; index++) {
        await new Promise<void>((resolve, reject) => {
          schedule(() => {
            client.insert(scopedJob, {}).then(() => resolve(), reject);
          });
        });
      }
    }
    expect(count("river_job")).toBe(40);
  });

  test("keeps a plain insertion within one turn of the event loop", async () => {
    const { client, count, database } = await setup();
    database.exec("CREATE TABLE application_row (id integer PRIMARY KEY)");
    const busy: unknown[] = [];
    let writes = 0;
    let running = true;
    const write = (): void => {
      if (!running) return;
      try {
        database
          .prepare("INSERT INTO application_row (id) VALUES (?)")
          .run(++writes);
      } catch (error: unknown) {
        busy.push(error);
      }
      setImmediate(write);
    };
    setImmediate(write);

    for (let index = 0; index < 200; index++) {
      await new Promise((resolve) => setImmediate(resolve));
      await client.insert(scopedJob, {});
    }
    await Promise.all(
      Array.from({ length: 50 }, () => client.insert(scopedJob, {}))
    );
    running = false;

    // The writer ran between insertions and never met River's lock.
    expect(busy).toEqual([]);
    expect(writes).toBeGreaterThan(100);
    expect(count("river_job")).toBe(250);
  });

  test("keeps its transaction while COMMIT waits across turns of the event loop", async () => {
    using driver = testSqliteMemory(STRICT);
    await migrate(driver.database);
    const reader = driver.connect();
    onTestFinished(() => reader.close());
    // An in-memory database's COMMIT waits for readers holding a snapshot.
    reader.exec("BEGIN");
    reader.prepare("SELECT count(*) FROM river_job").get();
    setTimeout(() => reader.exec("COMMIT"), 30);

    await driver.operationScope(undefined, async (tx) => {
      await driver.jobInsert({ args: {}, kind: "slow_commit" }, { tx });
    });

    expect(driver.database.prepare("SELECT kind FROM river_job").all()).toEqual(
      [{ kind: "slow_commit" }]
    );
  });
});

async function setup(
  options:
    | ClientOptions
    | ((database: DatabaseSync, driver: SqliteRuntime) => ClientOptions) = {}
) {
  const directory = mkdtempSync(join(tmpdir(), "river-sqlite-scope-"));
  const database = new DatabaseSync(join(directory, "river.db"));
  await migrate(database);
  const driver = testSqliteDriver(database, STRICT);
  onTestFinished(() => {
    driver.close();
    database.close();
    rmSync(directory, { force: true, recursive: true });
  });
  const client = new Client(
    driver,
    typeof options === "function" ? options(database, driver) : options
  );
  const count = (table: string): number =>
    Number(
      database.prepare(`SELECT count(*) AS count FROM ${table}`).get()?.count
    );
  return { client, count, database, driver };
}

async function migrate(database: DatabaseSync): Promise<void> {
  const moduleUrl = new URL("../../../migrate/dist/index.js", import.meta.url);
  const migrationModule = (await import(moduleUrl.href)) as {
    createMigrator(target: { database: DatabaseSync }): {
      migrateUp(): Promise<unknown>;
    };
  };
  await migrationModule.createMigrator({ database }).migrateUp();
}
