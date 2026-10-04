import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { DatabaseOperationError } from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";

import { FifoLock, retryBusy } from "./coordination.js";
import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteDriver,
  testSqliteMemory,
} from "./driver.js";
import { transaction } from "./scope.js";
import type { SqliteDriverOptions, SqliteJobRow } from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

describe("FifoLock", () => {
  test("grants the lock in request order and releases idempotently", async () => {
    const lock = new FifoLock();
    const order: string[] = [];
    const releaseFirst = await lock.acquire();
    const second = lock.acquire().then((release) => {
      order.push("second");
      return release;
    });
    const third = lock.acquire().then((release) => {
      order.push("third");
      release();
    });

    await Promise.resolve();
    expect(order).toEqual([]);
    releaseFirst();
    releaseFirst();
    const releaseSecond = await second;
    expect(order).toEqual(["second"]);
    releaseSecond();
    await third;
    expect(order).toEqual(["second", "third"]);
    (await lock.acquire())();
  });
});

describe("retryBusy", () => {
  test("backs off asynchronously and gives up at the deadline", async () => {
    let now = 0;
    const sleeps: number[] = [];
    const busy = Object.assign(new Error("database is locked"), {
      code: "ERR_SQLITE_ERROR",
      errcode: 5,
    });
    let attempts = 0;
    const policy = {
      now: () => now,
      sleep: (milliseconds: number) => {
        sleeps.push(milliseconds);
        now += milliseconds;
        return Promise.resolve();
      },
      timeoutMs: 100,
    };

    await expect(
      retryBusy(policy, () => {
        attempts++;
        throw busy;
      })
    ).rejects.toBe(busy);
    expect(sleeps).toEqual([2, 4, 8, 16, 32, 38]);
    expect(attempts).toBe(7);

    now = 0;
    sleeps.length = 0;
    let remainingFailures = 2;
    await expect(
      retryBusy(policy, () => {
        if (remainingFailures-- > 0) throw busy;
        return "done";
      })
    ).resolves.toBe("done");
    expect(sleeps).toEqual([2, 4]);

    const other = new Error("constraint failed");
    await expect(
      retryBusy(policy, () => {
        throw other;
      })
    ).rejects.toBe(other);
  });
});

describe("SqliteDriver connection coordination", () => {
  test("keeps the event loop running while another connection holds the write lock", async () => {
    const { driver, path } = await fileSetup();
    await driver.jobInsert({ args: {}, kind: "busy_claim" });
    const other = lockingConnection(path);

    let ticks = 0;
    const interval = setInterval(() => {
      ticks++;
    }, 1);
    onTestFinished(() => clearInterval(interval));
    let settled = false;
    const claim = driver.jobClaim(claimParams());
    void claim.finally(() => {
      settled = true;
    });

    await waitUntil(() => ticks >= 20);
    expect(settled).toBe(false);
    other.exec("COMMIT");

    expect((await claim).jobs).toHaveLength(1);
  });

  test("migrates through the driver while another connection holds the write lock", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-sqlite-"));
    const path = join(directory, "river.db");
    const database = new DatabaseSync(path);
    const driver = testSqliteDriver(database, STRICT);
    onTestFinished(() => {
      driver.close();
      database.close();
      rmSync(directory, { force: true, recursive: true });
    });
    const other = lockingConnection(path);

    const migrated = createMigrator(driver).migrateUp();
    await new Promise((resolve) => setTimeout(resolve, 100));
    other.exec("COMMIT");

    await expect(migrated).resolves.toMatchObject({
      versions: expect.arrayContaining([
        expect.objectContaining({ version: 1 }),
      ]),
    });
    await expect(
      driver.jobInsert({ args: {}, kind: "migrated" })
    ).resolves.toMatchObject({ status: "inserted" });
  });

  test("switches to WAL on its first operation when construction finds the database busy", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-sqlite-"));
    const path = join(directory, "river.db");
    const database = new DatabaseSync(path);
    onTestFinished(() => {
      database.close();
      rmSync(directory, { force: true, recursive: true });
    });
    await migrate(database);
    // A fresh connection reads the mode SQLite recorded in the file.
    const journalMode = () => {
      const fresh = new DatabaseSync(path);
      try {
        return fresh.prepare("PRAGMA journal_mode").get()?.journal_mode;
      } finally {
        fresh.close();
      }
    };
    expect(journalMode()).toBe("delete");
    const other = lockingConnection(path);

    const busy = busyWaits();
    const driver = testSqliteDriver(database, {
      [SQLITE_DRIVER_TEST_HOOKS]: { sleep: busy.sleep, strictLockWindow: true },
    } as SqliteDriverOptions);
    onTestFinished(() => driver.close());
    expect(journalMode()).toBe("delete");
    let settled = false;
    const insert = driver.jobInsert({ args: {}, kind: "wal_pending" });
    void insert.finally(() => {
      settled = true;
    });
    // The insertion is retrying the busy WAL switch, not finished.
    await waitUntil(() => busy.count >= 2);
    expect(settled).toBe(false);
    other.exec("COMMIT");

    await expect(insert).resolves.toMatchObject({ status: "inserted" });
    expect(journalMode()).toBe("wal");
  });

  test("fails with a retryable error once the busy bound passes", async () => {
    const { driver, path } = await fileSetup({
      busyTimeout: { milliseconds: 30 },
    });
    const other = lockingConnection(path);

    const failure = driver.jobInsert({ args: {}, kind: "busy_bound" });

    await expect(failure).rejects.toBeInstanceOf(DatabaseOperationError);
    await expect(failure).rejects.toMatchObject({
      backend: "sqlite",
      code: "database",
      message: expect.stringContaining("database is locked"),
      operation: "insert",
      retryable: true,
    });
    other.exec("ROLLBACK");
    await expect(
      driver.jobInsert({ args: {}, kind: "busy_bound" })
    ).resolves.toMatchObject({ status: "inserted" });
  });

  test("retries a busy application transaction begin before running the callback", async () => {
    const { database, driver, path } = await fileSetup();
    const other = lockingConnection(path);
    let entered = false;

    const pending = transaction(database, async (tx) => {
      entered = true;
      return (await driver.jobInsert({ args: {}, kind: "busy_tx" }, { tx }))
        .job;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(entered).toBe(false);
    other.exec("COMMIT");

    const job = await pending;
    expect((await driver.jobGet(job.id))?.kind).toBe("busy_tx");
  });

  test("hides an application transaction's jobs from workers until it commits", async () => {
    const busy = busyWaits();
    const { database, driver } = await fileSetup({
      [SQLITE_DRIVER_TEST_HOOKS]: { sleep: busy.sleep },
    } as SqliteDriverOptions);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let inserted: SqliteJobRow | undefined;

    const pending = transaction(database, async (tx) => {
      inserted = (
        await driver.jobInsert(
          { args: {}, kind: "hidden_until_commit" },
          { tx }
        )
      ).job;
      await gate;
    });
    await waitUntil(() => inserted !== undefined);
    const id = (inserted as SqliteJobRow).id;

    // Reads see the committed state; a claim waits for the write lock.
    await expect(driver.jobGet(id)).resolves.toBeNull();
    let claimed: readonly SqliteJobRow[] | undefined;
    const claim = driver.jobClaim(claimParams()).then((result) => {
      claimed = result.jobs;
    });
    // The claim is retrying against the application's write lock.
    await waitUntil(() => busy.count >= 2);
    expect(claimed).toBeUndefined();

    release();
    await pending;
    await claim;
    expect(claimed?.map((job) => job.id)).toEqual([id]);
  });

  test("keeps working with an application handle that was closed and reopened", async () => {
    const { database, driver } = await fileSetup();
    const insert = () =>
      transaction(database, (tx) =>
        driver.jobInsert({ args: {}, kind: "reopened" }, { tx })
      );
    await insert();

    // Closing a handle finalizes the statements River prepared on it.
    database.close();
    database.open();

    await expect(insert()).resolves.toMatchObject({ status: "inserted" });
  });

  test("rolls back an application transaction's jobs with its rows", async () => {
    const { database, driver } = await fileSetup();
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");
    const rollback = new Error("roll back");

    await expect(
      transaction(database, async (tx) => {
        tx.prepare("INSERT INTO application_row (id) VALUES (?)").run("row");
        await driver.jobInsert({ args: {}, id: 401n, kind: "rolled" }, { tx });
        await new Promise((resolve) => setTimeout(resolve, 5));
        throw rollback;
      })
    ).rejects.toBe(rollback);

    expect(await driver.jobGet(401n)).toBeNull();
    expect(
      database.prepare("SELECT count(*) AS count FROM application_row").get()
    ).toEqual({ count: 0 });
  });

  test("delivers notifications written in an application transaction after it commits", async () => {
    const { database, driver } = await fileSetup();
    const controller = new AbortController();
    onTestFinished(() => controller.abort());
    let ready!: () => void;
    const subscribed = new Promise<void>((resolve) => {
      ready = resolve;
    });
    const iterator = driver
      .runtimeNotificationSubscribe(["insert"], controller.signal, ready)
      [Symbol.asyncIterator]();
    const next = iterator.next();
    await subscribed;

    await transaction(database, async (tx) => {
      await driver.jobInsert({ args: {}, kind: "notify_during_tx" }, { tx });
      await driver.notifyInsert(["default"], { tx });
      // Spans at least one poll interval of the subscription.
      await new Promise((resolve) => setTimeout(resolve, 150));
    });

    await expect(next).resolves.toEqual({
      done: false,
      value: { payload: '{"queue": "default"}', topic: "insert" },
    });
  });

  test("keeps River's background work waiting asynchronously during a long application transaction", async () => {
    const { database, driver } = await fileSetup();
    await driver.jobInsert({ args: {}, kind: "background_waits" });
    let maxLagMs = 0;
    let last = performance.now();
    const interval = setInterval(() => {
      const now = performance.now();
      maxLagMs = Math.max(maxLagMs, now - last);
      last = now;
    }, 1);
    onTestFinished(() => clearInterval(interval));

    let started!: () => void;
    const open = new Promise<void>((resolve) => {
      started = resolve;
    });
    const pending = transaction(database, async (tx) => {
      await driver.jobInsert({ args: {}, kind: "application_tx" }, { tx });
      started();
      await new Promise((resolve) => setTimeout(resolve, 300));
    });
    await open;
    const claim = driver.jobClaim(claimParams());
    await pending;
    expect((await claim).jobs).toHaveLength(2);

    // River's connection never lets SQLite wait for a lock synchronously; it
    // retries between turns of the event loop. Blocking instead would have
    // frozen timers for the whole 300 ms transaction (or deadlocked it), so
    // a bound well below that is generous even on a loaded machine.
    expect(
      await driver.execute("busy_timeout", {}, (db) =>
        db.prepare("PRAGMA busy_timeout").get()
      )
    ).toEqual({ timeout: 0 });
    expect(maxLagMs).toBeLessThan(250);
  });

  test("stops a River call waiting behind the transaction it runs inside", async () => {
    const { driver } = await fileSetup();

    await driver.operationScope(undefined, async (tx) => {
      // The insertion takes the connection lock synchronously; the read,
      // issued from the same scope before the lock was granted, would wait
      // for the scope, which waits for the read.
      const insert = driver.jobInsert({ args: {}, kind: "scope" }, { tx });
      const read = driver.jobGet(1n);
      await expect(read).rejects.toMatchObject({
        code: "transaction_scope",
        reason: "reentrant",
      });
      await insert;
    });
  });

  test("lets River calls run inside its transaction before the first statement", async () => {
    const { database, driver } = await fileSetup();
    const other = await fileSetup();

    await driver.operationScope(undefined, async (tx) => {
      // Nothing is locked yet, so these run as they would on PostgreSQL.
      await driver.jobInsert({ args: {}, kind: "before_first" });
      await expect(driver.jobGet(1n)).resolves.not.toBeNull();
      await transaction(database, () => undefined);
      await driver.jobInsert({ args: {}, kind: "scope" }, { tx });
      // Once River holds the lock, only another database is reachable.
      await transaction(other.database, () => undefined);
      await expect(
        transaction(database, () => undefined)
      ).rejects.toMatchObject({ reason: "reentrant" });
    });

    await transaction(database, () =>
      transaction(other.database, () => undefined)
    );
  });

  test("names another driver on the same database in a re-entry error", async () => {
    const { database, driver } = await fileSetup();
    using second = testSqliteDriver(database, STRICT);

    await driver.operationScope(undefined, async (tx) => {
      await driver.jobInsert({ args: {}, kind: "scope" }, { tx });
      await expect(second.jobGet(1n)).rejects.toMatchObject({
        message: expect.stringContaining("another SqliteDriver"),
        reason: "reentrant",
      });
    });
  });

  test("points at { tx } when an application transaction in this process holds the lock", async () => {
    const { database, driver } = await fileSetup({
      busyTimeout: { milliseconds: 30 },
    });

    database.exec("BEGIN IMMEDIATE");
    await expect(
      driver.jobInsert({ args: {}, kind: "raw_begin" })
    ).rejects.toMatchObject({
      code: "database",
      message: expect.stringContaining("pass the handle as { tx }"),
    });
    database.exec("ROLLBACK");
  });

  test("fails an open River transaction when the driver closes", async () => {
    const { database, driver } = await fileSetup();

    await expect(
      driver.operationScope(undefined, async (tx) => {
        await driver.jobInsert({ args: {}, kind: "closed" }, { tx });
        driver.close();
        await driver.jobInsert({ args: {}, kind: "after_close" }, { tx });
      })
    ).rejects.toMatchObject({ code: "lifecycle" });
    await expect(
      driver.operationScope(undefined, async (tx) => {
        driver.close();
        await driver.jobInsert({ args: {}, kind: "closed" }, { tx });
      })
    ).rejects.toMatchObject({ code: "lifecycle" });
    expect(
      database.prepare("SELECT count(*) AS count FROM river_job").get()
    ).toEqual({ count: 0 });
  });

  test("fails River calls that would wait for a transaction they run inside", async () => {
    const { database, driver } = await fileSetup();

    await driver.operationScope(undefined, async (tx) => {
      await driver.jobInsert({ args: {}, kind: "scope" }, { tx });
      const reentrant = {
        code: "transaction_scope",
        reason: "reentrant",
        retryable: false,
      };
      await expect(driver.jobGet(1n)).rejects.toMatchObject(reentrant);
      await expect(
        driver.jobInsert({ args: {}, kind: "reentrant" })
      ).rejects.toMatchObject(reentrant);
      await expect(
        driver.operationScope(undefined, () => Promise.resolve())
      ).rejects.toMatchObject(reentrant);
      await expect(
        transaction(database, () => undefined)
      ).rejects.toMatchObject(reentrant);
    });

    await transaction(database, async () => {
      // Reads don't wait for the application's write lock; writes would.
      await expect(driver.jobGet(1n)).resolves.not.toBeNull();
      await expect(
        driver.jobInsert({ args: {}, kind: "reentrant" })
      ).rejects.toMatchObject({
        code: "transaction_scope",
        reason: "reentrant",
      });
      await expect(
        transaction(database, () => undefined)
      ).rejects.toMatchObject({ code: "transaction_scope", reason: "nested" });
    });

    database.exec("BEGIN");
    await expect(transaction(database, () => undefined)).rejects.toMatchObject({
      code: "transaction_scope",
      reason: "nested",
    });
    database.exec("ROLLBACK");
  });

  test("ignores the async context of a transaction that already ended", async () => {
    const { database, driver } = await fileSetup();
    let later!: Promise<readonly unknown[]>;

    await driver.operationScope(undefined, async (tx) => {
      await driver.jobInsert({ args: {}, id: 1n, kind: "ended" }, { tx });
      later = new Promise((resolve) => setTimeout(resolve, 1)).then(() =>
        Promise.all([
          driver.operationScope(undefined, () =>
            Promise.resolve("scope after commit")
          ),
          transaction(database, () => "transaction after commit"),
          driver.jobGet(1n).then((job) => job?.kind),
        ])
      );
    });

    await expect(later).resolves.toEqual([
      "scope after commit",
      "transaction after commit",
      "ended",
    ]);
  });

  test("never ends an application transaction it did not begin", async () => {
    const { database, driver } = await fileSetup();
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");
    database.exec("BEGIN IMMEDIATE");
    database
      .prepare("INSERT INTO application_row (id) VALUES (?)")
      .run("application");

    await driver.jobInsert(
      { args: {}, kind: "inside_application_tx" },
      { tx: database }
    );

    expect(database.isTransaction).toBe(true);
    database.exec("ROLLBACK");
    expect(
      database.prepare("SELECT count(*) AS count FROM application_row").get()
    ).toEqual({ count: 0 });
    expect(
      database.prepare("SELECT count(*) AS count FROM river_job").get()
    ).toEqual({ count: 0 });
  });

  test("leaves a failed operation's writes for the caller transaction to roll back", async () => {
    const { database, driver } = await fileSetup();
    database.exec(
      `CREATE TRIGGER fail_insert BEFORE INSERT ON river_job
       WHEN NEW.kind = 'failing'
       BEGIN SELECT RAISE(ABORT, 'insert failed'); END`
    );

    // Like River for Go, River opens no savepoint in the caller's
    // transaction: the batch's first row stays in it next to the caller's
    // earlier work until the caller rolls back.
    const rollback = new Error("roll back");
    await expect(
      transaction(database, async (tx) => {
        await driver.jobInsert({ args: {}, id: 1n, kind: "before" }, { tx });
        await expect(
          driver.jobInsertMany(
            [
              { args: {}, id: 2n, kind: "partial" },
              { args: {}, id: 3n, kind: "failing" },
            ],
            { tx }
          )
        ).rejects.toMatchObject({
          code: "database",
          message: expect.stringContaining("insert failed"),
        });
        expect((await driver.jobGet(1n, { tx }))?.kind).toBe("before");
        expect((await driver.jobGet(2n, { tx }))?.kind).toBe("partial");
        throw rollback;
      })
    ).rejects.toBe(rollback);

    expect(await driver.jobGet(1n)).toBeNull();
    expect(await driver.jobGet(2n)).toBeNull();
  });

  test("shares an in-memory database between River and connect() handles", async () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => driver.close());
    await migrate(driver.database);
    const application = driver.connect();
    onTestFinished(() => application.close());
    application.exec("CREATE TABLE application_row (id text PRIMARY KEY)");

    await transaction(application, async (tx) => {
      tx.prepare("INSERT INTO application_row (id) VALUES (?)").run("row");
      await driver.jobInsert({ args: {}, id: 7n, kind: "memory" }, { tx });
    });

    expect((await driver.jobGet(7n))?.kind).toBe("memory");
    expect(
      driver.database
        .prepare("SELECT count(*) AS count FROM application_row")
        .get()
    ).toEqual({ count: 1 });
    expect(application.prepare("PRAGMA busy_timeout").get()).toEqual({
      timeout: 0,
    });
  });
});

function claimParams() {
  return {
    attemptedBy: "coordination-worker",
    kinds: [],
    queues: [{ limit: 10, name: "default" }],
  };
}

async function fileSetup(options: SqliteDriverOptions = {}): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
  path: string;
}> {
  const directory = mkdtempSync(join(tmpdir(), "river-sqlite-"));
  const path = join(directory, "river.db");
  const database = new DatabaseSync(path);
  await migrate(database);
  const hooks = (options as { [SQLITE_DRIVER_TEST_HOOKS]?: object })[
    SQLITE_DRIVER_TEST_HOOKS
  ];
  const driver = testSqliteDriver(database, {
    ...options,
    [SQLITE_DRIVER_TEST_HOOKS]: { ...hooks, strictLockWindow: true },
  } as SqliteDriverOptions);
  onTestFinished(() => {
    driver.close();
    database.close();
    rmSync(directory, { force: true, recursive: true });
  });
  return { database, driver, path };
}

/**
 * A busy-retry sleep for the driver's test hooks that counts the retries,
 * so a test can wait until an operation is actually waiting on a lock.
 */
function busyWaits(): {
  readonly count: number;
  sleep(milliseconds: number): Promise<void>;
} {
  let count = 0;
  return {
    get count() {
      return count;
    },
    sleep: (milliseconds) => {
      count++;
      return new Promise((resolve) => setTimeout(resolve, milliseconds));
    },
  };
}

/** Open a second connection that holds SQLite's write lock. */
function lockingConnection(path: string): DatabaseSync {
  const other = new DatabaseSync(path);
  onTestFinished(() => {
    if (other.isTransaction) other.exec("ROLLBACK");
    other.close();
  });
  other.exec("BEGIN IMMEDIATE");
  return other;
}

function createMigrator(target: object): {
  migrateUp(): Promise<unknown>;
} {
  return migrationModule.createMigrator(target);
}

async function migrate(database: DatabaseSync): Promise<void> {
  await createMigrator({ database }).migrateUp();
}

const migrationModule = (await import(
  new URL("../../../migrate/dist/index.js", import.meta.url).href
)) as {
  createMigrator(target: object): { migrateUp(): Promise<unknown> };
};

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let index = 0; index < 2_000; index++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition was not reached");
}
