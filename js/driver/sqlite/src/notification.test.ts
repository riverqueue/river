import type { DatabaseSync } from "node:sqlite";

import type { RuntimeNotification } from "riverqueue/unstable-driver";
import { describe, expect, onTestFinished, test, vi } from "vitest";

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

// Mirrors River's driver tests for SQLite's `river_notification` outbox: its
// listener and `NotificationDeleteBefore`.
describe("SqliteDriver notifications", () => {
  describe("cleanup", () => {
    test("deletes notifications before a horizon", async () => {
      const { database, driver } = await setup();
      const createdBefore = insertAgedNotifications(database);

      await expect(
        driver.notificationCleanup({ createdBefore, limit: 10 })
      ).resolves.toBe(2);
      expect(payloadsByAge(database)).toEqual([
        "horizon_payload",
        "new_payload",
      ]);
    });

    test("deletes at most a limit of notifications, oldest first", async () => {
      const { database, driver } = await setup();
      const params = {
        createdBefore: insertAgedNotifications(database),
        limit: 1,
      };

      await expect(driver.notificationCleanup(params)).resolves.toBe(1);
      // Delete by age, even when the oldest notification was inserted later.
      expect(payloadsByAge(database)[0]).toBe("old_payload");
      await expect(driver.notificationCleanup(params)).resolves.toBe(1);
      await expect(driver.notificationCleanup(params)).resolves.toBe(0);
      // Keeps the notification exactly at the horizon.
      expect(payloadsByAge(database)).toEqual([
        "horizon_payload",
        "new_payload",
      ]);
    });
  });

  describe("subscriptions", () => {
    test("delivers more notifications than one read batch, in order", async () => {
      const { database, driver } = await setup();
      const poll = vi.spyOn(driver, "notificationPoll");
      const subscription = await subscribe(driver, ["insert"]);
      const payloads = Array.from(
        { length: 600 },
        (_, index) => `payload_${index.toString()}`
      );

      notify(database, "river_control", payloads);
      notify(database, "river_insert", payloads);

      for (const payload of payloads) {
        await expect(subscription.next()).resolves.toEqual({
          payload,
          topic: "insert",
        });
      }
      await subscription.requireNone();
      expect(poll.mock.calls.map(([params]) => params?.limit)).toEqual(
        poll.mock.calls.map(() => 256)
      );
      // Three full batches of notifications, 256 at a time.
      expect(poll.mock.calls.length).toBeGreaterThanOrEqual(3);
    });

    test("discards notifications read but not delivered when closed", async () => {
      const { database, driver } = await setup();
      const first = await subscribe(driver, ["insert"]);

      notify(database, "river_insert", ["first", "buffered"]);
      await expect(first.next()).resolves.toMatchObject({ payload: "first" });
      first.close();
      await expect(first.done()).resolves.toBe(true);

      const second = await subscribe(driver, ["insert"]);
      notify(database, "river_insert", ["new"]);
      await expect(second.next()).resolves.toMatchObject({ payload: "new" });
      await second.requireNone();
    });

    test("does not replay notifications written before it subscribed", async () => {
      const { database, driver } = await setup();

      notify(database, "river_insert", ["old"]);
      const subscription = await subscribe(driver, ["insert"]);
      notify(database, "river_insert", ["new"]);

      await expect(subscription.next()).resolves.toEqual({
        payload: "new",
        topic: "insert",
      });
      await subscription.requireNone();
    });

    test("resubscribes after cleanup deleted every notification", async () => {
      const { database, driver } = await setup();
      const first = await subscribe(driver, ["insert"]);

      notify(database, "river_insert", ["first", "buffered"]);
      await expect(first.next()).resolves.toMatchObject({ payload: "first" });
      first.close();
      database.exec("DELETE FROM river_notification");

      const second = await subscribe(driver, ["insert"]);
      notify(database, "river_insert", ["new"]);
      await expect(second.next()).resolves.toMatchObject({ payload: "new" });
      await second.requireNone();
    });

    test("skips notifications written between subscriptions", async () => {
      const { database, driver } = await setup();
      const first = await subscribe(driver, ["insert"]);
      first.close();

      notify(database, "river_insert", ["gap"]);
      const second = await subscribe(driver, ["insert"]);
      notify(database, "river_insert", ["new"]);

      await expect(second.next()).resolves.toMatchObject({ payload: "new" });
      await second.requireNone();
    });
  });
});

interface Subscription {
  close(): void;
  /** Whether the subscription ends without delivering anything else. */
  done(): Promise<boolean>;
  next(): Promise<RuntimeNotification>;
  /** Wait a few polls and fail if anything else is delivered. */
  requireNone(): Promise<void>;
}

async function subscribe(
  driver: SqliteRuntime,
  topics: readonly RuntimeNotification["topic"][]
): Promise<Subscription> {
  const controller = new AbortController();
  onTestFinished(() => {
    controller.abort();
  });
  const ready = Promise.withResolvers<undefined>();
  const iterator = driver
    .runtimeNotificationSubscribe(topics, controller.signal, () => {
      ready.resolve(undefined);
    })
    [Symbol.asyncIterator]();
  // The generator runs until its first yield, reading the outbox's last ID.
  let pending: Promise<IteratorResult<RuntimeNotification>> | undefined =
    iterator.next();
  await ready.promise;
  // Ask for each notification only when the test wants it, so nothing is
  // read ahead of the subscription's own buffering.
  const take = (): Promise<IteratorResult<RuntimeNotification>> => {
    const result = pending ?? iterator.next();
    pending = undefined;
    return result;
  };
  return {
    close: () => {
      controller.abort();
    },
    done: async () => (await take()).done === true,
    next: async () => {
      const result = await take();
      if (result.done === true) throw new Error("subscription ended");
      return result.value;
    },
    requireNone: async () => {
      pending = take();
      const timeout = new Promise<"none">((resolve) =>
        setTimeout(resolve, 300, "none")
      );
      await expect(Promise.race([pending, timeout])).resolves.toBe("none");
    },
  };
}

/**
 * Insert four notifications out of age order, one exactly at the returned
 * horizon, in the fixed-width timestamp format River's SQLite driver writes.
 */
function insertAgedNotifications(database: DatabaseSync): Temporal.Instant {
  // Include a trailing fractional zero to exercise the fixed-width format.
  const now = Temporal.Now.instant()
    .round({ roundingMode: "floor", smallestUnit: "second" })
    .add({ milliseconds: 120 });
  const timestamp = (instant: Temporal.Instant): string =>
    instant
      .toString({ fractionalSecondDigits: 3 })
      .replace("T", " ")
      .replace(/Z$/, "");
  const insert = database.prepare(
    "INSERT INTO river_notification (created_at, payload, topic) VALUES (?, ?, 'topic')"
  );
  insert.run(timestamp(now.subtract({ minutes: 61 })), "old_payload");
  insert.run(timestamp(now.subtract({ hours: 2 })), "oldest_payload");
  insert.run(timestamp(now.subtract({ hours: 1 })), "horizon_payload");
  insert.run(timestamp(now.subtract({ minutes: 30 })), "new_payload");
  return now.subtract({ hours: 1 });
}

function notify(
  database: DatabaseSync,
  topic: string,
  payloads: readonly string[]
): void {
  const insert = database.prepare(
    "INSERT INTO river_notification (payload, topic) VALUES (?, ?)"
  );
  database.exec("BEGIN");
  for (const payload of payloads) insert.run(payload, topic);
  database.exec("COMMIT");
}

function payloadsByAge(database: DatabaseSync): string[] {
  return database
    .prepare("SELECT payload FROM river_notification ORDER BY created_at")
    .all()
    .map((row) => row.payload as string);
}

async function setup(): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const driver = testSqliteMemory(STRICT);
  const database = driver.connect();
  onTestFinished(() => {
    database.close();
    driver.close();
  });
  await migrate(database);
  return { database, driver };
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
