import { AsyncLocalStorage } from "node:async_hooks";
import { statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { setTimeout as sleep } from "node:timers/promises";

import { TransactionScopeError, ValidationError } from "riverqueue";
import type { DurationInput } from "riverqueue";
import { durationToMilliseconds } from "riverqueue/unstable-driver";

import { retryBusy, type BusyRetryPolicy } from "./coordination.js";
import { databaseError, isSqliteError, SQLITE_BACKEND } from "./errors.js";

const BUSY_TIMEOUT_MS_DEFAULT = 5_000;

/**
 * A transaction open in an async context: River's own transaction around an
 * operation, or an application transaction begun with {@link transaction}.
 *
 * Callbacks keep the async context they were created in, so one created
 * inside a transaction may run after it ended. `ended` makes such a callback
 * ignore the frame.
 */
export type TransactionFrame =
  | {
      readonly database: DatabaseSync;
      ended: boolean;
      /** The {@link databaseKey} of `database`. */
      readonly key: string | null;
      readonly kind: "application";
      readonly parent: TransactionFrame | undefined;
    }
  | {
      /** The `SqliteDriver` whose private connection holds the transaction. */
      readonly driver: object;
      ended: boolean;
      /**
       * Whether River's transaction holds its connection. It begins lazily,
       * so code it runs before its first statement (insert middleware before
       * `next()`, `beforeInsert` hooks) may still call River and SQLite
       * freely.
       */
      readonly holdsLock: () => boolean;
      /** The {@link databaseKey} of the driver's database. */
      readonly key: string;
      readonly kind: "river";
      readonly parent: TransactionFrame | undefined;
      /** The driver's own state for the transaction. */
      readonly scope: object;
    };

/** Identities of handles whose database can't be told from a file. */
const registeredKeys = new WeakMap<DatabaseSync, string>();
/** Cached file identities of other handles. */
const fileKeys = new WeakMap<DatabaseSync, string | null>();

/**
 * A key identifying the database `database` is open on: its file's device
 * and inode, or the key a driver registered for an in-memory database it
 * shares. Null when it can't be told, such as for a private `:memory:`
 * database.
 */
export function databaseKey(database: DatabaseSync): string | null {
  const registered = registeredKeys.get(database);
  if (registered !== undefined) return registered;
  if (!database.isOpen) return null;
  let key = fileKeys.get(database);
  if (key === undefined) {
    const location = database.location();
    key = location === null ? null : fileKey(location);
    fileKeys.set(database, key);
  }
  return key;
}

/** The identity of a database file, or null when it can't be read. */
export function fileKey(location: string): string | null {
  try {
    const { dev, ino } = statSync(location, { bigint: true });
    return `file:${dev}:${ino}`;
  } catch {
    return null;
  }
}

/** Record the database a handle whose file can't identify it is open on. */
export function registerDatabase(database: DatabaseSync, key: string): void {
  registeredKeys.set(database, key);
}

/** Whether two database keys are known to name the same database. */
export function sameDatabase(a: string | null, b: string | null): boolean {
  return a !== null && a === b;
}

const frames = new AsyncLocalStorage<TransactionFrame>();

/** The transactions still open in the current async context, innermost first. */
export function openFrames(): TransactionFrame[] {
  const open: TransactionFrame[] = [];
  for (let frame = frames.getStore(); frame !== undefined;) {
    if (!frame.ended) open.push(frame);
    frame = frame.parent;
  }
  return open;
}

/** The async-context frame for River's own transaction on `driver`. */
export function riverFrame(
  driver: object,
  key: string,
  scope: object,
  holdsLock: () => boolean
): TransactionFrame {
  return {
    driver,
    ended: false,
    holdsLock,
    key,
    kind: "river",
    parent: frames.getStore(),
    scope,
  };
}

/** Run `callback` with `frame` as the innermost open transaction. */
export function runInFrame<T>(frame: TransactionFrame, callback: () => T): T {
  return frames.run(frame, callback);
}

/** Options for {@link transaction}. */
export interface SqliteTransactionOptions {
  /**
   * Total time to keep retrying `BEGIN IMMEDIATE`, and `COMMIT`, while
   * another connection holds SQLite's write lock, such as `{ seconds: 5 }`.
   * Retries wait asynchronously, so the event loop keeps running. Defaults
   * to 5 seconds.
   */
  busyTimeout?: DurationInput;
}

/**
 * Run `callback` in an immediate transaction on an application handle,
 * committing when it resolves and rolling back when it throws or rejects.
 *
 * Pass `database` to River as `{ tx }` inside the callback to insert jobs
 * that commit or roll back with the application's rows. The callback may
 * await freely: River runs on its own connection and waits for the
 * transaction to end.
 *
 * `BEGIN IMMEDIATE` takes SQLite's write lock up front. While another
 * connection or process holds it, the helper retries with an asynchronous
 * backoff rather than blocking the event loop, and fails with a retryable
 * `DatabaseOperationError` after `busyTimeout`.
 *
 * Transactions on one database don't nest. SQLite has a single writer, so
 * calling `transaction` on a handle that already has a transaction open,
 * inside another `transaction` callback on the same database, or inside
 * River's own transaction on it once River holds the write lock (insert
 * middleware after `next()`, `afterInsert` hooks) could only fail after
 * `busyTimeout`. It fails at once with a `TransactionScopeError` instead.
 * Transactions on different databases may nest.
 */
export async function transaction<T>(
  database: DatabaseSync,
  callback: (database: DatabaseSync) => T | PromiseLike<T>,
  options: SqliteTransactionOptions = {}
): Promise<T> {
  if (!(database instanceof DatabaseSync) || !database.isOpen) {
    throw new ValidationError(
      "transaction() requires an open node:sqlite DatabaseSync",
      { details: { backend: SQLITE_BACKEND, operation: "transaction" } }
    );
  }
  assertCanBegin(database);
  const policy = busyPolicy(options);

  await begin(database, policy);
  const frame: TransactionFrame = {
    database,
    ended: false,
    key: databaseKey(database),
    kind: "application",
    parent: frames.getStore(),
  };
  try {
    let result: T;
    try {
      result = await frames.run(frame, () => callback(database));
    } catch (error: unknown) {
      rollbackQuietly(database);
      throw error;
    }
    try {
      // SQLite keeps the transaction open when COMMIT is busy, so COMMIT
      // alone can be retried.
      await retryBusy(policy, () => {
        database.exec("COMMIT");
      });
    } catch (error: unknown) {
      rollbackQuietly(database);
      throw wrapError("transaction_commit", error);
    }
    return result;
  } finally {
    frame.ended = true;
  }
}

function assertCanBegin(database: DatabaseSync): void {
  const details = { backend: SQLITE_BACKEND, operation: "transaction" };
  const key = databaseKey(database);
  for (const frame of openFrames()) {
    if (frame.kind === "river") {
      if (!frame.holdsLock() || !sameDatabase(frame.key, key)) continue;
      throw new TransactionScopeError(
        "reentrant",
        "transaction() was called from inside River's own transaction on " +
          "the same database, which insert middleware and hooks run in. " +
          "River holds SQLite's write lock until the middleware or hook " +
          "returns, so the transaction could only fail after its " +
          "busyTimeout. Begin it before inserting and pass { tx }, or run " +
          "it after the insertion returns",
        { details }
      );
    }
    if (frame.database !== database && !sameDatabase(frame.key, key)) {
      continue;
    }
    throw new TransactionScopeError(
      "nested",
      "transaction() was called inside another transaction() on the same " +
        "database. SQLite has one writer, so the inner transaction could " +
        "only fail after its busyTimeout. Use the outer transaction's " +
        "handle instead",
      { details }
    );
  }
  if (database.isTransaction) {
    throw new TransactionScopeError(
      "nested",
      "the handle passed to transaction() already has an open transaction; " +
        "pass it to River as { tx } directly instead",
      { details }
    );
  }
}

async function begin(
  database: DatabaseSync,
  policy: BusyRetryPolicy
): Promise<void> {
  try {
    await retryBusy(policy, () => {
      database.exec("BEGIN IMMEDIATE");
    });
  } catch (error: unknown) {
    throw wrapError("transaction_begin", error);
  }
}

function busyPolicy(options: SqliteTransactionOptions): BusyRetryPolicy {
  return {
    now: () => performance.now(),
    sleep: (milliseconds) => sleep(milliseconds),
    timeoutMs:
      options.busyTimeout === undefined
        ? BUSY_TIMEOUT_MS_DEFAULT
        : durationToMilliseconds("busyTimeout", options.busyTimeout, {
            allowZero: true,
          }),
  };
}

function rollbackQuietly(database: DatabaseSync): void {
  if (!database.isOpen || !database.isTransaction) return;
  try {
    database.exec("ROLLBACK");
  } catch {
    // Preserve the callback's failure as the primary cause.
  }
}

function wrapError(operation: string, cause: unknown): unknown {
  if (!isSqliteError(cause)) return cause;
  return databaseError(
    operation,
    `SQLite ${operation} failed: ${cause.message}`,
    { cause }
  );
}
