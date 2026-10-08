/**
 * The Postgres database River gives a client's pilot: pool connections,
 * transactions, and the claim and notification statements a
 * companion runs inside its own transactions.
 */
import type { ClientBase, Pool } from "pg";
import { TransactionScopeError, ValidationError } from "riverqueue";
import type {
  FinalizedJobDeleteParams,
  JobClaimResult,
  PilotDatabase,
} from "riverqueue/unstable-driver";

import { isQueryable, type PgDatabase } from "./database.js";
import { backendMismatchError } from "./errors.js";
import { abortablePromise, PgClientLease } from "./lease.js";
import { jobLoadClaimed } from "./sql/jobs.js";
import { jobDeleteFinalized } from "./sql/maintenance.js";
import { notifyMany } from "./sql/notify.js";

/** A pilot's view of a pool-backed `PgDriver`. */
export class PgPilotDatabase implements PilotDatabase<ClientBase> {
  readonly backend = "postgres";
  readonly schema: string | null;

  readonly #db: PgDatabase;
  readonly #pool: Pool;

  constructor(db: PgDatabase, pool: Pool) {
    this.#db = db;
    this.#pool = pool;
    this.schema = db.schemaName;
  }

  async connection<Result>(
    callback: (handle: ClientBase) => PromiseLike<Result> | Result,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<Result> {
    const acquiring = this.#pool.connect();
    let lease: PgClientLease;
    try {
      lease = new PgClientLease(
        await abortablePromise(acquiring, options.signal)
      );
    } catch (error: unknown) {
      void acquiring.then((late) => late.release()).catch(() => undefined);
      throw error;
    }
    try {
      let result: Result;
      try {
        result = await lease.race(callback(lease.client));
      } catch (error: unknown) {
        await this.#closeTransaction(lease);
        throw error;
      }
      if (await this.#closeTransaction(lease)) {
        throw new TransactionScopeError(
          "nested",
          "a companion's connection callback left a transaction open; " +
            "River rolled it back. Use the companion database's " +
            "transaction() instead",
          { details: { backend: "postgres", operation: "pilotConnection" } }
        );
      }
      return result;
    } finally {
      lease.release();
    }
  }

  /**
   * Roll back a transaction a connection callback left open, so the pooled
   * connection never returns to the pool inside one, and report whether
   * there was one. Within a transaction block a later statement's start
   * time differs from the transaction's; a single autocommit statement's
   * doesn't. A statement rejected because the transaction is aborted means
   * one is open too.
   */
  async #closeTransaction(lease: PgClientLease): Promise<boolean> {
    if (lease.failed) return false;
    let open: boolean;
    try {
      const result = await lease.race(
        lease.client.query<{ open: boolean }>(
          "SELECT now() <> statement_timestamp() AS open"
        )
      );
      open = result.rows[0]?.open === true;
    } catch {
      open = true;
    }
    if (!open) return false;
    try {
      await lease.race(lease.client.query("ROLLBACK"));
    } catch {
      lease.destroy();
    }
    return true;
  }

  async deleteFinalizedJobs(
    params: FinalizedJobDeleteParams,
    options: { readonly tx?: ClientBase } = {}
  ): Promise<number> {
    return jobDeleteFinalized(
      this.#db,
      params,
      options.tx === undefined
        ? {}
        : { tx: requireTransaction(options, "deleteFinalizedJobs") }
    );
  }

  async loadClaimed(
    ids: readonly bigint[],
    options: { readonly tx: ClientBase }
  ): Promise<JobClaimResult> {
    return jobLoadClaimed(
      this.#db,
      ids,
      requireTransaction(options, "loadClaimed")
    );
  }

  async notify(
    topic: "control" | "insert",
    payloads: readonly string[],
    options: { readonly tx: ClientBase }
  ): Promise<void> {
    const tx = requireTransaction(options, "notify");
    await notifyMany(this.#db, notificationTopic(topic), payloads, { tx });
  }

  async transaction<Result>(
    callback: (tx: ClientBase) => PromiseLike<Result> | Result,
    options: {
      readonly signal?: AbortSignal;
      readonly tx?: ClientBase;
    } = {}
  ): Promise<Result> {
    if (options.tx !== undefined) {
      // Like River for Go, run directly in the caller's transaction without
      // a savepoint; the caller rolls it back when this fails.
      const tx = requireTransaction(options, "pilotTransaction");
      options.signal?.throwIfAborted();
      const result = await callback(tx);
      options.signal?.throwIfAborted();
      return result;
    }
    return this.#db.transaction(
      "pilotTransaction",
      this.#pool,
      options.signal,
      callback
    );
  }
}

function notificationTopic(topic: string): string {
  switch (topic) {
    case "control":
      return "river_control";
    case "insert":
      return "river_insert";
    default:
      throw new ValidationError(
        `River notifications can be sent on "control" or "insert", not ${JSON.stringify(topic)}`
      );
  }
}

function requireTransaction(
  options: { readonly tx?: unknown } | undefined,
  operation: string
): ClientBase {
  const tx = options?.tx;
  if (!isQueryable(tx)) {
    throw backendMismatchError(
      operation,
      "the transaction is not a node-postgres client"
    );
  }
  return tx as ClientBase;
}
