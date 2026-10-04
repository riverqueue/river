/**
 * Schema-qualified naming and query execution for one caller-owned
 * node-postgres pool or client. The SQL modules under `sql/` run every
 * statement through a {@link PgDatabase}.
 */
import {
  POSTGRES_CAPABILITIES_SQL,
  postgresCapabilitiesFromRow,
  quoteIdentifier,
  type PostgresCapabilities,
} from "riverqueue/unstable-driver";
import type {
  Client as PgClient,
  ClientBase,
  Pool,
  PoolClient,
  QueryConfig,
  QueryResult,
  QueryResultRow,
} from "pg";
import { RiverError } from "riverqueue";
import type { InsertDriverOptions } from "riverqueue/unstable-driver";
import {
  backendMismatchError,
  configurationError,
  databaseError,
} from "./errors.js";
import { PG_EXACT_TYPES } from "./exact-types.js";
import { abortablePromise, PgClientLease } from "./lease.js";
import type { PgOperationOptions } from "./types.js";

/** PostgreSQL's maximum identifier length (`NAMEDATALEN - 1`). */
export const POSTGRES_IDENTIFIER_MAX_BYTES = 63;

/** How long an abort waits for a pooled connection to cancel its statement. */
const CANCEL_CONNECT_TIMEOUT_MS = 2_000;

/** Anything River can send a query through: a pool, client, or pool client. */
export type PgQueryable = Pick<PgClient, "query">;

/** A caller-owned pool or client together with River's schema naming. */
export class PgDatabase {
  /** The caller-owned pool, or null for a single client. */
  readonly pool: Pool | null;
  /** The sequence backing `river_job.id`, schema-qualified when configured. */
  readonly qualifiedJobSequence: string;
  /** The configured schema, or null to use the connection's `search_path`. */
  readonly schemaName: string | null;
  /** A quoted `schema.` prefix, or the empty string without a schema. */
  readonly schemaPrefix: string;

  /**
   * The server's capabilities once detected. Like River for Go's drivers,
   * River caches them for this database's lifetime.
   */
  #capabilities: PostgresCapabilities | undefined;
  readonly #client: PgQueryable;

  /** Wrap a validated pool or client; `schema` is already validated. */
  constructor(client: Pool | PoolClient | PgClient, schema: string | null) {
    this.#client = client;
    this.pool = isPool(client) ? client : null;
    this.schemaName = schema;
    this.schemaPrefix = schema === null ? "" : `${quoteIdentifier(schema)}.`;
    this.qualifiedJobSequence = `${this.schemaPrefix}${quoteIdentifier("river_job_id_seq")}`;
  }

  /**
   * The server's capabilities, such as whether it delivers notifications
   * and how a unique insert detects a conflict, detected on `options`'
   * connection the first time. Concurrent first callers may each detect;
   * the first result stored wins, and nothing is cached after a failure.
   */
  async capabilities(
    options?: PgOperationOptions | InsertDriverOptions<ClientBase>
  ): Promise<PostgresCapabilities> {
    if (this.#capabilities !== undefined) return this.#capabilities;
    const result = await this.query<{
      date_style: unknown;
      product: unknown;
      version_num: unknown;
      yb_listen_notify_enabled: unknown;
    }>("detectCapabilities", POSTGRES_CAPABILITIES_SQL, [], options);
    const row = result.rows[0];
    if (row === undefined) {
      throw databaseError(
        "detectCapabilities",
        "PostgreSQL returned no server capabilities"
      );
    }
    // node-postgres reads timestamps as text, which River parses in the ISO
    // format only; River for Go reads them in binary, whatever the style.
    if (typeof row.date_style !== "string" || !/^ISO\b/.test(row.date_style)) {
      throw configurationError(
        "detectCapabilities",
        `River needs PostgreSQL's DateStyle to be ISO, not ${JSON.stringify(row.date_style)}; ` +
          "set it for River's connections, for example with the Pool option " +
          "options: \"-c DateStyle=ISO\", or with ALTER ROLE ... SET DateStyle = 'ISO'"
      );
    }
    this.#capabilities ??= postgresCapabilitiesFromRow(row);
    return this.#capabilities;
  }

  /**
   * Best-effort server-side cancellation of the statement an abandoned leased
   * connection is running. The request runs on another pooled connection
   * and only signals a backend that is still active with a statement starting
   * with `statementPrefix`. Failures are ignored: the leased socket is
   * destroyed regardless, and River's attempt identity guard keeps a
   * statement that commits anyway from overwriting newer work.
   */
  cancelBackend(client: PoolClient, statementPrefix: string): void {
    const pool = this.pool;
    const processID = (client as { readonly processID?: unknown }).processID;
    if (
      pool === null ||
      typeof processID !== "number" ||
      !Number.isSafeInteger(processID) ||
      processID <= 0
    ) {
      return;
    }
    void (async () => {
      const acquiring = pool.connect();
      const timeout = AbortSignal.timeout(CANCEL_CONNECT_TIMEOUT_MS);
      let canceller: PoolClient;
      try {
        canceller = await abortablePromise(acquiring, timeout);
      } catch {
        void acquiring.then((late) => late.release()).catch(() => undefined);
        return;
      }
      const lease = new PgClientLease(canceller);
      try {
        await lease.race(
          lease.client.query({
            text: `
              SELECT pg_cancel_backend(pid)
              FROM pg_catalog.pg_stat_activity
              WHERE pid = $1::int
                AND state = 'active'
                AND left(query, length($2::text)) = $2::text
            `,
            types: PG_EXACT_TYPES,
            values: [processID, statementPrefix],
          })
        );
        lease.release();
      } catch {
        lease.destroy();
      }
    })();
  }

  /** Schema-qualify and quote a River SQL function name. */
  function(name: string): string {
    return `${this.schemaPrefix}${quoteIdentifier(name)}`;
  }

  /**
   * Run one parameterized statement with River's exact type parsers, on
   * `options.tx` when given. Driver failures become {@link databaseError}s
   * naming `operation`; River's own errors pass through unchanged.
   */
  async query<Row extends QueryResultRow = QueryResultRow>(
    operation: string,
    text: string,
    values: unknown[],
    options?: PgOperationOptions | InsertDriverOptions<ClientBase>
  ): Promise<QueryResult<Row>> {
    const queryable = this.resolveQueryable(operation, options);
    const config: QueryConfig<unknown[]> = {
      text,
      types: PG_EXACT_TYPES,
      values,
    };

    try {
      return await queryable.query<Row>(config);
    } catch (cause) {
      if (cause instanceof RiverError) throw cause;
      throw databaseError(
        operation,
        `PostgreSQL operation ${operation} failed`,
        cause
      );
    }
  }

  /**
   * Like {@link PgDatabase.query}, but stop waiting for a pool connection when
   * `signal` aborts. Once a connection is leased the statement always runs to
   * completion, so work such as a claim is never abandoned half done.
   */
  async queryAfterAcquire<Row extends QueryResultRow = QueryResultRow>(
    signal: AbortSignal | undefined,
    operation: string,
    text: string,
    values: unknown[]
  ): Promise<QueryResult<Row>> {
    return this.withConnection(signal, (options) =>
      this.query<Row>(operation, text, values, options)
    );
  }

  /**
   * Run `run` on a pool connection leased for it, stopping only the wait for
   * that connection when `signal` aborts. Once leased, `run`'s statements
   * always finish, so writes such as a claim or heartbeat are never abandoned
   * half done. Without a pool or signal, `run` uses the configured client.
   */
  async withConnection<T>(
    signal: AbortSignal | undefined,
    run: (options: { readonly tx?: ClientBase }) => Promise<T>
  ): Promise<T> {
    if (signal === undefined || this.pool === null) return run({});
    const acquiring = this.pool.connect();
    let lease: PgClientLease;
    try {
      lease = new PgClientLease(await abortablePromise(acquiring, signal));
    } catch (error: unknown) {
      void acquiring.then((lateClient) => lateClient.release()).catch(() => {});
      throw error;
    }
    try {
      return await lease.race(run({ tx: lease.client }));
    } finally {
      lease.release();
    }
  }

  /**
   * Like {@link PgDatabase.query}, but stop waiting when `options.signal`
   * aborts. Without a caller transaction the statement runs on its own
   * leased pool connection, which an abort destroys, first asking PostgreSQL
   * to cancel a statement that starts with `cancelPrefix`.
   */
  async queryAbortable<Row extends QueryResultRow = QueryResultRow>(
    operation: string,
    text: string,
    values: unknown[],
    options: { readonly signal?: AbortSignal; readonly tx?: ClientBase } = {},
    cancelPrefix?: string
  ): Promise<QueryResult<Row>> {
    const signal = options.signal;
    if (signal === undefined) {
      return this.query(
        operation,
        text,
        values,
        options.tx === undefined ? undefined : { tx: options.tx }
      );
    }
    if (options.tx !== undefined || this.pool === null) {
      // River cannot destroy a caller-owned connection. Cancellation still
      // bounds the caller's wait; pool-backed runtime queries below also end
      // their PostgreSQL session so blocked work cannot continue in the pool.
      return abortablePromise(
        this.query(
          operation,
          text,
          values,
          options.tx === undefined ? undefined : { tx: options.tx }
        ),
        signal
      );
    }

    const acquiring = this.pool.connect();
    let lease: PgClientLease;
    try {
      lease = new PgClientLease(await abortablePromise(acquiring, signal));
    } catch (error: unknown) {
      void acquiring.then((lateClient) => lateClient.release()).catch(() => {});
      throw error;
    }
    if (signal.aborted) {
      lease.release();
      throw signal.reason;
    }

    const client = lease.client;
    let queryActive = true;
    const abort = (): void => {
      if (!queryActive) return;
      // Ask PostgreSQL to stop the statement as well: a destroyed socket is
      // only noticed once the statement finishes, so a lock wait would keep
      // running and could still commit.
      if (cancelPrefix !== undefined) this.cancelBackend(client, cancelPrefix);
      lease.destroy();
    };
    signal.addEventListener("abort", abort, { once: true });
    try {
      return await lease.race(
        abortablePromise(
          this.query(operation, text, values, { tx: client }),
          signal
        )
      );
    } finally {
      queryActive = false;
      signal.removeEventListener("abort", abort);
      lease.release();
    }
  }

  /**
   * Run `callback` in a transaction on a pool connection leased for it:
   * `BEGIN`, then `COMMIT` once `callback` resolves, or `ROLLBACK` when it
   * rejects. `signal` stops the wait for a connection, and once it has
   * aborted when `callback` resolves the transaction rolls back instead of
   * committing. `callback` runs at most once. A failed `COMMIT` rejects
   * without claiming the transaction rolled back.
   */
  async transaction<T>(
    operation: string,
    pool: Pool,
    signal: AbortSignal | undefined,
    callback: (client: PoolClient) => PromiseLike<T> | T
  ): Promise<T> {
    const acquiring = pool.connect();
    let lease: PgClientLease;
    try {
      lease = new PgClientLease(await abortablePromise(acquiring, signal));
    } catch (error: unknown) {
      void acquiring.then((late) => late.release()).catch(() => undefined);
      throw error;
    }
    const client = lease.client;
    let transactionStarted = false;
    try {
      signal?.throwIfAborted();
      await lease.race(
        this.query(`${operation}Begin`, "BEGIN", [], { tx: client })
      );
      transactionStarted = true;
      const result = await lease.race(callback(client));
      signal?.throwIfAborted();
      await lease.race(
        this.query(`${operation}Commit`, "COMMIT", [], { tx: client })
      );
      transactionStarted = false;
      return result;
    } catch (cause: unknown) {
      if (transactionStarted && !lease.failed) {
        try {
          await lease.race(client.query("ROLLBACK"));
        } catch {
          lease.destroy();
        }
      }
      throw cause;
    } finally {
      lease.release();
    }
  }

  /** The caller's transaction client when given, else the configured pool or client. */
  resolveQueryable(
    operation: string,
    options?: PgOperationOptions | InsertDriverOptions<ClientBase>
  ): PgQueryable {
    if (options?.tx === undefined) return this.#client;
    if (!isQueryable(options.tx)) {
      throw backendMismatchError(
        operation,
        "the transaction is not a node-postgres client"
      );
    }
    return options.tx;
  }

  /** Schema-qualify and quote a River table name. */
  table(name: string): string {
    return `${this.schemaPrefix}${quoteIdentifier(name)}`;
  }

  /** Schema-qualify and quote a River type name. */
  type(name: string): string {
    return `${this.schemaPrefix}${quoteIdentifier(name)}`;
  }
}

/** Whether a node-postgres queryable is a `Pool` rather than a client. */
function isPool(value: Pool | PoolClient | PgClient): value is Pool {
  return "totalCount" in value && "idleCount" in value;
}

/** Whether a value can run node-postgres queries. */
export function isQueryable(value: unknown): value is PgQueryable {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    "query" in value &&
    typeof value.query === "function"
  );
}
