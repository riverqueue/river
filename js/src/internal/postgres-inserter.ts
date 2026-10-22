import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import type { JobRow } from "../job.js";
import { parseJsonObject } from "../json.js";
import type {
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
} from "../driver.js";
import { decodeAttemptError, decodeJobState } from "../driver-codecs.js";
import {
  POSTGRES_CAPABILITIES_SQL,
  postgresCapabilitiesFromRow,
  UNIQUE_INSERT_NONCE_KEY,
  uniqueInsertConflictSql,
  type PostgresCapabilities,
} from "./postgres-capabilities.js";
import { postgresTimestamp } from "./timestamp.js";
import { quoteIdentifier } from "./sql.js";
import {
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
} from "../unique-bitmask.js";

const RIVER_SCHEMA_MAX_BYTES = 46;
const RIVER_SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Shared insertion SQL and exact row decoding for Postgres ORM adapters. */
export class PostgresInserter<
  Transaction,
> implements InsertDriver<Transaction> {
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: Transaction;
  };

  readonly backend: string;
  #capabilities: PostgresCapabilities | undefined;
  readonly #options: PostgresInserterOptions<Transaction>;
  readonly #qualifiedJobSequence: string;
  readonly #schemaName: string | null;
  readonly #schemaPrefix: string;

  constructor(options: PostgresInserterOptions<Transaction>) {
    if (options.schema !== undefined) validateSchema(options.schema);
    this.backend = options.backend;
    this.#options = options;
    this.#schemaName = options.schema ?? null;
    this.#schemaPrefix =
      options.schema === undefined ? "" : `${quoteIdentifier(options.schema)}.`;
    this.#qualifiedJobSequence = `${this.#schemaPrefix}${quoteIdentifier("river_job_id_seq")}`;
  }

  /**
   * Insert one job, or return the existing job that holds its unique key.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  async jobInsert(
    params: JobInsertParams,
    options?: InsertDriverOptions<Transaction>
  ): Promise<DriverInsertResult> {
    const results = await this.jobInsertMany([params], options);
    const result = results[0];
    if (result === undefined) {
      throw new Error(
        `${this.#options.name} returned no row for an inserted River job`
      );
    }
    return result;
  }

  /**
   * Insert an ordered batch of jobs atomically.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  async jobInsertMany(
    params: readonly JobInsertParams[],
    options?: InsertDriverOptions<Transaction>
  ): Promise<readonly DriverInsertResult[]> {
    if (params.length === 0) return [];

    const queryable = options?.tx;
    // Without `xmax`, as on YugabyteDB, each row carries a nonce like
    // SQLite's, and a returned row without its own nonce already existed.
    const { uniqueInsertMode } = await this.#detect(queryable);
    const nonces =
      uniqueInsertMode === "metadata_nonce"
        ? params.map(() => randomBytes(8).toString("hex"))
        : null;
    const jobTable = this.#name("river_job");
    const stateInBitmask = this.#name("river_job_state_in_bitmask");
    const stateType = this.#name("river_job_state");
    const sql = `
      WITH raw_job_data AS (
        SELECT
          input_order, args, coalesce(created_at, now()) AS created_at,
          kind, max_attempts, metadata, priority, queue,
          coalesce(scheduled_at, now()) AS scheduled_at,
          state_text AS state,
          ARRAY(SELECT jsonb_array_elements_text(tags_json)) AS tags,
          CASE WHEN unique_key_hex IS NULL THEN NULL
            ELSE decode(unique_key_hex, 'hex') END AS unique_key,
          unique_states_text::bit(8) AS unique_states
        FROM unnest(
          $1::jsonb[], $2::text[], $3::smallint[], $4::jsonb[],
          $5::smallint[], $6::text[], $7::timestamptz[], $8::text[],
          $9::jsonb[], $10::text[], $11::text[], $13::timestamptz[]
        ) WITH ORDINALITY AS input(
          args, kind, max_attempts, metadata, priority, queue,
          scheduled_at, state_text, tags_json, unique_key_hex,
          unique_states_text, created_at, input_order
        )
      ),
      normalized_job_data AS (
        SELECT
          *,
          unique_key IS NOT NULL
            AND unique_states IS NOT NULL
            AND ${stateInBitmask}(unique_states, state::${stateType})
            AS is_unique
        FROM raw_job_data
      ),
      prepared_job_data AS (
        SELECT
          *,
          nextval($12::regclass) AS proposed_id
        FROM normalized_job_data
      ),
      inserted_jobs AS (
        INSERT INTO ${jobTable} (
          id, args, created_at, kind, max_attempts, metadata, priority,
          queue, scheduled_at, state, tags, unique_key, unique_states
        )
        SELECT
          proposed_id, args, created_at, kind, max_attempts, metadata,
          priority, queue, scheduled_at, state::${stateType}, tags,
          unique_key, unique_states
        FROM prepared_job_data
        ORDER BY input_order
        ON CONFLICT (unique_key)
          WHERE unique_key IS NOT NULL
            AND unique_states IS NOT NULL
            AND ${stateInBitmask}(unique_states, state)
        DO UPDATE SET kind = river_job.kind
        RETURNING *, ${uniqueInsertConflictSql(uniqueInsertMode)} AS conflicted
      )
      SELECT
        inserted_jobs.id::text AS id,
        inserted_jobs.args::text AS args,
        inserted_jobs.attempt,
        ${utcText("inserted_jobs.attempted_at")} AS attempted_at,
        inserted_jobs.attempted_by,
        ${utcText("inserted_jobs.created_at")} AS created_at,
        inserted_jobs.errors::text[] AS errors,
        ${utcText("inserted_jobs.finalized_at")} AS finalized_at,
        inserted_jobs.kind,
        inserted_jobs.max_attempts,
        inserted_jobs.metadata::text AS metadata,
        inserted_jobs.priority,
        inserted_jobs.queue,
        ${utcText("inserted_jobs.scheduled_at")} AS scheduled_at,
        inserted_jobs.state::text AS state,
        inserted_jobs.tags,
        encode(inserted_jobs.unique_key, 'hex') AS unique_key,
        inserted_jobs.unique_states::text AS unique_states,
        inserted_jobs.conflicted AS unique_skipped_as_duplicate
      FROM prepared_job_data
      JOIN inserted_jobs ON CASE
        WHEN prepared_job_data.is_unique THEN
          inserted_jobs.unique_key = prepared_job_data.unique_key
          AND inserted_jobs.unique_states IS NOT NULL
          AND ${stateInBitmask}(inserted_jobs.unique_states, inserted_jobs.state)
        ELSE inserted_jobs.id = prepared_job_data.proposed_id
      END
      ORDER BY prepared_job_data.input_order
    `;

    const rows = await this.#options.query<PostgresInsertRow[]>(
      queryable,
      sql,
      params.map(({ encodedArgs }) => encodedArgs),
      params.map(({ kind }) => kind),
      params.map(({ maxAttempts }) => clampSmallint(maxAttempts)),
      params.map(({ metadata }, index) =>
        JSON.stringify(
          nonces === null
            ? metadata
            : { ...metadata, [UNIQUE_INSERT_NONCE_KEY]: nonces[index] }
        )
      ),
      params.map(({ priority }) => clampSmallint(priority)),
      params.map(({ queue }) => queue),
      params.map(({ scheduledAt }) =>
        scheduledAt === undefined ? null : postgresTimestamp(scheduledAt)
      ),
      params.map(({ state }) => state),
      params.map(({ tags }) => JSON.stringify(tags)),
      params.map(({ uniqueKey }) =>
        uniqueKey === null ? null : Buffer.from(uniqueKey).toString("hex")
      ),
      params.map(({ uniqueStates }) =>
        uniqueStates === null ? null : uniqueBitmaskFromStates(uniqueStates)
      ),
      this.#qualifiedJobSequence,
      params.map(({ createdAt }) =>
        createdAt === undefined ? null : postgresTimestamp(createdAt)
      )
    );
    if (rows.length !== params.length) {
      throw new Error(
        `${this.#options.name} returned ${rows.length} rows for ${params.length} River inserts`
      );
    }
    return rows.map((row, index) => {
      const job = toJobRow(row);
      const duplicate =
        nonces === null
          ? row.unique_skipped_as_duplicate
          : job.metadata[UNIQUE_INSERT_NONCE_KEY] !== nonces[index];
      return { job, status: duplicate ? "duplicate" : "inserted" };
    });
  }

  /**
   * Notify producers of new jobs in each of `queues`. NOTIFY is
   * transactional, so in a caller-owned transaction it's delivered only on
   * commit.
   *
   * Part of River's unstable driver interface, which `Client` calls after
   * inserting jobs. Its parameter types come from
   * `riverqueue/unstable-driver` and may change in any release.
   */
  async notifyInsert(
    queues: readonly string[],
    options?: InsertDriverOptions<Transaction>
  ): Promise<void> {
    if (queues.length === 0) return;
    const queryable = options?.tx;
    // A server without LISTEN/NOTIFY, like YugabyteDB by default, gets none.
    if (!(await this.#detect(queryable)).supportsListenNotify) return;
    // Counting the notifications' rows makes Postgres send each one while
    // returning no `void` column for the adapter to decode.
    await this.#options.query(
      queryable,
      `
      WITH notifications AS (
        SELECT pg_notify(
          concat(coalesce($1::text, current_schema()), '.', 'river_insert'),
          concat('{"queue": ', to_json(queue)::text, '}')
        )
        FROM unnest($2::text[]) AS queue
      )
      SELECT count(*)::int AS notified FROM notifications
      `,
      this.#schemaName,
      queues
    );
  }

  /** Run the operation in the adapter's transaction scope. */
  async operationScope<T>(
    tx: Transaction | undefined,
    callback: (tx: Transaction) => Promise<T>
  ): Promise<T> {
    return this.#options.operationScope(tx, callback);
  }

  /**
   * The server's capabilities, detected with `queryable` the first time.
   * Concurrent first callers may each detect; the first result stored wins.
   */
  async #detect(
    queryable: Transaction | undefined
  ): Promise<PostgresCapabilities> {
    if (this.#capabilities !== undefined) return this.#capabilities;
    const [row] = await this.#options.query<
      {
        product: unknown;
        version_num: unknown;
        yb_listen_notify_enabled: unknown;
      }[]
    >(queryable, POSTGRES_CAPABILITIES_SQL);
    if (row === undefined) {
      throw new Error("Postgres returned no server capabilities");
    }
    this.#capabilities ??= postgresCapabilitiesFromRow(row);
    return this.#capabilities;
  }

  #name(value: string): string {
    return `${this.#schemaPrefix}${quoteIdentifier(value)}`;
  }
}

/** Transport and transaction callbacks for Postgres insertion adapters. */
export interface PostgresInserterOptions<Transaction> {
  backend: string;
  name: string;
  operationScope<T>(
    tx: Transaction | undefined,
    callback: (tx: Transaction) => Promise<T>
  ): Promise<T>;
  query<T>(
    tx: Transaction | undefined,
    sql: string,
    ...values: unknown[]
  ): Promise<T>;
  schema?: string;
}

/**
 * Row shape returned by the insert query. JSON, timestamps, IDs, and bytes
 * are selected as text so values are decoded exactly by River rather than
 * by a database adapter that may parse JSON numbers as doubles.
 */
interface PostgresInsertRow extends Record<string, unknown> {
  args: string;
  attempt: number;
  attempted_at: string | null;
  attempted_by: string[] | null;
  created_at: string;
  errors: string[] | null;
  finalized_at: string | null;
  id: string;
  kind: string;
  max_attempts: number;
  metadata: string;
  priority: number;
  queue: string;
  scheduled_at: string;
  state: string;
  tags: string[] | null;
  unique_key: string | null;
  unique_skipped_as_duplicate: boolean;
  unique_states: string | null;
}

/** Parse a timestamp selected through {@link utcText}. */
function parseUtcInstant(value: string): Temporal.Instant {
  return Temporal.Instant.from(value);
}

/**
 * Decode an inserted row exactly. Every value arrives as text chosen by the
 * query, so decoding cannot lose precision: an integer beyond 2^53 in args,
 * metadata, or errors stays exact instead of making a committed insert
 * throw.
 */
function toJobRow(row: PostgresInsertRow): JobRow {
  const state = decodeJobState(row.state);
  if (!/^-?(0|[1-9]\d*)$/.test(row.id)) {
    throw new TypeError(`invalid River job ID: ${JSON.stringify(row.id)}`);
  }

  return {
    args: parseJsonObject(row.args),
    attempt: row.attempt,
    attemptedAt:
      row.attempted_at === null ? null : parseUtcInstant(row.attempted_at),
    attemptedBy: row.attempted_by ?? [],
    createdAt: parseUtcInstant(row.created_at),
    errors: (row.errors ?? []).map(decodeAttemptError),
    finalizedAt:
      row.finalized_at === null ? null : parseUtcInstant(row.finalized_at),
    id: BigInt(row.id),
    kind: row.kind,
    maxAttempts: row.max_attempts,
    metadata: parseJsonObject(row.metadata),
    priority: row.priority,
    queue: row.queue,
    scheduledAt: parseUtcInstant(row.scheduled_at),
    state,
    tags: row.tags ?? [],
    uniqueKey:
      row.unique_key === null
        ? null
        : new Uint8Array(Buffer.from(row.unique_key, "hex")),
    uniqueStates:
      row.unique_states === null
        ? null
        : uniqueBitmaskToStates(Number.parseInt(row.unique_states, 2)),
  };
}

/**
 * Select a timestamptz as UTC ISO 8601 text with microseconds, independent of
 * the session's `DateStyle` and `TimeZone`.
 */
function utcText(column: string): string {
  return `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
}

function validateSchema(value: string): void {
  if (!RIVER_SCHEMA_RE.test(value)) {
    throw new TypeError(
      "Postgres schema must start with a letter or underscore and contain only letters, numbers, and underscores"
    );
  }
  if (Buffer.byteLength(value, "utf8") > RIVER_SCHEMA_MAX_BYTES) {
    throw new TypeError(
      `Postgres schema must not exceed ${RIVER_SCHEMA_MAX_BYTES} bytes so River notification topics remain valid`
    );
  }
}

/**
 * Clamps an integer to Postgres's 16-bit `smallint` columns. Like River
 * for Go's Postgres drivers, an insert stores a wider `max_attempts` or
 * `priority` as 32767 instead of failing; SQLite stores the full value.
 */
function clampSmallint(value: number): number {
  return Math.min(value, 32_767);
}
