import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import type { DurationInput, JobRow } from "riverqueue";
import { ConfigurationError, parseJsonObject } from "riverqueue";
import type {
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
  PostgresCapabilities,
} from "riverqueue/unstable-driver";
import {
  decodeAttemptError,
  decodeJobState,
  durationToMilliseconds,
  POSTGRES_CAPABILITIES_SQL,
  postgresCapabilitiesFromRow,
  postgresTimestamp,
  quoteIdentifier,
  registerDriver,
  UNIQUE_INSERT_NONCE_KEY,
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
  uniqueInsertConflictSql,
} from "riverqueue/unstable-driver";

const RIVER_SCHEMA_MAX_BYTES = 46;
const RIVER_SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** The raw-query surface shared by Prisma clients and transaction clients. */
export interface PrismaClientLike {
  $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
  /**
   * Prisma's interactive transaction, present on a root client (not on the
   * transaction client it passes to the callback). River uses it to run an
   * insertion without `{ tx }` in a transaction of its own, like River for
   * Go.
   */
  $transaction?<R>(
    callback: (tx: PrismaClientLike) => Promise<R>,
    options?: PrismaTransactionOptions
  ): Promise<R>;
}

/** Options River passes to Prisma's interactive `$transaction`. */
export interface PrismaTransactionOptions {
  /** Longest wait, in milliseconds, for a connection from Prisma's pool. */
  maxWait?: number;
  /** Longest time, in milliseconds, the transaction may stay open. */
  timeout?: number;
}

/** Postgres configuration owned by the adapter. */
export interface PrismaDriverOptions {
  /** Postgres schema containing River's tables and functions. */
  schema?: string;
  /**
   * Limits for the interactive transaction River opens for an insertion
   * without `{ tx }`, such as `{ timeout: { seconds: 10 } }`. Prisma's
   * defaults apply otherwise: a 2 second wait for a connection and a 5
   * second transaction timeout, which also bounds any I/O insert middleware
   * awaits before calling `next()`.
   */
  transactionOptions?: {
    maxWait?: DurationInput;
    timeout?: DurationInput;
  };
}

/**
 * Row shape returned by the insert query. JSON, timestamps, IDs, and bytes
 * are selected as text so values are decoded exactly by River rather than
 * by Prisma, which parses JSON numbers as doubles.
 */
interface PrismaJobRow extends Record<string, unknown> {
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

/**
 * River's insertion adapter for Prisma on Postgres.
 *
 * The Prisma client is caller-owned. A caller-owned transaction client may be
 * supplied as `{ tx }` and is used for the exact operation. Schema selection
 * belongs to the adapter constructor so it cannot accidentally vary within a
 * transaction.
 */
export class PrismaDriver {
  /**
   * Type-only marker: an insert-only driver whose transactions are Prisma
   * interactive-transaction clients. `new Client(new PrismaDriver(prisma))`
   * is therefore typed as an `InsertClient`.
   */
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: PrismaClientLike;
  };

  constructor(prisma: PrismaClientLike, options: PrismaDriverOptions = {}) {
    const inserter = new PrismaInserter(prisma, options);
    registerDriver<PrismaClientLike>(this, {
      backend: inserter.backend,
      capability: "insert",
      operations: inserter,
    });
  }
}

/**
 * @internal A registered driver whose operations are callable, for this
 * package's tests.
 */
export function testPrismaDriver(
  prisma: PrismaClientLike,
  options: PrismaDriverOptions = {}
): PrismaInserter {
  const inserter = new PrismaInserter(prisma, options);
  registerDriver<PrismaClientLike>(inserter, {
    backend: inserter.backend,
    capability: "insert",
    operations: inserter,
  });
  return inserter;
}

/**
 * @internal The operations behind a {@link PrismaDriver}, which River
 * reaches through its private driver registry.
 */
export class PrismaInserter implements InsertDriver<PrismaClientLike> {
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: PrismaClientLike;
  };

  /** Identifies this driver's backend in River's errors and diagnostics. */
  readonly backend = "postgres-prisma-insert" as const;

  /**
   * The server's capabilities once detected, cached for this driver's
   * lifetime like River for Go's drivers.
   */
  #capabilities: PostgresCapabilities | undefined;
  readonly #prisma: PrismaClientLike;
  readonly #qualifiedJobSequence: string;
  readonly #schemaName: string | null;
  readonly #schemaPrefix: string;
  readonly #transactionOptions: PrismaTransactionOptions | undefined;

  constructor(prisma: PrismaClientLike, options: PrismaDriverOptions = {}) {
    if (!isPrismaClientLike(prisma)) {
      throw new ConfigurationError(
        "PrismaDriver requires a Prisma client with $queryRawUnsafe"
      );
    }
    if (options.schema !== undefined) validateSchema(options.schema);

    this.#prisma = prisma;
    this.#transactionOptions = transactionOptions(options.transactionOptions);
    this.#schemaName = options.schema ?? null;
    this.#schemaPrefix =
      options.schema === undefined ? "" : `${quoteIdentifier(options.schema)}.`;
    this.#qualifiedJobSequence =
      options.schema === undefined
        ? quoteIdentifier("river_job_id_seq")
        : `${quoteIdentifier(options.schema)}.${quoteIdentifier("river_job_id_seq")}`;
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
    options?: InsertDriverOptions<PrismaClientLike>
  ): Promise<DriverInsertResult> {
    const results = await this.jobInsertMany([params], options);
    const result = results[0];
    if (result === undefined) {
      throw new Error("Prisma returned no row for an inserted River job");
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
    options?: InsertDriverOptions<PrismaClientLike>
  ): Promise<readonly DriverInsertResult[]> {
    if (params.length === 0) return [];

    const queryable = options?.tx ?? this.#prisma;
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

    const rows = await queryable.$queryRawUnsafe<PrismaJobRow[]>(
      sql,
      params.map(({ encodedArgs }) => encodedArgs),
      params.map(({ kind }) => kind),
      params.map(({ maxAttempts }) => maxAttempts),
      params.map(({ metadata }, index) =>
        JSON.stringify(
          nonces === null
            ? metadata
            : { ...metadata, [UNIQUE_INSERT_NONCE_KEY]: nonces[index] }
        )
      ),
      params.map(({ priority }) => priority),
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
        `Prisma returned ${rows.length} rows for ${params.length} River inserts`
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
    options?: InsertDriverOptions<PrismaClientLike>
  ): Promise<void> {
    if (queues.length === 0) return;
    const queryable = options?.tx ?? this.#prisma;
    // A server without LISTEN/NOTIFY, like YugabyteDB by default, gets none.
    if (!(await this.#detect(queryable)).supportsListenNotify) return;
    // Counting the notifications' rows makes Postgres send each one while
    // returning no `void` column for Prisma to decode.
    await queryable.$queryRawUnsafe(
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

  /**
   * Run one River operation in a transaction, like River for Go's
   * `dbutil.WithTxV`. With `tx`, the operation joins that caller-owned
   * transaction client. Otherwise it runs in an interactive transaction on
   * the root Prisma client, which commits when `callback` resolves and rolls
   * back when it rejects.
   *
   * @internal
   */
  async operationScope<T>(
    tx: PrismaClientLike | undefined,
    callback: (tx: PrismaClientLike) => Promise<T>
  ): Promise<T> {
    if (tx !== undefined) return callback(tx);
    const prisma = this.#prisma;
    if (typeof prisma.$transaction !== "function") {
      throw new ConfigurationError(
        "the Prisma client given to PrismaDriver has no $transaction, so " +
          "River can't open a transaction of its own for this operation; " +
          "pass { tx } or construct PrismaDriver with a root PrismaClient"
      );
    }
    const run = (transaction: PrismaClientLike): Promise<T> =>
      callback(transaction);
    return this.#transactionOptions === undefined
      ? prisma.$transaction(run)
      : prisma.$transaction(run, this.#transactionOptions);
  }

  /**
   * The server's capabilities, detected with `queryable` the first time.
   * Concurrent first callers may each detect; the first result stored wins.
   */
  async #detect(queryable: PrismaClientLike): Promise<PostgresCapabilities> {
    if (this.#capabilities !== undefined) return this.#capabilities;
    const [row] = await queryable.$queryRawUnsafe<
      {
        product: unknown;
        version_num: unknown;
        yb_listen_notify_enabled: unknown;
      }[]
    >(POSTGRES_CAPABILITIES_SQL);
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

function transactionOptions(
  options: PrismaDriverOptions["transactionOptions"]
): PrismaTransactionOptions | undefined {
  if (options === undefined) return undefined;
  const result: PrismaTransactionOptions = {};
  if (options.maxWait !== undefined) {
    result.maxWait = durationToMilliseconds(
      "transactionOptions.maxWait",
      options.maxWait
    );
  }
  if (options.timeout !== undefined) {
    result.timeout = durationToMilliseconds(
      "transactionOptions.timeout",
      options.timeout
    );
  }
  return result;
}

function isPrismaClientLike(value: unknown): value is PrismaClientLike {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    "$queryRawUnsafe" in value &&
    typeof value.$queryRawUnsafe === "function"
  );
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
function toJobRow(row: PrismaJobRow): JobRow {
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
