/** Job insertion, claiming, completion, and administration queries. */
import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import type { ClientBase, QueryResult } from "pg";
import type { JobRow, JobState, JsonObject } from "riverqueue";
import { ValidationError } from "riverqueue";
import type {
  DriverAttemptError,
  DriverInsertResult,
  InsertDriverOptions,
  JobClaimParams,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobDeleteManyParams,
  JobDeleteResult,
  JobInsertParams,
  JobListParams,
  JobUpdateParams,
} from "riverqueue/unstable-driver";
import {
  jobCompletionKey,
  jobListKeyset,
  jobListKeysetSql,
  UNIQUE_INSERT_NONCE_KEY,
  uniqueInsertConflictSql,
  uniqueBitmaskFromStates,
} from "riverqueue/unstable-driver";
import type { PgDatabase } from "../database.js";
import { databaseError } from "../errors.js";
import type {
  PgJobCancelParams,
  PgJobRetryParams,
  PgOperationOptions,
} from "../types.js";
import { instantParameter, validateLimit } from "./params.js";
import type { PgCompletionRow, PgInsertRow, PgJobRow } from "./rows.js";
import { mapOneJob, mapOneJobPartial, toJobRowPartial } from "./rows.js";

/**
 * Leading comment identifying completion batches River may cancel server
 * side. The cancellation only signals a backend still running a statement
 * with this prefix, so a reused process ID can never cancel unrelated work.
 */
const COMPLETE_MANY_STATEMENT = "/* river:jobCompleteMany */";

/** Cancel a job using River's canonical persisted cancellation semantics. */
export async function jobCancel(
  db: PgDatabase,
  id: bigint,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  return jobCancelWithOptions(
    db,
    {
      cancelAttemptedAt: Temporal.Now.instant(),
      controlTopic: "river_control",
      id,
    },
    options
  );
}

/** Backend test hook for deterministic cancellation clocks and topics. */
export async function jobCancelWithOptions(
  db: PgDatabase,
  params: PgJobCancelParams,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  const jobTable = db.table("river_job");
  const { supportsListenNotify } = await db.capabilities(options);
  const sql = `
    WITH locked_job AS (
      SELECT id, queue, state, finalized_at
      FROM ${jobTable}
      WHERE id = $1::bigint
      FOR UPDATE
    ),
    notification AS (
      SELECT
        id,
        CASE WHEN $6::boolean THEN pg_notify(
          concat(coalesce($4::text, current_schema()), '.', $2::text),
          json_build_object(
            'action', 'cancel',
            'job_id', id,
            'queue', queue
          )::text
        ) END
      FROM locked_job
      WHERE state NOT IN ('cancelled', 'completed', 'discarded')
        AND finalized_at IS NULL
    ),
    updated_job AS (
      UPDATE ${jobTable} AS river_job
      SET
        state = CASE
          WHEN river_job.state = 'running' THEN river_job.state
          ELSE 'cancelled'
        END,
        finalized_at = CASE
          WHEN river_job.state = 'running' THEN river_job.finalized_at
          ELSE coalesce($5::timestamptz, now())
        END,
        metadata = jsonb_set(
          river_job.metadata,
          '{cancel_attempted_at}'::text[],
          $3::jsonb,
          true
        )
      FROM notification
      WHERE river_job.id = notification.id
      RETURNING river_job.*
    )
    -- A cancel that lost a race to another committed after this statement
    -- began updates nothing. Like River for Go, the fallback read locks the
    -- row so it returns the committed row, not the one this statement first
    -- saw.
    SELECT * FROM (
      SELECT *
      FROM ${jobTable}
      WHERE id = $1::bigint
      FOR UPDATE
    ) AS fallback_job
    WHERE fallback_job.id NOT IN (SELECT id FROM updated_job)
    UNION
    SELECT * FROM updated_job
  `;

  const result = await db.query<PgJobRow>(
    "jobCancel",
    sql,
    [
      params.id.toString(10),
      params.controlTopic,
      JSON.stringify(params.cancelAttemptedAt.toString()),
      db.schemaName,
      instantParameter(params.now),
      supportsListenNotify,
    ],
    options
  );
  return mapOneJobPartial(result);
}

/** Delete a job unless it is currently running. */
export async function jobDelete(
  db: PgDatabase,
  id: bigint,
  options?: PgOperationOptions
): Promise<JobDeleteResult> {
  const jobTable = db.table("river_job");
  const sql = `
    WITH job_to_delete AS (
      SELECT id
      FROM ${jobTable}
      WHERE id = $1::bigint
      FOR UPDATE
    ),
    deleted_job AS (
      DELETE FROM ${jobTable} AS river_job
      USING job_to_delete
      WHERE river_job.id = job_to_delete.id
        AND river_job.state != 'running'
      RETURNING river_job.*
    )
    SELECT *, false AS was_deleted
    FROM ${jobTable}
    WHERE id = $1::bigint
      AND id NOT IN (SELECT id FROM deleted_job)
    UNION
    SELECT *, true AS was_deleted
    FROM deleted_job
  `;
  const result = await db.query<PgJobRow & { was_deleted: boolean }>(
    "jobDelete",
    sql,
    [id.toString(10)],
    options
  );
  const row = result.rows[0];
  if (row === undefined) return { status: "not_found" };

  const job = toJobRowPartial(row).job;
  return row.was_deleted
    ? { job, status: "deleted" }
    : { job, status: "running" };
}

/** Delete a bounded, explicitly authorized set of non-running jobs. */
export async function jobDeleteMany(
  db: PgDatabase,
  params: JobDeleteManyParams,
  options?: PgOperationOptions
): Promise<readonly JobRow[]> {
  if (
    !Number.isInteger(params.limit) ||
    params.limit < 1 ||
    params.limit > 10_000
  ) {
    throw new RangeError("bulk delete maximum must be from 1 to 10000");
  }
  const hasFilter =
    params.ids.length > 0 ||
    params.kinds.length > 0 ||
    params.priorities.length > 0 ||
    params.queues.length > 0 ||
    params.states.length > 0;
  if (!params.all && !hasFilter) {
    throw new RangeError("bulk delete requires a filter or all=true");
  }
  if (params.all && hasFilter) {
    throw new RangeError(
      "bulk delete all=true cannot be combined with filters"
    );
  }
  const jobTable = db.table("river_job");
  const result = await db.query<PgJobRow>(
    "jobDeleteMany",
    `
      WITH jobs_to_delete AS (
        SELECT id FROM ${jobTable}
        WHERE state != 'running'
          AND (cardinality($1::bigint[]) = 0 OR id = ANY($1::bigint[]))
          AND (cardinality($2::text[]) = 0 OR kind = ANY($2::text[]))
          AND (cardinality($3::smallint[]) = 0 OR priority = ANY($3::smallint[]))
          AND (cardinality($4::text[]) = 0 OR queue = ANY($4::text[]))
          AND (cardinality($5::text[]) = 0 OR state::text = ANY($5::text[]))
        ORDER BY id ASC
        LIMIT $6::int
        FOR UPDATE SKIP LOCKED
      ),
      deleted AS (
        DELETE FROM ${jobTable} AS river_job
        USING jobs_to_delete
        WHERE river_job.id = jobs_to_delete.id
        RETURNING river_job.*
      )
      SELECT * FROM deleted ORDER BY id ASC
    `,
    [
      params.ids.map((id) => id.toString(10)),
      params.kinds,
      params.priorities,
      params.queues,
      params.states,
      params.limit,
    ],
    options
  );
  return result.rows.map((row) => toJobRowPartial(row).job);
}

/** Get a job by exact 64-bit ID. */
export async function jobGet(
  db: PgDatabase,
  id: bigint,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  const result = await db.query<PgJobRow>(
    "jobGet",
    `SELECT * FROM ${db.table("river_job")} WHERE id = $1::bigint LIMIT 1`,
    [id.toString(10)],
    options
  );
  return mapOneJob(result);
}

/**
 * The IDs among `ids` of running jobs with a cancellation request, like
 * River for Go's `JobGetCancelRequested`.
 */
export async function jobGetCancelRequested(
  db: PgDatabase,
  ids: readonly bigint[],
  options: { readonly signal?: AbortSignal } = {}
): Promise<readonly bigint[]> {
  if (ids.length === 0) return [];
  const result = await db.queryAbortable<{ id: string }>(
    "jobGetCancelRequested",
    `
      SELECT id::text AS id
      FROM ${db.table("river_job")}
      WHERE id = any($1::bigint[])
        AND metadata ? 'cancel_attempted_at'
        AND state = 'running'
      ORDER BY id
    `,
    [ids.map((id) => id.toString(10))],
    options
  );
  return result.rows.map(({ id }) => BigInt(id));
}

/** Atomically claim runnable jobs using River's priority order and SKIP LOCKED. */
export async function jobClaim(
  db: PgDatabase,
  params: JobClaimParams,
  options: { readonly signal?: AbortSignal; readonly tx?: ClientBase } = {}
): Promise<JobClaimResult> {
  if (params.queues.length === 0) return { jobs: [] };
  const names = new Set<string>();
  for (const queue of params.queues) {
    validateLimit(queue.limit, "claim queue limit");
    if (names.has(queue.name)) {
      throw new RangeError("claim queues must contain each queue once");
    }
    names.add(queue.name);
  }
  const jobTable = db.table("river_job");
  const query = (
    text: string,
    values: unknown[]
  ): Promise<QueryResult<PgJobRow>> =>
    options.tx === undefined
      ? db.queryAfterAcquire<PgJobRow>(options.signal, "jobClaim", text, values)
      : db.query<PgJobRow>("jobClaim", text, values, { tx: options.tx });
  const result = await query(
    `
      WITH queue_limits AS (
        SELECT * FROM unnest($1::text[], $2::int[]) AS queue_limit(name, max)
      ),
      locked_jobs AS (
        SELECT candidate.id
        FROM queue_limits
        CROSS JOIN LATERAL (
          SELECT river_job.id
          FROM ${jobTable} AS river_job
          WHERE river_job.state = 'available'
            AND river_job.queue = queue_limits.name
            AND river_job.scheduled_at <= now()
            AND ($3::text[] IS NULL OR river_job.kind = ANY($3::text[]))
            AND NOT EXISTS (
              SELECT 1 FROM ${db.table("river_queue")} AS river_queue
              WHERE river_queue.name = river_job.queue
                AND river_queue.paused_at IS NOT NULL
            )
          ORDER BY river_job.priority, river_job.scheduled_at, river_job.id
          LIMIT queue_limits.max
          FOR UPDATE SKIP LOCKED
        ) AS candidate
      )
      UPDATE ${jobTable} AS river_job
      SET
        state = 'running',
        attempt = river_job.attempt + 1,
        attempted_at = now(),
        attempted_by = array_append(
          CASE
            WHEN coalesce(array_length(river_job.attempted_by, 1), 0) >= 100
            THEN river_job.attempted_by[
              array_length(river_job.attempted_by, 1) - 98:
            ]
            ELSE river_job.attempted_by
          END,
          $4::text
        )
      FROM locked_jobs
      WHERE river_job.id = locked_jobs.id
      RETURNING river_job.*
    `,
    [
      params.queues.map(({ name }) => name),
      params.queues.map(({ limit }) => limit),
      params.kinds.length === 0 ? null : params.kinds,
      params.attemptedBy,
    ]
  );
  return decodeClaimedJobs(result.rows);
}

/** Persist attempt-conditional worker outcomes in one bounded query. */
export async function jobCompleteMany(
  db: PgDatabase,
  commands: readonly JobCompletionCommand[],
  options?: { readonly signal?: AbortSignal; readonly tx?: ClientBase }
): Promise<readonly JobCompletionResult[]> {
  if (commands.length === 0) return [];
  validateCompletionCommands(commands);
  if (options?.signal?.aborted === true) throw options.signal.reason;
  const jobTable = db.table("river_job");
  const result = await db.queryAbortable<PgCompletionRow>(
    "jobCompleteMany",
    `${COMPLETE_MANY_STATEMENT}
      WITH job_input AS (
        SELECT *
        FROM unnest(
          $1::bigint[],
          $2::smallint[],
          $3::text[],
          $4::text[],
          $5::text[],
          $6::timestamptz[],
          $7::jsonb[],
          $8::timestamptz[],
          $9::boolean[]
        ) WITH ORDINALITY AS input(
          id, expected_attempt, attempted_by, state_text, error_text,
          finalized_at, metadata_updates, scheduled_at, attempt_refund,
          input_order
        )
      ),
      updated AS (
        UPDATE ${jobTable} AS river_job
        SET
          attempt = CASE
            WHEN job_input.attempt_refund
              AND NOT (river_job.metadata ? 'cancel_attempted_at')
            THEN greatest(river_job.attempt - 1, 0)
            ELSE river_job.attempt
          END,
          errors = CASE
            WHEN job_input.error_text IS NOT NULL
            THEN array_append(river_job.errors, job_input.error_text::jsonb)
            ELSE river_job.errors
          END,
          finalized_at = CASE
            WHEN job_input.state_text IN ('available', 'retryable', 'scheduled')
              AND river_job.metadata ? 'cancel_attempted_at'
            THEN now()
            WHEN job_input.state_text IN ('cancelled', 'completed', 'discarded')
            THEN coalesce(job_input.finalized_at, now())
            ELSE NULL
          END,
          metadata = CASE
            WHEN job_input.metadata_updates = '{}'::jsonb
            THEN river_job.metadata
            ELSE river_job.metadata || job_input.metadata_updates
          END,
          scheduled_at = CASE
            WHEN job_input.state_text IN ('available', 'retryable', 'scheduled')
              AND river_job.metadata ? 'cancel_attempted_at'
            THEN river_job.scheduled_at
            ELSE coalesce(job_input.scheduled_at, river_job.scheduled_at)
          END,
          state = CASE
            WHEN job_input.state_text IN ('available', 'retryable', 'scheduled')
              AND river_job.metadata ? 'cancel_attempted_at'
            THEN 'cancelled'::${db.type("river_job_state")}
            ELSE job_input.state_text::${db.type("river_job_state")}
          END
        FROM job_input
        WHERE river_job.id = job_input.id
          AND river_job.state = 'running'
          AND river_job.attempt = job_input.expected_attempt
          AND river_job.attempted_by[
            array_length(river_job.attempted_by, 1)
          ] = job_input.attempted_by
        RETURNING river_job.*, job_input.input_order
      ),
      metadata_updated AS (
        UPDATE ${jobTable} AS river_job
        SET metadata = river_job.metadata || job_input.metadata_updates
        FROM job_input
        WHERE river_job.id = job_input.id
          -- Like Go, an attempt's output and metadata still merge once the
          -- job has left running, such as after a rescue or cancellation,
          -- but only while the row is still that attempt's.
          AND river_job.state <> 'running'
          AND river_job.attempt = job_input.expected_attempt
          AND river_job.attempted_by[
            array_length(river_job.attempted_by, 1)
          ] = job_input.attempted_by
          AND job_input.metadata_updates <> '{}'::jsonb
          AND NOT EXISTS (
            SELECT 1 FROM updated WHERE updated.id = river_job.id
          )
        RETURNING river_job.*, job_input.input_order
      )
      SELECT updated.*, true AS transition_applied
      FROM updated
      UNION ALL
      SELECT metadata_updated.*, false AS transition_applied
      FROM metadata_updated
      UNION ALL
      SELECT river_job.*, job_input.input_order, false AS transition_applied
      FROM job_input
      JOIN ${jobTable} AS river_job ON river_job.id = job_input.id
      WHERE NOT EXISTS (SELECT 1 FROM updated WHERE updated.id = river_job.id)
        AND NOT EXISTS (
          SELECT 1
          FROM metadata_updated
          WHERE metadata_updated.id = river_job.id
        )
      ORDER BY input_order
    `,
    [
      commands.map(({ id }) => id.toString(10)),
      commands.map(({ attempt }) => attempt),
      commands.map(({ attemptedBy }) => attemptedBy),
      commands.map(completionState),
      commands.map(({ attempt, error }) =>
        error === null
          ? null
          : JSON.stringify(encodeDriverAttemptError(attempt, error))
      ),
      commands.map(({ finalizedAt }) => instantParameter(finalizedAt)),
      commands.map(({ metadata, output, outputSet }) =>
        JSON.stringify({ ...metadata, ...(outputSet ? { output } : {}) })
      ),
      commands.map(({ scheduledAt }) => instantParameter(scheduledAt)),
      commands.map(completionRefundsAttempt),
    ],
    options,
    COMPLETE_MANY_STATEMENT
  );
  const rowsByID = new Map(result.rows.map((row) => [row.id, row]));
  return commands.map((command) => {
    const row = rowsByID.get(command.id);
    return {
      // A row that can't be fully decoded is still returned, with those
      // fields empty, so it can't fail the rest of the batch.
      job: row === undefined ? null : toJobRowPartial(row).job,
      key: jobCompletionKey(command),
      status: row?.transition_applied === true ? "applied" : "stale",
    };
  });
}

/** List jobs through a fixed parameterized filter grammar. */
export async function jobList(
  db: PgDatabase,
  params: JobListParams,
  options?: PgOperationOptions
): Promise<readonly JobRow[]> {
  validateLimit(params.limit, "job list maximum");
  const keyset = jobListKeyset(validateJobListOrder(params));
  const field = keyset.timeField;
  const sql = jobListKeysetSql(keyset, (value) =>
    typeof value === "bigint" ? "$10::bigint" : "$9::timestamptz"
  );
  const jobState = db.type("river_job_state");
  // Like River's Go list builder: equality on a single state lets
  // PostgreSQL use an index's time ordering (ANY does not fix the state),
  // and an explicit non-null finalized time matches the partial index for
  // finalized states.
  const singleState =
    params.states.length === 1 ? (params.states[0] ?? null) : null;
  const statePredicate =
    singleState === null
      ? `(cardinality($4::text[]) = 0 OR state = ANY($4::text[]::${jobState}[]))`
      : `state = $4::${jobState}` +
        (field === "finalized_at" &&
        (singleState === "cancelled" ||
          singleState === "completed" ||
          singleState === "discarded")
          ? " AND finalized_at IS NOT NULL"
          : "");
  const result = await db.query<PgJobRow>(
    "jobList",
    `
      SELECT *
      FROM ${db.table("river_job")}
      WHERE (cardinality($1::bigint[]) = 0 OR id = ANY($1::bigint[]))
        AND (cardinality($2::text[]) = 0 OR kind = ANY($2::text[]))
        AND (cardinality($3::text[]) = 0 OR queue = ANY($3::text[]))
        AND ${statePredicate}
        AND (cardinality($5::smallint[]) = 0 OR priority = ANY($5::smallint[]))
        AND (cardinality($6::varchar[]) = 0 OR tags @> $6::varchar[])
        AND (cardinality($7::varchar[]) = 0 OR tags && $7::varchar[])
        AND ($8::jsonb IS NULL OR metadata @> $8::jsonb)
        AND ($9::timestamptz IS NULL OR $10::bigint IS NULL OR true)
        AND ${sql.after ?? "true"}
      ORDER BY ${sql.orderBy}
      LIMIT $11::int
    `,
    [
      params.ids.map((id) => id.toString(10)),
      params.kinds,
      params.queues,
      singleState ?? params.states,
      params.priorities,
      params.tagsAll,
      params.tagsAny,
      params.metadata === null ? null : JSON.stringify(params.metadata),
      instantParameter(
        keyset.after?.kind === "time" ? keyset.after.time : undefined
      ),
      params.after?.id.toString(10) ?? null,
      params.limit,
    ],
    options
  );
  return result.rows.map((row) => toJobRowPartial(row).job);
}

/**
 * Merge metadata and set output on a job, like River for Go's `JobUpdate`.
 * Output is set after the metadata merge, so it wins over a metadata
 * `output` key.
 */
export async function jobUpdate(
  db: PgDatabase,
  id: bigint,
  params: JobUpdateParams,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  const jobTable = db.table("river_job");
  const result = await db.query<PgJobRow>(
    "jobUpdate",
    `
      UPDATE ${jobTable}
      SET metadata = CASE
        WHEN $4::boolean THEN jsonb_set(
          CASE WHEN $2::boolean THEN metadata || $3::jsonb ELSE metadata END,
          '{output}'::text[],
          $5::jsonb,
          true
        )
        WHEN $2::boolean THEN metadata || $3::jsonb
        ELSE metadata
      END
      WHERE id = $1::bigint
      RETURNING *
    `,
    [
      id.toString(10),
      params.metadata !== undefined,
      JSON.stringify(params.metadata ?? {}),
      params.output !== undefined,
      params.output === undefined ? null : JSON.stringify(params.output),
    ],
    options
  );
  return mapOneJob(result);
}

/** Insert one job, honoring its unique key. */
export async function jobInsert(
  db: PgDatabase,
  params: JobInsertParams,
  options?: InsertDriverOptions<ClientBase>
): Promise<DriverInsertResult> {
  const results = await jobInsertMany(db, [params], options);
  const result = results[0];
  if (result === undefined) {
    throw databaseError(
      "jobInsert",
      "PostgreSQL returned no row for an inserted job"
    );
  }
  return result;
}

/** Insert jobs in order, honoring unique keys. */
export async function jobInsertMany(
  db: PgDatabase,
  params: readonly JobInsertParams[],
  options?: InsertDriverOptions<ClientBase>
): Promise<readonly DriverInsertResult[]> {
  if (params.length === 0) return [];

  // Without `xmax`, as on YugabyteDB, each row carries a nonce like SQLite's,
  // and a returned row without its own nonce already existed.
  const { uniqueInsertMode } = await db.capabilities(options);
  const nonces =
    uniqueInsertMode === "metadata_nonce"
      ? params.map(() => randomBytes(8).toString("hex"))
      : null;
  const jobTable = db.table("river_job");
  const stateInBitmask = db.function("river_job_state_in_bitmask");
  const sql = `
    WITH raw_job_data AS (
      SELECT
        input_order, args, coalesce(created_at, now()) AS created_at, kind,
        max_attempts, metadata, priority, queue,
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
          AND ${stateInBitmask}(unique_states, state::${db.type("river_job_state")})
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
        priority, queue, scheduled_at, state::${db.type("river_job_state")},
        tags, unique_key, unique_states
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
      inserted_jobs.*,
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

  const result = await db.query<PgInsertRow>(
    "jobInsertMany",
    sql,
    [
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
      params.map(({ scheduledAt }) => instantParameter(scheduledAt)),
      params.map(({ state }) => state),
      params.map(({ tags }) => JSON.stringify(tags)),
      params.map(({ uniqueKey }) =>
        uniqueKey === null ? null : Buffer.from(uniqueKey).toString("hex")
      ),
      params.map(({ uniqueStates }) =>
        uniqueStates === null ? null : uniqueBitmaskFromStates(uniqueStates)
      ),
      db.qualifiedJobSequence,
      params.map(({ createdAt }) => instantParameter(createdAt)),
    ],
    options
  );
  if (result.rows.length !== params.length) {
    throw databaseError(
      "jobInsertMany",
      `PostgreSQL returned ${result.rows.length} rows for ${params.length} inserts`
    );
  }
  return result.rows.map((row, index) => {
    const { job } = toJobRowPartial(row);
    const duplicate =
      nonces === null
        ? row.unique_skipped_as_duplicate
        : job.metadata[UNIQUE_INSERT_NONCE_KEY] !== nonces[index];
    return { job, status: duplicate ? "duplicate" : "inserted" };
  });
}

/** Retry a non-running job immediately using River's canonical transition. */
export async function jobRetry(
  db: PgDatabase,
  id: bigint,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  return jobRetryWithOptions(db, { id }, options);
}

/** Backend test hook for deterministic retry clocks. */
export async function jobRetryWithOptions(
  db: PgDatabase,
  params: PgJobRetryParams,
  options?: PgOperationOptions
): Promise<JobRow | null> {
  const jobTable = db.table("river_job");
  const sql = `
    WITH job_to_update AS (
      SELECT id
      FROM ${jobTable}
      WHERE id = $1::bigint
      FOR UPDATE
    ),
    updated_job AS (
      UPDATE ${jobTable} AS river_job
      SET
        state = 'available',
        max_attempts = CASE
          WHEN river_job.attempt = river_job.max_attempts
            THEN river_job.max_attempts + 1
          ELSE river_job.max_attempts
        END,
        finalized_at = NULL,
        scheduled_at = coalesce($2::timestamptz, now())
      FROM job_to_update
      WHERE river_job.id = job_to_update.id
        AND river_job.state != 'running'
        AND NOT (
          river_job.state = 'available'
          AND river_job.scheduled_at < coalesce($2::timestamptz, now())
        )
      RETURNING river_job.*
    )
    -- Like a cancel's, the fallback read locks the row so that a retry that
    -- lost a race returns the committed row.
    SELECT * FROM (
      SELECT *
      FROM ${jobTable}
      WHERE id = $1::bigint
      FOR UPDATE
    ) AS fallback_job
    WHERE fallback_job.id NOT IN (SELECT id FROM updated_job)
    UNION
    SELECT updated_job.*
    FROM updated_job
  `;
  const result = await db.query<PgJobRow>(
    "jobRetry",
    sql,
    [params.id.toString(10), instantParameter(params.now)],
    options
  );
  return mapOneJobPartial(result);
}

/**
 * Decode freshly claimed rows one at a time, in order. The claim has already
 * moved every row to `running`, so a row that can't be decoded doesn't fail
 * the rest: it is returned with its error, and the runtime fails its attempt.
 */
function decodeClaimedJobs(rows: readonly PgJobRow[]): JobClaimResult {
  const jobs: JobRow[] = [];
  const decodeErrors = new Map<bigint, Error>();
  for (const row of rows) {
    const { error, job } = toJobRowPartial(row);
    jobs.push(job);
    if (error !== undefined) decodeErrors.set(job.id, error);
  }
  return decodeErrors.size === 0 ? { jobs } : { decodeErrors, jobs };
}

/**
 * Read claimed jobs by ID in `tx`, in the order of `ids`, decoding each as
 * a claim does. Rejects when an ID repeats or has no row.
 */
export async function jobLoadClaimed(
  db: PgDatabase,
  ids: readonly bigint[],
  tx: ClientBase
): Promise<JobClaimResult> {
  const unique = new Set<bigint>();
  for (const id of ids) {
    if (typeof id !== "bigint" || id <= 0n) {
      throw new ValidationError("claimed job IDs must be positive bigints");
    }
    if (unique.has(id)) {
      throw new ValidationError(`claimed job ID ${id} is repeated`);
    }
    unique.add(id);
  }
  if (ids.length === 0) return { jobs: [] };
  const result = await db.query<PgJobRow>(
    "loadClaimed",
    `SELECT * FROM ${db.table("river_job")} WHERE id = ANY($1::bigint[])`,
    [ids.map((id) => id.toString(10))],
    { tx }
  );
  const rows = new Map<string, PgJobRow>();
  for (const row of result.rows) rows.set(String(row.id), row);
  return decodeClaimedJobs(
    ids.map((id) => {
      const row = rows.get(id.toString(10));
      if (row === undefined) {
        throw databaseError(
          "loadClaimed",
          `claimed job ${id} has no row`,
          undefined
        );
      }
      return row;
    })
  );
}

/**
 * Whether a completion returns its attempt, like River's `Attempt - 1`
 * parameters: snoozes and interruptions do, errors never do, including an
 * error retried immediately through the near-future `available` path.
 */
function completionRefundsAttempt(command: JobCompletionCommand): boolean {
  return command.kind === "interrupt" || command.kind === "snooze";
}

function completionState(command: JobCompletionCommand): JobState {
  if (command.available === true) return "available";
  switch (command.kind) {
    case "cancel":
      return "cancelled";
    case "complete":
      return "completed";
    case "discard":
      return "discarded";
    case "interrupt":
      return "available";
    case "retry":
      return "retryable";
    case "snooze":
      return "scheduled";
  }
}

function encodeDriverAttemptError(
  attempt: number,
  error: DriverAttemptError
): JsonObject {
  return {
    at: error.at.toString(),
    attempt,
    error: error.error,
    trace: error.trace,
  };
}

/** Reject a list ordering the query can't serve. */
function validateJobListOrder(params: JobListParams): JobListParams {
  if (params.after !== null && params.after.sortField !== params.sortField) {
    throw new RangeError("job list cursor sort field does not match ordering");
  }
  if (
    params.sortField === "finalizedAt" &&
    (params.states.length === 0 ||
      params.states.some(
        (state) =>
          state !== "cancelled" &&
          state !== "completed" &&
          state !== "discarded"
      ))
  ) {
    throw new RangeError(
      "finalizedAt ordering requires only terminal job states"
    );
  }
  return params;
}

function validateCompletionCommands(
  items: readonly JobCompletionCommand[]
): void {
  const ids = new Set<bigint>();
  for (const item of items) {
    if (ids.has(item.id)) {
      throw new RangeError("a completion batch must contain each job ID once");
    }
    ids.add(item.id);
    if (
      !Number.isInteger(item.attempt) ||
      item.attempt < 1 ||
      item.attempt > 32_767
    ) {
      throw new RangeError(
        "completion attempt must be an integer from 1 to 32767"
      );
    }
    if (item.attemptedBy.length === 0) {
      throw new RangeError("completion attemptedBy must not be empty");
    }
    const terminal =
      item.kind === "cancel" ||
      item.kind === "complete" ||
      item.kind === "discard";
    if (terminal !== (item.finalizedAt !== null)) {
      throw new RangeError(
        terminal
          ? `${item.kind} completion requires a finalizedAt instant`
          : `${item.kind} completion requires a null finalizedAt`
      );
    }
    if (
      item.available === true &&
      item.kind !== "retry" &&
      item.kind !== "snooze"
    ) {
      throw new RangeError(
        `${item.kind} completion cannot use the available fast path`
      );
    }
    if (
      (item.kind === "interrupt" ||
        item.kind === "retry" ||
        item.kind === "snooze") &&
      item.scheduledAt === null
    ) {
      throw new RangeError(
        `${item.kind} completion requires a scheduledAt instant`
      );
    }
  }
}
