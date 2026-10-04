import type { DatabaseSync, StatementSync } from "node:sqlite";
import type {
  JobClaimParams,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobListParams,
  JobUpdateParams,
} from "riverqueue/unstable-driver";
import type { JobListCursorValue } from "riverqueue/unstable-driver";
import {
  jobCompletionKey,
  jobListCursorValue,
  jobListKeyset,
  jobListKeysetSql,
} from "riverqueue/unstable-driver";
import type { JsonObject, JsonValue, RiverError } from "riverqueue";
import { isExactJsonNumber } from "riverqueue";

import {
  JOB_COLUMNS,
  invalidJsonTextSql,
  nullableBytes,
  decodeJobRow,
  decodeJobRowPartial,
  encodeJson,
  parseSqliteTimestamp,
  sqliteTimestamp,
  sqliteTimestampOrNull,
  validateInt64,
  validateName,
  validateSmallInteger,
} from "./codecs.js";
import { invalidInputError, invalidRowError } from "./errors.js";
import type {
  SqliteCleanupJobsParams,
  SqliteJobRow,
  SqliteJobState,
  SqliteLeader,
  SqliteNotification,
  SqliteRescueJobParams,
  SqliteScheduleResult,
} from "./types.js";

const JOB_STATES: readonly SqliteJobState[] = [
  "available",
  "cancelled",
  "completed",
  "discarded",
  "pending",
  "retryable",
  "running",
  "scheduled",
];

/** `attempted_by` keeps at most this many client IDs, like Go. */
const MAX_ATTEMPTED_BY = 100;

/**
 * Append the bound attempt error (bound three times) to `errors` like Go's
 * statements: SQL `NULL` starts a new array, and any other value that isn't
 * an array (another tool may have written one) is kept, wrapped in an array,
 * as a string when it is text that isn't valid JSON.
 */
const APPEND_ERROR_SQL = `CASE
  WHEN ${invalidJsonTextSql("errors")}
  THEN jsonb(json_array(errors, json(?)))
  WHEN coalesce(json_type(errors), 'array') <> 'array'
  THEN jsonb(json_array(json(errors), json(?)))
  ELSE jsonb(json_insert(json(coalesce(errors, jsonb('[]'))), '$[#]', json(?)))
END`;

/**
 * SQL that is true when the bound client ID made the latest attempt. When
 * `attempted_by` is text that isn't valid JSON (left in place when the job was
 * claimed, as in Go), the attempt number alone fences the completion.
 */
const ATTEMPTED_BY_FENCE_SQL = `(CASE WHEN ${invalidJsonTextSql("attempted_by")}
  THEN 1 ELSE json_extract(attempted_by, '$[#-1]') = ? END)`;

/**
 * `metadata` for metadata filters: SQL `NULL`, which matches no filter, when
 * it is text that isn't valid JSON.
 */
const PREFILTER_METADATA_SQL = `(CASE WHEN ${invalidJsonTextSql("metadata")} THEN NULL ELSE metadata END)`;

/** SQL that is true when `metadata` can be read as JSON. */
const VALID_METADATA_SQL = `NOT ${invalidJsonTextSql("metadata")}`;

/**
 * Append the bound client ID to `attempted_by`, keeping the newest
 * `MAX_ATTEMPTED_BY - 1` (bound first) existing entries. Matches Go's
 * statement, except that a non-array value (Go may write JSON `null`) starts a
 * fresh array instead of producing `[null, id]`. Text that isn't valid JSON is
 * left in place, as in Go.
 */
const ATTEMPTED_BY_APPEND_SQL = `CASE WHEN ${invalidJsonTextSql("attempted_by")}
THEN attempted_by
ELSE jsonb(json_insert(
  coalesce((
    SELECT jsonb_group_array(value)
    FROM (
      SELECT value FROM (
        SELECT key, value
        FROM json_each(
          CASE WHEN ${invalidJsonTextSql("attempted_by")} THEN jsonb('[]')
            WHEN json_type(attempted_by) IS 'array' THEN attempted_by
            ELSE jsonb('[]') END
        )
        ORDER BY key DESC
        LIMIT ?
      ) ORDER BY key ASC
    )
  ), jsonb('[]')),
  '$[#]', ?
)) END`;

/** Atomically claim due work in River priority order. */
export function claimJobs(
  database: DatabaseSync,
  params: JobClaimParams
): JobClaimResult {
  validateName(params.attemptedBy, "attemptedBy");
  for (const kind of params.kinds) validateName(kind, "kinds");
  const claimedAt = Temporal.Now.instant();
  const now = sqliteTimestamp(claimedAt);
  const jobs: SqliteJobRow[] = [];
  const decodeErrors = new Map<bigint, Error>();
  const queueNames = new Set<string>();
  for (const queue of params.queues) {
    validateName(queue.name, "queues.name");
    if (queueNames.has(queue.name)) {
      throw invalidInput("queues", "must contain each queue once");
    }
    queueNames.add(queue.name);
    const limit = validateSmallInteger(queue.limit, "queues.limit", 0, 10_000);
    if (limit === 0) continue;
    const values: (number | string)[] = [
      now,
      MAX_ATTEMPTED_BY - 1,
      params.attemptedBy,
      queue.name,
      now,
    ];
    let kindFilter = "";
    if (params.kinds.length > 0) {
      kindFilter = ` AND kind IN (${placeholders(params.kinds.length)})`;
      values.push(...params.kinds);
    }
    values.push(limit);
    const rows = resultStatement(
      database,
      `
      UPDATE river_job
      SET
        attempt = attempt + 1,
        attempted_at = ?,
        attempted_by = ${ATTEMPTED_BY_APPEND_SQL},
        state = 'running'
      WHERE id IN (
        SELECT river_job.id
        FROM river_job
        WHERE queue = ?
          AND scheduled_at <= ?
          AND state = 'available'
          ${kindFilter}
          AND NOT EXISTS (
            SELECT 1 FROM river_queue
            WHERE river_queue.name = river_job.queue
              AND river_queue.paused_at IS NOT NULL
          )
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT ?
      )
      RETURNING ${JOB_COLUMNS}
      `
    ).all(...values);
    // The claim has moved every row to `running`, so a row that can't be
    // decoded is returned with its error, in its place in claim order, for
    // the runtime to fail rather than failing, and rolling back, the whole
    // claim.
    const decoded = rows.map(decodeJobRowPartial);
    jobs.push(...decoded.map(({ job }) => job).sort(compareClaimedJobs));
    for (const { error, job } of decoded) {
      if (error !== undefined) decodeErrors.set(job.id, error);
    }
  }
  return decodeErrors.size === 0 ? { jobs } : { decodeErrors, jobs };
}

/**
 * Read claimed jobs by ID, in the order of `ids`, decoding each as a claim
 * does. Rejects when an ID repeats or has no row.
 */
export function loadClaimedJobs(
  database: DatabaseSync,
  ids: readonly bigint[]
): JobClaimResult {
  const unique = new Set<bigint>();
  for (const id of ids) {
    validateInt64(id, "ids");
    if (id <= 0n) throw invalidInput("ids", "must be positive");
    if (unique.has(id)) throw invalidInput("ids", `repeat job ${id}`);
    unique.add(id);
  }
  const jobs: SqliteJobRow[] = [];
  const decodeErrors = new Map<bigint, Error>();
  if (ids.length === 0) return { jobs };
  const rows = resultStatement(
    database,
    `SELECT ${JOB_COLUMNS} FROM river_job
     WHERE id IN (SELECT value FROM json_each(?))`
  ).all(`[${ids.map((id) => id.toString(10)).join(",")}]`);
  const byId = new Map<bigint, ReturnType<typeof decodeJobRowPartial>>();
  for (const row of rows) {
    const decoded = decodeJobRowPartial(row);
    byId.set(decoded.job.id, decoded);
  }
  for (const id of ids) {
    const decoded = byId.get(id);
    if (decoded === undefined) {
      throw invalidRowError("load_claimed", `claimed job ${id} has no row`);
    }
    jobs.push(decoded.job);
    if (decoded.error !== undefined) decodeErrors.set(id, decoded.error);
  }
  return decodeErrors.size === 0 ? { jobs } : { decodeErrors, jobs };
}

/** Complete a batch while rejecting late results from older attempts. */
export function completeJobs(
  database: DatabaseSync,
  items: readonly JobCompletionCommand[]
): readonly JobCompletionResult[] {
  const results: JobCompletionResult[] = [];
  for (const item of items) {
    validateInt64(item.id, "id");
    // SQLite stores attempts as unbounded integers; accept whatever a claim
    // could have produced.
    validateSmallInteger(item.attempt, "attempt", 1, Number.MAX_SAFE_INTEGER);
    validateName(item.attemptedBy, "attemptedBy");
    validateCompletionTiming(item);
    const now = Temporal.Now.instant();
    const transition = completionTransition(item);
    const error =
      item.error === null
        ? "{}"
        : encodeJson(
            {
              at: item.error.at.toString(),
              attempt: item.attempt,
              error: item.error.error,
              trace: item.error.trace,
            },
            "error"
          );
    const metadataUpdates = {
      ...(item.metadata ?? {}),
      ...(item.outputSet ? { output: item.output } : {}),
    };
    const metadata = encodeJson(metadataUpdates, "metadata");
    const updatesMetadata = Object.keys(metadataUpdates).length > 0;
    // Like Go, a present `cancel_attempted_at` key requests cancellation even
    // when its value is JSON null (`->` yields the text 'null', not SQL NULL).
    // Metadata that isn't valid JSON is treated as having no
    // `cancel_attempted_at` and is left in place, as in Go.
    const shouldCancel = `(
      (? IN ('available', 'retryable', 'scheduled'))
      AND (CASE WHEN ${VALID_METADATA_SQL}
        THEN metadata -> '$.cancel_attempted_at' END) IS NOT NULL
    )`;
    const statement = resultStatement(
      database,
      `
      UPDATE river_job
      SET
        attempt = CASE
          WHEN NOT ${shouldCancel} AND ? THEN ?
          ELSE attempt
        END,
        errors = CASE WHEN ? THEN ${APPEND_ERROR_SQL} ELSE errors END,
        finalized_at = CASE
          WHEN ${shouldCancel} THEN ?
          WHEN ? THEN ?
          ELSE finalized_at
        END,
        metadata = CASE
          WHEN ? AND ${VALID_METADATA_SQL}
          THEN jsonb_patch(json(metadata), json(?))
          ELSE metadata
        END,
        scheduled_at = CASE
          WHEN NOT ${shouldCancel} AND ? THEN ?
          ELSE scheduled_at
        END,
        state = CASE WHEN ${shouldCancel} THEN 'cancelled' ELSE ? END
      WHERE id = ?
        AND state = 'running'
        AND attempt = ?
        AND ${ATTEMPTED_BY_FENCE_SQL}
      RETURNING ${JOB_COLUMNS}
      `
    );
    const finalizedAt = sqliteTimestampOrNull(transition.finalizedAt);
    const scheduledAt = sqliteTimestampOrNull(transition.scheduledAt);
    const raw = statement.get(
      transition.state,
      transition.nextAttempt === null ? 0 : 1,
      transition.nextAttempt ?? 0,
      item.error === null ? 0 : 1,
      error,
      error,
      error,
      transition.state,
      sqliteTimestamp(now),
      transition.finalizedAt === null ? 0 : 1,
      finalizedAt,
      updatesMetadata ? 1 : 0,
      metadata,
      transition.state,
      transition.scheduledAt === null ? 0 : 1,
      scheduledAt,
      transition.state,
      transition.state,
      item.id,
      item.attempt,
      item.attemptedBy
    );
    const staleRaw =
      raw === undefined && updatesMetadata
        ? resultStatement(
            database,
            `UPDATE river_job
             SET metadata = CASE WHEN ${VALID_METADATA_SQL}
               THEN jsonb_patch(json(metadata), json(?)) ELSE metadata END
             WHERE id = ?
               AND state != 'running'
               AND attempt = ?
               AND ${ATTEMPTED_BY_FENCE_SQL}
             RETURNING ${JOB_COLUMNS}`
          ).get(metadata, item.id, item.attempt, item.attemptedBy)
        : undefined;
    const key = jobCompletionKey(item);
    results.push(
      raw === undefined
        ? {
            job:
              staleRaw === undefined
                ? getJobPartial(database, item.id)
                : decodeJobRowPartial(staleRaw).job,
            key,
            status: "stale",
          }
        : {
            // A row that can't be fully decoded is still returned, with those
            // fields empty, so it can't roll back the rest of the batch.
            job: decodeJobRowPartial(raw).job,
            key,
            status: "applied",
          }
    );
  }
  return results;
}

/**
 * List jobs with safe filters and stable keyset ordering. A metadata filter
 * scans in pages with {@link listJobsMetadataPage} instead, so the event
 * loop runs between them.
 */
export function listJobs(
  database: DatabaseSync,
  params: JobListParams & { readonly metadata: null }
): readonly SqliteJobRow[] {
  const limit = validateSmallInteger(params.limit, "limit", 0, 10_000);
  if (limit === 0) return [];
  return listJobPage(database, params);
}

/** Rows scanned per page of a metadata-filtered job list. */
const METADATA_LIST_PAGE_SIZE = 1_000;

/**
 * Scan one bounded page of a metadata-filtered job list.
 *
 * SQLite has no JSON containment operator, so metadata filtering happens in
 * JavaScript after a conservative SQL prefilter. Returns up to `params.limit`
 * matches from at most one page of candidates, and the cursor to continue
 * from, or `null` when the scan is complete. Callers yield to the event
 * loop between pages so a sparse match never blocks it for a whole scan.
 */
export function listJobsMetadataPage(
  database: DatabaseSync,
  params: JobListParams
): {
  readonly jobs: readonly SqliteJobRow[];
  readonly next: JobListCursorValue | null;
} {
  const limit = validateSmallInteger(params.limit, "limit", 0, 10_000);
  const metadata = params.metadata;
  if (limit === 0 || metadata === null) {
    return {
      jobs: limit === 0 ? [] : listJobPage(database, params),
      next: null,
    };
  }
  // The prefilter is selected as a flag rather than filtered on, so the
  // statement reads at most one page of rows however sparse the matches are.
  const prefilter = metadataPrefilter(metadata);
  const query = jobListQuery(
    { ...params, limit: METADATA_LIST_PAGE_SIZE, metadata: null },
    {
      sql: `, CASE WHEN true${prefilter.sql} THEN 1 ELSE 0 END AS river_metadata_prefilter`,
      values: prefilter.values,
    }
  );
  const rows = resultStatement(database, query.sql).all(...query.values);
  const jobs: SqliteJobRow[] = [];
  for (const raw of rows) {
    if (raw.river_metadata_prefilter !== 1n) continue;
    const row = decodeJobRowPartial(raw).job;
    if (!jsonContains(row.metadata, metadata)) continue;
    jobs.push(row);
    if (jobs.length === limit) {
      return { jobs, next: jobListCursorValue(row, params) };
    }
  }
  const last = rows.at(-1);
  return {
    jobs,
    next:
      rows.length < METADATA_LIST_PAGE_SIZE || last === undefined
        ? null
        : jobListCursorValue(decodeJobRowPartial(last).job, params),
  };
}

function listJobPage(
  database: DatabaseSync,
  params: JobListParams
): readonly SqliteJobRow[] {
  const query = jobListQuery(params);
  // Like Go, list rows another engine wrote that River can't fully read,
  // with the fields it can't decode left empty.
  return resultStatement(database, query.sql)
    .all(...query.values)
    .map((raw) => decodeJobRowPartial(raw).job);
}

/**
 * Build a job list statement. `select` adds columns after the job's own,
 * with their parameters bound first.
 */
function jobListQuery(
  params: JobListParams,
  select: {
    readonly sql: string;
    readonly values: readonly (bigint | string)[];
  } = {
    sql: "",
    values: [],
  }
): {
  readonly sql: string;
  readonly values: readonly (bigint | number | string | Uint8Array | null)[];
} {
  const limit = params.limit;
  const direction = params.sortDirection;
  const orderBy = params.sortField;
  const states = params.states;
  if (!["asc", "desc"].includes(direction)) {
    throw invalidInput("direction", "must be asc or desc");
  }
  if (!["finalizedAt", "id", "scheduledAt", "time"].includes(orderBy)) {
    throw invalidInput("orderBy", "unknown ordering field");
  }
  if (
    orderBy === "finalizedAt" &&
    (states.length === 0 ||
      states.some(
        (state) =>
          state !== "cancelled" &&
          state !== "completed" &&
          state !== "discarded"
      ))
  ) {
    throw invalidInput(
      "orderBy",
      "finalized_at requires only terminal state filters"
    );
  }
  for (const state of states) validateState(state, "states");

  if (params.after !== null) validateInt64(params.after.id, "after.id");
  const values: (bigint | number | string | Uint8Array | null)[] = [
    ...select.values,
  ];
  const keyset = jobListKeysetSql(jobListKeyset(params), (value) => {
    values.push(typeof value === "bigint" ? value : sqliteTimestamp(value));
    return "?";
  });
  let sql = `SELECT ${JOB_COLUMNS}${select.sql} FROM river_job WHERE true`;
  if (keyset.after !== null) sql += ` AND ${keyset.after}`;
  sql = addInFilter(sql, values, "id", params.ids, (value) =>
    validateInt64(value, "ids")
  );
  sql = addInFilter(sql, values, "kind", params.kinds, (value) =>
    validateName(value, "kinds")
  );
  sql = addInFilter(sql, values, "priority", params.priorities, (value) =>
    validateSmallInteger(value, "priorities", 1, 4)
  );
  sql = addInFilter(sql, values, "queue", params.queues, (value) =>
    validateName(value, "queues")
  );
  sql = addInFilter(sql, values, "state", states, (value) => value);
  for (const tag of params.tagsAll) {
    sql += " AND EXISTS (SELECT 1 FROM json_each(json(tags)) WHERE value = ?)";
    values.push(tag);
  }
  if (params.tagsAny.length > 0) {
    sql += ` AND EXISTS (SELECT 1 FROM json_each(json(tags)) WHERE value IN (${placeholders(
      params.tagsAny.length
    )}))`;
    values.push(...params.tagsAny);
  }
  sql += ` ORDER BY ${keyset.orderBy} LIMIT ?`;
  values.push(limit);
  return { sql, values };
}

function jsonContains(actual: JsonValue, expected: JsonValue): boolean {
  if (expected === null || typeof expected !== "object") {
    return actual === expected;
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual)) return false;
    return expected.every((expectedItem) =>
      actual.some((actualItem) => jsonContains(actualItem, expectedItem))
    );
  }
  if (actual === null || typeof actual !== "object" || Array.isArray(actual)) {
    return false;
  }
  return Object.entries(expected as JsonObject).every(
    ([key, value]) =>
      Object.hasOwn(actual, key) &&
      jsonContains((actual as JsonObject)[key] as JsonValue, value)
  );
}

/**
 * Merge metadata and set output on a job with a JSON merge patch, like River
 * for Go's SQLite `JobUpdate`.
 */
export function updateJob(
  database: DatabaseSync,
  id: bigint,
  params: JobUpdateParams
): SqliteJobRow | null {
  validateInt64(id, "id");
  const metadataPatch = Object.create(null) as JsonObject;
  for (const [key, value] of Object.entries(params.metadata ?? {})) {
    metadataPatch[key] = value;
  }
  if (Object.hasOwn(params, "output")) {
    metadataPatch.output = params.output as JsonValue;
  }
  const hasMetadataPatch = Object.keys(metadataPatch).length > 0;
  const raw = resultStatement(
    database,
    `UPDATE river_job SET
       metadata = CASE
         WHEN ? THEN jsonb_patch(json(metadata), json(?)) ELSE metadata END
     WHERE id = ?
     RETURNING ${JOB_COLUMNS}`
  ).get(hasMetadataPatch ? 1 : 0, encodeJson(metadataPatch, "metadata"), id);
  return raw === undefined ? null : decodeJobRow(raw);
}

/** Fetch stuck running jobs in deterministic rescue order. */
export function stuckJobs(
  database: DatabaseSync,
  params: {
    afterId?: bigint;
    attemptedBefore: Temporal.Instant;
    limit?: number;
  }
): readonly SqliteJobRow[] {
  const afterId = validateInt64(params.afterId ?? 0n, "afterId");
  const limit = validateSmallInteger(params.limit ?? 100, "limit", 0, 10_000);
  if (limit === 0) return [];
  return (
    resultStatement(
      database,
      `SELECT ${JOB_COLUMNS} FROM river_job
     WHERE state = 'running' AND id > ? AND attempted_at < ?
     ORDER BY id ASC LIMIT ?`
    )
      .all(afterId, sqliteTimestamp(params.attemptedBefore), limit)
      // A row that can't be fully decoded is returned with those fields empty
      // so it can't keep the rescuer from recovering every stuck job.
      .map((raw) => decodeJobRowPartial(raw).job)
  );
}

/**
 * Rescue selected stuck jobs with the same semantics as Go and Rust.
 *
 * Only jobs that are still `running` with `attempted_at` strictly before the
 * horizon the rescuer selected them with are updated. A job that completed,
 * was released, or was claimed again after the rescuer read it is left
 * untouched, including its errors, metadata, and timestamps.
 */
export function rescueJobs(
  database: DatabaseSync,
  items: readonly SqliteRescueJobParams[],
  attemptedBefore: Temporal.Instant
): readonly SqliteJobRow[] {
  const horizon = sqliteTimestamp(attemptedBefore);
  const statement = resultStatement(
    database,
    `
    UPDATE river_job SET
      errors = ${APPEND_ERROR_SQL},
      finalized_at = ?,
      scheduled_at = ?,
      metadata = CASE WHEN NOT ${VALID_METADATA_SQL} THEN metadata ELSE jsonb_set(
        metadata, '$."river:rescue_count"',
        coalesce(
          CASE json_type(metadata, '$."river:rescue_count"')
            WHEN 'integer' THEN json_extract(metadata, '$."river:rescue_count"')
            WHEN 'real' THEN json_extract(metadata, '$."river:rescue_count"')
          END, 0
        ) + 1
      ) END,
      state = ?
    WHERE id = ?
      AND state = 'running'
      AND attempted_at < ?
    RETURNING ${JOB_COLUMNS}
    `
  );
  const rows: SqliteJobRow[] = [];
  for (const item of items) {
    validateInt64(item.id, "id");
    const error = encodeJson(
      {
        at: item.error.at.toString(),
        attempt: item.error.attempt,
        error: item.error.error,
        trace: item.error.trace,
      },
      "error"
    );
    const raw = statement.get(
      error,
      error,
      error,
      sqliteTimestampOrNull(item.finalizedAt),
      sqliteTimestamp(item.scheduledAt),
      item.state,
      item.id,
      horizon
    );
    if (raw !== undefined) rows.push(decodeJobRowPartial(raw).job);
  }
  return rows;
}

/** Delete a bounded set of terminal jobs past their retention horizons. */
export function cleanupJobs(
  database: DatabaseSync,
  params: SqliteCleanupJobsParams
): number {
  const limit = validateSmallInteger(params.limit ?? 1_000, "limit", 0, 10_000);
  const queuesExcluded = (params.queuesExcluded ?? []).map((queue) =>
    validateName(queue, "queuesExcluded")
  );
  const queuesIncluded =
    params.queuesIncluded === undefined || params.queuesIncluded === null
      ? null
      : params.queuesIncluded.map((queue) =>
          validateName(queue, "queuesIncluded")
        );
  // An empty inclusion list matches no queues, unlike an absent one.
  if (limit === 0 || queuesIncluded?.length === 0) return 0;
  const cancelledBefore = sqliteTimestampOrNull(params.cancelledBefore);
  const completedBefore = sqliteTimestampOrNull(params.completedBefore);
  const discardedBefore = sqliteTimestampOrNull(params.discardedBefore);
  const values: (null | number | string)[] = [
    cancelledBefore,
    cancelledBefore,
    completedBefore,
    completedBefore,
    discardedBefore,
    discardedBefore,
  ];
  let sql = `
    DELETE FROM river_job WHERE id IN (
      SELECT id FROM river_job WHERE (
        (? IS NOT NULL AND state = 'cancelled' AND finalized_at < ?)
        OR (? IS NOT NULL AND state = 'completed' AND finalized_at < ?)
        OR (? IS NOT NULL AND state = 'discarded' AND finalized_at < ?)
      )`;
  // Queue filters apply inside the limited subquery, so retained jobs never
  // use up a batch.
  if (queuesExcluded.length > 0) {
    sql += ` AND queue NOT IN (${placeholders(queuesExcluded.length)})`;
    values.push(...queuesExcluded);
  }
  if (queuesIncluded !== null) {
    sql += ` AND queue IN (${placeholders(queuesIncluded.length)})`;
    values.push(...queuesIncluded);
  }
  for (const key of params.metadataExclusions ?? []) {
    sql += " AND json_extract(metadata, ?) IS NULL";
    values.push(jsonPathForKey(key));
  }
  sql += " ORDER BY id ASC LIMIT ?)";
  values.push(limit);
  return Number(resultStatement(database, sql).run(...values).changes);
}

/** Make due retryable/scheduled jobs available, discarding unique collisions. */
export function scheduleJobs(
  database: DatabaseSync,
  params: {
    limit?: number;
    now?: Temporal.Instant;
    scheduledAtHorizon?: Temporal.Instant;
  }
): readonly SqliteScheduleResult[] {
  const limit = validateSmallInteger(params.limit ?? 1_000, "limit", 0, 10_000);
  if (limit === 0) return [];
  const now = params.now ?? Temporal.Now.instant();
  const scheduledAtHorizon = params.scheduledAtHorizon ?? now;
  const candidates = resultStatement(
    database,
    // Only the columns scheduling needs, so a job with a JSON column that
    // isn't valid JSON can't fail the scheduler, as in Go.
    `SELECT id, unique_key FROM river_job
     WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= ?
     ORDER BY priority ASC, scheduled_at ASC, id ASC LIMIT ?`
  )
    .all(sqliteTimestamp(scheduledAtHorizon), limit)
    .map((raw) => ({
      id: validateInt64(raw.id as bigint, "id"),
      uniqueKey: nullableBytes(raw.unique_key, "unique_key"),
    }));
  const results: SqliteScheduleResult[] = [];
  for (const candidate of candidates) {
    let collision = false;
    if (candidate.uniqueKey !== null) {
      collision =
        resultStatement(
          database,
          `SELECT EXISTS (
            SELECT 1 FROM river_job
            WHERE id != ? AND unique_key = ? AND unique_states IS NOT NULL
              AND CASE state
                WHEN 'available' THEN unique_states & (1 << 0)
                WHEN 'cancelled' THEN unique_states & (1 << 1)
                WHEN 'completed' THEN unique_states & (1 << 2)
                WHEN 'discarded' THEN unique_states & (1 << 3)
                WHEN 'pending' THEN unique_states & (1 << 4)
                WHEN 'retryable' THEN unique_states & (1 << 5)
                WHEN 'running' THEN unique_states & (1 << 6)
                WHEN 'scheduled' THEN unique_states & (1 << 7)
                ELSE 0 END >= 1
          ) AS present`
        ).get(candidate.id, candidate.uniqueKey)?.present === 1n;
    }
    const raw = collision
      ? resultStatement(
          database,
          `UPDATE river_job SET
             metadata = CASE WHEN ${VALID_METADATA_SQL}
               THEN jsonb_patch(
                 json(metadata),
                 json('{"unique_key_conflict":"scheduler_discarded"}')
               )
               ELSE metadata END,
             finalized_at = ?, state = 'discarded'
           WHERE id = ? AND state IN ('retryable', 'scheduled')
           RETURNING ${JOB_COLUMNS}`
        ).get(sqliteTimestamp(now), candidate.id)
      : resultStatement(
          database,
          `UPDATE river_job SET state = 'available'
           WHERE id = ? AND state IN ('retryable', 'scheduled')
           RETURNING ${JOB_COLUMNS}`
        ).get(candidate.id);
    if (raw !== undefined) {
      results.push({
        conflictDiscarded: collision,
        // A row that can't be fully decoded is still scheduled and returned
        // with those fields empty.
        job: decodeJobRowPartial(raw).job,
      });
    }
  }
  return results;
}

/** Attempt to acquire the portable singleton leader after expiring old terms. */
export function leaderAttemptElect(
  database: DatabaseSync,
  params: { leaderId: string; now?: Temporal.Instant; ttlMs: number }
): SqliteLeader | null {
  validateName(params.leaderId, "leaderId");
  const ttlMs = validateSmallInteger(params.ttlMs, "ttlMs", 1, 2_147_483_647);
  const now = params.now ?? Temporal.Now.instant();
  resultStatement(
    database,
    "DELETE FROM river_leader WHERE expires_at < ?"
  ).run(sqliteTimestamp(now));
  const raw = resultStatement(
    database,
    `INSERT INTO river_leader (leader_id, elected_at, expires_at)
     VALUES (?, ?, ?) ON CONFLICT (name) DO NOTHING
     RETURNING elected_at, expires_at, leader_id`
  ).get(
    params.leaderId,
    sqliteTimestamp(now),
    sqliteTimestamp(now.add({ milliseconds: ttlMs }))
  );
  return raw === undefined ? null : decodeLeader(raw);
}

/** Renew exactly the still-current, unexpired leadership term. */
export function leaderAttemptReelect(
  database: DatabaseSync,
  leader: SqliteLeader,
  params: { now?: Temporal.Instant; ttlMs: number }
): SqliteLeader | null {
  validateName(leader.leaderId, "leaderId");
  const ttlMs = validateSmallInteger(params.ttlMs, "ttlMs", 1, 2_147_483_647);
  const now = params.now ?? Temporal.Now.instant();
  const raw = resultStatement(
    database,
    // Compare terms as instants, like Go, so equal times written in another
    // engine's text format (for example without milliseconds) still match.
    `UPDATE river_leader SET expires_at = ?
     WHERE unixepoch(elected_at, 'subsec') = unixepoch(?, 'subsec')
       AND expires_at >= ? AND leader_id = ?
     RETURNING elected_at, expires_at, leader_id`
  ).get(
    sqliteTimestamp(now.add({ milliseconds: ttlMs })),
    sqliteTimestamp(leader.electedAt),
    sqliteTimestamp(now),
    leader.leaderId
  );
  return raw === undefined ? null : decodeLeader(raw);
}

/** Read the singleton elected leader, including an expired term for diagnostics. */
export function leaderGet(database: DatabaseSync): SqliteLeader | null {
  const raw = resultStatement(
    database,
    "SELECT elected_at, expires_at, leader_id FROM river_leader LIMIT 1"
  ).get();
  return raw === undefined ? null : decodeLeader(raw);
}

/** Resign only the exact fenced leadership term. */
export function leaderResign(
  database: DatabaseSync,
  leader: Pick<SqliteLeader, "electedAt" | "leaderId">
): boolean {
  const result = resultStatement(
    database,
    `DELETE FROM river_leader
     WHERE unixepoch(elected_at, 'subsec') = unixepoch(?, 'subsec')
       AND leader_id = ?`
  ).run(sqliteTimestamp(leader.electedAt), leader.leaderId);
  return Number(result.changes) > 0;
}

/**
 * Read durable notifications after an exact outbox ID, in ID order. Like Go's
 * `NotificationGetAfter`, topics are bound as one JSON array.
 */
export function notificationPoll(
  database: DatabaseSync,
  params: { afterId?: bigint; limit?: number; topics?: readonly string[] } = {}
): readonly SqliteNotification[] {
  const afterId = validateInt64(params.afterId ?? 0n, "afterId");
  const limit = validateSmallInteger(params.limit ?? 1_000, "limit", 0, 10_000);
  if (limit === 0) return [];
  const values: (bigint | number | string)[] = [afterId];
  let sql = `SELECT created_at, id, payload, topic
             FROM river_notification WHERE id > ?`;
  const topics = params.topics ?? [];
  if (topics.length > 0) {
    sql += " AND topic IN (SELECT value FROM json_each(?))";
    values.push(
      JSON.stringify(topics.map((topic) => validateName(topic, "topics")))
    );
  }
  sql += " ORDER BY id ASC LIMIT ?";
  values.push(limit);
  return resultStatement(database, sql)
    .all(...values)
    .map(decodeNotification);
}

/** Return the current durable outbox high-water ID. */
export function notificationLastId(database: DatabaseSync): bigint {
  const raw = resultStatement(
    database,
    "SELECT coalesce(max(id), 0) AS id FROM river_notification"
  ).get();
  return raw?.id as bigint;
}

/**
 * Delete up to `limit` notifications created before a horizon, oldest first,
 * like Go's `NotificationDeleteBefore`.
 */
export function notificationCleanup(
  database: DatabaseSync,
  params: { createdBefore: Temporal.Instant; limit?: number }
): number {
  const limit = validateSmallInteger(params.limit ?? 1_000, "limit", 0, 10_000);
  if (limit === 0) return 0;
  return Number(
    resultStatement(
      database,
      `DELETE FROM river_notification WHERE id IN (
         SELECT id FROM river_notification WHERE created_at < ?
         ORDER BY created_at, id
         LIMIT ?
       )`
    ).run(sqliteTimestamp(params.createdBefore), limit).changes
  );
}

/** Delete inactive queues in bounded deterministic order. */
export function queueDeleteExpired(
  database: DatabaseSync,
  params: { limit?: number; updatedBefore: Temporal.Instant }
): readonly string[] {
  const limit = validateSmallInteger(params.limit ?? 1_000, "limit", 0, 10_000);
  if (limit === 0) return [];
  return resultStatement(
    database,
    `DELETE FROM river_queue WHERE name IN (
       SELECT name FROM river_queue WHERE updated_at < ?
       ORDER BY name ASC LIMIT ?
     ) RETURNING name`
  )
    .all(sqliteTimestamp(params.updatedBefore), limit)
    .map((row) => row.name as string);
}

function addInFilter<T extends bigint | number | string>(
  sql: string,
  values: (bigint | number | string | Uint8Array | null)[],
  column: string,
  filter: readonly T[] | undefined,
  validate: (value: T) => bigint | number | string
): string {
  if (filter === undefined || filter.length === 0) return sql;
  sql += ` AND ${column} IN (${placeholders(filter.length)})`;
  for (const value of filter) values.push(validate(value));
  return sql;
}

function compareClaimedJobs(left: SqliteJobRow, right: SqliteJobRow): number {
  return (
    left.priority - right.priority ||
    Temporal.Instant.compare(left.scheduledAt, right.scheduledAt) ||
    (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)
  );
}

/**
 * The row changes for one completion. A snooze or interruption refunds its
 * attempt like Go's `Attempt - 1` parameters; an error never does, even when
 * the near-future fast path persists it as `available`.
 */
function completionTransition(item: JobCompletionCommand): {
  finalizedAt: Temporal.Instant | null;
  nextAttempt: number | null;
  scheduledAt: Temporal.Instant | null;
  state: SqliteJobState;
} {
  switch (item.kind) {
    case "cancel":
      return {
        finalizedAt: item.finalizedAt,
        nextAttempt: null,
        scheduledAt: null,
        state: "cancelled",
      };
    case "complete":
      return {
        finalizedAt: item.finalizedAt,
        nextAttempt: null,
        scheduledAt: null,
        state: "completed",
      };
    case "discard":
      return {
        finalizedAt: item.finalizedAt,
        nextAttempt: null,
        scheduledAt: null,
        state: "discarded",
      };
    case "interrupt":
      if (item.scheduledAt === null) {
        throw invalidInput("scheduledAt", "is required for interrupt");
      }
      return {
        finalizedAt: null,
        nextAttempt: Math.max(item.attempt - 1, 0),
        scheduledAt: item.scheduledAt,
        state: "available",
      };
    case "retry":
      if (item.scheduledAt === null) {
        throw invalidInput("scheduledAt", "is required for retry");
      }
      return {
        finalizedAt: null,
        nextAttempt: null,
        scheduledAt: item.scheduledAt,
        state: item.available === true ? "available" : "retryable",
      };
    case "snooze":
      if (item.scheduledAt === null) {
        throw invalidInput("scheduledAt", "is required for snooze");
      }
      return {
        finalizedAt: null,
        nextAttempt: Math.max(item.attempt - 1, 0),
        scheduledAt: item.scheduledAt,
        state: item.available === true ? "available" : "scheduled",
      };
  }
}

function validateCompletionTiming(item: JobCompletionCommand): void {
  const terminal =
    item.kind === "cancel" ||
    item.kind === "complete" ||
    item.kind === "discard";
  if (terminal !== (item.finalizedAt !== null)) {
    throw invalidInput(
      "finalizedAt",
      terminal
        ? `is required for ${item.kind}`
        : `must be null for ${item.kind}`
    );
  }
  if (
    item.available === true &&
    item.kind !== "retry" &&
    item.kind !== "snooze"
  ) {
    throw invalidInput(
      "available",
      `the near-future fast path does not apply to ${item.kind}`
    );
  }
}

function decodeLeader(raw: Record<string, unknown>): SqliteLeader {
  if (typeof raw.leader_id !== "string") {
    throw invalidRow("leader_id", "is not text");
  }
  return {
    electedAt: parseSqliteTimestamp(raw.elected_at, "elected_at"),
    expiresAt: parseSqliteTimestamp(raw.expires_at, "expires_at"),
    leaderId: raw.leader_id,
  };
}

function decodeNotification(raw: Record<string, unknown>): SqliteNotification {
  if (
    typeof raw.id !== "bigint" ||
    typeof raw.payload !== "string" ||
    typeof raw.topic !== "string"
  ) {
    throw invalidRow("notification", "has an invalid column type");
  }
  return {
    createdAt: parseSqliteTimestamp(raw.created_at, "created_at"),
    id: raw.id,
    payload: raw.payload,
    topic: raw.topic,
  };
}

function invalidInput(field: string, message: string): RiverError {
  return invalidInputError(
    "runtime",
    `invalid SQLite River input ${field}: ${message}`
  );
}

/** Read one job, leaving fields that can't be decoded empty. */
function getJobPartial(
  database: DatabaseSync,
  id: bigint
): SqliteJobRow | null {
  const raw = resultStatement(
    database,
    `SELECT ${JOB_COLUMNS} FROM river_job WHERE id = ? LIMIT 1`
  ).get(id);
  return raw === undefined ? null : decodeJobRowPartial(raw).job;
}

function invalidRow(field: string, message: string): RiverError {
  return invalidRowError(
    "runtime",
    `invalid SQLite River row ${field}: ${message}`
  );
}

function jsonPathForKey(key: string): string {
  if (key.length === 0) throw invalidInput("metadataExclusions", "empty key");
  return `$.${JSON.stringify(key)}`;
}

/**
 * SQL that narrows a metadata filter to rows that can possibly match it.
 *
 * The exact comparison still happens in JavaScript (River JSON numbers are
 * arbitrary precision), so this only has to be necessary, never sufficient:
 * every top-level key must be present with a compatible JSON type, and string,
 * boolean, and `null` values must match exactly. It keeps metadata-filtered
 * scans from decoding and comparing every row in the table.
 */
function metadataPrefilter(expected: JsonObject): {
  readonly sql: string;
  readonly values: readonly (bigint | string)[];
} {
  let sql = "";
  const values: (bigint | string)[] = [];
  for (const [key, value] of Object.entries(expected)) {
    // SQLite JSON paths cannot escape a double quote inside a label.
    if (key.includes('"') || key.includes("\\")) continue;
    const path = `$."${key}"`;
    if (typeof value === "string") {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) = 'text' AND json_extract(${PREFILTER_METADATA_SQL}, ?) = ?`;
      values.push(path, path, value);
    } else if (typeof value === "boolean") {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) = '${value ? "true" : "false"}'`;
      values.push(path);
    } else if (value === null) {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) = 'null'`;
      values.push(path);
    } else if (Array.isArray(value)) {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) = 'array'`;
      values.push(path);
    } else if (Number.isSafeInteger(value)) {
      // Any JSON spelling of this integer (`5`, `5.0`, `5e0`) reads as a
      // number equal to it, so comparing numerically never drops a match.
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) IN ('integer', 'real') AND json_extract(${PREFILTER_METADATA_SQL}, ?) = ?`;
      values.push(path, path, BigInt(value as number));
    } else if (typeof value === "number" || isExactJsonNumber(value)) {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) IN ('integer', 'real')`;
      values.push(path);
    } else {
      sql += ` AND json_type(${PREFILTER_METADATA_SQL}, ?) = 'object'`;
      values.push(path);
    }
  }
  return { sql, values };
}

function placeholders(length: number): string {
  return Array.from({ length }, () => "?").join(", ");
}

const STATEMENT_CACHE_LIMIT = 256;
const statementCaches = new WeakMap<DatabaseSync, Map<string, StatementSync>>();

/**
 * Return a prepared statement for `sql`, reusing one per database handle.
 *
 * Every statement reads integers as `bigint`. The cache is a small LRU keyed
 * by SQL text; statements whose text varies with input length (for example
 * `IN (?, ?)` lists) share the bound instead of growing without limit.
 */
export function resultStatement(
  database: DatabaseSync,
  sql: string
): StatementSync {
  let cache = statementCaches.get(database);
  if (cache === undefined) {
    cache = new Map();
    statementCaches.set(database, cache);
  }
  const cached = cache.get(sql);
  if (cached !== undefined) {
    if (statementIsLive(cached)) {
      // Refresh recency.
      cache.delete(sql);
      cache.set(sql, cached);
      return cached;
    }
    // The handle was closed and opened again, which finalized every
    // statement prepared on it.
    cache.clear();
  }
  const statement = database.prepare(sql);
  statement.setReadBigInts(true);
  cache.set(sql, statement);
  if (cache.size > STATEMENT_CACHE_LIMIT) {
    const oldest = cache.keys().next();
    if (oldest.done !== true) cache.delete(oldest.value);
  }
  return statement;
}

/** Whether `statement` can still run: closing its database finalizes it. */
function statementIsLive(statement: StatementSync): boolean {
  try {
    void statement.sourceSQL;
    return true;
  } catch {
    return false;
  }
}

function validateState(state: SqliteJobState, field: string): SqliteJobState {
  if (!JOB_STATES.includes(state)) {
    throw invalidInput(field, `unknown state ${JSON.stringify(state)}`);
  }
  return state;
}
