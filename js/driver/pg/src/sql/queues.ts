/** Queue listing, upsert, pause/resume, and cleanup queries. */
import type {
  QueueListParams,
  QueueRow,
  QueueUpdateParams,
} from "riverqueue/unstable-driver";
import { queueMetadataUpdate } from "riverqueue/unstable-driver";
import type { PgDatabase } from "../database.js";
import { databaseError } from "../errors.js";
import type {
  PgOperationOptions,
  PgQueueControlParams,
  PgQueueRow,
  PgQueueUpsertParams,
} from "../types.js";
import { instantParameter, validateLimit } from "./params.js";
import type { PgQueueDatabaseRow } from "./rows.js";
import { toQueueRow } from "./rows.js";

export async function queueGet(
  db: PgDatabase,
  name: string,
  options?: PgOperationOptions
): Promise<PgQueueRow | null> {
  const result = await db.query<PgQueueDatabaseRow>(
    "queueGet",
    `SELECT *, metadata::text AS metadata_text FROM ${db.table("river_queue")} WHERE name = $1::text`,
    [name],
    options
  );
  const row = result.rows[0];
  return row === undefined ? null : toQueueRow(row);
}

/** Create a queue or refresh its liveness timestamp without erasing metadata. */
export async function queueUpsert(
  db: PgDatabase,
  params: PgQueueUpsertParams,
  options?: PgOperationOptions
): Promise<PgQueueRow> {
  const result = await db.query<PgQueueDatabaseRow>(
    "queueUpsert",
    `
      INSERT INTO ${db.table("river_queue")} (
        created_at, metadata, name, paused_at, updated_at
      ) VALUES (
        coalesce($1::timestamptz, now()),
        $2::jsonb,
        $3::text,
        $4::timestamptz,
        coalesce($5::timestamptz, $1::timestamptz, now())
      )
      ON CONFLICT (name) DO UPDATE
      SET updated_at = EXCLUDED.updated_at
      RETURNING *, metadata::text AS metadata_text
    `,
    [
      instantParameter(params.now),
      JSON.stringify(params.metadata ?? {}),
      params.name,
      instantParameter(params.pausedAt),
      instantParameter(params.updatedAt),
    ],
    options
  );
  const row = result.rows[0];
  if (row === undefined) {
    throw databaseError(
      "queueUpsert",
      "PostgreSQL returned no row for an upserted queue"
    );
  }
  return toQueueRow(row);
}

/** Delete stale queue rows in stable name order. */
export async function queueDeleteExpired(
  db: PgDatabase,
  params: { max: number; updatedAtHorizon: Temporal.Instant },
  options?: PgOperationOptions
): Promise<readonly PgQueueRow[]> {
  validateLimit(params.max, "expired queue maximum");
  const result = await db.query<PgQueueDatabaseRow>(
    "queueDeleteExpired",
    `
      DELETE FROM ${db.table("river_queue")}
      WHERE name IN (
        SELECT name FROM ${db.table("river_queue")}
        WHERE updated_at < $1::timestamptz
        ORDER BY name ASC
        LIMIT $2::int
      )
      RETURNING *, metadata::text AS metadata_text
    `,
    [instantParameter(params.updatedAtHorizon), params.max],
    options
  );
  return result.rows
    .sort((left, right) => left.name.localeCompare(right.name))
    .map(toQueueRow);
}

/** List persisted queues in canonical name order. */
export async function queueList(
  db: PgDatabase,
  params: QueueListParams,
  options?: PgOperationOptions
): Promise<readonly QueueRow[]> {
  validateLimit(params.limit);
  const result = await db.query<PgQueueDatabaseRow>(
    "queueList",
    `
      SELECT *, metadata::text AS metadata_text FROM ${db.table("river_queue")}
      WHERE name > coalesce($1::text, '')
      ORDER BY name ASC LIMIT $2::int
    `,
    [params.nameAfter, params.limit],
    options
  );
  return result.rows.map(toQueueRow);
}

/** Pause one queue, or all queues with the `"*"` sentinel. */
export async function queuePause(
  db: PgDatabase,
  name: string,
  options?: PgOperationOptions
): Promise<PgQueueRow | null> {
  const result = await queueSetPaused(db, { name }, true, options);
  return name === "*" ? null : (result.rows[0] ?? null);
}

/** Backend test hook for deterministic queue pause clocks. */
export async function queuePauseWithOptions(
  db: PgDatabase,
  params: PgQueueControlParams,
  options?: PgOperationOptions
): Promise<number> {
  return (await queueSetPaused(db, params, true, options)).rowCount;
}

/** Resume one queue, or all queues with the `"*"` sentinel. */
export async function queueResume(
  db: PgDatabase,
  name: string,
  options?: PgOperationOptions
): Promise<PgQueueRow | null> {
  const result = await queueSetPaused(db, { name }, false, options);
  return name === "*" ? null : (result.rows[0] ?? null);
}

/** Backend test hook for deterministic queue resume clocks. */
export async function queueResumeWithOptions(
  db: PgDatabase,
  params: PgQueueControlParams,
  options?: PgOperationOptions
): Promise<number> {
  return (await queueSetPaused(db, params, false, options)).rowCount;
}

/**
 * Pause or resume matching queues like River's `QueuePause`/`QueueResume`:
 * every matching row is returned, an already paused or resumed queue keeps
 * its timestamps, and one control notification naming the requested queue
 * (possibly `"*"`) is sent in the same transaction.
 */
async function queueSetPaused(
  db: PgDatabase,
  params: PgQueueControlParams,
  paused: boolean,
  options?: PgOperationOptions
): Promise<{ readonly rowCount: number; readonly rows: PgQueueRow[] }> {
  const { supportsListenNotify } = await db.capabilities(options);
  const result = await db.query<
    PgQueueDatabaseRow | { [Key in keyof PgQueueDatabaseRow]: null }
  >(
    paused ? "queuePause" : "queueResume",
    `
      WITH updated AS (
        UPDATE ${db.table("river_queue")}
        SET
          paused_at = CASE
            WHEN NOT $5::boolean THEN NULL
            WHEN paused_at IS NULL THEN coalesce($1::timestamptz, now())
            ELSE paused_at
          END,
          updated_at = CASE
            WHEN (paused_at IS NULL) = $5::boolean
            THEN coalesce($1::timestamptz, now())
            ELSE updated_at
          END
        WHERE CASE WHEN $2::text = '*' THEN true ELSE name = $2::text END
        RETURNING *, metadata::text AS metadata_text
      ),
      notification AS (
        SELECT count(CASE WHEN $6::boolean THEN pg_notify(
          concat(coalesce($3::text, current_schema()), '.', $4::text),
          concat(
            '{"action":"',
            CASE WHEN $5::boolean THEN 'pause' ELSE 'resume' END,
            '","queue":',
            to_json($2::text)::text,
            '}'
          )
        ) END) AS sent
        WHERE $2::text = '*' OR EXISTS (SELECT 1 FROM updated)
      )
      SELECT updated.*
      FROM notification
      LEFT JOIN updated ON true
      ORDER BY updated.name
    `,
    [
      instantParameter(params.now),
      params.name,
      db.schemaName,
      "river_control",
      paused,
      supportsListenNotify,
    ],
    options
  );
  // The single notification row anchors the join, so an empty match
  // returns one row of nulls.
  const rows = result.rows
    .filter((row): row is PgQueueDatabaseRow => row.name !== null)
    .map(toQueueRow);
  return { rowCount: rows.length, rows };
}

/** Update the mutable fields of a persisted queue. */
export async function queueUpdate(
  db: PgDatabase,
  name: string,
  params: QueueUpdateParams,
  options?: PgOperationOptions
): Promise<PgQueueRow | null> {
  const update = queueMetadataUpdate(name, params);
  const { supportsListenNotify } = await db.capabilities(options);
  const result = await db.query<PgQueueDatabaseRow>(
    "queueUpdate",
    `
      WITH updated AS (
        UPDATE ${db.table("river_queue")}
        SET
          metadata = CASE WHEN $1::boolean THEN $2::jsonb ELSE metadata END,
          updated_at = now()
        WHERE name = $3::text
        RETURNING *, metadata::text AS metadata_text
      ),
      notification AS (
        SELECT CASE WHEN $7::boolean THEN pg_notify(
          concat(coalesce($4::text, current_schema()), '.', $5::text),
          $6::text
        ) END FROM updated WHERE $1::boolean
      )
      SELECT updated.* FROM updated LEFT JOIN notification ON true
    `,
    [
      update !== undefined,
      update?.text ?? "{}",
      name,
      db.schemaName,
      "river_control",
      update?.notification ?? "",
      supportsListenNotify,
    ],
    options
  );
  const row = result.rows[0];
  return row === undefined ? null : toQueueRow(row);
}
