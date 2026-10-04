/** Leader election queries on `river_leader`. */
import type { QueryResult } from "pg";
import type { PgDatabase } from "../database.js";
import type {
  PgLeader,
  PgLeaderElectParams,
  PgLeaderTermParams,
  PgOperationOptions,
} from "../types.js";
import { instantParameter } from "./params.js";
import type { PgLeaderDatabaseRow } from "./rows.js";

/** Attempt to acquire the singleton River leadership lease. */
export async function leaderElect(
  db: PgDatabase,
  params: PgLeaderElectParams,
  options?: PgOperationOptions
): Promise<PgLeader | null> {
  validateLeaderParams(params);
  const result = await db.query<PgLeaderDatabaseRow>(
    "leaderElect",
    `
      INSERT INTO ${db.table("river_leader")} (
        leader_id, elected_at, expires_at
      ) VALUES (
        $1::text,
        coalesce($2::timestamptz, now()),
        coalesce($2::timestamptz, now()) + make_interval(secs => $3::double precision)
      )
      ON CONFLICT (name) DO NOTHING
      RETURNING *
    `,
    [params.leaderId, instantParameter(params.now), params.ttlSeconds],
    options
  );
  return mapOneLeader(result);
}

/** Renew a leadership lease only for the exact current election term. */
export async function leaderReelect(
  db: PgDatabase,
  params: PgLeaderTermParams,
  options?: PgOperationOptions
): Promise<PgLeader | null> {
  validateLeaderParams(params);
  const result = await db.query<PgLeaderDatabaseRow>(
    "leaderReelect",
    `
      UPDATE ${db.table("river_leader")}
      SET expires_at = coalesce($1::timestamptz, now())
        + make_interval(secs => $2::double precision)
      WHERE elected_at = $3::timestamptz
        AND expires_at >= coalesce($1::timestamptz, now())
        AND leader_id = $4::text
      RETURNING *
    `,
    [
      instantParameter(params.now),
      params.ttlSeconds,
      instantParameter(params.electedAt),
      params.leaderId,
    ],
    options
  );
  return mapOneLeader(result);
}

/** Read the currently persisted leader, whether or not its lease is expired. */
export async function leaderGet(
  db: PgDatabase,
  options?: PgOperationOptions
): Promise<PgLeader | null> {
  const result = await db.query<PgLeaderDatabaseRow>(
    "leaderGet",
    `SELECT * FROM ${db.table("river_leader")} LIMIT 1`,
    [],
    options
  );
  return mapOneLeader(result);
}

/** Remove expired leadership rows so a new election can proceed. */
export async function leaderDeleteExpired(
  db: PgDatabase,
  now?: Temporal.Instant,
  options?: PgOperationOptions
): Promise<number> {
  const result = await db.query(
    "leaderDeleteExpired",
    `
      DELETE FROM ${db.table("river_leader")}
      WHERE expires_at < coalesce($1::timestamptz, now())
    `,
    [instantParameter(now)],
    options
  );
  return result.rowCount ?? 0;
}

/** Resign only the exact election term and notify leadership observers. */
export async function leaderResign(
  db: PgDatabase,
  params: PgLeaderTermParams & { leadershipTopic: string },
  options?: PgOperationOptions
): Promise<boolean> {
  const { supportsListenNotify } = await db.capabilities(options);
  const result = await db.query(
    "leaderResign",
    `
      WITH held AS (
        SELECT * FROM ${db.table("river_leader")}
        WHERE elected_at = $1::timestamptz AND leader_id = $2::text
        FOR UPDATE
      ),
      notified AS (
        SELECT CASE WHEN $5::boolean THEN pg_notify(
          concat(coalesce($3::text, current_schema()), '.', $4::text),
          json_build_object('leader_id', leader_id, 'action', 'resigned')::text
        ) END FROM held
      )
      DELETE FROM ${db.table("river_leader")} USING notified
    `,
    [
      instantParameter(params.electedAt),
      params.leaderId,
      db.schemaName,
      params.leadershipTopic,
      supportsListenNotify,
    ],
    options
  );
  return (result.rowCount ?? 0) > 0;
}

function mapOneLeader(
  result: QueryResult<PgLeaderDatabaseRow>
): PgLeader | null {
  const row = result.rows[0];
  return row === undefined
    ? null
    : {
        electedAt: row.elected_at,
        expiresAt: row.expires_at,
        leaderId: row.leader_id,
      };
}

function validateLeaderParams(params: PgLeaderElectParams): void {
  if (params.leaderId.length === 0 || params.leaderId.length >= 128) {
    throw new RangeError("leaderId must contain from 1 to 127 characters");
  }
  if (!Number.isFinite(params.ttlSeconds) || params.ttlSeconds <= 0) {
    throw new RangeError("ttlSeconds must be a positive finite number");
  }
}
