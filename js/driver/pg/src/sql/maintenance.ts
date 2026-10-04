/** Leader-fenced maintenance: scheduling, rescue, cleanup, and reindexing. */
import { quoteIdentifier } from "riverqueue/unstable-driver";
import { Buffer } from "node:buffer";
import type { ClientBase, PoolClient } from "pg";
import type { AttemptError, JobRow, JsonObject } from "riverqueue";
import type {
  FinalizedJobDeleteParams,
  RuntimeJobCleanupParams,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
  RuntimeScheduleParams,
} from "riverqueue/unstable-driver";
import type { PgDatabase } from "../database.js";
import { POSTGRES_IDENTIFIER_MAX_BYTES } from "../database.js";
import { configurationError, unsupportedError } from "../errors.js";
import { abortablePromise, PgClientLease } from "../lease.js";
import type {
  PgJobDeleteBeforeParams,
  PgJobRescueManyParams,
  PgJobScheduleResult,
  PgOperationOptions,
} from "../types.js";
import {
  leaderDeleteExpired,
  leaderElect,
  leaderReelect,
  leaderResign,
} from "./leader.js";
import { notifyInsert } from "./notify.js";
import { instantParameter, validateLimit } from "./params.js";
import { queueDeleteExpired } from "./queues.js";
import type { PgJobRow, PgScheduleRow } from "./rows.js";
import { toJobRow, toJobRowPartial } from "./rows.js";

/** Statement timeout for dropping an aborted reindex's artifacts. */
const REINDEX_CLEANUP_TIMEOUT_MS = 15_000;

/**
 * Leading comment identifying reindex statements River may cancel server
 * side. The cancellation only signals a backend still running a statement
 * with this prefix, so a reused process ID can never cancel unrelated work.
 */
const REINDEX_STATEMENT = "/* river:reindex */";

/** Delete terminal jobs below configured retention horizons. */
export async function jobDeleteBefore(
  db: PgDatabase,
  params: PgJobDeleteBeforeParams,
  options?: PgOperationOptions
): Promise<number> {
  validateLimit(params.max, "cleaner maximum");
  const result = await db.query(
    "jobDeleteBefore",
    `
      DELETE FROM ${db.table("river_job")}
      WHERE id IN (
        SELECT id
        FROM ${db.table("river_job")}
        WHERE (
            ($1::timestamptz IS NOT NULL AND state = 'cancelled' AND finalized_at < $1::timestamptz)
            OR ($2::timestamptz IS NOT NULL AND state = 'completed' AND finalized_at < $2::timestamptz)
            OR ($3::timestamptz IS NOT NULL AND state = 'discarded' AND finalized_at < $3::timestamptz)
          )
          AND ($4::text[] IS NULL OR NOT (queue = ANY($4::text[])))
          AND ($5::text[] IS NULL OR queue = ANY($5::text[]))
        ORDER BY id ASC
        LIMIT $6::int
      )
    `,
    [
      instantParameter(params.cancelledFinalizedAt),
      instantParameter(params.completedFinalizedAt),
      instantParameter(params.discardedFinalizedAt),
      params.queuesExcluded ?? null,
      params.queuesIncluded ?? null,
      params.max,
    ],
    options
  );
  return result.rowCount ?? 0;
}

/**
 * Delete one batch of finalized jobs by per-state cutoffs, with `null`
 * keeping a state, through River's `JobDeleteBefore` statement.
 */
export function jobDeleteFinalized(
  db: PgDatabase,
  params: FinalizedJobDeleteParams,
  options?: PgOperationOptions
): Promise<number> {
  return jobDeleteBefore(
    db,
    {
      ...(params.cancelledBefore === null
        ? {}
        : { cancelledFinalizedAt: params.cancelledBefore }),
      ...(params.completedBefore === null
        ? {}
        : { completedFinalizedAt: params.completedBefore }),
      ...(params.discardedBefore === null
        ? {}
        : { discardedFinalizedAt: params.discardedBefore }),
      max: params.limit,
      ...(params.queuesExcluded === undefined ||
      params.queuesExcluded.length === 0
        ? {}
        : { queuesExcluded: params.queuesExcluded }),
      ...(params.queuesIncluded === undefined || params.queuesIncluded === null
        ? {}
        : { queuesIncluded: params.queuesIncluded }),
    },
    options
  );
}

/** Read running jobs old enough for rescuer inspection. */
export async function jobGetStuck(
  db: PgDatabase,
  params: { afterId?: bigint; max: number; stuckHorizon: Temporal.Instant },
  options?: PgOperationOptions
): Promise<readonly JobRow[]> {
  validateLimit(params.max, "stuck job maximum");
  const result = await db.query<PgJobRow>(
    "jobGetStuck",
    `
      SELECT * FROM ${db.table("river_job")}
      WHERE state = 'running'
        AND id > $1::bigint
        AND attempted_at < $2::timestamptz
      ORDER BY id ASC
      LIMIT $3::int
    `,
    [
      (params.afterId ?? 0n).toString(10),
      instantParameter(params.stuckHorizon),
      params.max,
    ],
    options
  );
  // A row that can't be fully decoded is returned with those fields empty so
  // it can't keep the rescuer from recovering every stuck job.
  return result.rows.map((row) => toJobRowPartial(row).job);
}

/** Rescue still-stuck running jobs without overwriting concurrent completion. */
export async function jobRescueMany(
  db: PgDatabase,
  params: PgJobRescueManyParams,
  options?: PgOperationOptions
): Promise<number> {
  if (params.items.length === 0) return 0;
  const result = await db.query(
    "jobRescueMany",
    `
      WITH rescued AS (
        SELECT *
        FROM unnest(
          $1::bigint[], $2::jsonb[], $3::timestamptz[],
          $4::timestamptz[], $5::text[]
        ) AS input(id, error, finalized_at, scheduled_at, state_text)
      )
      UPDATE ${db.table("river_job")} AS river_job
      SET
        errors = array_append(river_job.errors, rescued.error),
        finalized_at = rescued.finalized_at,
        scheduled_at = rescued.scheduled_at,
        metadata = river_job.metadata || jsonb_build_object(
          'river:rescue_count',
          coalesce(
            CASE
              WHEN jsonb_typeof(river_job.metadata -> 'river:rescue_count') = 'number'
              THEN (river_job.metadata ->> 'river:rescue_count')::int
            END,
            0
          ) + 1
        ),
        state = rescued.state_text::${db.type("river_job_state")}
      FROM rescued
      WHERE river_job.id = rescued.id
        AND river_job.state = 'running'
        AND river_job.attempted_at < $6::timestamptz
    `,
    [
      params.items.map(({ id }) => id.toString(10)),
      params.items.map(({ error }) =>
        JSON.stringify(encodeAttemptError(error))
      ),
      params.items.map(({ finalizedAt }) => instantParameter(finalizedAt)),
      params.items.map(({ scheduledAt }) => instantParameter(scheduledAt)),
      params.items.map(({ state }) => state),
      instantParameter(params.stuckHorizon),
    ],
    options
  );
  return result.rowCount ?? 0;
}

/** Move due scheduled/retryable jobs into the runnable state. */
export async function jobSchedule(
  db: PgDatabase,
  params: {
    max: number;
    now?: Temporal.Instant;
    scheduledAtHorizon?: Temporal.Instant;
  },
  options?: PgOperationOptions
): Promise<readonly PgJobScheduleResult[]> {
  validateLimit(params.max, "scheduler maximum");
  const jobTable = db.table("river_job");
  const jobState = db.type("river_job_state");
  const inBitmask = db.function("river_job_state_in_bitmask");
  const result = await db.query<PgScheduleRow>(
    "jobSchedule",
    `
      WITH jobs_to_schedule AS (
        SELECT id, unique_key, unique_states, priority, scheduled_at
        FROM ${jobTable}
        WHERE state IN ('retryable', 'scheduled')
          AND scheduled_at <= coalesce($1::timestamptz, now())
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT $3::int
        FOR UPDATE
      ),
      jobs_with_rownum AS (
        SELECT *, CASE
          WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL
          THEN row_number() OVER (
            PARTITION BY unique_key ORDER BY priority, scheduled_at, id
          )
          ELSE NULL
        END AS row_num
        FROM jobs_to_schedule
      ),
      unique_conflicts AS (
        SELECT DISTINCT river_job.unique_key
        FROM ${jobTable} AS river_job
        JOIN jobs_with_rownum AS job
          ON river_job.unique_key = job.unique_key AND river_job.id != job.id
        WHERE river_job.unique_key IS NOT NULL
          AND river_job.unique_states IS NOT NULL
          AND ${inBitmask}(river_job.unique_states, river_job.state)
      ),
      job_updates AS (
        SELECT
          job.id,
          CASE
            WHEN job.row_num IS NULL THEN 'available'::${jobState}
            WHEN conflict.unique_key IS NOT NULL OR job.row_num > 1
              THEN 'discarded'::${jobState}
            ELSE 'available'::${jobState}
          END AS new_state,
          (job.row_num IS NOT NULL AND (conflict.unique_key IS NOT NULL OR job.row_num > 1))
            AS conflict_discarded
        FROM jobs_with_rownum AS job
        LEFT JOIN unique_conflicts AS conflict ON conflict.unique_key = job.unique_key
      ),
      updated AS (
        UPDATE ${jobTable} AS river_job
        SET
          state = job_updates.new_state,
          finalized_at = CASE WHEN job_updates.conflict_discarded
            THEN coalesce($2::timestamptz, now()) ELSE river_job.finalized_at END,
          metadata = CASE WHEN job_updates.conflict_discarded
            THEN river_job.metadata || '{"unique_key_conflict":"scheduler_discarded"}'::jsonb
            ELSE river_job.metadata END
        FROM job_updates
        WHERE river_job.id = job_updates.id
        RETURNING river_job.*, job_updates.conflict_discarded
      )
      SELECT updated.*
      FROM updated
      ORDER BY priority ASC, scheduled_at ASC, id ASC
    `,
    [
      instantParameter(params.scheduledAtHorizon ?? params.now),
      instantParameter(params.now),
      params.max,
    ],
    options
  );
  return result.rows.map((row) => ({
    conflictDiscarded: row.conflict_discarded,
    job: toJobRow(row),
  }));
}

/** Acquire or renew one exact leadership lease after expiring stale terms. */
export async function maintenanceLeaderAcquire(
  db: PgDatabase,
  leaderId: string,
  ttlMs: number,
  held: RuntimeLeader | null,
  signal?: AbortSignal
): Promise<RuntimeLeader | null> {
  // PostgreSQL's clock is authoritative for cross-host lease expiry. The
  // runtime keeps a separate monotonic local trust deadline. Like Go River,
  // only the held term is renewed and an unexpired term is never adopted,
  // even one with this client's leader ID.
  return db.withConnection(signal, async (options) => {
    if (held !== null) {
      return leaderReelect(
        db,
        { electedAt: held.electedAt, leaderId, ttlSeconds: ttlMs / 1_000 },
        options
      );
    }
    await leaderDeleteExpired(db, undefined, options);
    return leaderElect(db, { leaderId, ttlSeconds: ttlMs / 1_000 }, options);
  });
}

/** Resign a maintenance lease and announce it on the leadership topic. */
export function maintenanceLeaderResign(
  db: PgDatabase,
  leader: RuntimeLeader
): Promise<boolean> {
  return leaderResign(db, {
    ...leader,
    leadershipTopic: "river_leadership",
    now: Temporal.Now.instant(),
    ttlSeconds: 1,
  });
}

/** Schedule due jobs while `leader` still holds its lease. */
export async function maintenanceSchedule(
  db: PgDatabase,
  leader: RuntimeLeader,
  params: RuntimeScheduleParams,
  batch?: RuntimeMaintenanceBatch
): Promise<number> {
  return (
    (await withMaintenanceLeader(
      db,
      leader,
      "maintenanceSchedule",
      batch,
      async (tx) => {
        const results = await jobSchedule(
          db,
          {
            max: params.limit,
            now: params.now,
            scheduledAtHorizon: params.scheduledAtHorizon,
          },
          { tx }
        );
        // Like River for Go's scheduler, wake producers of jobs that are
        // due, or nearly due, through the client's insert notification
        // limiter, in the same transaction.
        const queues = params.allowInsertNotifications(
          results.flatMap(({ job }) =>
            Temporal.Instant.compare(
              job.scheduledAt,
              params.notificationHorizon
            ) <= 0
              ? [job.queue]
              : []
          )
        );
        await notifyInsert(db, queues, { tx });
        return results.length;
      }
    )) ?? 0
  );
}

/** Read stuck running jobs while `leader` still holds its lease. */
export function maintenanceGetStuck(
  db: PgDatabase,
  leader: RuntimeLeader,
  attemptedBefore: Temporal.Instant,
  afterId: bigint,
  limit: number,
  batch?: RuntimeMaintenanceBatch
): Promise<readonly JobRow[]> {
  return withMaintenanceLeader(db, leader, "maintenanceGetStuck", batch, (tx) =>
    jobGetStuck(
      db,
      { afterId, max: limit, stuckHorizon: attemptedBefore },
      { tx }
    )
  ).then((jobs) => jobs ?? []);
}

/** Rescue stuck jobs while `leader` still holds its lease. */
export function maintenanceRescue(
  db: PgDatabase,
  leader: RuntimeLeader,
  attemptedBefore: Temporal.Instant,
  jobs: readonly RuntimeJobRescue[],
  tx?: ClientBase
): Promise<number> {
  const rescue = (transaction: ClientBase): Promise<number> =>
    jobRescueMany(
      db,
      {
        items: jobs.map((job) => ({
          error: job.error,
          ...(job.finalizedAt === null ? {} : { finalizedAt: job.finalizedAt }),
          id: job.id,
          scheduledAt: job.scheduledAt,
          state: job.state,
        })),
        stuckHorizon: attemptedBefore,
      },
      { tx: transaction }
    );
  const fenced =
    tx === undefined
      ? withMaintenanceLeader(
          db,
          leader,
          "maintenanceRescue",
          undefined,
          rescue
        )
      : withLeaderFenceIn(db, leader, "maintenanceRescue", tx, rescue);
  return fenced.then((count) => count ?? 0);
}

/** Delete expired terminal jobs while `leader` still holds its lease. */
export function maintenanceCleanJobs(
  db: PgDatabase,
  leader: RuntimeLeader,
  params: RuntimeJobCleanupParams,
  timeoutMs: number | null,
  signal: AbortSignal
): Promise<number> {
  return withMaintenanceLeader(
    db,
    leader,
    "maintenanceCleanJobs",
    { signal, timeoutMs },
    async (tx) => {
      signal.throwIfAborted();
      return jobDeleteFinalized(db, params, { tx });
    }
  ).then((count) => count ?? 0);
}

/** Delete expired queues while `leader` still holds its lease. */
export async function maintenanceCleanQueues(
  db: PgDatabase,
  leader: RuntimeLeader,
  updatedBefore: Temporal.Instant,
  limit: number,
  batch?: RuntimeMaintenanceBatch
): Promise<number> {
  return (
    (await withMaintenanceLeader(
      db,
      leader,
      "maintenanceCleanQueues",
      batch,
      async (tx) =>
        (
          await queueDeleteExpired(
            db,
            { max: limit, updatedAtHorizon: updatedBefore },
            { tx }
          )
        ).length
    )) ?? 0
  );
}

/**
 * Delete up to `max` durable SQLite-style notification rows retained by
 * migration v7 from before a horizon, oldest first, like Go's
 * `NotificationDeleteBefore`.
 */
export async function notificationDeleteBefore(
  db: PgDatabase,
  params: { createdAtHorizon: Temporal.Instant; max: number },
  options?: PgOperationOptions
): Promise<number> {
  const table = db.table("river_notification");
  const result = await db.query(
    "notificationDeleteBefore",
    `DELETE FROM ${table}
     WHERE id IN (
       SELECT id
       FROM ${table}
       WHERE created_at < $1::timestamptz
       ORDER BY created_at, id
       LIMIT $2::bigint
     )`,
    [instantParameter(params.createdAtHorizon), params.max],
    options
  );
  return result.rowCount ?? 0;
}

/** Discover `_ccnew`/`_ccold` artifacts from an interrupted reindex. */
export async function indexReindexArtifacts(
  db: PgDatabase,
  index: string,
  options?: PgOperationOptions
): Promise<readonly string[]> {
  const result = await db.query<{ artifact_name: string }>(
    "indexReindexArtifacts",
    `
      WITH index_artifacts AS (
        SELECT
          c.relname::text AS artifact_name,
          substring(c.relname FROM length($2::text) + 1) AS suffix
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = coalesce($1::text, current_schema())
          AND c.relkind = 'i'
          AND left(c.relname, length($2::text)) = $2::text
      )
      SELECT artifact_name
      FROM index_artifacts
      WHERE suffix ~ '^_cc(new|old)[0-9]*$'
      ORDER BY artifact_name
    `,
    [db.schemaName, index],
    options
  );
  return result.rows.map(({ artifact_name }) => artifact_name);
}

/** Reindex one allow-listed River index using a safely quoted identifier. */
export async function indexReindex(
  db: PgDatabase,
  index: string,
  options?: PgOperationOptions
): Promise<void> {
  validateIndexName(index);
  await db.query(
    "indexReindex",
    `${REINDEX_STATEMENT} REINDEX INDEX CONCURRENTLY ${db.schemaPrefix}${quoteIdentifier(index)}`,
    [],
    options
  );
}

/** Drop a concurrent-reindex artifact using a safely quoted identifier. */
export async function indexDropIfExists(
  db: PgDatabase,
  index: string,
  options?: PgOperationOptions
): Promise<void> {
  validateIndexName(index);
  await db.query(
    "indexDropIfExists",
    `DROP INDEX CONCURRENTLY IF EXISTS ${db.schemaPrefix}${quoteIdentifier(index)}`,
    [],
    options
  );
}

/** Return exact existence results for indexes in the configured schema. */
export async function indexesExist(
  db: PgDatabase,
  indexes: readonly string[],
  options?: PgOperationOptions
): Promise<ReadonlyMap<string, boolean>> {
  for (const index of indexes) validateIndexName(index);
  if (indexes.length === 0) return new Map();
  const result = await db.query<{
    exists: boolean;
    index_name: string;
  }>(
    "indexesExist",
    `
      WITH index_names AS (
        SELECT unnest($2::text[]) AS index_name
      )
      SELECT
        index_names.index_name::text AS index_name,
        EXISTS (
          SELECT 1
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = coalesce($1::text, current_schema())
            AND c.relname = index_names.index_name
            AND c.relkind = 'i'
        ) AS exists
      FROM index_names
    `,
    [db.schemaName, indexes],
    options
  );
  return new Map(result.rows.map((row) => [row.index_name, row.exists]));
}

/** Rebuild existing configured indexes with River's artifact safeguards. */
export async function maintenanceReindex(
  db: PgDatabase,
  leader: RuntimeLeader,
  indexes: readonly string[],
  timeoutMs: number | null,
  signal: AbortSignal
): Promise<number> {
  if (db.pool === null) {
    throw unsupportedError(
      "maintenance",
      "PostgreSQL reindex maintenance requires a Pool"
    );
  }
  if (
    timeoutMs !== null &&
    (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1)
  ) {
    throw configurationError(
      "maintenanceReindex",
      "reindex timeout must be a positive safe integer or null"
    );
  }
  if (!(await maintenanceLeaderIsCurrent(db, leader))) return 0;
  const exists = await indexesExist(db, indexes);
  let reindexed = 0;
  for (const index of indexes) {
    if (signal.aborted) throw signal.reason;
    if (!(await maintenanceLeaderIsCurrent(db, leader))) return reindexed;
    if (exists.get(index) !== true) continue;
    const artifacts = await indexReindexArtifacts(db, index);
    if (artifacts.length > 0) continue;
    try {
      await reindexOne(db, index, timeoutMs, signal);
      reindexed++;
    } catch (error: unknown) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
      if (signal.aborted) await cleanupReindexArtifacts(db, index);
      throw error;
    }
  }
  return reindexed;
}

/** Whether `leader`'s lease is still current in `river_leader`. */
async function maintenanceLeaderIsCurrent(
  db: PgDatabase,
  leader: RuntimeLeader
): Promise<boolean> {
  const result = await db.query(
    "maintenanceLeaderIsCurrent",
    `
      SELECT 1
      FROM ${db.table("river_leader")}
      WHERE elected_at = $1::timestamptz
        AND expires_at >= now()
        AND leader_id = $2::text
    `,
    [instantParameter(leader.electedAt), leader.leaderId]
  );
  return (result.rowCount ?? 0) > 0;
}

/**
 * Lock `leader`'s exact term in `tx` for the rest of the transaction, or
 * return false when the lease has moved on.
 *
 * `FOR KEY SHARE` fences the term against resignation, expiry cleanup, and
 * replacement (all deletes) for the whole operation, but not against
 * renewal, which updates only `expires_at`. Like Go River, the leader keeps
 * renewing its lease while maintenance is slow or blocked.
 */
async function holdLeaderFence(
  db: PgDatabase,
  operation: string,
  leader: RuntimeLeader,
  tx: ClientBase
): Promise<boolean> {
  const held = await db.query(
    `${operation}LeaderFence`,
    `
    SELECT 1
    FROM ${db.table("river_leader")}
    WHERE elected_at = $1::timestamptz
      AND expires_at >= now()
      AND leader_id = $2::text
    FOR KEY SHARE
  `,
    [instantParameter(leader.electedAt), leader.leaderId],
    { tx }
  );
  return (held.rowCount ?? 0) > 0;
}

/**
 * Run `run` in the caller's transaction `tx`, fenced by `leader`'s exact
 * term like {@link withMaintenanceLeader}, or return null without running
 * it once the lease has moved on. The caller ends `tx`.
 */
async function withLeaderFenceIn<T>(
  db: PgDatabase,
  leader: RuntimeLeader,
  operation: string,
  tx: ClientBase,
  run: (transaction: ClientBase) => Promise<T>
): Promise<T | null> {
  if (!(await holdLeaderFence(db, operation, leader, tx))) return null;
  return run(tx);
}

/**
 * Run `run` in a transaction fenced by `leader`'s exact term, or return null
 * without running it once the lease has moved on. A `batch` bounds every
 * statement with its timeout (`statement_timeout`, or none for `null`), so a
 * timed-out batch fails and rolls back like Go's. When the batch's signal
 * aborts, as when its term ends or the client stops, the connection is
 * destroyed, which rolls the transaction back, and the call rejects at
 * once, like a cancelled context in River for Go, so a statement stuck on
 * a half-open socket can't hold up a stop.
 */
async function withMaintenanceLeader<T>(
  db: PgDatabase,
  leader: RuntimeLeader,
  operation: string,
  batch: RuntimeMaintenanceBatch | undefined,
  run: (transaction: PoolClient) => Promise<T>
): Promise<T | null> {
  if (db.pool === null) {
    throw unsupportedError(
      "maintenance",
      "PostgreSQL maintenance requires a Pool"
    );
  }
  batch?.signal.throwIfAborted();
  // A stop or timeout ends the wait for a connection, such as during an
  // outage; a connection that arrives later goes back to the pool unused.
  const acquiring = db.pool.connect();
  let lease: PgClientLease;
  try {
    lease = new PgClientLease(await abortablePromise(acquiring, batch?.signal));
  } catch (error: unknown) {
    void acquiring.then((late) => late.release()).catch(() => {});
    throw error;
  }
  const client = lease.client;
  const signal = batch?.signal;
  const destroy = (): void => {
    lease.destroy();
  };
  signal?.addEventListener("abort", destroy, { once: true });
  const step = <T>(operation: Promise<T>): Promise<T> =>
    abortablePromise(lease.race(operation), signal);
  let transactionStarted = false;
  try {
    await step(client.query("BEGIN"));
    transactionStarted = true;
    if (batch !== undefined) {
      await step(
        db.query(
          `${operation}Timeout`,
          "SELECT set_config('statement_timeout', $1::text, true)",
          [(batch.timeoutMs ?? 0).toString(10)],
          { tx: client }
        )
      );
    }
    const held = await step(holdLeaderFence(db, operation, leader, client));
    if (!held) {
      await step(client.query("ROLLBACK"));
      transactionStarted = false;
      return null;
    }
    const result = await step(run(client));
    await step(client.query("COMMIT"));
    transactionStarted = false;
    return result;
  } catch (cause: unknown) {
    if (transactionStarted && !lease.failed && signal?.aborted !== true) {
      try {
        await lease.race(client.query("ROLLBACK"));
      } catch {
        lease.destroy();
        throw cause;
      }
    }
    throw cause;
  } finally {
    signal?.removeEventListener("abort", destroy);
    lease.release();
  }
}

/**
 * Reindex one index concurrently on a dedicated connection with a
 * statement timeout, cancelling the statement server side on abort.
 */
async function reindexOne(
  db: PgDatabase,
  index: string,
  timeoutMs: number | null,
  signal: AbortSignal
): Promise<void> {
  if (db.pool === null) throw new Error("reindex pool preflight failed");
  if (signal.aborted) throw signal.reason;
  const lease = new PgClientLease(await db.pool.connect());
  const client = lease.client;
  const abort = () => {
    // Destroying the socket alone leaves a concurrent reindex running on
    // the server until it next writes to the dead connection.
    db.cancelBackend(client, REINDEX_STATEMENT);
    lease.destroy();
  };
  let operationError: unknown;
  let operationFailed = false;
  try {
    signal.addEventListener("abort", abort, { once: true });
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
    if (signal.aborted) throw signal.reason;
    await lease.race(
      client.query("SELECT set_config('statement_timeout', $1::text, false)", [
        timeoutMs === null ? "0" : timeoutMs.toString(10),
      ])
    );
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
    if (signal.aborted) throw signal.reason;
    await lease.race(indexReindex(db, index, { tx: client }));
  } catch (error: unknown) {
    operationError = error;
    operationFailed = true;
  } finally {
    signal.removeEventListener("abort", abort);
  }

  let resetError: unknown;
  let resetFailed = false;
  try {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
    if (!lease.failed && !signal.aborted) {
      await lease.race(client.query("RESET statement_timeout"));
    }
    lease.release();
  } catch (error: unknown) {
    resetError = error;
    resetFailed = true;
    lease.destroy();
  }
  if (operationFailed) throw operationError;
  if (resetFailed) throw resetError;
}

/** Drop `_ccnew`/`_ccold` artifacts an aborted reindex left behind. */
async function cleanupReindexArtifacts(
  db: PgDatabase,
  index: string
): Promise<void> {
  if (db.pool === null) return;
  const lease = new PgClientLease(await db.pool.connect());
  const client = lease.client;
  try {
    await lease.race(
      client.query("SELECT set_config('statement_timeout', $1::text, false)", [
        REINDEX_CLEANUP_TIMEOUT_MS.toString(10),
      ])
    );
    const artifacts = await lease.race(
      indexReindexArtifacts(db, index, { tx: client })
    );
    for (const artifact of artifacts) {
      await lease.race(indexDropIfExists(db, artifact, { tx: client }));
    }
  } finally {
    if (!lease.failed) {
      try {
        await lease.race(client.query("RESET statement_timeout"));
        lease.release();
      } catch {
        lease.destroy();
      }
    }
  }
}

function encodeAttemptError(error: AttemptError): JsonObject {
  return {
    at: error.at.toString(),
    attempt: error.attempt,
    error: error.error,
    trace: error.trace,
  };
}

function validateIndexName(value: string): void {
  if (
    value.length === 0 ||
    value.includes("\0") ||
    Buffer.byteLength(value, "utf8") > POSTGRES_IDENTIFIER_MAX_BYTES
  ) {
    throw configurationError(
      "index",
      `PostgreSQL index names must contain 1 to ${POSTGRES_IDENTIFIER_MAX_BYTES} bytes without NUL`
    );
  }
}
