/** Raw node-postgres row shapes and their exact River decoders. */
import type { Buffer } from "node:buffer";
import type { QueryResult, QueryResultRow } from "pg";
import type { JobRow } from "riverqueue";
import { toJsonObject } from "riverqueue";
import {
  decodeAttemptError,
  decodeJobState,
  recordQueueMetadataText,
  uniqueBitmaskToStates,
} from "riverqueue/unstable-driver";
import type { PgQueueRow } from "../types.js";

/** A `river_job` row as node-postgres returns it with River's type parsers. */
export interface PgJobRow extends QueryResultRow {
  args: unknown;
  attempt: number;
  attempted_at: Temporal.Instant | null;
  attempted_by: string[] | null;
  created_at: Temporal.Instant;
  /** Each attempt error's JSON text. */
  errors: (string | null)[] | null;
  finalized_at: Temporal.Instant | null;
  id: bigint;
  kind: string;
  max_attempts: number;
  metadata: unknown;
  priority: number;
  queue: string;
  scheduled_at: Temporal.Instant;
  state: string;
  tags: string[] | null;
  unique_key: Buffer | null;
  unique_states: string | null;
}

/** An inserted job row with the insert's duplicate flag. */
export interface PgInsertRow extends PgJobRow {
  unique_skipped_as_duplicate: boolean;
}

export interface PgQueueDatabaseRow extends QueryResultRow {
  created_at: Temporal.Instant;
  metadata: unknown;
  /** `metadata::text`, Postgres's rendering of the stored JSONB. */
  metadata_text: string;
  name: string;
  paused_at: Temporal.Instant | null;
  updated_at: Temporal.Instant;
}

/** A job row returned by a completion batch, with whether it applied. */
export interface PgCompletionRow extends PgJobRow {
  transition_applied: boolean;
}

export interface PgLeaderDatabaseRow extends QueryResultRow {
  elected_at: Temporal.Instant;
  expires_at: Temporal.Instant;
  leader_id: string;
}

/** A scheduled job row with whether a unique conflict discarded it. */
export interface PgScheduleRow extends PgJobRow {
  conflict_discarded: boolean;
}

/** Decode the first row of a result, or null when there is none. */
export function mapOneJob(result: QueryResult<PgJobRow>): JobRow | null {
  const row = result.rows[0];
  return row === undefined ? null : toJobRow(row);
}

/**
 * Decode the first row of a result, or null when there is none, leaving any
 * field that can't be decoded empty like {@link toJobRowPartial}. Operations
 * that act on a job by ID use it, so an operator can cancel, retry, or delete
 * a row another engine wrote that River can't fully read.
 */
export function mapOneJobPartial(result: QueryResult<PgJobRow>): JobRow | null {
  const row = result.rows[0];
  return row === undefined ? null : toJobRowPartial(row).job;
}

/** Decode a raw job row exactly, throwing if any field can't be decoded. */
export function toJobRow(row: PgJobRow): JobRow {
  const { error, job } = toJobRowPartial(row);
  if (error !== undefined) throw error;
  return job;
}

/**
 * Decode a raw job row, leaving any `args`, `metadata`, or `errors` value
 * that can't be decoded empty and returning the decode error alongside, so
 * one bad row can't fail a claim, a completion batch, or the rescuer.
 */
export function toJobRowPartial(row: PgJobRow): {
  readonly error?: Error;
  readonly job: JobRow;
} {
  const failures: Error[] = [];
  const decode = <T>(field: string, empty: T, decoder: () => T): T => {
    try {
      return decoder();
    } catch (cause: unknown) {
      failures.push(
        new TypeError(
          `could not decode \`${field}\`: ${cause instanceof Error ? cause.message : String(cause)}`,
          { cause }
        )
      );
      return empty;
    }
  };
  const job: JobRow = {
    args: decode("args", {}, () => toJsonObject(row.args)),
    attempt: row.attempt,
    attemptedAt: row.attempted_at,
    attemptedBy: row.attempted_by ?? [],
    createdAt: row.created_at,
    errors: decode("errors", [], () =>
      (row.errors ?? []).map((error) => {
        // Like River for Go, an element that is NULL isn't valid JSON.
        if (error === null) throw new TypeError("attempt error is NULL");
        return decodeAttemptError(error);
      })
    ),
    finalizedAt: row.finalized_at,
    id: row.id,
    kind: row.kind,
    maxAttempts: row.max_attempts,
    metadata: decode("metadata", {}, () => toJsonObject(row.metadata)),
    priority: row.priority,
    queue: row.queue,
    scheduledAt: row.scheduled_at,
    state: decodeJobState(row.state),
    tags: row.tags ?? [],
    uniqueKey: row.unique_key === null ? null : new Uint8Array(row.unique_key),
    uniqueStates:
      row.unique_states === null
        ? null
        : uniqueBitmaskToStates(Number.parseInt(row.unique_states, 2)),
  };
  if (failures.length === 0) return { job };
  return {
    error:
      failures.length === 1 && failures[0] !== undefined
        ? failures[0]
        : new AggregateError(
            failures,
            failures.map((f) => f.message).join("; ")
          ),
    job,
  };
}

export function toQueueRow(row: PgQueueDatabaseRow): PgQueueRow {
  const queue: PgQueueRow = {
    createdAt: row.created_at,
    metadata: toJsonObject(row.metadata),
    name: row.name,
    pausedAt: row.paused_at,
    updatedAt: row.updated_at,
  };
  recordQueueMetadataText(queue, row.metadata_text);
  return queue;
}
