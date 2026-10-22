import { parseJson, stringifyJson, toJsonObject } from "riverqueue";
import type { JobRow } from "riverqueue";
import {
  decodeAttemptErrors,
  decodeJobState,
  recordQueueMetadataText,
  uniqueBitmaskToStates,
} from "riverqueue/unstable-driver";
import type { QueueRow } from "riverqueue/unstable-driver";

export type Row = Record<string, unknown>;

// Cast at the SQL boundary so caller pool options cannot truncate IDs, JSON
// numbers, or microseconds, or interpret UTC DATETIME values in local time.
export const JOB_COLUMNS =
  `CAST(id AS CHAR) AS id, args, attempt, attempted_at, attempted_by,
created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue,
scheduled_at, state, tags, unique_key, unique_states`.replace(
    /\b(args|attempted_by|errors|metadata|tags|attempted_at|created_at|finalized_at|scheduled_at)\b/g,
    "CAST($1 AS CHAR) AS $1"
  );
export const QUEUE_COLUMNS = `name, CAST(created_at AS CHAR) AS created_at,
CAST(metadata AS CHAR) AS metadata, CAST(paused_at AS CHAR) AS paused_at, CAST(updated_at AS CHAR) AS updated_at`;

export function timestamp(
  value: Temporal.Instant | null | undefined
): string | null {
  return value == null
    ? null
    : value
        .toString({ fractionalSecondDigits: 6 })
        .replace("T", " ")
        .replace(/Z$/, "");
}

export function instant(value: unknown): Temporal.Instant {
  if (typeof value !== "string")
    throw new TypeError("MySQL timestamp is not text");
  return Temporal.Instant.from(`${value.replace(" ", "T")}Z`);
}

function strings(value: unknown): string[] {
  if (value === null) return [];
  const array = parseJson(text(value));
  if (array === null) return [];
  if (!Array.isArray(array) || array.some((item) => typeof item !== "string"))
    throw new TypeError("Expected a JSON array of strings");
  return array as string[];
}

export function decodeJob(row: Row): JobRow {
  const result = decodeJobPartial(row);
  if (result.error) throw result.error;
  return result.job;
}

export function decodeJobPartial(row: Row): { job: JobRow; error?: Error } {
  const errors: Error[] = [];
  const decode = <T>(fallback: T, callback: () => T): T => {
    try {
      return callback();
    } catch (error) {
      errors.push(error instanceof Error ? error : new Error(String(error)));
      return fallback;
    }
  };
  const job: JobRow = {
    args: decode({}, () => toJsonObject(parseJson(String(row.args)))),
    attempt: Math.max(0, Number(row.attempt)),
    attemptedAt: row.attempted_at === null ? null : instant(row.attempted_at),
    attemptedBy: decode([], () => strings(row.attempted_by)),
    createdAt: instant(row.created_at),
    errors: decode([], () =>
      row.errors === null ? [] : decodeAttemptErrors(text(row.errors))
    ),
    finalizedAt: row.finalized_at === null ? null : instant(row.finalized_at),
    id: BigInt(String(row.id)),
    kind: String(row.kind),
    maxAttempts: Math.max(0, Number(row.max_attempts)),
    metadata: decode({}, () => toJsonObject(parseJson(String(row.metadata)))),
    priority: Number(row.priority),
    queue: String(row.queue),
    scheduledAt: instant(row.scheduled_at),
    state: decodeJobState(String(row.state)),
    tags: decode([], () => strings(row.tags)),
    uniqueKey:
      row.unique_key === null
        ? null
        : new Uint8Array(row.unique_key as Uint8Array),
    uniqueStates:
      row.unique_states === null
        ? null
        : uniqueBitmaskToStates(Number(row.unique_states)),
  };
  return errors.length
    ? { job, error: new AggregateError(errors, "Invalid MySQL job row") }
    : { job };
}

export function decodeQueue(row: Row): QueueRow {
  const queue = {
    createdAt: instant(row.created_at),
    metadata: toJsonObject(parseJson(String(row.metadata))),
    name: String(row.name),
    pausedAt: row.paused_at === null ? null : instant(row.paused_at),
    updatedAt: instant(row.updated_at),
  };
  recordQueueMetadataText(queue, String(row.metadata));
  return queue;
}

export { stringifyJson as json };

export function text(value: unknown): string {
  if (typeof value !== "string")
    throw new TypeError("Expected MySQL text column");
  return value;
}
