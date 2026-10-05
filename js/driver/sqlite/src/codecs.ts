import {
  isExactJsonNumber,
  parseJson,
  RiverError,
  stringifyJson,
} from "riverqueue";

import {
  decodeAttemptErrors,
  recordQueueMetadataText,
} from "riverqueue/unstable-driver";

import { invalidInputError, invalidRowError } from "./errors.js";
import { SQLITE_JOB_STATE } from "./types.js";
import type {
  SqliteAttemptError,
  SqliteJobRow,
  SqliteJobState,
  SqliteJsonObject,
  SqliteJsonValue,
  SqliteQueueRow,
} from "./types.js";

const INT64_MAX = 9_223_372_036_854_775_807n;
const INT64_MIN = -9_223_372_036_854_775_808n;
const JOB_STATES = Object.values(SQLITE_JOB_STATE);
const MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * SQL that is true when a JSON column holds text that isn't valid JSON, which
 * another tool can write and which makes SQLite's JSON functions fail with
 * "malformed JSON". River writes these columns as JSONB blobs, so only text
 * values are checked, as Go's driver does.
 */
export function invalidJsonTextSql(column: string): string {
  return `(typeof(${column}) = 'text' AND NOT json_valid(${column}))`;
}

/** Read a JSON column as text, returning text that isn't valid JSON as is. */
function tolerantJsonColumn(column: string): string {
  return `CASE WHEN ${invalidJsonTextSql(column)} THEN ${column} ELSE json(${column}) END`;
}

/**
 * Every `river_job` column. JSON columns holding text that isn't valid JSON
 * are returned as is, so the row can be reported as undecodable instead of
 * failing the whole query, as in Go's driver.
 */
export const JOB_COLUMNS = `
  id,
  attempt,
  attempted_at,
  ${tolerantJsonColumn("attempted_by")} AS attempted_by,
  created_at,
  ${tolerantJsonColumn("args")} AS encoded_args,
  ${tolerantJsonColumn("errors")} AS errors,
  finalized_at,
  kind,
  max_attempts,
  ${tolerantJsonColumn("metadata")} AS metadata,
  priority,
  queue,
  scheduled_at,
  state,
  ${tolerantJsonColumn("tags")} AS tags,
  unique_key,
  unique_states
`;

export const QUEUE_COLUMNS = `
  created_at,
  json(metadata) AS metadata,
  name,
  paused_at,
  updated_at
`;

/** Encode an instant in River's exact-millisecond SQLite representation. */
export function sqliteTimestamp(value: Temporal.Instant): string {
  try {
    const iso = value
      .round({ roundingMode: "halfExpand", smallestUnit: "millisecond" })
      .toString({ fractionalSecondDigits: 3 });
    return iso.replace("T", " ").replace(/Z$/, "");
  } catch (cause: unknown) {
    throw invalidInput("timestamp", "expected a Temporal.Instant", cause);
  }
}

export function sqliteTimestampOrNull(
  value: Temporal.Instant | null | undefined
): string | null {
  return value === null || value === undefined ? null : sqliteTimestamp(value);
}

/** Parse a River SQLite timestamp without passing through lossy Date. */
export function parseSqliteTimestamp(
  value: unknown,
  field: string
): Temporal.Instant {
  if (typeof value !== "string") {
    throw invalidRow(field, "timestamp is not text");
  }
  const normalized = /(?:Z|[+-]\d\d:\d\d)$/.test(value)
    ? value.replace(" ", "T")
    : `${value.replace(" ", "T")}Z`;
  try {
    return Temporal.Instant.from(normalized);
  } catch (cause: unknown) {
    throw invalidRow(field, "timestamp is invalid", cause);
  }
}

/** Encode JSON River writes. */
export function encodeJson(value: unknown, field: string): string {
  try {
    return stringifyJson(value);
  } catch (cause: unknown) {
    if (cause instanceof RiverError) throw cause;
    throw invalidInput(field, "value is not River JSON", cause);
  }
}

/**
 * Check a caller's already encoded JSON for storage. Like Go's driver, River
 * stores the text as given rather than re-encoding it.
 */
export function encodeEncodedJson(text: string, field: string): string {
  if (typeof text !== "string") {
    throw invalidInput(field, "encoded JSON must be a string");
  }
  try {
    parseJson(text);
  } catch (cause: unknown) {
    throw invalidInput(field, "value is not encoded JSON", cause);
  }
  return text;
}

function decodeJsonObject(value: unknown, field: string): SqliteJsonObject {
  const decoded = decodeJson(value, field);
  if (
    decoded === null ||
    Array.isArray(decoded) ||
    typeof decoded !== "object" ||
    isExactJsonNumber(decoded)
  ) {
    throw invalidRow(field, "JSON value is not an object");
  }
  return decoded;
}

/**
 * Decode one `river_job` row exactly as leniently as Go's `riversqlite` does.
 *
 * SQLite columns are wider than PostgreSQL's, so another engine may persist
 * values River's PostgreSQL schema would reject: `attempt` and `max_attempts`
 * are unbounded integers, and JSON `null` is accepted for `tags`,
 * `attempted_by`, and `errors` (Go decodes it as an empty list). Negative counts
 * clamp to zero like Go, and counts beyond `Number.MAX_SAFE_INTEGER` saturate,
 * which preserves their "effectively unlimited" meaning without losing
 * precision silently in arithmetic.
 */
export function decodeJobRow(raw: Record<string, unknown>): SqliteJobRow {
  const { error, job } = decodeJobRowPartial(raw);
  if (error !== undefined) throw error;
  return job;
}

/**
 * Decode a `river_job` row like {@link decodeJobRow}, except that an `args`,
 * `attempted_by`, `errors`, `metadata`, `tags`, or `unique_states` value that
 * can't be decoded is left empty and the decode error is returned alongside,
 * as Go's driver does, so one bad row can't fail a claim, a completion batch,
 * or the rescuer. Other fields still throw.
 */
export function decodeJobRowPartial(raw: Record<string, unknown>): {
  readonly error?: Error;
  readonly job: SqliteJobRow;
} {
  const failures: Error[] = [];
  const partial = <T>(empty: T, decoder: () => T): T => {
    try {
      return decoder();
    } catch (cause: unknown) {
      failures.push(
        cause instanceof Error ? cause : invalidRow("job", String(cause))
      );
      return empty;
    }
  };
  let job: SqliteJobRow;
  try {
    job = {
      args: partial({}, () => decodeJsonObject(raw.encoded_args, "args")),
      attempt: count(raw.attempt, "attempt"),
      attemptedAt: nullableTimestamp(raw.attempted_at, "attempted_at"),
      attemptedBy: partial([], () =>
        nullableStringArray(raw.attempted_by, "attempted_by")
      ),
      createdAt: parseSqliteTimestamp(raw.created_at, "created_at"),
      errors: partial([], () => attemptErrors(raw.errors)),
      finalizedAt: nullableTimestamp(raw.finalized_at, "finalized_at"),
      id: int64(raw.id, "id"),
      kind: requiredString(raw.kind, "kind"),
      maxAttempts: count(raw.max_attempts, "max_attempts"),
      metadata: partial({}, () => decodeJsonObject(raw.metadata, "metadata")),
      priority: smallInteger(raw.priority, "priority", 1, 4),
      queue: requiredString(raw.queue, "queue"),
      scheduledAt: parseSqliteTimestamp(raw.scheduled_at, "scheduled_at"),
      state: jobState(raw.state, "state"),
      tags: partial([], () => nullableStringArray(raw.tags, "tags")),
      uniqueKey: nullableBytes(raw.unique_key, "unique_key"),
      uniqueStates: partial(null, () => decodeUniqueStates(raw.unique_states)),
    };
  } catch (cause: unknown) {
    if (cause instanceof RiverError) throw cause;
    throw invalidRow("job", "could not decode row", cause);
  }
  if (failures.length === 0) return { job };
  const [first] = failures;
  return {
    error:
      failures.length === 1 && first !== undefined
        ? first
        : new AggregateError(
            failures,
            failures.map((failure) => failure.message).join("; ")
          ),
    job,
  };
}

export function decodeQueueRow(raw: Record<string, unknown>): SqliteQueueRow {
  try {
    const row: SqliteQueueRow = {
      createdAt: parseSqliteTimestamp(raw.created_at, "created_at"),
      metadata: decodeJsonObject(raw.metadata, "metadata"),
      name: requiredString(raw.name, "name"),
      pausedAt: nullableTimestamp(raw.paused_at, "paused_at"),
      updatedAt: parseSqliteTimestamp(raw.updated_at, "updated_at"),
    };
    // `json(metadata)`, the stored text with its number literals as written.
    recordQueueMetadataText(row, requiredString(raw.metadata, "metadata"));
    return row;
  } catch (cause: unknown) {
    if (cause instanceof RiverError) throw cause;
    throw invalidRow("queue", "could not decode row", cause);
  }
}

/**
 * Encode unique states as Go does: an empty set is stored as `NULL`, which
 * never participates in the partial unique index.
 */
export function encodeUniqueStates(
  states: readonly SqliteJobState[] | null | undefined
): bigint | null {
  if (states === null || states === undefined) return null;
  let bits = 0n;
  for (const state of states) {
    const index = JOB_STATES.indexOf(state);
    if (index < 0) throw invalidInput("uniqueStates", `unknown state ${state}`);
    bits |= 1n << BigInt(index);
  }
  return bits === 0n ? null : bits;
}

export function validateInt64(value: bigint, field: string): bigint {
  if (value < INT64_MIN || value > INT64_MAX) {
    throw invalidInput(
      field,
      "integer is outside SQLite's signed 64-bit range"
    );
  }
  return value;
}

export function validateSmallInteger(
  value: number,
  field: string,
  minimum: number,
  maximum: number
): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw invalidInput(
      field,
      `expected an integer in the range ${minimum}..=${maximum}`
    );
  }
  return value;
}

/**
 * Check a name that only looks up existing rows. Like River for Go, any
 * string is accepted, and a name no row can have is simply not found.
 */
export function validateLookupName(value: string, field: string): string {
  if (typeof value !== "string") throw invalidInput(field, "must be a string");
  return value;
}

export function validateName(value: string, field: string): string {
  validateUnicode(value, field, invalidInput);
  if (value.length === 0 || value.length >= 128) {
    throw invalidInput(field, "must contain between 1 and 127 characters");
  }
  return value;
}

function attemptErrors(value: unknown): readonly SqliteAttemptError[] {
  if (value === null) return [];
  if (typeof value !== "string")
    throw invalidRow("errors", "JSON projection is not text");
  // Each element decodes leniently, as River for Go's drivers decode it.
  try {
    return decodeAttemptErrors(value);
  } catch (cause: unknown) {
    throw invalidRow(
      "errors",
      cause instanceof TypeError ? cause.message : "contains invalid JSON",
      cause
    );
  }
}

/** Clamp a persisted count to Go's `max(n, 0)` and JavaScript's safe range. */
function count(value: unknown, field: string): number {
  return saturate(int64(value, field));
}

function decodeJson(value: unknown, field: string): SqliteJsonValue {
  if (typeof value !== "string")
    throw invalidRow(field, "JSON projection is not text");
  try {
    return parseJson(value);
  } catch (cause: unknown) {
    throw invalidRow(
      field,
      cause instanceof RiverError ? cause.message : "contains invalid JSON",
      cause
    );
  }
}

function decodeUniqueStates(value: unknown): readonly SqliteJobState[] | null {
  if (value === null) return null;
  const bits = int64(value, "unique_states");
  if (bits < 0n || bits > 255n) {
    throw invalidRow("unique_states", "bit mask is outside 0..=255");
  }
  return JOB_STATES.filter((_, index) => (bits & (1n << BigInt(index))) !== 0n);
}

function int64(value: unknown, field: string): bigint {
  if (typeof value !== "bigint") {
    throw invalidRow(field, "integer was not decoded as bigint");
  }
  if (value < INT64_MIN || value > INT64_MAX) {
    throw invalidRow(field, "integer is outside signed 64-bit range");
  }
  return value;
}

function jobState(value: unknown, field: string): SqliteJobState {
  if (
    typeof value !== "string" ||
    !JOB_STATES.includes(value as SqliteJobState)
  ) {
    throw invalidRow(field, `unknown River state ${JSON.stringify(value)}`);
  }
  return value as SqliteJobState;
}

export function nullableBytes(
  value: unknown,
  field: string
): Uint8Array | null {
  if (value === null) return null;
  // Go scans a TEXT value into []byte as its UTF-8 bytes.
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (!ArrayBuffer.isView(value) || value instanceof DataView) {
    throw invalidRow(field, "BLOB is not a byte array");
  }
  return Uint8Array.from(
    new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  );
}

function nullableTimestamp(
  value: unknown,
  field: string
): Temporal.Instant | null {
  return value === null ? null : parseSqliteTimestamp(value, field);
}

function requiredString(
  value: unknown,
  field: string,
  emptyAllowed = false
): string {
  if (typeof value !== "string" || (!emptyAllowed && value.length === 0)) {
    throw invalidRow(field, "value is not valid text");
  }
  validateUnicode(value, field, invalidRow);
  return value;
}

function smallInteger(
  value: unknown,
  field: string,
  minimum: number,
  maximum: number
): number {
  const integer = int64(value, field);
  if (integer < BigInt(minimum) || integer > BigInt(maximum)) {
    throw invalidRow(field, `integer is outside ${minimum}..=${maximum}`);
  }
  return Number(integer);
}

function nullableStringArray(value: unknown, field: string): readonly string[] {
  // Go decodes both SQL `NULL` and JSON `null` as an empty slice, and a `null`
  // element as an empty string.
  const decoded = value === null ? null : decodeJson(value, field);
  if (decoded === null) return [];
  if (!Array.isArray(decoded)) {
    throw invalidRow(field, "JSON is not an array of strings");
  }
  return decoded.map((item, index) => {
    if (item === null) return "";
    if (typeof item !== "string") {
      throw invalidRow(`${field}[${index}]`, "JSON value is not a string");
    }
    return item;
  });
}

function saturate(value: bigint): number {
  if (value < 0n) return 0;
  if (value > MAX_SAFE_INTEGER) return Number.MAX_SAFE_INTEGER;
  return Number(value);
}

function validateUnicode(
  value: string,
  field: string,
  error: (field: string, message: string, cause?: unknown) => RiverError
): void {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw error(field, "string contains an unpaired surrogate");
      }
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw error(field, "string contains an unpaired surrogate");
    }
  }
}

function invalidInput(
  field: string,
  message: string,
  cause?: unknown
): RiverError {
  return invalidInputError(
    "encode",
    `invalid SQLite River input ${field}: ${message}`,
    cause
  );
}

function invalidRow(
  field: string,
  message: string,
  cause?: unknown
): RiverError {
  return invalidRowError(
    "decode",
    `invalid SQLite River row ${field}: ${message}`,
    cause
  );
}
