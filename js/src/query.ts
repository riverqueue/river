import type {
  JobListAfter,
  JobListCursorValue,
  JobListKeyset,
  JobListOrderBy,
  JobListParams,
  JobListTimeField,
  JobDeleteManyParams,
  JobUpdateParams as DriverJobUpdateParams,
  QueueListParams,
  QueueRow,
  SortDirection,
} from "./driver.js";
import { ValidationError } from "./errors.js";
import { JOB_STATE } from "./job.js";
import type { JobRow, JobState } from "./job.js";
import type { JsonObject, JsonValue } from "./json.js";
import { stringifyGoJsonString, toJsonObject, toJsonValue } from "./json.js";

const ALL_STATES = Object.freeze(Object.values(JOB_STATE));
const MAX_CURSOR_BYTES = 16 * 1024;
const MAX_INT64 = 9_223_372_036_854_775_807n;
const MIN_INT64 = -9_223_372_036_854_775_808n;
// Go's zero time, which a job list cursor carries when it has no time.
const GO_ZERO_TIME = "0001-01-01T00:00:00Z";
const GO_ZERO_TIME_NS = Temporal.Instant.from(GO_ZERO_TIME).epochNanoseconds;
const JOB_LIST_SORT_FIELDS: Readonly<Record<JobListOrderBy, string>> =
  Object.freeze({
    finalizedAt: "finalized_at",
    id: "id",
    scheduledAt: "scheduled_at",
    time: "time",
  });
const STRICT_UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export type { QueueRow } from "./driver.js";

/** Filters and exact keyset pagination for {@link JobOperations.list | `client.jobs.list`}. */
export interface JobListOptions {
  readonly after?: string;
  readonly ids?: readonly bigint[];
  readonly kinds?: readonly string[];
  readonly limit?: number;
  /** Require stored metadata to contain this JSON object. */
  readonly metadata?: JsonObject;
  readonly orderBy?: JobListOrderBy;
  readonly priorities?: readonly number[];
  readonly queues?: readonly string[];
  readonly sortDirection?: SortDirection;
  readonly states?: readonly JobState[];
  readonly tagsAll?: readonly string[];
  readonly tagsAny?: readonly string[];
}

/**
 * One page of jobs. Pass `nextCursor` as `after` for the next page; it is
 * `null` on the last page. The cursor is River for Go's `JobListCursor`
 * text, so River for Go and Rust can continue a listing from it and vice
 * versa.
 */
export interface JobListResult {
  readonly jobs: readonly JobRow[];
  readonly nextCursor: string | null;
}

/** Safe filters for {@link JobOperations.deleteMany | `client.jobs.deleteMany`}. */
export interface JobDeleteManyOptions {
  /** Authorize an unfiltered deletion. Cannot be combined with filters. */
  readonly all?: true;
  readonly ids?: readonly bigint[];
  readonly kinds?: readonly string[];
  readonly limit?: number;
  readonly priorities?: readonly number[];
  readonly queues?: readonly string[];
  readonly states?: readonly JobState[];
}

/**
 * Changes accepted by {@link JobOperations.update | `client.jobs.update`},
 * like River for Go's `JobUpdateParams`. Omitted fields leave the job
 * unchanged.
 */
export interface JobUpdateOptions {
  /** Merge these top-level keys into the job's metadata. */
  readonly metadata?: JsonObject;
  /** Set the job's output, stored at `metadata.output`. */
  readonly output?: JsonValue;
}

export function normalizeJobDeleteManyOptions(
  options: JobDeleteManyOptions
): JobDeleteManyParams {
  rejectExplicitUndefined(options);
  const limit = options.limit ?? 100;
  requireInteger("deleteMany limit", limit, 1, 10_000);
  const ids = Object.freeze([...(options.ids ?? [])]);
  for (const id of ids) {
    if (typeof id !== "bigint" || id <= 0n) {
      throw new ValidationError("deleteMany ids must be positive bigints");
    }
  }
  const priorities = Object.freeze([...(options.priorities ?? [])]);
  for (const priority of priorities) {
    requireInteger("deleteMany priority", priority, 1, 4);
  }
  const states = Object.freeze([...(options.states ?? [])]);
  for (const state of states) {
    if (!ALL_STATES.includes(state)) {
      throw new ValidationError(`unknown River job state: ${state}`);
    }
  }
  const kinds = copyStrings("kinds", options.kinds);
  const queues = copyStrings("queues", options.queues);
  const hasFilter =
    ids.length > 0 ||
    kinds.length > 0 ||
    priorities.length > 0 ||
    queues.length > 0 ||
    states.length > 0;
  if (options.all === true && hasFilter) {
    throw new ValidationError("deleteMany all cannot be combined with filters");
  }
  if (options.all !== true && !hasFilter) {
    throw new ValidationError("deleteMany requires a filter or all: true");
  }
  return {
    all: options.all === true,
    ids,
    kinds,
    limit,
    priorities,
    queues,
    states,
  };
}

/** Pagination for listing queues: `after` is a previous page's `nextCursor`. */
export interface QueueListOptions {
  readonly after?: string;
  readonly limit?: number;
}

/** One page of queues; `nextCursor` is `null` on the last page. */
export interface QueueListResult {
  readonly nextCursor: string | null;
  readonly queues: readonly QueueRow[];
}

/** Changes to a queue's persisted settings. Omitted fields are unchanged. */
export interface QueueUpdateOptions {
  readonly metadata?: JsonObject;
}

export function normalizeJobListOptions(
  options: JobListOptions = {}
): JobListParams {
  rejectExplicitUndefined(options);
  const limit = options.limit ?? 100;
  requireInteger("list limit", limit, 1, 10_000);
  const sortField = options.orderBy ?? "id";
  if (!["finalizedAt", "id", "scheduledAt", "time"].includes(sortField)) {
    throw new ValidationError("invalid list orderBy");
  }
  const sortDirection = options.sortDirection ?? "asc";
  if (!["asc", "desc"].includes(sortDirection)) {
    throw new ValidationError("invalid list sortDirection");
  }

  const states = [...(options.states ?? ALL_STATES)];
  for (const state of states) {
    if (!ALL_STATES.includes(state)) {
      throw new ValidationError(`unknown River job state: ${state}`);
    }
  }
  if (
    sortField === "finalizedAt" &&
    states.some((state) =>
      ["available", "pending", "retryable", "running", "scheduled"].includes(
        state
      )
    )
  ) {
    throw new ValidationError(
      "finalizedAt ordering requires only finalized job states"
    );
  }

  const after =
    options.after === undefined ? null : decodeJobListCursor(options.after);
  if (after !== null && after.sortField !== sortField) {
    throw new ValidationError("cursor order does not match list orderBy");
  }

  return {
    after,
    ids: Object.freeze([...(options.ids ?? [])]),
    kinds: copyStrings("kinds", options.kinds),
    limit,
    metadata:
      options.metadata === undefined ? null : toJsonObject(options.metadata),
    priorities: Object.freeze([...(options.priorities ?? [])]),
    queues: copyStrings("queues", options.queues),
    sortDirection,
    sortField,
    states: Object.freeze(states),
    tagsAll: copyStrings("tagsAll", options.tagsAll),
    tagsAny: copyStrings("tagsAny", options.tagsAny),
  };
}

export function normalizeJobUpdateOptions(
  options: JobUpdateOptions
): DriverJobUpdateParams {
  rejectExplicitUndefined(options);
  rejectUnknownKeys("job update", options, ["metadata", "output"]);
  return {
    ...(options.metadata === undefined
      ? {}
      : { metadata: toJsonObject(options.metadata) }),
    ...(options.output === undefined
      ? {}
      : { output: normalizeOutput(options.output) }),
  };
}

export function normalizeQueueListOptions(
  options: QueueListOptions = {}
): QueueListParams {
  rejectExplicitUndefined(options);
  const limit = options.limit ?? 100;
  requireInteger("queue list limit", limit, 1, 10_000);
  return {
    limit,
    nameAfter:
      options.after === undefined ? null : decodeQueueCursor(options.after),
  };
}

/**
 * Encode the cursor after `job` in a list with `params`' ordering exactly as
 * River for Go's `JobListCursor` marshals it, so a token from any River
 * implementation can continue a listing in any other.
 */
export function encodeJobListCursor(
  job: JobRow,
  params: Pick<JobListParams, "sortField" | "states">
): string {
  return encodeJobListCursorValue(jobListCursorValue(job, params));
}

/**
 * The cursor value after `job` in a list with `params`' ordering. Like River
 * for Go, its time is the job's value of the field the list is ordered by,
 * which for `time` ordering over several states can differ from the field of
 * the job's own state, and `null` when that field is null for the job.
 */
export function jobListCursorValue(
  job: JobRow,
  params: Pick<JobListParams, "sortField" | "states">
): JobListCursorValue {
  const timeField = jobListTimeField(params);
  let time: Temporal.Instant | null = null;
  if (timeField !== null) {
    time = jobTimeFieldValue(job, timeField);
    if (time === null && !jobListTimeFieldNullable(timeField, params.states)) {
      throw new ValidationError(
        `cannot create a ${params.sortField} cursor from a job without ${timeField}`
      );
    }
  }
  return {
    id: job.id,
    kind: job.kind,
    queue: job.queue,
    sortField: params.sortField,
    time,
  };
}

/**
 * How a job list with `params` is ordered and where it resumes. Every
 * backend renders it with {@link jobListKeysetSql}.
 */
export function jobListKeyset(
  params: Pick<
    JobListParams,
    "after" | "sortDirection" | "sortField" | "states"
  >
): JobListKeyset {
  const timeField = jobListTimeField(params);
  const nullable =
    timeField !== null && jobListTimeFieldNullable(timeField, params.states);
  let after: JobListAfter | null = null;
  if (params.after !== null) {
    const { id, time } = params.after;
    after =
      timeField === null
        ? { id, kind: "id" }
        : time !== null
          ? { id, kind: "time", time }
          : // Like Go, a cursor without a time for a field that can't be
            // null resumes by ID.
            { id, kind: nullable ? "nullTime" : "id" };
  }
  return { after, direction: params.sortDirection, nullable, timeField };
}

/**
 * Render `keyset` as SQL: the condition selecting rows after its cursor, or
 * `null` without one, and the `ORDER BY` terms. `bind` returns the
 * placeholder of each parameter, in the order they appear in the condition.
 */
export function jobListKeysetSql(
  keyset: JobListKeyset,
  bind: (value: bigint | Temporal.Instant) => string
): { readonly after: string | null; readonly orderBy: string } {
  const ascending = keyset.direction === "asc";
  const direction = ascending ? "ASC" : "DESC";
  const comparison = ascending ? ">" : "<";
  const field = keyset.timeField;
  let orderBy = `id ${direction}`;
  if (field !== null) {
    const nulls = keyset.nullable
      ? ascending
        ? " NULLS LAST"
        : " NULLS FIRST"
      : "";
    orderBy = `${field} ${direction}${nulls}, ${orderBy}`;
  }
  const after = keyset.after;
  if (after === null) return { after: null, orderBy };
  if (field === null || after.kind === "id") {
    return { after: `id ${comparison} ${bind(after.id)}`, orderBy };
  }
  if (after.kind === "nullTime") {
    // After a null time, only nulls with a later ID follow ascending, and
    // every non-null time also follows descending.
    return {
      after: ascending
        ? `(${field} IS NULL AND id > ${bind(after.id)})`
        : `(${field} IS NOT NULL OR id < ${bind(after.id)})`,
      orderBy,
    };
  }
  // Nulls follow every time ascending and precede every time descending.
  const time = bind(after.time);
  const sameTime = bind(after.time);
  const id = bind(after.id);
  const orNull = keyset.nullable && ascending ? ` OR ${field} IS NULL` : "";
  return {
    after:
      `(${field} ${comparison} ${time} OR ` +
      `(${field} = ${sameTime} AND id ${comparison} ${id})${orNull})`,
    orderBy,
  };
}

/**
 * The time field a list with `params` is ordered by before ID, or `null`
 * for ID ordering. `time` ordering uses the first listed state's field, and
 * `scheduled_at` without a state filter, like Go, whose default states
 * start with `available`.
 */
function jobListTimeField(
  params: Pick<JobListParams, "sortField" | "states">
): JobListTimeField | null {
  switch (params.sortField) {
    case "id":
      return null;
    case "finalizedAt":
      return "finalized_at";
    case "scheduledAt":
      return "scheduled_at";
    case "time": {
      const first = params.states[0];
      if (first === "running") return "attempted_at";
      if (
        first === "cancelled" ||
        first === "completed" ||
        first === "discarded"
      ) {
        return "finalized_at";
      }
      return "scheduled_at";
    }
  }
}

/**
 * Encode a cursor value in River for Go's `JobListCursor` text format:
 * padded URL-safe base64 of Go's JSON encoding of `id`, `kind`, `queue`,
 * `sort_field`, and `time`. A cursor without a time carries Go's zero time.
 */
export function encodeJobListCursorValue(value: JobListCursorValue): string {
  const time =
    value.sortField === "id" || value.time === null
      ? GO_ZERO_TIME
      : goCursorTime(value.time);
  const json =
    `{"id":${value.id.toString(10)}` +
    `,"kind":${stringifyGoJsonString(value.kind)}` +
    `,"queue":${stringifyGoJsonString(value.queue)}` +
    `,"sort_field":"${JOB_LIST_SORT_FIELDS[value.sortField]}"` +
    `,"time":"${time}"}`;
  const bytes = Buffer.from(json, "utf8");
  return bytes
    .toString("base64url")
    .padEnd(Math.ceil(bytes.length / 3) * 4, "=");
}

export function encodeQueueCursor(queue: QueueRow): string {
  return encodeOpaque({ name: queue.name, v: 1 });
}

/**
 * Decode a job list cursor from any River implementation. Like River for
 * Rust, this accepts the URL-safe or standard base64 alphabet, with or
 * without padding, so tokens from every River for Go release decode. Fields
 * other than Go's five are ignored. A zero time, as Go writes for ID
 * ordering, decodes to no time.
 */
export function decodeJobListCursor(cursor: string): JobListCursorValue {
  let value: unknown;
  try {
    value = parseWithSource(
      STRICT_UTF8.decode(decodeCursorBase64(cursor)),
      (key, parsed, context) =>
        // Go reads `id` as an int64, which JavaScript numbers can't hold.
        key === "id" && typeof parsed === "number"
          ? /^-?\d+$/.test(context.source)
            ? BigInt(context.source)
            : Number.NaN
          : parsed
    );
  } catch (cause) {
    throw new ValidationError("invalid job list cursor", { cause });
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ValidationError("invalid job list cursor");
  }
  const fields = value as Record<string, unknown>;
  const sortField = (
    Object.keys(JOB_LIST_SORT_FIELDS) as JobListOrderBy[]
  ).find((field) => JOB_LIST_SORT_FIELDS[field] === fields.sort_field);
  if (
    typeof fields.id !== "bigint" ||
    fields.id < MIN_INT64 ||
    fields.id > MAX_INT64 ||
    typeof fields.kind !== "string" ||
    typeof fields.queue !== "string" ||
    sortField === undefined ||
    typeof fields.time !== "string" ||
    // Go parses only four-digit years.
    !/^\d{4}-/.test(fields.time)
  ) {
    throw new ValidationError("invalid job list cursor");
  }
  let time: Temporal.Instant;
  try {
    time = Temporal.Instant.from(fields.time);
  } catch (cause) {
    throw new ValidationError("invalid job list cursor", { cause });
  }
  return {
    id: fields.id,
    kind: fields.kind,
    queue: fields.queue,
    sortField,
    time:
      sortField === "id" || time.epochNanoseconds === GO_ZERO_TIME_NS
        ? null
        : time,
  };
}

function decodeQueueCursor(cursor: string): string {
  const value = decodeOpaque(cursor);
  if (
    Object.keys(value).sort().join("\0") !== ["name", "v"].join("\0") ||
    value.v !== 1 ||
    typeof value.name !== "string" ||
    value.name.length === 0
  ) {
    throw new ValidationError("invalid queue list cursor");
  }
  return value.name;
}

/** Whether `field` may be null for jobs in `states` (every state if empty). */
function jobListTimeFieldNullable(
  field: JobListTimeField,
  states: readonly JobState[]
): boolean {
  switch (field) {
    case "attempted_at":
      return true;
    case "finalized_at":
      // The schema requires `finalized_at` for exactly the finalized states,
      // and no filter here can widen the state filter.
      return (
        states.length === 0 ||
        states.some(
          (state) =>
            state !== "cancelled" &&
            state !== "completed" &&
            state !== "discarded"
        )
      );
    case "scheduled_at":
      return false;
  }
}

function jobTimeFieldValue(
  job: JobRow,
  field: JobListTimeField
): Temporal.Instant | null {
  switch (field) {
    case "attempted_at":
      return job.attemptedAt;
    case "finalized_at":
      return job.finalizedAt;
    case "scheduled_at":
      return job.scheduledAt;
  }
}

/**
 * Format a time as Go's `time.Time` JSON does in UTC: RFC 3339 with
 * trailing fractional zeros removed. Go can't encode years outside 0–9999.
 */
function goCursorTime(time: Temporal.Instant): string {
  const text = time.toString();
  if (!/^\d{4}-/.test(text)) {
    throw new ValidationError(
      "job list cursor time must be within years 0000 through 9999"
    );
  }
  return text;
}

/** Base64 in either alphabet, with or without padding. */
function decodeCursorBase64(cursor: string): Buffer {
  if (
    typeof cursor !== "string" ||
    cursor.length === 0 ||
    cursor.length > MAX_CURSOR_BYTES
  ) {
    throw new TypeError("cursor is empty or too long");
  }
  const unpadded = cursor.replace(/={1,2}$/, "");
  if (
    (unpadded.length !== cursor.length && cursor.length % 4 !== 0) ||
    unpadded.length % 4 === 1 ||
    !(/^[A-Za-z0-9_-]*$/.test(unpadded) || /^[A-Za-z0-9+/]*$/.test(unpadded))
  ) {
    throw new TypeError("cursor is not base64");
  }
  // Node's base64 decoder reads both alphabets.
  return Buffer.from(unpadded, "base64");
}

function copyStrings(
  name: string,
  values: readonly string[] | undefined
): readonly string[] {
  const result = [...(values ?? [])];
  if (result.some((value) => typeof value !== "string" || value.length === 0)) {
    throw new ValidationError(`${name} must contain non-empty strings`);
  }
  return Object.freeze(result);
}

function normalizeOutput(value: JsonValue): JsonValue {
  const output = toJsonValue(value);
  if (Buffer.byteLength(JSON.stringify(output), "utf8") > 32 * 1024 * 1024) {
    throw new ValidationError("job output must not exceed 32 MiB");
  }
  return output;
}

/** `JSON.parse` with the source text of each primitive (ES2026). */
const parseWithSource = JSON.parse as (
  text: string,
  reviver: (
    key: string,
    value: unknown,
    context: { readonly source: string }
  ) => unknown
) => unknown;

function decodeOpaque(cursor: string): Record<string, unknown> {
  try {
    if (
      cursor.length === 0 ||
      cursor.length > MAX_CURSOR_BYTES ||
      !/^[A-Za-z0-9_-]+$/.test(cursor)
    ) {
      throw new TypeError("cursor is not bounded unpadded URL-safe base64");
    }
    const decoded = Buffer.from(cursor, "base64url");
    if (decoded.toString("base64url") !== cursor) {
      throw new TypeError("cursor is not canonical URL-safe base64");
    }
    // Reject rather than silently replace invalid UTF-8 in a tampered cursor.
    const value: unknown = JSON.parse(STRICT_UTF8.decode(decoded));
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError("cursor payload is not an object");
    }
    return value as Record<string, unknown>;
  } catch (cause) {
    throw new ValidationError("invalid pagination cursor", { cause });
  }
}

function encodeOpaque(value: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function rejectExplicitUndefined(value: object): void {
  for (const [key, item] of Object.entries(value)) {
    if (item === undefined) {
      throw new ValidationError(`${key} must be omitted instead of undefined`);
    }
  }
}

function rejectUnknownKeys(
  scope: string,
  value: object,
  known: readonly string[]
): void {
  for (const key of Object.keys(value)) {
    if (!known.includes(key)) {
      throw new ValidationError(
        `${scope} ${key} is not an option; expected one of ${known.join(", ")}`
      );
    }
  }
}

function requireInteger(
  name: string,
  value: number,
  min: number,
  max: number
): void {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new ValidationError(
      `${name} must be a safe integer between ${min} and ${max}`
    );
  }
}
