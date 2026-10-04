import { ValidationError } from "./errors.js";
import { bytesToHex } from "./internal/hex.js";
import type { JsonObject, JsonValue } from "./json.js";
import { toJsonObject } from "./json.js";

/** Persisted River job states. */
export const JOB_STATE = {
  available: "available",
  cancelled: "cancelled",
  completed: "completed",
  discarded: "discarded",
  pending: "pending",
  retryable: "retryable",
  running: "running",
  scheduled: "scheduled",
} as const;

/** A job's state, one of the {@link JOB_STATE} values. */
export type JobState = (typeof JOB_STATE)[keyof typeof JOB_STATE];

// Retain the established constants while making JOB_STATE the preferred API.

export const MAX_ATTEMPTS_DEFAULT = 25;

export const PRIORITY_DEFAULT = 1;

export const QUEUE_DEFAULT = "default";

/** A failed work attempt persisted with a job. */
export interface AttemptError {
  readonly at: Temporal.Instant;
  readonly attempt: number;
  readonly error: string;
  readonly trace: string;
}

/** Exact properties of a persisted River job. */
export interface JobRow<TArgs extends object = JsonObject> {
  readonly args: TArgs;
  readonly attempt: number;
  readonly attemptedAt: Temporal.Instant | null;
  readonly attemptedBy: readonly string[];
  readonly createdAt: Temporal.Instant;
  readonly errors: readonly AttemptError[];
  readonly finalizedAt: Temporal.Instant | null;
  readonly id: bigint;
  readonly kind: string;
  readonly maxAttempts: number;
  readonly metadata: JsonObject;
  readonly priority: number;
  readonly queue: string;
  readonly scheduledAt: Temporal.Instant;
  readonly state: JobState;
  readonly tags: readonly string[];
  readonly uniqueKey: Uint8Array | null;
  readonly uniqueStates: readonly JobState[] | null;
}

/** JSON-safe form of {@link AttemptError}. */
export interface AttemptErrorJson extends JsonObject {
  at: string;
  attempt: number;
  error: string;
  trace: string;
}

/** JSON-safe form of {@link JobRow}. */
export interface JobRowJson extends JsonObject {
  args: JsonObject;
  attempt: number;
  attemptedAt: string | null;
  attemptedBy: string[];
  createdAt: string;
  errors: AttemptErrorJson[];
  finalizedAt: string | null;
  id: string;
  kind: string;
  maxAttempts: number;
  metadata: JsonObject;
  priority: number;
  queue: string;
  scheduledAt: string;
  state: JobState;
  tags: string[];
  uniqueKey: string | null;
  uniqueStates: JobState[] | null;
}

/** Convert a job to a form that is safe to pass to JSON.stringify. */
export function jobToJsonValue(job: JobRow): JobRowJson {
  return {
    args: toJsonObject(job.args),
    attempt: job.attempt,
    attemptedAt: job.attemptedAt?.toString() ?? null,
    attemptedBy: [...job.attemptedBy],
    createdAt: job.createdAt.toString(),
    errors: job.errors.map((error) => ({
      at: error.at.toString(),
      attempt: error.attempt,
      error: error.error,
      trace: error.trace,
    })),
    finalizedAt: job.finalizedAt?.toString() ?? null,
    id: job.id.toString(10),
    kind: job.kind,
    maxAttempts: job.maxAttempts,
    metadata: toJsonObject(job.metadata),
    priority: job.priority,
    queue: job.queue,
    scheduledAt: job.scheduledAt.toString(),
    state: job.state,
    tags: [...job.tags],
    uniqueKey: job.uniqueKey === null ? null : bytesToHex(job.uniqueKey),
    uniqueStates: job.uniqueStates === null ? null : [...job.uniqueStates],
  };
}

/** Decode a JSON-safe job while restoring exact bigint and Temporal values. */
export function jobFromJsonValue(value: unknown): JobRow {
  const object = toJsonObject(value);
  const attemptedAt = nullableString(object, "attemptedAt");
  const finalizedAt = nullableString(object, "finalizedAt");
  const uniqueKey = nullableString(object, "uniqueKey");

  return {
    args: toJsonObject(object.args),
    attempt: integer(object, "attempt", { min: 0 }),
    attemptedAt: attemptedAt === null ? null : parseInstant(attemptedAt),
    attemptedBy: stringArray(object, "attemptedBy"),
    createdAt: parseInstant(string(object, "createdAt")),
    errors: errors(object.errors),
    finalizedAt: finalizedAt === null ? null : parseInstant(finalizedAt),
    id: decimalBigInt(object, "id"),
    kind: string(object, "kind"),
    maxAttempts: integer(object, "maxAttempts", { min: 0 }),
    metadata: toJsonObject(object.metadata),
    priority: integer(object, "priority", { max: 4, min: 1 }),
    queue: string(object, "queue"),
    scheduledAt: parseInstant(string(object, "scheduledAt")),
    state: jobState(object.state),
    tags: stringArray(object, "tags"),
    uniqueKey: uniqueKey === null ? null : hexToBytes(uniqueKey),
    uniqueStates: nullableJobStateArray(object, "uniqueStates"),
  };
}

function decimalBigInt(object: JsonObject, key: string): bigint {
  const value = string(object, key);
  if (!/^-?(0|[1-9]\d*)$/.test(value)) {
    throw invalidField(key, "must be a canonical decimal integer string");
  }
  return BigInt(value);
}

function errors(value: JsonValue | undefined): AttemptError[] {
  if (!Array.isArray(value)) throw invalidField("errors", "must be an array");
  return value.map((item) => {
    const object = toJsonObject(item);
    return {
      at: parseInstant(string(object, "at")),
      attempt: integer(object, "attempt", { min: 0 }),
      error: string(object, "error"),
      trace: string(object, "trace"),
    };
  });
}

function hexToBytes(value: string): Uint8Array {
  if (value.length % 2 !== 0 || !/^[0-9a-f]*$/.test(value)) {
    throw invalidField("uniqueKey", "must be lowercase hexadecimal");
  }
  return Uint8Array.from(
    value.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? []
  );
}

function integer(
  object: JsonObject,
  key: string,
  range: { max?: number; min?: number }
): number {
  const value = object[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw invalidField(key, "must be a safe integer");
  }
  if (range.min !== undefined && value < range.min) {
    throw invalidField(key, `must be at least ${range.min}`);
  }
  if (range.max !== undefined && value > range.max) {
    throw invalidField(key, `must be at most ${range.max}`);
  }
  return value;
}

function invalidField(key: string, message: string): ValidationError {
  return new ValidationError(`invalid job.${key}: ${message}`, {
    details: { field: key },
  });
}

function jobState(value: JsonValue | undefined): JobState {
  if (
    typeof value !== "string" ||
    !Object.values(JOB_STATE).includes(value as JobState)
  ) {
    throw new ValidationError(
      `unknown River job state: ${JSON.stringify(value)}`
    );
  }
  return value as JobState;
}

function jobStateArray(object: JsonObject, key: string): JobState[] {
  const value = object[key];
  if (!Array.isArray(value)) throw invalidField(key, "must be an array");
  return value.map(jobState);
}

function nullableJobStateArray(
  object: JsonObject,
  key: string
): JobState[] | null {
  return object[key] === null ? null : jobStateArray(object, key);
}

function nullableString(object: JsonObject, key: string): string | null {
  const value = object[key];
  if (value === null) return null;
  if (typeof value !== "string")
    throw invalidField(key, "must be a string or null");
  return value;
}

function parseInstant(value: string): Temporal.Instant {
  try {
    return Temporal.Instant.from(value);
  } catch (cause) {
    throw new ValidationError(
      `invalid Temporal.Instant: ${JSON.stringify(value)}`,
      {
        cause,
      }
    );
  }
}

function string(object: JsonObject, key: string): string {
  const value = object[key];
  if (typeof value !== "string") throw invalidField(key, "must be a string");
  return value;
}

function stringArray(object: JsonObject, key: string): string[] {
  const value = object[key];
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw invalidField(key, "must be an array of strings");
  }
  return value as string[];
}
