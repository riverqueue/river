/**
 * Strict decoders for conformance JSON-RPC parameters, shared by the SQLite
 * and PostgreSQL adapters so both reject malformed harness requests the same
 * way.
 *
 * Optional values treat `undefined` as absent; optional collections and
 * records also accept JSON `null`, which Go encodes for nil slices and maps.
 * Every decoding failure is the contract's `invalid_params` error.
 */
import {
  JOB_STATE,
  parseJsonObject,
  type JobState,
  type JsonObject,
  type QueueUpdateOptions,
} from "riverqueue";
import type { FinalizedJobDeleteParams } from "riverqueue/unstable-driver";

import { invalidParams } from "./errors.js";

/** Every River job state, in declaration order. */
export const ALL_JOB_STATES: readonly JobState[] = Object.freeze(
  Object.values(JOB_STATE)
);

/** Decode an exact integer from a number, bigint, or decimal string. */
function integerToBigInt(value: unknown, name: string): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return BigInt(value);
  }
  if (typeof value === "string" && /^-?(?:0|[1-9]\d*)$/.test(value)) {
    return BigInt(value);
  }
  throw invalidParams(`${name} must be an exact integer`);
}

/**
 * Decode `delete_finalized` params into one batch of the job cleaner's
 * deletion covering every finalized state. A null or absent
 * `queues_included` matches every queue, while an empty list matches none.
 */
export function deleteFinalizedParams(
  params: Record<string, unknown>
): FinalizedJobDeleteParams {
  const before = requiredString(params, "before");
  let instant: Temporal.Instant;
  try {
    instant = Temporal.Instant.from(before);
  } catch {
    throw invalidParams("before must be an RFC 3339 timestamp");
  }
  return {
    cancelledBefore: instant,
    completedBefore: instant,
    discardedBefore: instant,
    limit: requiredInteger(params, "limit", 1, 10_000),
    queuesExcluded: optionalStrings(params, "queues_excluded"),
    queuesIncluded:
      params.queues_included === undefined || params.queues_included === null
        ? null
        : optionalStrings(params, "queues_included"),
  };
}

/** Decode an optional exact integer parameter. */
export function optionalBigInt(
  params: Record<string, unknown>,
  name: string,
  fallback: bigint
): bigint {
  return params[name] === undefined
    ? fallback
    : integerToBigInt(params[name], name);
}

/** Decode an optional array of exact integers, empty when absent. */
export function optionalBigInts(
  params: Record<string, unknown>,
  name: string
): readonly bigint[] {
  const value = params[name];
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invalidParams(`${name} must be an array`);
  return value.map((item, index) => integerToBigInt(item, `${name}[${index}]`));
}

export function optionalBoolean(
  params: Record<string, unknown>,
  name: string,
  fallback: boolean
): boolean {
  const value = params[name];
  if (value === undefined) return fallback;
  if (typeof value !== "boolean") {
    throw invalidParams(`${name} must be a boolean`);
  }
  return value;
}

/** Decode an optional safe integer parameter within inclusive bounds. */
export function optionalInteger(
  params: Record<string, unknown>,
  name: string,
  fallback: number,
  minimum: number,
  maximum: number
): number {
  return optionalIntegerValue(params[name], name, fallback, minimum, maximum);
}

/** Decode an optional safe integer value within inclusive bounds. */
export function optionalIntegerValue(
  value: unknown,
  name: string,
  fallback: number,
  minimum: number,
  maximum: number
): number {
  return value === undefined
    ? fallback
    : requiredIntegerValue(value, name, minimum, maximum);
}

/** Decode an optional array of bounded safe integers, empty when absent. */
export function optionalIntegers(
  params: Record<string, unknown>,
  name: string,
  minimum: number,
  maximum: number
): readonly number[] {
  const value = params[name];
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invalidParams(`${name} must be an array`);
  return value.map((item, index) =>
    requiredIntegerValue(item, `${name}[${index}]`, minimum, maximum)
  );
}

/** Decode an optional object parameter, or null when absent. */
export function optionalRecord(
  params: Record<string, unknown>,
  name: string
): Record<string, unknown> | null {
  const value = params[name];
  if (value === undefined || value === null) return null;
  return requireRecordValue(value, name);
}

/** A queue update's changes: the `metadata` param, or no change without one. */
export function queueUpdateOptions(
  params: Record<string, unknown>
): QueueUpdateOptions {
  const metadata = optionalRecord(params, "metadata");
  return metadata === null ? {} : { metadata: metadata as JsonObject };
}

/** Decode an optional array of River job states. */
export function optionalStates(
  params: Record<string, unknown>,
  name: string,
  fallback: readonly JobState[] | undefined
): readonly JobState[] | undefined {
  const value = params[name];
  if (value === undefined || value === null) return fallback;
  if (!Array.isArray(value)) throw invalidParams(`${name} must be an array`);
  return value.map((state, index) => {
    if (
      typeof state !== "string" ||
      !ALL_JOB_STATES.includes(state as JobState)
    ) {
      throw invalidParams(`${name}[${index}] is not a River job state`);
    }
    return state as JobState;
  });
}

export function optionalString(
  params: Record<string, unknown>,
  name: string,
  fallback: string
): string {
  return optionalStringValue(params[name], name, fallback);
}

/** Validate a raw JSON object without replacing exact numeric tokens. */
export function optionalRawJsonObject(
  params: Record<string, unknown>,
  name: string,
  fallback: string
): string {
  const value = optionalString(params, name, fallback);
  try {
    parseJsonObject(value);
  } catch {
    throw invalidParams(`${name} must be a JSON object`);
  }
  return value;
}

export function optionalStringValue(
  value: unknown,
  name: string,
  fallback: string
): string {
  if (value === undefined) return fallback;
  if (typeof value !== "string") {
    throw invalidParams(`${name} must be a string`);
  }
  return value;
}

/** Decode an optional array of strings, empty when absent. */
export function optionalStrings(
  params: Record<string, unknown>,
  name: string
): readonly string[] {
  const value = params[name];
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invalidParams(`${name} must be an array`);
  return value.map((item, index) => {
    if (typeof item !== "string") {
      throw invalidParams(`${name}[${index}] must be a string`);
    }
    return item;
  });
}

export function requiredBoolean(
  params: Record<string, unknown>,
  name: string
): boolean {
  const value = params[name];
  if (typeof value !== "boolean") {
    throw invalidParams(`${name} must be a boolean`);
  }
  return value;
}

/** Decode the required positive signed 64-bit `id` parameter. */
export function requiredId(params: Record<string, unknown>): bigint {
  const id = requiredSignedBigInt(params, "id");
  if (id < 1n) throw invalidParams("id must be positive");
  return id;
}

/** Decode a required safe integer parameter within inclusive bounds. */
export function requiredInteger(
  params: Record<string, unknown>,
  name: string,
  minimum: number,
  maximum: number
): number {
  return requiredIntegerValue(params[name], name, minimum, maximum);
}

/** Decode a required string parameter that must not be empty. */
export function requiredNonEmptyString(
  params: Record<string, unknown>,
  name: string
): string {
  const value = params[name];
  if (typeof value !== "string" || value.length === 0) {
    throw invalidParams(`${name} must be a non-empty string`);
  }
  return value;
}

/** Decode a required object parameter. */
export function requiredRecord(
  params: Record<string, unknown>,
  name: string
): Record<string, unknown> {
  return requireRecordValue(params[name], name);
}

/** Decode a required exact integer within PostgreSQL's signed `bigint`. */
export function requiredSignedBigInt(
  params: Record<string, unknown>,
  name: string
): bigint {
  const value = integerToBigInt(params[name], name);
  if (
    value < -9_223_372_036_854_775_808n ||
    value > 9_223_372_036_854_775_807n
  ) {
    throw invalidParams(`${name} is outside signed 64-bit range`);
  }
  return value;
}

/** Decode a required string parameter, which may be empty. */
export function requiredString(
  params: Record<string, unknown>,
  name: string
): string {
  const value = params[name];
  if (typeof value !== "string") {
    throw invalidParams(`missing string parameter ${name}`);
  }
  return value;
}

/** Decode a required exact integer within the unsigned 64-bit range. */
export function requiredUnsignedBigInt(
  params: Record<string, unknown>,
  name: string
): bigint {
  const value = integerToBigInt(params[name], name);
  if (value < 0n || value > 18_446_744_073_709_551_615n) {
    throw invalidParams(`${name} is outside unsigned 64-bit range`);
  }
  return value;
}

/** Require a plain JSON object (not null or an array). */
export function requireRecordValue(
  value: unknown,
  name: string
): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw invalidParams(`${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requiredIntegerValue(
  value: unknown,
  name: string,
  minimum: number,
  maximum: number
): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw invalidParams(
      `${name} must be an integer between ${minimum} and ${maximum}`
    );
  }
  return value;
}
