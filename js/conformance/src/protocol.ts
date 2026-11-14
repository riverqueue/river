// The conformance contract's wire types and errors, mirroring the Go
// definitions in conformance/protocol, which are the contract.

import {
  exactJsonNumber,
  isExactJsonNumber,
  jsonNumberToBigInt,
  type JobRow,
  type JsonObject,
  type JsonValue,
} from "riverqueue";

/** JSON-RPC 2.0's error codes, and the contract's own. */
export const CODE = {
  internal: -32603,
  invalidParams: -32602,
  invalidRequest: -32600,
  methodNotFound: -32601,
  notFound: -32001,
  parseError: -32700,
  rejected: -32002,
} as const;

/** An error with a protocol code. Any other error is reported as rejected. */
export class ProtocolError extends Error {
  readonly code: number;

  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

export const invalidParams = (message: string) =>
  new ProtocolError(CODE.invalidParams, message);

export const notFound = (message: string) =>
  new ProtocolError(CODE.notFound, message);

export const rejected = (message: string) =>
  new ProtocolError(CODE.rejected, message);

/**
 * A request's params object, read strictly: fields it doesn't name are
 * invalid, and so are values of the wrong type. Absent and null fields are
 * undefined, which the adapter treats as River's defaults, as Go's zero
 * values are.
 */
export class Params {
  readonly #path: string;
  readonly #value: Readonly<Record<string, JsonValue>>;

  constructor(value: JsonValue | undefined, path: string, fields: string[]) {
    if (value === undefined || value === null) value = {};
    if (
      typeof value !== "object" ||
      Array.isArray(value) ||
      isExactJsonNumber(value)
    ) {
      throw invalidParams(`${path} must be an object`);
    }
    for (const key of Object.keys(value)) {
      if (!fields.includes(key)) {
        throw invalidParams(`${path} has unknown field ${JSON.stringify(key)}`);
      }
    }
    this.#path = path;
    this.#value = value;
  }

  array(key: string): readonly JsonValue[] | undefined {
    const value = this.#get(key);
    if (value === undefined || Array.isArray(value)) return value;
    throw this.#invalid(key, "an array");
  }

  bigint(key: string): bigint | undefined {
    const value = this.#get(key);
    return value === undefined ? undefined : this.#bigint(key, value);
  }

  bigints(key: string): bigint[] | undefined {
    return this.array(key)?.map((value) => this.#bigint(key, value));
  }

  boolean(key: string): boolean {
    const value = this.#get(key) ?? false;
    if (typeof value === "boolean") return value;
    throw this.#invalid(key, "a boolean");
  }

  integer(key: string): number {
    const value = this.#get(key) ?? 0;
    if (typeof value === "number" && Number.isSafeInteger(value)) return value;
    throw this.#invalid(key, "an integer");
  }

  integers(key: string): number[] | undefined {
    return this.array(key)?.map((value) => {
      if (typeof value === "number" && Number.isSafeInteger(value))
        return value;
      throw this.#invalid(key, "an array of integers");
    });
  }

  json(key: string): JsonValue | undefined {
    return this.#get(key);
  }

  object(key: string, fields: string[]): Params | undefined {
    const value = this.#get(key);
    return value === undefined
      ? undefined
      : new Params(value, `${this.#path}.${key}`, fields);
  }

  string(key: string): string {
    const value = this.#get(key) ?? "";
    if (typeof value === "string") return value;
    throw this.#invalid(key, "a string");
  }

  strings(key: string): string[] | undefined {
    return this.array(key)?.map((value) => {
      if (typeof value === "string") return value;
      throw this.#invalid(key, "an array of strings");
    });
  }

  #bigint(key: string, value: JsonValue): bigint {
    if (typeof value === "number" || isExactJsonNumber(value)) {
      try {
        return jsonNumberToBigInt(value);
      } catch {
        // Reported below.
      }
    }
    throw this.#invalid(key, "an integer");
  }

  #get(key: string): JsonValue | undefined {
    const value = this.#value[key];
    return value === null ? undefined : value;
  }

  #invalid(key: string, expected: string): ProtocolError {
    return invalidParams(`${this.#path}.${key} must be ${expected}`);
  }
}

/**
 * A job as the contract reports it: every column, times in RFC 3339 UTC,
 * the unique key in hex, unique states sorted, and metadata without the
 * random unique nonce.
 */
export function toProtocolJob(job: JobRow): JsonObject {
  const metadata = { ...job.metadata };
  delete metadata["river:unique_nonce"];
  return {
    args: job.args,
    attempt: job.attempt,
    attempted_at: job.attemptedAt?.toString() ?? null,
    attempted_by: [...job.attemptedBy],
    created_at: job.createdAt.toString(),
    errors: job.errors.map((error) => ({
      at: error.at.toString(),
      attempt: error.attempt,
      error: error.error,
      trace: error.trace,
    })),
    finalized_at: job.finalizedAt?.toString() ?? null,
    // An exact number, since IDs can exceed 2^53.
    id: exactJsonNumber(job.id.toString()),
    kind: job.kind,
    max_attempts: job.maxAttempts,
    metadata,
    priority: job.priority,
    queue: job.queue,
    scheduled_at: job.scheduledAt.toString(),
    state: job.state,
    tags: [...job.tags],
    unique_key:
      job.uniqueKey === null
        ? null
        : Buffer.from(job.uniqueKey).toString("hex"),
    unique_states:
      job.uniqueStates === null ? null : [...job.uniqueStates].sort(),
  };
}
