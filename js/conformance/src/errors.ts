/**
 * Stable JSON-RPC error codes from River's adapter contract
 * (`conformance/adapter/contract.json`). Scenarios assert these codes rather
 * than message text, so every failure the adapters report is classified.
 */
import { DatabaseOperationError, RiverError } from "riverqueue";

export const ADAPTER_ERROR_CODE = Object.freeze({
  databaseError: -32_003,
  internal: -32_000,
  invalidParams: -32_602,
  invalidRequest: -32_600,
  methodNotFound: -32_601,
  notFound: -32_001,
  parseError: -32_700,
  rejected: -32_002,
  unsupported: -32_004,
} as const);

/** One of the contract's stable error codes. */
export type AdapterErrorCode =
  (typeof ADAPTER_ERROR_CODE)[keyof typeof ADAPTER_ERROR_CODE];

/** A failure the adapter classifies with a contract error code itself. */
export class AdapterError extends Error {
  readonly code: AdapterErrorCode;

  constructor(code: AdapterErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
    this.name = "AdapterError";
  }
}

/** Params that do not match the method's params schema. */
export function invalidParams(message: string): AdapterError {
  return new AdapterError(ADAPTER_ERROR_CODE.invalidParams, message);
}

/** A method outside the adapter's advertised profile. */
export function methodNotFound(method: string): AdapterError {
  return new AdapterError(
    ADAPTER_ERROR_CODE.methodNotFound,
    `method not found: ${method}`
  );
}

/** A missing job, queue, transaction handle, or barrier. */
export function notFound(message: string): AdapterError {
  return new AdapterError(ADAPTER_ERROR_CODE.notFound, message);
}

/** A request River or the adapter refused or could not complete. */
export function rejected(
  message: string,
  options?: ErrorOptions
): AdapterError {
  return new AdapterError(ADAPTER_ERROR_CODE.rejected, message, options);
}

/** A valid optional parameter or feature the adapter cannot honor. */
export function unsupported(message: string): AdapterError {
  return new AdapterError(ADAPTER_ERROR_CODE.unsupported, message);
}

/**
 * Map a failure to its contract error code, like the Go reference adapter:
 * adapter-classified failures keep their code, database failures are
 * `database_error`, and every other failure River reports is `rejected`.
 */
export function adapterErrorCode(error: unknown): AdapterErrorCode {
  if (error instanceof AdapterError) return error.code;
  return isDatabaseError(error)
    ? ADAPTER_ERROR_CODE.databaseError
    : ADAPTER_ERROR_CODE.rejected;
}

function isDatabaseError(error: unknown): boolean {
  if (error instanceof DatabaseOperationError) return true;
  // River wraps driver failures, so look through causes for the database's
  // own error: node-postgres reports a five-character SQLSTATE, and
  // `node:sqlite` reports `ERR_SQLITE_ERROR`.
  for (
    let current: unknown = error, depth = 0;
    current instanceof Error && depth < 8;
    current = current.cause, depth++
  ) {
    if (current instanceof RiverError) continue;
    const code = (current as { readonly code?: unknown }).code;
    if (code === "ERR_SQLITE_ERROR") return true;
    if (typeof code === "string" && /^[0-9A-Z]{5}$/u.test(code)) return true;
  }
  return false;
}
