import {
  BackendMismatchError,
  ConfigurationError,
  DatabaseOperationError,
  ValidationError,
} from "riverqueue";

/** Backend name recorded on SQLite errors. */
export const SQLITE_BACKEND = "sqlite";

/** SQLite result codes that mean another connection holds a lock. */
const SQLITE_BUSY = 5;
const SQLITE_LOCKED = 6;

/** A failed SQLite operation, retryable when the database is busy. */
export function databaseError(
  operation: string,
  message: string,
  options: { cause?: unknown; reason?: string; retryable?: boolean } = {}
): DatabaseOperationError {
  const { cause, reason } = options;
  return new DatabaseOperationError(message, {
    backend: SQLITE_BACKEND,
    ...(cause === undefined ? {} : { cause }),
    ...(reason === undefined ? {} : { details: { reason } }),
    operation,
    retryable: options.retryable ?? isRetryableSqliteError(cause),
  });
}

/** A persisted row River cannot decode. */
export function invalidRowError(
  operation: string,
  message: string,
  cause?: unknown
): DatabaseOperationError {
  return databaseError(operation, message, {
    ...(cause === undefined ? {} : { cause }),
    reason: "invalid_row",
    retryable: false,
  });
}

/** Input rejected before it reached SQLite. */
export function invalidInputError(
  operation: string,
  message: string,
  cause?: unknown
): ValidationError {
  return new ValidationError(message, {
    ...(cause === undefined ? {} : { cause }),
    details: { backend: SQLITE_BACKEND, operation },
  });
}

/** Invalid SQLite driver configuration or misuse of its transactions. */
export function configurationError(
  operation: string,
  message: string
): ConfigurationError {
  return new ConfigurationError(message, {
    details: { backend: SQLITE_BACKEND, operation },
  });
}

/** A transaction token that belongs to another driver or transaction. */
export function backendMismatchError(
  operation: string,
  message: string
): BackendMismatchError {
  return new BackendMismatchError(SQLITE_BACKEND, message, {
    details: { operation },
  });
}

/** Whether an error was raised by `node:sqlite` itself. */
export function isSqliteError(value: unknown): value is Error & {
  readonly code: string;
  readonly errcode?: number;
  readonly errstr?: string;
} {
  return (
    value instanceof Error &&
    "code" in value &&
    value.code === "ERR_SQLITE_ERROR"
  );
}

/** Whether a SQLite failure is a transient busy or locked database. */
export function isRetryableSqliteError(value: unknown): boolean {
  if (!isSqliteError(value)) return false;
  const primary = (value.errcode ?? -1) & 0xff;
  return primary === SQLITE_BUSY || primary === SQLITE_LOCKED;
}
