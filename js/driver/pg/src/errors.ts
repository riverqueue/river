import {
  BackendMismatchError,
  ConfigurationError,
  DatabaseOperationError,
  UnsupportedCapabilityError,
} from "riverqueue";

/** Backend name recorded on PostgreSQL errors. */
const POSTGRES_BACKEND = "postgres";

/** A failed PostgreSQL operation, retryable when the cause is transient. */
export function databaseError(
  operation: string,
  message: string,
  cause?: unknown
): DatabaseOperationError {
  return new DatabaseOperationError(message, {
    backend: POSTGRES_BACKEND,
    ...(cause === undefined ? {} : { cause }),
    operation,
    retryable: cause !== undefined && isRetryablePostgresError(cause),
  });
}

/** Invalid PostgreSQL driver configuration or input. */
export function configurationError(
  operation: string,
  message: string
): ConfigurationError {
  return new ConfigurationError(message, {
    details: { backend: POSTGRES_BACKEND, operation },
  });
}

/** A PostgreSQL operation unavailable for the configured connection. */
export function unsupportedError(
  capability: string,
  message: string
): UnsupportedCapabilityError {
  return new UnsupportedCapabilityError(POSTGRES_BACKEND, capability, {
    message,
  });
}

/** A transaction value that does not belong to node-postgres. */
export function backendMismatchError(
  operation: string,
  message: string
): BackendMismatchError {
  return new BackendMismatchError(POSTGRES_BACKEND, message, {
    details: { operation },
  });
}

/**
 * Network error codes reported by Node's socket layer that describe a
 * connection-level failure rather than a rejected statement.
 */
const TRANSIENT_NETWORK_CODES: ReadonlySet<string> = new Set([
  "EAI_AGAIN",
  "ECONNABORTED",
  "ECONNREFUSED",
  "ECONNRESET",
  "EHOSTUNREACH",
  "ENETDOWN",
  "ENETUNREACH",
  "ENOTFOUND",
  "EPIPE",
  "ETIMEDOUT",
]);

/**
 * SQLSTATE codes whose failures are transient: the statement was rejected
 * because of contention, resource pressure, or a server-side timeout, so the
 * same River operation can safely be attempted again.
 */
const TRANSIENT_SQLSTATES: ReadonlySet<string> = new Set([
  // idle_in_transaction_session_timeout ends the session.
  "25P03",
  // serialization_failure and deadlock_detected.
  "40001",
  "40P01",
  // lock_not_available, including `lock_timeout`.
  "55P03",
  // query_canceled, including `statement_timeout`.
  "57014",
  // Administrator, crash, startup, and idle-session shutdowns.
  "57P01",
  "57P02",
  "57P03",
  "57P05",
]);

/**
 * Message fragments node-postgres and pg-pool use for code-less connection
 * failures, such as a pool `connectionTimeoutMillis` expiring.
 */
const TRANSIENT_MESSAGE =
  /connection (?:ended|terminated)(?: unexpectedly)?|timeout exceeded when trying to connect/i;

/**
 * Whether a node-postgres failure is transient, so retrying the whole River
 * operation is safe.
 *
 * Transient failures are connection exceptions (SQLSTATE class 08),
 * insufficient resources (class 53), serialization failures and deadlocks,
 * lock and statement timeouts, server shutdowns, socket and DNS errors, and
 * pg-pool connection timeouts. The error and its `cause` chain are inspected.
 */
function isRetryablePostgresError(value: unknown): boolean {
  const seen = new Set<unknown>();
  let current = value;
  for (let depth = 0; depth < 8 && current !== null; depth++) {
    if (
      (typeof current !== "object" && typeof current !== "function") ||
      seen.has(current)
    ) {
      return false;
    }
    seen.add(current);
    const error = current as {
      readonly cause?: unknown;
      readonly code?: unknown;
      readonly message?: unknown;
    };
    if (typeof error.code === "string") {
      if (
        error.code.startsWith("08") ||
        error.code.startsWith("53") ||
        TRANSIENT_SQLSTATES.has(error.code) ||
        TRANSIENT_NETWORK_CODES.has(error.code)
      ) {
        return true;
      }
    }
    if (
      typeof error.message === "string" &&
      TRANSIENT_MESSAGE.test(error.message)
    ) {
      return true;
    }
    current = error.cause;
  }
  return false;
}
