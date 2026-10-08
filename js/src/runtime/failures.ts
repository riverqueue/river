/**
 * Encoding and classification of errors raised while running jobs.
 */
import {
  BackendMismatchError,
  ConfigurationError,
  ExtensionError,
  RiverError,
  UnsupportedCapabilityError,
} from "../errors.js";

/**
 * Encode a thrown value as River's attempt error record.
 *
 * Go records a stack trace only for panics, never for returned errors. The
 * JavaScript analog of a panic is a runtime fault surfaced as one of the
 * native error classes, such as the `TypeError` from reading a property of
 * `undefined` or a `RangeError`; those keep their (bounded) stack. Errors a
 * handler throws deliberately, including subclasses of `Error`, are ordinary
 * failures and record an empty trace, which also keeps rows small.
 */
export function canonicalError(
  thrown: unknown,
  at: Temporal.Instant
): { at: Temporal.Instant; error: string; trace: string } {
  if (thrown instanceof Error) {
    return {
      at,
      error: truncate(thrown.message || thrown.name, 32_768),
      trace: isRuntimeFault(thrown)
        ? truncate(thrown.stack ?? thrown.name, 32_768)
        : "",
    };
  }
  let value: string;
  try {
    value = typeof thrown === "string" ? thrown : String(thrown);
  } catch {
    value = "non-Error value";
  }
  return { at, error: truncate(value, 32_768), trace: "" };
}

/** Native error classes whose instances signal a JavaScript runtime fault. */
const RUNTIME_FAULT_PROTOTYPES: readonly object[] = [
  EvalError.prototype,
  RangeError.prototype,
  ReferenceError.prototype,
  SyntaxError.prototype,
  TypeError.prototype,
  URIError.prototype,
];

/** Whether an error is a JavaScript runtime fault, River's panic analog. */
export function isRuntimeFault(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const prototype: unknown = Object.getPrototypeOf(error);
  return (
    prototype !== null &&
    typeof prototype === "object" &&
    RUNTIME_FAULT_PROTOTYPES.includes(prototype)
  );
}

/**
 * Whether a background failure cannot be fixed by retrying: the backend is
 * misconfigured or lacks a required capability. Everything else a backend
 * throws is operational and retried with backoff.
 */
export function isPermanentRuntimeError(error: unknown): boolean {
  return (
    error instanceof ConfigurationError ||
    error instanceof UnsupportedCapabilityError ||
    error instanceof BackendMismatchError
  );
}

/**
 * One-line operator description of an error and its `cause` chain, including
 * backend codes such as a Postgres SQLSTATE, for background-failure logs.
 */
export function describeError(error: unknown): string {
  const parts: string[] = [];
  const seen = new Set<unknown>();
  let current: unknown = error;
  for (let depth = 0; depth < 4 && current !== undefined; depth++) {
    if (current === null || seen.has(current)) break;
    seen.add(current);
    if (current instanceof Error) {
      // River error codes are categories; backend codes such as SQLSTATEs
      // are what an operator needs.
      const code =
        current instanceof RiverError
          ? undefined
          : (current as { readonly code?: unknown }).code;
      const message = current.message || current.name;
      parts.push(
        typeof code === "string" && code !== "" && !message.includes(code)
          ? `${message} (${code})`
          : message
      );
      current = current.cause;
    } else {
      parts.push(canonicalError(current, Temporal.Now.instant()).error);
      break;
    }
  }
  return truncate(parts.join(": "), 4_096);
}

/** Cut `value` to at most `length` UTF-16 code units. */
export function truncate(value: string, length: number): string {
  return value.length <= length ? value : value.slice(0, length);
}

/** Run a lifecycle hook, wrapping any failure in an `ExtensionError`. */
export async function invokeHook(
  name: string,
  hook: () => unknown
): Promise<void> {
  try {
    await hook();
  } catch (cause: unknown) {
    throw new ExtensionError(`River ${name} hook failed`, { cause });
  }
}
