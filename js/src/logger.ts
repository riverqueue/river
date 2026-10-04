/** Structured fields attached to a log message. */
export type LogAttributes = Readonly<Record<string, unknown>>;

/** A log severity River writes at. */
export type LogLevel = "debug" | "error" | "info" | "warn";

/**
 * A structured logger using pino's argument order: an attributes object
 * first, then the message. Pino, Bunyan, and most structured loggers satisfy
 * it directly, so `logger: pino()` works as is.
 *
 * River logs background failures (database retries, hook failures, dropped
 * completions) at `warn` and `error`. Without a configured logger those two
 * levels go to `console`; pass `logger: false` to silence them.
 */
export interface Logger {
  debug(attributes: LogAttributes, message: string): void;
  error(attributes: LogAttributes, message: string): void;
  info(attributes: LogAttributes, message: string): void;
  warn(attributes: LogAttributes, message: string): void;
}

/** One level of a {@link WorkLogger}: `(message)` or `(attributes, message)`. */
export interface WorkLogFunction {
  (message: string): void;
  (attributes: LogAttributes, message: string): void;
}

/**
 * The job-scoped logger in a work context. It writes to the client's
 * {@link Logger} with `jobId`, `jobKind`, and `attempt` attached, and accepts
 * either `logger.info("message")` or `logger.info({ key }, "message")`.
 */
export interface WorkLogger {
  readonly debug: WorkLogFunction;
  readonly error: WorkLogFunction;
  readonly info: WorkLogFunction;
  readonly warn: WorkLogFunction;
}

/** @internal River's own call sites: message first, optional attributes. */
export interface InternalLogger {
  debug(message: string, attributes?: LogAttributes): void;
  error(message: string, attributes?: LogAttributes): void;
  info(message: string, attributes?: LogAttributes): void;
  warn(message: string, attributes?: LogAttributes): void;
}

const LEVELS: readonly LogLevel[] = ["debug", "error", "info", "warn"];

/** The default logger: warnings and errors to `console`, nothing else. */
export const consoleLogger: Logger = Object.freeze({
  debug: () => undefined,
  error: (attributes: LogAttributes, message: string) => {
    console.error(`[riverqueue] ${message}`, attributes);
  },
  info: () => undefined,
  warn: (attributes: LogAttributes, message: string) => {
    console.warn(`[riverqueue] ${message}`, attributes);
  },
});

const silentLogger: Logger = Object.freeze({
  debug: () => undefined,
  error: () => undefined,
  info: () => undefined,
  warn: () => undefined,
});

/** @internal Resolve the configured logger option. */
export function resolveLogger(option: Logger | false | undefined): Logger {
  if (option === false) return silentLogger;
  return option ?? consoleLogger;
}

/** @internal Adapt a configured logger to River's call sites. */
export function internalLogger(logger: Logger): InternalLogger {
  const call =
    (level: LogLevel) =>
    (message: string, attributes: LogAttributes = {}): void => {
      logger[level](attributes, message);
    };
  return Object.freeze({
    debug: call("debug"),
    error: call("error"),
    info: call("info"),
    warn: call("warn"),
  });
}

/** @internal Validate a logger option at configuration time. */
export function isLoggerOption(value: unknown): value is Logger | false {
  if (value === false) return true;
  return (
    value !== null &&
    typeof value === "object" &&
    LEVELS.every(
      (level) => typeof (value as Record<string, unknown>)[level] === "function"
    )
  );
}

/** @internal Bind job attributes onto the client's logger for handlers. */
export function createWorkLogger(
  logger: Logger,
  bound: LogAttributes
): WorkLogger {
  const call =
    (level: LogLevel): WorkLogFunction =>
    (first: LogAttributes | string, message?: string): void => {
      if (typeof first === "string") {
        logger[level]({ ...bound }, first);
      } else {
        logger[level]({ ...bound, ...first }, message ?? "");
      }
    };
  return Object.freeze({
    debug: call("debug"),
    error: call("error"),
    info: call("info"),
    warn: call("warn"),
  });
}
