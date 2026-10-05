/**
 * Entry point for River-owned worker threads.
 *
 * This module runs only inside threads started by the pool. It imports
 * `./protocol.js` for types only and depends at runtime only on Node and
 * `riverqueue`, so Node's built-in type stripping can load the TypeScript
 * source directly in development and tests.
 */
import { Buffer } from "node:buffer";
import { registerHooks } from "node:module";
import { parentPort } from "node:worker_threads";

import {
  jobFromJsonValue,
  parseJson,
  stringifyJson,
  ValidationError,
} from "riverqueue";
import type { JsonValue } from "riverqueue";

import type {
  HostMessage,
  LogLevel,
  RunMessage,
  SerializedError,
  ThreadMessage,
} from "./protocol.js";

// Keep in sync with the limits in `./protocol.ts`, which this module can only
// import as types.
const MAX_ERROR_NAME_LENGTH = 256;
const MAX_ERROR_TEXT_LENGTH = 32_768;
const MAX_LOG_TEXT_LENGTH = 32_768;
const MAX_VALUE_BYTES = 32 * 1024 * 1024;

const LOG_LEVELS: readonly LogLevel[] = ["debug", "error", "info", "warn"];

const port = parentPort;
if (port === null) {
  throw new Error("River's worker-thread entry point must run in a thread");
}

if (process.features.typescript) {
  // TypeScript sources import siblings by their compiled `.js` names. When a
  // `.js` module does not exist but a `.ts` source beside it does, load the
  // source, so a handler URL and its imports work unchanged whether the
  // application runs from source under Node's type stripping or from its
  // build. Only a failed resolution takes this path; a build is never
  // affected.
  registerHooks({
    resolve(specifier, context, nextResolve) {
      try {
        return nextResolve(specifier, context);
      } catch (error: unknown) {
        const source = typeScriptSourceSpecifier(specifier, error);
        if (source === undefined) throw error;
        try {
          return nextResolve(source, context);
        } catch {
          throw error;
        }
      }
    },
  });
}

let current:
  { readonly controller: AbortController; readonly taskId: number } | undefined;

port.on("message", (message: HostMessage) => {
  if (message.type === "abort") {
    if (current?.taskId === message.taskId) {
      current.controller.abort(reviveError(message.reason));
    }
    return;
  }
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates messages from the host thread
  if (message.type !== "run") return;
  if (current !== undefined) {
    // The pool only dispatches to idle threads; refuse rather than hang.
    post({
      error: serializeError(new Error("worker thread is already busy")),
      taskId: message.taskId,
      type: "error",
    });
    return;
  }

  const controller = new AbortController();
  current = { controller, taskId: message.taskId };
  post({ taskId: message.taskId, type: "started" });
  void run(message, controller.signal);
});

async function run(message: RunMessage, signal: AbortSignal): Promise<void> {
  const { taskId } = message;
  let settled: ThreadMessage;
  try {
    const context = workContext(message, signal);
    const module = (await import(message.moduleUrl)) as Record<string, unknown>;
    const handler = module[message.exportName];
    if (typeof handler !== "function") {
      throw new TypeError(
        `ESM export ${JSON.stringify(message.exportName)} is not a function`
      );
    }
    const outcome: unknown = await (
      handler as (workContext: typeof context) => unknown
    )(context);
    settled =
      outcome === undefined
        ? { taskId, type: "result" }
        : {
            outcome: stringifyJson(encodeOutcome(outcome)),
            taskId,
            type: "result",
          };
  } catch (error: unknown) {
    settled = { error: serializeError(error), taskId, type: "error" };
  }
  // Become idle before reporting so the pool can dispatch the next task.
  current = undefined;
  post(settled);
}

function workContext(message: RunMessage, signal: AbortSignal) {
  const { taskId } = message;
  const row = jobFromJsonValue(parseJson(message.job));
  const args =
    message.args.encoding === "json"
      ? parseJson(message.args.text)
      : message.args.value;
  const logger = Object.fromEntries(
    LOG_LEVELS.map((level) => [
      level,
      (first: unknown, second?: unknown) => {
        const [text, attributes] =
          typeof first === "string" ? [first, undefined] : [second, first];
        if (
          attributes !== undefined &&
          (attributes === null ||
            typeof attributes !== "object" ||
            Array.isArray(attributes))
        ) {
          throw new TypeError("log attributes must be a JSON object");
        }
        // Bound what one log call sends to the host. Oversized attributes
        // are dropped and noted rather than failing the handler.
        let message = truncate(safeString(text), MAX_LOG_TEXT_LENGTH);
        let encoded =
          attributes === undefined ? undefined : stringifyJson(attributes);
        if (encoded !== undefined && encoded.length > MAX_LOG_TEXT_LENGTH) {
          message = `${message} [log attributes omitted: ${encoded.length.toString(10)} characters]`;
          encoded = undefined;
        }
        post({
          ...(encoded === undefined ? {} : { attributes: encoded }),
          level,
          message,
          taskId,
          type: "log",
        });
      },
    ])
  );
  return Object.freeze({
    execution: Object.freeze({
      attemptedBy: message.execution.attemptedBy,
      startedAt: Temporal.Instant.from(message.execution.startedAt),
    }),
    job: Object.freeze({ ...row, args, rawArgs: row.args }),
    logger: Object.freeze(logger),
    recordOutput: (value: JsonValue) => {
      post({
        output: boundedJson(value, "job output"),
        taskId,
        type: "output",
      });
    },
    setMetadata: (key: string, value: JsonValue) => {
      if (typeof key !== "string") {
        throw new TypeError("metadata key must be a string");
      }
      post({
        key,
        taskId,
        type: "metadata",
        value: boundedJson(value, "job metadata value"),
      });
    },
    signal,
  });
}

function typeScriptSourceSpecifier(
  specifier: string,
  error: unknown
): string | undefined {
  if (
    (error as { code?: unknown } | null)?.code !== "ERR_MODULE_NOT_FOUND" ||
    !/^(?:\.{1,2}\/|file:)/.test(specifier)
  ) {
    return undefined;
  }
  const match = /\.([cm]?)js$/.exec(specifier);
  return match === null
    ? undefined
    : `${specifier.slice(0, match.index)}.${match[1] ?? ""}ts`;
}

function post(message: ThreadMessage): void {
  port?.postMessage(message);
}

function reviveError(value: SerializedError): Error {
  const error = new Error(value.message);
  error.name = value.name;
  if (value.stack.length > 0) error.stack = value.stack;
  return error;
}

/**
 * Encode a value the thread sends to the host, rejecting one larger than
 * the host accepts for output (32 MiB) before it crosses the boundary.
 */
function boundedJson(value: JsonValue, description: string): string {
  const encoded = stringifyJson(value);
  if (Buffer.byteLength(encoded, "utf8") > MAX_VALUE_BYTES) {
    throw new ValidationError(`${description} must not exceed 32 MiB`);
  }
  return encoded;
}

/** Read a property that a hostile or broken getter may throw from. */
function safeProperty(
  value: object,
  key: "message" | "name" | "stack"
): unknown {
  try {
    return (value as Record<typeof key, unknown>)[key];
  } catch {
    return undefined;
  }
}

function serializeError(error: unknown): SerializedError {
  let isError = false;
  try {
    isError = error instanceof Error;
  } catch {
    // A proxy can throw from `instanceof`; describe it as a non-Error value.
  }
  if (isError) {
    const value = error as Error;
    const message = safeProperty(value, "message");
    const stack = safeProperty(value, "stack");
    return {
      message: truncate(
        message === undefined ? "unreadable error message" : safeString(message)
      ),
      name: truncate(
        safeString(safeProperty(value, "name") ?? "") || "Error",
        MAX_ERROR_NAME_LENGTH
      ),
      stack: truncate(stack === undefined ? "" : safeString(stack)),
    };
  }
  return { message: truncate(safeString(error)), name: "Error", stack: "" };
}

function safeString(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return String(value);
  } catch {
    return "non-Error value";
  }
}

function truncate(value: string, limit = MAX_ERROR_TEXT_LENGTH): string {
  return value.length <= limit ? value : value.slice(0, limit);
}

/**
 * A handler's outcome as River JSON: a snooze's `Temporal.Duration` crosses
 * as its ISO 8601 text, which the executor reads back.
 */
function encodeOutcome(outcome: unknown): unknown {
  if (
    typeof outcome === "object" &&
    outcome !== null &&
    (outcome as { readonly type?: unknown }).type === "snooze"
  ) {
    const { duration } = outcome as { readonly duration?: unknown };
    if (duration instanceof Temporal.Duration) {
      return { ...outcome, duration: duration.toString() };
    }
  }
  return outcome;
}
