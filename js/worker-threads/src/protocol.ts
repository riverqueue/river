/**
 * Messages exchanged between the pool and a River-owned worker thread.
 *
 * Every JSON payload crosses the boundary as text produced by `stringifyJson`
 * and read back with `parseJson`. Structured clone cannot represent the exact
 * JSON numbers River uses for values such as Go-produced int64 IDs, and text
 * keeps each message a small set of strings and numbers that always clones.
 * Decoded args that are not River JSON cross by structured clone only after
 * `encodeArgs` verifies they arrive unchanged.
 *
 * The thread entry point imports this module for types only, so it stays
 * loadable by Node's built-in type stripping during development.
 */

/** Bounded description of an error or abort reason. */
export interface SerializedError {
  readonly message: string;
  readonly name: string;
  readonly stack: string;
}

/** Ask the thread to abort the task's signal cooperatively. */
interface AbortMessage {
  readonly reason: SerializedError;
  readonly taskId: number;
  readonly type: "abort";
}

/**
 * A job's decoded args: River JSON as exact text, or a value checked to
 * survive structured clone unchanged.
 */
export type EncodedArgs =
  | { readonly encoding: "clone"; readonly value: unknown }
  | { readonly encoding: "json"; readonly text: string };

/** Run one attempt in an idle thread. */
export interface RunMessage {
  readonly args: EncodedArgs;
  readonly execution: {
    readonly attemptedBy: string;
    readonly startedAt: string;
  };
  readonly exportName: string;
  /** `JobRowJson` text whose `args` are the persisted input. */
  readonly job: string;
  readonly moduleUrl: string;
  readonly taskId: number;
  readonly type: "run";
}

/** Messages posted from the pool to a thread. */
export type HostMessage = AbortMessage | RunMessage;

/** Levels accepted by the forwarded job logger. */
export type LogLevel = "debug" | "error" | "info" | "warn";

/** Messages posted from a thread to the pool. */
export type ThreadMessage =
  | {
      readonly error: SerializedError;
      readonly taskId: number;
      readonly type: "error";
    }
  | {
      /** JSON object text, when attributes were supplied. */
      readonly attributes?: string;
      readonly level: LogLevel;
      readonly message: string;
      readonly taskId: number;
      readonly type: "log";
    }
  | {
      readonly key: string;
      readonly taskId: number;
      readonly type: "metadata";
      /** JSON value text. */
      readonly value: string;
    }
  | {
      /** JSON value text. */
      readonly output: string;
      readonly taskId: number;
      readonly type: "output";
    }
  | {
      /** JSON text of the returned outcome, absent for `undefined`. */
      readonly outcome?: string;
      readonly taskId: number;
      readonly type: "result";
    }
  | {
      /** The thread accepted the run message and is starting the task. */
      readonly taskId: number;
      readonly type: "started";
    };

/** Longest error name retained across the boundary. */
const MAX_ERROR_NAME_LENGTH = 256;

/** Longest error message or stack retained, matching persisted attempt errors. */
const MAX_ERROR_TEXT_LENGTH = 32_768;

/** Describe any thrown value within the boundary's size limits. */
export function serializeError(error: unknown): SerializedError {
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
