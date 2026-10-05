/**
 * The work context of a running attempt: async-local lookup, attempt
 * metadata and output, and the decoded context handed to handlers.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { Buffer } from "node:buffer";
import { channel } from "node:diagnostics_channel";
import type { JobCompletionCommand, RuntimeDriver } from "../driver.js";
import { LifecycleError, ValidationError } from "../errors.js";
import type { ErrorHandlerContext, WorkAttemptResult } from "../extensions.js";
import type { JobRow } from "../job.js";
import type { JsonObject, JsonValue } from "../json.js";
import { stringifyJson, toJsonObject, toJsonValue } from "../json.js";
import { Resumable } from "../resumable.js";
import type { WorkAttemptContext, WorkContext } from "../worker.js";
import type { PilotOperations } from "./pilot-operations.js";

/** The work context `currentWorkContext()` returns inside a job. */
export type CurrentWorkContext = WorkAttemptContext | WorkContext;

/** Either context shape a handler, hook, or middleware may hold. */
export type AnyWorkContext = CurrentWorkContext;

/** Output recorded on an attempt; `output` is absent until something is recorded. */
export interface WorkOutputState {
  output?: JsonValue;
}

/** Metadata an attempt accumulates through `setMetadata`. */
export interface WorkMetadataState {
  /** Cleared when the attempt ends, after which writes are rejected. */
  active: boolean;
  readonly values: JsonObject;
}

const MAX_OUTPUT_BYTES = 32 * 1024 * 1024;

const workChannel = channel("riverqueue:work");
const workMetadata = new WeakMap<AnyWorkContext, WorkMetadataState>();
const workOutput = new WeakMap<AnyWorkContext, WorkOutputState>();
const workStorage = new AsyncLocalStorage<AnyWorkContext>();

/**
 * Start tracking metadata and output for a new attempt's context. The caller
 * marks the returned state inactive once the attempt ends.
 */
export function beginWorkAttempt(
  context: AnyWorkContext,
  output: WorkOutputState
): WorkMetadataState {
  const metadata: WorkMetadataState = {
    active: true,
    values: Object.create(null) as JsonObject,
  };
  workMetadata.set(context, metadata);
  workOutput.set(context, output);
  return metadata;
}

/**
 * The state of the attempt `context` belongs to, shared by its raw and
 * decoded contexts, or undefined for a context River didn't create.
 */
export function workAttemptState(
  context: AnyWorkContext
): WorkMetadataState | undefined {
  return workMetadata.get(context);
}

/** Publish a settled attempt on the `riverqueue:work` diagnostics channel. */
export function publishWorkResult(
  context: AnyWorkContext,
  result: WorkAttemptResult
): void {
  workChannel.publish({ context, result });
}

/** Run `callback` with `context` as the current work context. */
export function runInWorkContext<T>(
  context: AnyWorkContext,
  callback: () => T
): T {
  return workStorage.run(context, callback);
}

/**
 * Let a derived context (the decoded context handed to handlers) share the
 * metadata and output its raw attempt context tracks.
 */
export function shareWorkAttempt(
  from: AnyWorkContext,
  to: AnyWorkContext,
  output: WorkOutputState
): void {
  const metadata = workMetadata.get(from);
  if (metadata !== undefined) workMetadata.set(to, metadata);
  workOutput.set(to, output);
}

/**
 * Return the context of the job attempt the caller is running inside, or
 * undefined outside a job. River establishes it with `AsyncLocalStorage`, so
 * code called from a handler can reach the job without passing `ctx` along.
 */
export function currentWorkContext(): CurrentWorkContext | undefined {
  return workStorage.getStore();
}

/** Record JSON output on the current attempt, with last write winning. */
export function recordOutput(value: JsonValue): void {
  const context = currentWorkContext();
  if (context === undefined) {
    throw new LifecycleError("recordOutput must be called while working a job");
  }
  context.recordOutput(value);
}

/** Merge one JSON value into metadata on the current work attempt. */
export function setMetadata(key: string, value: JsonValue): void {
  const context = currentWorkContext();
  if (context === undefined) {
    throw new LifecycleError("setMetadata must be called while working a job");
  }
  context.setMetadata(key, value);
}

/** Validate recorded output as River JSON no larger than 32 MiB. */
export function normalizeOutput(value: JsonValue): JsonValue {
  const output = toJsonValue(value);
  if (Buffer.byteLength(stringifyJson(output), "utf8") > MAX_OUTPUT_BYTES) {
    throw new ValidationError("job output must not exceed 32 MiB");
  }
  return output;
}

/** Attach the output recorded on `context`, if any, to `result`. */
export function resultWithOutput(
  result: WorkAttemptResult,
  context: AnyWorkContext
): WorkAttemptResult {
  const outputState = workOutput.get(context);
  return outputState !== undefined && "output" in outputState
    ? { ...result, output: outputState.output }
    : result;
}

/** Merge the metadata recorded on `context` into `result`. */
export function resultWithMetadata(
  result: WorkAttemptResult,
  context: AnyWorkContext
): WorkAttemptResult {
  const metadata = snapshotWorkMetadata(context);
  if (Object.keys(metadata).length === 0) return result;
  return {
    ...result,
    metadata: toJsonObject({ ...(result.metadata ?? {}), ...metadata }),
  };
}

/** Record one metadata value on an active attempt's context. */
export function setWorkMetadata(
  context: AnyWorkContext,
  key: string,
  value: JsonValue
): void {
  if (typeof key !== "string") {
    throw new ValidationError("metadata key must be a string");
  }
  if (key.startsWith("river:")) {
    throw new ValidationError(
      "metadata keys prefixed with river: are reserved for River"
    );
  }
  const metadata = workMetadata.get(context);
  if (metadata === undefined || !metadata.active) {
    throw new LifecycleError("work metadata is no longer available");
  }
  metadata.values[key] = toJsonValue(value);
}

/** Copy the metadata recorded so far on `context`. */
export function snapshotWorkMetadata(context: AnyWorkContext): JsonObject {
  return toJsonObject(workMetadata.get(context)?.values ?? {});
}

/**
 * Build the context handed to a handler: the raw attempt context plus the
 * decoded arguments, a resumable, and transactional completion.
 */
export function makeDecodedWorkContext(
  rawContext: WorkAttemptContext,
  decodedArgs: unknown,
  row: JobRow,
  outputState: WorkOutputState,
  clientId: string,
  driver: RuntimeDriver,
  operations: PilotOperations
): WorkContext {
  const context: WorkContext = {
    ...rawContext,
    completeTx: async (tx, options = {}) => {
      const explicitOutput = options.output !== undefined;
      const outputSet = explicitOutput || "output" in outputState;
      const output = explicitOutput
        ? normalizeOutput(options.output)
        : (outputState.output ?? null);
      const command: JobCompletionCommand = {
        attempt: row.attempt,
        attemptedBy: clientId,
        error: null,
        finalizedAt: Temporal.Now.instant(),
        id: row.id,
        kind: "complete",
        metadata: snapshotWorkMetadata(rawContext),
        output,
        outputSet,
        scheduledAt: null,
      };
      const result = (await operations.complete(driver, [command], { tx }))[0];
      if (result?.status !== "applied" || result.job === null) {
        throw new LifecycleError(
          `job ${row.id} attempt ${row.attempt} is no longer running`
        );
      }
      return result.job;
    },
    job: {
      ...row,
      args: decodedArgs,
      rawArgs: row.args,
    },
    resumable: new Resumable(rawContext.client, row),
  };
  return context;
}

/** The attempt context an error handler sees, without `setMetadata`. */
export function errorHandlerContext(
  context: WorkAttemptContext
): ErrorHandlerContext {
  const { setMetadata, ...visible } = context;
  void setMetadata;
  return Object.freeze(visible);
}
