/**
 * Construction and validation of work attempt results and outcomes.
 */
import { ValidationError } from "../errors.js";
import type { WorkAttemptResult, WorkMiddleware } from "../extensions.js";
import { toMilliseconds } from "../internal/duration.js";
import { toJsonObject } from "../json.js";
import type { WorkAttemptContext, WorkOutcome } from "../worker.js";
import { normalizeOutput } from "./work-context.js";

/** A succeeded result for `outcome`, keeping `previous` metadata and output. */
export function succeededResult(
  outcome: WorkOutcome | undefined,
  previous?: WorkAttemptResult
): WorkAttemptResult {
  const normalizedOutcome = normalizeOutcome(outcome);
  return {
    ...attemptData(previous),
    ...(normalizedOutcome === undefined ? {} : { outcome: normalizedOutcome }),
    status: "succeeded",
  };
}

/**
 * The result of a thrown `error`: cancelled when it is the attempt signal's
 * abort reason (or an error of the same name), otherwise failed.
 */
export function thrownResult(
  error: unknown,
  signal: AbortSignal,
  previous?: WorkAttemptResult
): WorkAttemptResult {
  const reason: unknown = signal.reason;
  const cancelled =
    signal.aborted &&
    (Object.is(error, reason) ||
      (error instanceof Error &&
        reason instanceof Error &&
        error.name === reason.name));
  return {
    ...attemptData(previous),
    error: cancelled ? reason : error,
    status: cancelled ? "cancelled" : "failed",
  };
}

/** The metadata and output carried by `result`, if any. */
export function attemptData(
  result: WorkAttemptResult | undefined
): Pick<WorkAttemptResult, "metadata" | "output"> {
  if (result === undefined) return {};
  return {
    ...(result.metadata === undefined ? {} : { metadata: result.metadata }),
    ...(result.output === undefined && !("output" in result)
      ? {}
      : { output: result.output }),
  };
}

/** Validate and freeze a handler's outcome. */
function normalizeOutcome(
  outcome: WorkOutcome | undefined
): WorkOutcome | undefined {
  validateOutcome(outcome);
  if (outcome === undefined) return undefined;
  if (outcome.type === "complete" && outcome.output !== undefined) {
    return Object.freeze({
      output: normalizeOutput(outcome.output),
      type: "complete",
    });
  }
  return Object.freeze({ ...outcome });
}

/** Validate and snapshot a result returned by an `afterWork` hook. */
export function normalizeWorkAttemptResult(
  value: WorkAttemptResult
): WorkAttemptResult {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (value === null || typeof value !== "object") {
    throw new ValidationError("afterWork must return a work attempt result");
  }
  const data = {
    ...(value.metadata === undefined
      ? {}
      : { metadata: toJsonObject(value.metadata) }),
    ...(value.output === undefined && !("output" in value)
      ? {}
      : { output: normalizeOutput(value.output) }),
  };
  switch (value.status) {
    case "cancelled":
      if (!("error" in value) || "outcome" in value || "cancel" in value) {
        throw new ValidationError("invalid cancelled work attempt result");
      }
      return { ...data, error: value.error, status: "cancelled" };
    case "failed":
      if (!("error" in value) || "outcome" in value) {
        throw new ValidationError("invalid failed work attempt result");
      }
      if (value.cancel !== undefined && typeof value.cancel !== "boolean") {
        throw new ValidationError("failed result cancel must be a boolean");
      }
      return {
        ...data,
        ...(value.cancel === undefined ? {} : { cancel: value.cancel }),
        error: value.error,
        status: "failed",
      };
    case "succeeded": {
      if ("error" in value || "cancel" in value) {
        throw new ValidationError("invalid succeeded work attempt result");
      }
      const outcome = normalizeOutcome(value.outcome);
      return {
        ...data,
        ...(outcome === undefined ? {} : { outcome }),
        status: "succeeded",
      };
    }
    default:
      throw new ValidationError("unknown work attempt result status");
  }
}

/** Reject a handler outcome River cannot persist. */
function validateOutcome(outcome: WorkOutcome | undefined): void {
  if (outcome === undefined) return;
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (outcome === null || typeof outcome !== "object") {
    throw new ValidationError("worker returned an invalid outcome");
  }
  if (outcome.type === "complete") {
    if (outcome.output !== undefined) normalizeOutput(outcome.output);
    return;
  }
  if (outcome.type === "cancel" || outcome.type === "discard") {
    if (
      outcome.reason !== undefined &&
      (typeof outcome.reason !== "string" || outcome.reason.length === 0)
    ) {
      throw new ValidationError(
        `${outcome.type} reason must be a non-empty string`
      );
    }
    return;
  }
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (outcome.type === "snooze") {
    toMilliseconds("snooze duration", outcome.duration, {
      allowZero: true,
      error: ValidationError,
    });
    return;
  }
  throw new ValidationError("worker returned an invalid outcome");
}

/**
 * Compose work middleware around `handler`, outermost first. Each
 * middleware may call `next` at most once.
 */
export function composeMiddleware(
  middleware: readonly WorkMiddleware[],
  handler: (
    context: WorkAttemptContext
  ) => PromiseLike<WorkOutcome | undefined> | WorkOutcome | undefined
): (context: WorkAttemptContext) => Promise<WorkOutcome | undefined> {
  return async (context) => {
    const dispatch = async (
      index: number
    ): Promise<WorkOutcome | undefined> => {
      const current = middleware[index];
      if (current === undefined) {
        return await handler(context);
      }
      let called = false;
      return (await current(context, async () => {
        if (called)
          throw new Error("work middleware called next more than once");
        called = true;
        return dispatch(index + 1);
      })) as WorkOutcome | undefined;
    };
    return dispatch(0);
  };
}

/** Whether a result completes the job without any explicit outcome. */
export function isPlainCompletion(result: WorkAttemptResult): boolean {
  return (
    result.status === "succeeded" &&
    (result.outcome === undefined || result.outcome.type === "complete")
  );
}
