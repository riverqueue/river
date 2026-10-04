import type {
  WorkContext,
  WorkExecutor,
  WorkExecutorAbortResult,
  WorkExecutorHandle,
  WorkOutcome,
  WorkerRegistration,
} from "../worker.js";
import type { JsonObject } from "../json.js";
import { millisecondsToDuration } from "./duration.js";

export interface AttemptExecutionHandle {
  readonly result: Promise<WorkOutcome | undefined>;
  /** Settles when an executor begins the attempt; absent means already. */
  readonly started?: PromiseLike<void>;
  /**
   * Abort the attempt's handler, letting an executor end it by force once
   * `gracePeriodMs` passes without it settling.
   */
  abort(
    reason: unknown,
    gracePeriodMs: number
  ): Promise<WorkExecutorAbortResult>;
  /**
   * Whether the executor stopped the attempt by force after it began, once
   * it ignored an abort through its grace period. Settles once the abort
   * requested so far, if any, settled.
   */
  forciblyStopped(): Promise<boolean>;
}

/** Coordinates ordinary closures and optional executors behind one seam. */
export class AttemptExecutor {
  readonly #executors = new Set<WorkExecutor>();

  diagnostics(): Readonly<Record<string, JsonObject>> {
    const result = Object.create(null) as Record<string, JsonObject>;
    for (const executor of this.#executors) {
      result[executor.name] = executor.diagnostics?.() ?? {};
    }
    return Object.freeze(result);
  }

  start(
    registration: WorkerRegistration,
    context: WorkContext
  ): AttemptExecutionHandle {
    if (registration.type === "in_process") {
      const result = Promise.resolve().then(
        async (): Promise<WorkOutcome | undefined> =>
          (await registration.handler(context)) as WorkOutcome | undefined
      );
      void result.catch(() => undefined);
      return {
        abort: () => Promise.resolve({ terminated: false }),
        forciblyStopped: () => Promise.resolve(false),
        result,
      };
    }

    const executor = registration.target.executor;
    this.#executors.add(executor);
    let handle: WorkExecutorHandle;
    try {
      handle = executor.start(context, registration.target.handler);
    } catch (error: unknown) {
      return {
        abort: () => Promise.resolve({ terminated: true }),
        forciblyStopped: () => Promise.resolve(false),
        result: Promise.reject(error),
      };
    }
    const result = Promise.resolve(handle.result);
    void result.catch(() => undefined);
    // An executor that ends an attempt still waiting for its capacity
    // reports that as a termination too, but that attempt never ran.
    let began = handle.started === undefined;
    if (handle.started !== undefined) {
      void Promise.resolve(handle.started).then(
        () => {
          began = true;
        },
        () => undefined
      );
    }
    let aborting: Promise<WorkExecutorAbortResult> | undefined;
    return {
      abort: (reason, gracePeriodMs) => {
        aborting = Promise.resolve(
          handle.abort(reason, {
            gracePeriod: millisecondsToDuration(gracePeriodMs),
          })
        );
        return aborting;
      },
      forciblyStopped: async () => {
        if (aborting === undefined) return false;
        const { terminated } = await aborting.catch(() => ({
          terminated: false,
        }));
        return terminated && began;
      },
      result,
      ...(handle.started === undefined ? {} : { started: handle.started }),
    };
  }
}
