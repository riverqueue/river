/**
 * Optional bounded worker-thread executor for CPU-heavy River handlers.
 *
 * @packageDocumentation
 */
import type { ResourceLimits } from "node:worker_threads";

import { ConfigurationError, jobToJsonValue, stringifyJson } from "riverqueue";
import type {
  JobDefinition,
  JsonValue,
  WorkContext,
  WorkExecutor,
  WorkExecutorHandle,
  WorkExecutorTarget,
  WorkOutcome,
} from "riverqueue";

import { encodeArgs } from "./args.js";
import { WorkerThreadPool } from "./pool.js";

export { WorkerThreadHandlerError } from "./pool.js";

/**
 * Type-level link from a module URL to the exports of that module.
 *
 * Never present at runtime; it only carries `Module` through inference.
 */
declare const workerThreadModuleExports: unique symbol;

/**
 * Snapshot of a {@link WorkerThreads} executor's threads and queue.
 *
 * River includes it under `executors.worker_threads` in client diagnostics.
 */
export type WorkerThreadsDiagnostics = {
  /** Threads currently running an attempt. */
  readonly activeThreads: number;
  /**
   * Threads lost since construction to an uncaught error, an unhandled
   * rejection, `process.exit()`, or a resource limit rather than to an abort
   * or `close()`. A rising count usually means handlers leave failing
   * background work behind after they return.
   */
  readonly crashedThreads: number;
  /** Live threads waiting for an attempt. */
  readonly idleThreads: number;
  /** Attempts waiting for a thread. */
  readonly pendingTasks: number;
  /** Live native threads, including any being terminated. */
  readonly totalThreads: number;
};

/**
 * Names of `Module`'s exports that can handle jobs of `Definition`.
 *
 * Resolves to `string` when the module's type is unknown, as it is for a
 * plain `URL`.
 */
export type WorkerThreadExportName<
  Module,
  Definition extends JobDefinition = JobDefinition,
> = unknown extends Module
  ? string
  : {
      [
        Name in keyof Module & string
      ]: Module[Name] extends WorkerThreadWorkHandler<Definition>
        ? Name
        : never;
    }[keyof Module & string];

/**
 * Where a worker thread finds a job's handler.
 *
 * Give `module` the {@link WorkerThreadModule} type of the handler module to
 * have TypeScript check that `exportName` names an export typed as
 * {@link WorkerThreadWorkHandler} for the registered definition.
 */
export interface WorkerThreadHandlerTarget<
  Definition extends JobDefinition = JobDefinition,
  Module = unknown,
> {
  /** Name of the module export that handles the job. */
  readonly exportName: WorkerThreadExportName<Module, Definition>;
  /** Absolute URL of the ESM module that exports the handler. */
  readonly module: WorkerThreadModule<Module>;
}

/**
 * An ES module URL annotated with the type of the module it points to.
 *
 * Any `URL` is assignable, so annotate the URL to give
 * {@link WorkerThreads.handler} the module's exports without importing the
 * module's code into the main thread:
 *
 * ```ts
 * import type * as primeHandlers from "./prime-handlers.js";
 *
 * const primeModule: WorkerThreadModule<typeof primeHandlers> = new URL(
 *   "./prime-handlers.js",
 *   import.meta.url
 * );
 * ```
 */
export type WorkerThreadModule<Module = unknown> = URL & {
  readonly [workerThreadModuleExports]?: Module;
};

/**
 * Options for a {@link WorkerThreads} executor.
 */
export interface WorkerThreadsOptions {
  /**
   * Maximum number of live native threads.
   *
   * Attempts beyond this limit wait for a thread without spending their job
   * timeout.
   */
  readonly maxThreads: number;
  /**
   * V8 heap and stack limits applied to every thread.
   *
   * A thread that exceeds its heap limit is terminated by Node; the attempt
   * it was running fails and the thread is replaced for later attempts.
   */
  readonly resourceLimits?: ResourceLimits;
}

/**
 * The context passed to a handler running in a worker thread.
 *
 * It carries the members of River's `WorkContext` that can cross a thread
 * boundary. `job.args` holds the args decoded and validated by the job
 * definition in the main thread, and `job.rawArgs` the persisted JSON.
 * `client`, `completeTx`, and `resumable` are unavailable because they depend
 * on the main thread's connections and state.
 *
 * `logger`, `recordOutput`, and `setMetadata` accept River JSON values and
 * are forwarded to the main thread asynchronously.
 */
export type WorkerThreadWorkContext<
  Definition extends JobDefinition = JobDefinition,
> = Pick<
  WorkContext<Definition>,
  "execution" | "job" | "logger" | "recordOutput" | "setMetadata" | "signal"
>;

/**
 * A job handler exported from a module that runs in a worker thread.
 *
 * Type the export with the job's definition so its args are typed, and so
 * {@link WorkerThreads.handler} accepts the export for that definition:
 *
 * ```ts
 * import type { findPrime } from "./jobs.js";
 *
 * export const findPrimeHandler: WorkerThreadWorkHandler<typeof findPrime> = ({
 *   job,
 * }) => complete({ output: { prime: nthPrime(job.args.ordinal) } });
 * ```
 *
 * A type-only import of the definition keeps the handler module from loading
 * anything it does not need. Like an in-process handler, it succeeds by
 * returning nothing or a River outcome and fails by throwing. Outcomes cross
 * back to the main thread as River JSON.
 */
/* eslint-disable @typescript-eslint/no-invalid-void-type -- ordinary and async no-return handlers are valid */
export type WorkerThreadWorkHandler<
  Definition extends JobDefinition = JobDefinition,
> = (
  context: WorkerThreadWorkContext<Definition>
) => PromiseLike<WorkOutcome | void> | WorkOutcome | void;
/* eslint-enable @typescript-eslint/no-invalid-void-type */

interface RegisteredHandler {
  readonly exportName: string;
  readonly kind: string;
  readonly kinds: readonly string[];
  readonly moduleUrl: string;
}

/**
 * A bounded pool of native threads that runs CPU-heavy job handlers without
 * blocking River's event loop.
 *
 * Handlers are ESM exports referenced by module URL and export name, because
 * closures cannot cross a thread boundary. Each attempt runs alone in a
 * reusable thread. Aborting an attempt aborts its handler's signal, waits
 * the client's `jobStuckThreshold`, then terminates the thread, and River
 * persists the outcome only after the thread has settled or exited. A thread
 * that crashes fails only the attempt it was running and is replaced lazily.
 *
 * The application owns the executor: several clients may share one, and
 * stopping a client never closes it. Close it with {@link WorkerThreads.close}
 * or `await using` once every client using it has stopped. Idle threads do
 * not keep the process alive.
 *
 * Worker threads isolate availability, not security. Only run trusted
 * handler modules.
 */
export class WorkerThreads implements AsyncDisposable, WorkExecutor {
  /** Executor name under which River reports this executor's diagnostics. */
  readonly name = "worker_threads";
  readonly #handlers = new WeakMap<object, RegisteredHandler>();
  readonly #pool: WorkerThreadPool;

  /**
   * Create an executor. Threads start lazily as attempts need them.
   *
   * @throws {ConfigurationError} when an option is out of range.
   */
  constructor(options: WorkerThreadsOptions) {
    requireInteger("maxThreads", options.maxThreads, 1);
    const resourceLimits = requireResourceLimits(options.resourceLimits);
    this.#pool = new WorkerThreadPool({
      maxThreads: options.maxThreads,
      ...(resourceLimits === undefined ? {} : { resourceLimits }),
    });
  }

  /**
   * Permanently close the executor and wait for its threads to exit.
   *
   * Queued attempts and attempts still running fail with a `LifecycleError`,
   * and later attempts fail immediately. Stop every client using the executor
   * first so running attempts finish or abort normally. Closing is idempotent.
   */
  close(): Promise<void> {
    return this.#pool.close();
  }

  /** Report the executor's current threads and queue. */
  diagnostics(): WorkerThreadsDiagnostics {
    return { ...this.#pool.diagnostics };
  }

  /**
   * Describe a thread handler for `Workers.addExecutor`.
   *
   * ```ts
   * import type * as primeHandlers from "./prime-handlers.js";
   *
   * const primeModule: WorkerThreadModule<typeof primeHandlers> = new URL(
   *   "./prime-handlers.js",
   *   import.meta.url
   * );
   * workers.addExecutor(
   *   findPrime,
   *   executor.handler(findPrime, {
   *     exportName: "findPrimeHandler",
   *     module: primeModule,
   *   })
   * );
   * ```
   *
   * With a typed `module`, TypeScript rejects an `exportName` that is missing
   * or not a {@link WorkerThreadWorkHandler} for `definition`. With a plain
   * `URL`, any name compiles and a missing export fails the attempt. Register
   * the returned target for the same definition; an attempt of another kind
   * fails with a `ConfigurationError`.
   *
   * @throws {ConfigurationError} when the definition, module, or export name
   *   is invalid.
   */
  handler<Definition extends JobDefinition, Module = unknown>(
    definition: Definition,
    target: WorkerThreadHandlerTarget<Definition, Module>
  ): WorkExecutorTarget {
    if (
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      definition === null ||
      typeof definition !== "object" ||
      typeof definition.kind !== "string"
    ) {
      throw new ConfigurationError(
        "worker thread handler needs a job definition"
      );
    }
    if (!(target.module instanceof URL)) {
      throw new ConfigurationError(
        "worker thread handler module must be an absolute URL"
      );
    }
    if (typeof target.exportName !== "string" || target.exportName === "") {
      throw new ConfigurationError(
        "worker thread handler exportName must be a non-empty string"
      );
    }
    const handler = Object.freeze({});
    this.#handlers.set(handler, {
      exportName: target.exportName,
      kind: definition.kind,
      // Like the Workers registry, the handler also works its kind aliases.
      kinds: [definition.kind, ...(definition.kindAliases ?? [])],
      moduleUrl: target.module.href,
    });
    return Object.freeze({ executor: this, handler });
  }

  /**
   * Queue one attempt for a thread.
   *
   * River's runtime calls this for jobs registered with a target from
   * {@link WorkerThreads.handler}; applications do not call it directly.
   *
   * @throws {ConfigurationError} when the handler was not created by this
   *   executor, belongs to another job kind, or the decoded args cannot cross
   *   the thread boundary unchanged.
   */
  start(context: WorkContext, handler: unknown): WorkExecutorHandle {
    const registered =
      handler !== null && typeof handler === "object"
        ? this.#handlers.get(handler)
        : undefined;
    if (registered === undefined) {
      throw new ConfigurationError(
        "worker thread handler was not created by this executor"
      );
    }
    if (!registered.kinds.includes(context.job.kind)) {
      throw new ConfigurationError(
        `worker thread handler for job kind ${JSON.stringify(registered.kind)} ` +
          `cannot work job kind ${JSON.stringify(context.job.kind)}`
      );
    }
    const task = this.#pool.execute({
      args: encodeArgs(context.job.kind, context.job.args),
      execution: {
        attemptedBy: context.execution.attemptedBy,
        startedAt: context.execution.startedAt.toString(),
      },
      exportName: registered.exportName,
      job: stringifyJson(
        jobToJsonValue({ ...context.job, args: context.job.rawArgs })
      ),
      logger: (level, message, attributes) => {
        context.logger[level](attributes ?? {}, message);
      },
      moduleUrl: registered.moduleUrl,
      recordOutput: (value) => context.recordOutput(value),
      setMetadata: (key, value) => context.setMetadata(key, value),
    });
    const result = task.result.then(decodeOutcome);
    // Like the task's own result, a rejection nobody awaited isn't
    // unhandled.
    void result.catch(() => undefined);
    return {
      // Report termination only when River stopped the handler itself. A
      // handler that settled on its own reports its result, and River
      // classifies it the same way as an in-process handler's.
      abort: async (reason, { gracePeriod }) => ({
        terminated: await task.abort(reason, gracePeriod.total("milliseconds")),
      }),
      result,
      started: task.started,
    };
  }

  /** Close the executor at the end of an `await using` scope. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}

function requireInteger(name: string, value: number, minimum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new ConfigurationError(
      `${name} must be a safe integer of at least ${minimum}`
    );
  }
}

const RESOURCE_LIMIT_KEYS = [
  "codeRangeSizeMb",
  "maxOldGenerationSizeMb",
  "maxYoungGenerationSizeMb",
  "stackSizeMb",
] as const;

/** Copy validated limits so a bad value fails construction, not a spawn. */
function requireResourceLimits(
  limits: ResourceLimits | undefined
): ResourceLimits | undefined {
  if (limits === undefined) return undefined;
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (limits === null || typeof limits !== "object") {
    throw new ConfigurationError("resourceLimits must be an object");
  }
  const copy: { -readonly [Key in keyof ResourceLimits]: number } = {};
  for (const key of Object.keys(limits)) {
    if (!(RESOURCE_LIMIT_KEYS as readonly string[]).includes(key)) {
      throw new ConfigurationError(
        `unknown worker thread resource limit ${JSON.stringify(key)}`
      );
    }
  }
  for (const key of RESOURCE_LIMIT_KEYS) {
    const value = limits[key];
    if (value === undefined) continue;
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      throw new ConfigurationError(
        `resourceLimits.${key} must be a positive finite number`
      );
    }
    copy[key] = value;
  }
  return Object.freeze(copy);
}

/**
 * A thread's outcome from River JSON, reading a snooze's duration back from
 * the ISO 8601 text the thread sent.
 */
function decodeOutcome(
  outcome: JsonValue | undefined
): WorkOutcome | undefined {
  if (typeof outcome === "object" && outcome !== null) {
    const { duration, type } = outcome as {
      readonly duration?: unknown;
      readonly type?: unknown;
    };
    if (type === "snooze" && typeof duration === "string") {
      return { duration: Temporal.Duration.from(duration), type: "snooze" };
    }
  }
  return outcome as WorkOutcome | undefined;
}
