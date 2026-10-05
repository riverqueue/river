import { Worker } from "node:worker_threads";
import type { ResourceLimits } from "node:worker_threads";

import { LifecycleError, parseJson, parseJsonObject } from "riverqueue";
import type { JsonObject, JsonValue } from "riverqueue";

import {
  serializeError,
  type LogLevel,
  type RunMessage,
  type ThreadMessage,
} from "./protocol.js";

export interface WorkerThreadPoolOptions {
  readonly maxThreads: number;
  readonly resourceLimits?: ResourceLimits;
}

export interface WorkerThreadPoolDiagnostics {
  readonly activeThreads: number;
  readonly crashedThreads: number;
  readonly idleThreads: number;
  readonly pendingTasks: number;
  readonly totalThreads: number;
}

export interface WorkerThreadTaskRequest {
  readonly args: RunMessage["args"];
  readonly execution: RunMessage["execution"];
  readonly exportName: string;
  /** `JobRowJson` text whose `args` are the persisted input. */
  readonly job: string;
  readonly logger: WorkerThreadTaskLogger;
  readonly moduleUrl: string;
  readonly recordOutput: (value: JsonValue) => void;
  readonly setMetadata: (key: string, value: JsonValue) => void;
}

interface WorkerThreadTaskLogger {
  (level: LogLevel, message: string, attributes: JsonObject | undefined): void;
}

export interface WorkerThreadTaskHandle {
  /** Settles with the handler's parsed outcome, or rejects with its failure. */
  readonly result: Promise<JsonValue | undefined>;
  /**
   * Resolves once the task is handed to a thread, or rejects if the task
   * settles while still queued.
   */
  readonly started: Promise<void>;

  /**
   * Stop the task and resolve only once it no longer runs.
   *
   * A queued task is removed. A running task's handler sees its signal abort;
   * if it has not settled after the grace period, its thread is terminated.
   * Resolves `true` when River stopped the task itself, by removing it from
   * the queue or terminating its thread, and `false` when the handler had
   * already settled or settled on its own within the grace period.
   */
  abort(reason: unknown, gracePeriodMs: number): Promise<boolean>;
}

/**
 * An error reported by an isolated handler or by the thread running it.
 *
 * An error thrown by the handler keeps its original `name`, `message`, and
 * `stack`, bounded to the limits River applies to persisted attempt errors.
 * An attempt whose thread crashes or exits also fails with this error.
 */
export class WorkerThreadHandlerError extends Error {
  /**
   * @param message - The original error's message.
   * @param options - The original error's `name` and `stack`, if known.
   */
  constructor(
    message: string,
    options: { readonly name?: string; readonly stack?: string } = {}
  ) {
    super(message);
    this.name = options.name || "WorkerThreadHandlerError";
    if (options.stack !== undefined && options.stack.length > 0) {
      this.stack = options.stack;
    }
  }
}

/**
 * A bounded set of reusable River-owned native threads.
 *
 * Each thread runs at most one task at a time, and tasks queue in FIFO order
 * while every thread is busy. A thread that crashes or exits, whether idle or
 * running, is discarded; a replacement starts only when a queued task needs
 * one.
 */
export class WorkerThreadPool {
  readonly maxThreads: number;

  readonly #idle: ThreadSlot[] = [];
  readonly #resourceLimits: ResourceLimits | undefined;
  readonly #queue: PoolTask[] = [];
  readonly #slots = new Set<ThreadSlot>();
  #closing: Promise<void> | undefined;
  #crashedThreads = 0;
  #nextTaskId = 1;
  #nextThreadNumber = 1;

  constructor(options: WorkerThreadPoolOptions) {
    this.maxThreads = options.maxThreads;
    this.#resourceLimits = options.resourceLimits;
  }

  get diagnostics(): WorkerThreadPoolDiagnostics {
    let activeThreads = 0;
    for (const slot of this.#slots) {
      if (slot.busy) activeThreads++;
    }
    return {
      activeThreads,
      crashedThreads: this.#crashedThreads,
      idleThreads: this.#idle.length,
      pendingTasks: this.#queue.length,
      totalThreads: this.#slots.size,
    };
  }

  /** Reject queued and running tasks, then wait for every thread to exit. */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  execute(request: WorkerThreadTaskRequest): WorkerThreadTaskHandle {
    const task = new PoolTask(this, request, this.#nextTaskId++);
    if (this.#closing !== undefined) {
      task.fail(closedError());
      return task;
    }
    this.#queue.push(task);
    this.#dispatch();
    return task;
  }

  /** @internal Remove a task that has not been handed to a thread. */
  dequeue(task: PoolTask): void {
    const index = this.#queue.indexOf(task);
    if (index !== -1) this.#queue.splice(index, 1);
  }

  /** @internal Retry a task whose reused thread died before starting it. */
  requeue(task: PoolTask): void {
    if (this.#closing !== undefined) {
      task.fail(closedError());
      return;
    }
    this.#queue.unshift(task);
    this.#dispatch();
  }

  async #close(): Promise<void> {
    const error = closedError();
    for (const task of this.#queue.splice(0)) task.fail(error);
    this.#idle.length = 0;
    await Promise.all(
      [...this.#slots].map((slot) =>
        slot.terminate(
          new LifecycleError(
            "worker thread executor closed while the attempt was running"
          )
        )
      )
    );
  }

  #dispatch(): void {
    while (this.#closing === undefined && this.#queue.length > 0) {
      let slot: ThreadSlot | undefined;
      try {
        slot = this.#takeIdle() ?? this.#spawn();
      } catch (error: unknown) {
        // Dispatch also runs from thread events, so a failed spawn must fail
        // the task it was for rather than escape as an uncaught exception.
        this.#queue.shift()?.fail(error);
        continue;
      }
      if (slot === undefined) return;
      const task = this.#queue.shift();
      if (task === undefined) {
        this.#idle.push(slot);
        return;
      }
      slot.run(task);
    }
  }

  #onExit(slot: ThreadSlot, crashed: boolean): void {
    this.#slots.delete(slot);
    const index = this.#idle.indexOf(slot);
    if (index !== -1) this.#idle.splice(index, 1);
    if (crashed) this.#crashedThreads++;
    this.#dispatch();
  }

  #onIdle(slot: ThreadSlot): void {
    if (this.#closing !== undefined) return;
    this.#idle.push(slot);
    this.#dispatch();
  }

  #spawn(): ThreadSlot | undefined {
    if (this.#slots.size >= this.maxThreads) return undefined;
    const slot = new ThreadSlot(
      {
        name: `riverqueue-worker-thread-${this.#nextThreadNumber++}`,
        ...(this.#resourceLimits === undefined
          ? {}
          : { resourceLimits: this.#resourceLimits }),
      },
      {
        onExit: (exited, crashed) => this.#onExit(exited, crashed),
        onIdle: (idle) => this.#onIdle(idle),
      }
    );
    this.#slots.add(slot);
    return slot;
  }

  /** Take the most recently used idle thread that is still alive. */
  #takeIdle(): ThreadSlot | undefined {
    for (;;) {
      const slot = this.#idle.pop();
      if (slot === undefined || slot.reusable) return slot;
    }
  }
}

interface ThreadSlotOwner {
  onExit(slot: ThreadSlot, crashed: boolean): void;
  onIdle(slot: ThreadSlot): void;
}

/**
 * One native thread and the task it is currently running.
 *
 * Its `error`, `exit`, and `message` listeners stay attached for the thread's
 * whole life. A failure while a task runs settles that task; a failure while
 * idle only evicts the thread. Without a permanent `error` listener, a stray
 * timer throwing after its handler returned would crash the host process.
 */
class ThreadSlot {
  readonly #exited: Promise<void>;
  readonly #owner: ThreadSlotOwner;
  readonly #worker: Worker;
  #crash: { readonly error: unknown } | undefined;
  #hasExited = false;
  #resolveExited: () => void = () => undefined;
  #task: PoolTask | undefined;
  #taskAcknowledged = false;
  #termination: { readonly reason: unknown } | undefined;
  #used = false;

  constructor(
    options: {
      readonly name: string;
      readonly resourceLimits?: ResourceLimits;
    },
    owner: ThreadSlotOwner
  ) {
    this.#owner = owner;
    this.#exited = new Promise((resolve) => {
      this.#resolveExited = resolve;
    });
    this.#worker = new Worker(THREAD_ENTRY_URL, options);
    this.#worker.on("error", (error: unknown) => this.#onError(error));
    this.#worker.on("exit", (code: number) => this.#onExit(code));
    this.#worker.on("message", (message: ThreadMessage) =>
      this.#onMessage(message)
    );
  }

  /** Whether a task currently occupies this thread. */
  get busy(): boolean {
    return this.#task !== undefined;
  }

  /** Whether the thread is idle, alive, and not being terminated. */
  get reusable(): boolean {
    return (
      this.#task === undefined &&
      this.#crash === undefined &&
      this.#termination === undefined &&
      !this.#hasExited &&
      this.#worker.threadId !== -1
    );
  }

  /** Forward a cooperative abort to `task` if it still runs here. */
  abort(task: PoolTask, reason: unknown): void {
    if (this.#task !== task) return;
    try {
      this.#worker.postMessage({
        reason: serializeError(reason),
        taskId: task.id,
        type: "abort",
      });
    } catch {
      // A thread that cannot receive the abort is terminated after the grace.
    }
  }

  run(task: PoolTask): void {
    this.#task = task;
    this.#taskAcknowledged = false;
    this.#worker.ref();
    try {
      this.#worker.postMessage(task.runMessage());
    } catch (error: unknown) {
      // Nothing reached the thread, so it remains healthy and reusable.
      this.#finishTask();
      task.fail(error);
      return;
    }
    task.begin(this);
  }

  /**
   * Terminate the thread and resolve once it has exited.
   *
   * A task still running here fails with `reason` once the thread is gone.
   */
  terminate(reason: unknown): Promise<void> {
    if (!this.#hasExited) {
      this.#termination ??= { reason };
      void this.#worker.terminate();
    }
    return this.#exited;
  }

  /** Fail the current task now and discard its still-running thread. */
  #abandon(error: unknown): void {
    const task = this.#task;
    this.#task = undefined;
    task?.fail(error);
    void this.terminate(error);
  }

  #finishTask(): void {
    this.#task = undefined;
    this.#used = true;
    this.#worker.unref();
    this.#owner.onIdle(this);
  }

  #onError(error: unknown): void {
    // An `error` is always followed by `exit`, and Node delivers every
    // message the thread posted before emitting `exit`. Settling the task
    // there keeps a result posted just before a crash from being lost.
    this.#crash ??= { error };
  }

  #onExit(code: number): void {
    this.#hasExited = true;
    const task = this.#task;
    this.#task = undefined;
    if (task !== undefined) this.#settleOrphan(task, code);
    this.#owner.onExit(
      this,
      this.#crash !== undefined || this.#termination === undefined
    );
    this.#resolveExited();
  }

  #onMessage(message: ThreadMessage): void {
    const task = this.#task;
    if (task === undefined || message.taskId !== task.id) return;
    switch (message.type) {
      case "error":
        this.#finishTask();
        task.fail(
          new WorkerThreadHandlerError(message.error.message, message.error)
        );
        return;
      case "log":
        this.#forward(() =>
          task.request.logger(
            message.level,
            message.message,
            message.attributes === undefined
              ? undefined
              : parseJsonObject(message.attributes)
          )
        );
        return;
      case "metadata":
        this.#forward(() =>
          task.request.setMetadata(message.key, parseJson(message.value))
        );
        return;
      case "output":
        this.#forward(() =>
          task.request.recordOutput(parseJson(message.output))
        );
        return;
      case "result": {
        let outcome: JsonValue | undefined;
        try {
          outcome =
            message.outcome === undefined
              ? undefined
              : parseJson(message.outcome);
        } catch (error: unknown) {
          this.#finishTask();
          task.fail(error);
          return;
        }
        this.#finishTask();
        task.succeed(outcome);
        return;
      }
      case "started":
        this.#taskAcknowledged = true;
        return;
    }
  }

  /** Settle a task whose thread exited underneath it. */
  #settleOrphan(task: PoolTask, code: number): void {
    if (this.#termination !== undefined) {
      task.fail(this.#termination.reason);
    } else if (!this.#taskAcknowledged && this.#used && task.requeueable) {
      // A thread reused from the idle set died before it could begin this
      // task, most likely from a previous handler's stray background work.
      task.requeue();
    } else if (this.#crash !== undefined) {
      task.fail(crashError(this.#crash.error));
    } else {
      task.fail(
        new WorkerThreadHandlerError(
          `worker thread exited unexpectedly with code ${code}`
        )
      );
    }
  }

  /**
   * Run a parent-side callback for a forwarded message.
   *
   * If it throws, the handler is still running in the thread, so the attempt
   * fails with that error and the thread is discarded rather than reused.
   */
  #forward(callback: () => void): void {
    try {
      callback();
    } catch (error: unknown) {
      this.#abandon(error);
    }
  }
}

class PoolTask implements WorkerThreadTaskHandle {
  readonly id: number;
  readonly request: WorkerThreadTaskRequest;
  readonly result: Promise<JsonValue | undefined>;
  readonly started: Promise<void>;
  readonly #pool: WorkerThreadPool;
  #rejectResult: (reason: unknown) => void = () => undefined;
  #rejectStarted: (reason: unknown) => void = () => undefined;
  #requeued = false;
  #resolveResult: (value: JsonValue | undefined) => void = () => undefined;
  #resolveStarted: () => void = () => undefined;
  #slot: ThreadSlot | undefined;
  #state: "queued" | "running" | "settled" = "queued";

  constructor(
    pool: WorkerThreadPool,
    request: WorkerThreadTaskRequest,
    id: number
  ) {
    this.#pool = pool;
    this.request = request;
    this.id = id;
    this.result = new Promise((resolve, reject) => {
      this.#resolveResult = resolve;
      this.#rejectResult = reject;
    });
    this.started = new Promise((resolve, reject) => {
      this.#resolveStarted = resolve;
      this.#rejectStarted = reject;
    });
    void this.result.catch(() => undefined);
    void this.started.catch(() => undefined);
  }

  /** Whether this running task may be retried once on another thread. */
  get requeueable(): boolean {
    return !this.#requeued && this.#state === "running";
  }

  async abort(reason: unknown, gracePeriodMs: number): Promise<boolean> {
    if (this.#state === "settled") return false;
    if (this.#state === "queued") {
      this.#pool.dequeue(this);
      this.fail(reason);
      return true;
    }
    const slot = this.#slot;
    if (slot === undefined) return false;
    slot.abort(this, reason);
    const settled = this.result.then(
      () => undefined,
      () => undefined
    );
    if (await settlesWithin(settled, gracePeriodMs)) return false;
    await slot.terminate(reason);
    await settled;
    return true;
  }

  /** Record that the run message reached `slot`. */
  begin(slot: ThreadSlot): void {
    this.#slot = slot;
    this.#state = "running";
    this.#resolveStarted();
  }

  fail(error: unknown): void {
    if (this.#state === "settled") return;
    this.#state = "settled";
    this.#rejectStarted(error);
    this.#rejectResult(error);
  }

  /** Return this task to the front of the queue, at most once. */
  requeue(): void {
    this.#requeued = true;
    this.#slot = undefined;
    this.#state = "queued";
    this.#pool.requeue(this);
  }

  runMessage(): RunMessage {
    return {
      args: this.request.args,
      execution: this.request.execution,
      exportName: this.request.exportName,
      job: this.request.job,
      moduleUrl: this.request.moduleUrl,
      taskId: this.id,
      type: "run",
    };
  }

  succeed(outcome: JsonValue | undefined): void {
    if (this.#state === "settled") return;
    this.#state = "settled";
    this.#resolveResult(outcome);
  }
}

function closedError(): LifecycleError {
  return new LifecycleError("worker thread executor is closed");
}

function crashError(error: unknown): WorkerThreadHandlerError {
  const serialized = serializeError(error);
  return new WorkerThreadHandlerError(serialized.message, serialized);
}

/** Resolve whether `promise` settles before an unreferenced timeout. */
async function settlesWithin(
  promise: Promise<void>,
  milliseconds: number
): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<false>((resolve) => {
    timer = setTimeout(() => resolve(false), milliseconds);
    timer.unref();
  });
  try {
    return await Promise.race([promise.then(() => true), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

// Development and tests load the TypeScript entry point through Node's
// built-in type stripping; published builds load the compiled JavaScript.
const THREAD_ENTRY_URL = new URL(
  import.meta.url.endsWith(".ts") ? "./thread.ts" : "./thread.js",
  import.meta.url
);
