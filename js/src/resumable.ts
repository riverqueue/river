import type { Client } from "./client.js";
import { LifecycleError, ValidationError } from "./errors.js";
import type { JobRow } from "./job.js";
import type { JsonObject, JsonValue } from "./json.js";
import { toJsonObject, toJsonValue } from "./json.js";

const CURSOR_KEY = "river:resumable_cursor";
const STEP_KEY = "river:resumable_step";

/**
 * Save resumable progress now, atomically with `tx`, instead of when the
 * attempt ends. If it rolls back, earlier unpersisted progress may be replayed.
 *
 * Commit the transaction before the step callback resolves, and let transaction
 * and commit errors reject the callback. Resolving it marks the step complete
 * even if an enclosing transaction later rolls back.
 */
export interface ResumableCheckpointOptions<Transaction = unknown> {
  readonly cursor?: JsonValue;
  readonly tx: Transaction;
}

interface ResumableFinish {
  readonly error: Error | null;
  readonly metadata: JsonObject;
}

/** Attempt-scoped resumable-step coordinator exposed on WorkContext. */
export class Resumable {
  readonly #allNames = new Set<string>();
  readonly #client: Client;
  readonly #cursors = new Map<string, JsonValue>();
  readonly #job: JobRow;
  readonly #pendingCursorSteps = new Set<string>();
  readonly #resumeStep: string | null;
  #completedStep: string | null = null;
  #cursorsDirty = false;
  #failure: Error | null = null;
  #resumeMatched: boolean;
  #stepName: string | null = null;

  /** @internal Constructed by the runtime for one attempt. */
  constructor(client: Client, job: JobRow) {
    this.#client = client;
    this.#job = job;
    const resumeStep = job.metadata[STEP_KEY];
    this.#resumeStep =
      typeof resumeStep === "string" && resumeStep.length > 0
        ? resumeStep
        : null;
    this.#resumeMatched = this.#resumeStep === null;
    const cursors = job.metadata[CURSOR_KEY];
    if (Array.isArray(cursors)) {
      throw new ValidationError(
        "river:resumable_cursor must be an object when present"
      );
    }
    if (
      cursors !== null &&
      typeof cursors === "object" &&
      !Array.isArray(cursors)
    ) {
      for (const [name, cursor] of Object.entries(
        cursors as Readonly<Record<string, JsonValue | undefined>>
      )) {
        if (cursor !== undefined) this.#cursors.set(name, cursor);
      }
    }
    // A step marker may accompany stale cursors written by older clients.
    // Require only targets from cursor-only checkpoints.
    if (this.#resumeStep === null) {
      for (const name of this.#cursors.keys())
        this.#pendingCursorSteps.add(name);
    }
  }

  /**
   * Run a named step unless an earlier failed attempt completed it.
   * Await steps sequentially. Steps cannot be nested inside other step callbacks.
   */
  async step(
    name: string,
    callback: () => PromiseLike<void> | void
  ): Promise<void> {
    if (!this.#begin(name, false)) return;
    try {
      await callback();
      if (this.#failure !== null) return;
      this.#completedStep = name;
    } catch (cause: unknown) {
      this.#failure = stepError(name, cause);
      throw cause;
    } finally {
      this.#stepName = null;
    }
  }

  /**
   * Run a named cursor step with the last JSON cursor, or null initially.
   * Steps cannot be nested inside other step callbacks. A saved cursor-only
   * step must be reached before the attempt completes or another step checkpoints.
   */
  async stepWithCursor(
    name: string,
    callback: (cursor: JsonValue | null) => PromiseLike<void> | void
  ): Promise<void> {
    if (!this.#begin(name, true)) return;
    try {
      this.#pendingCursorSteps.delete(name);
      await callback(this.#cursors.get(name) ?? null);
      if (this.#failure !== null) return;
      this.#completedStep = name;
      if (this.#cursors.delete(name)) this.#cursorsDirty = true;
    } catch (cause: unknown) {
      this.#failure = stepError(name, cause);
      throw cause;
    } finally {
      this.#stepName = null;
    }
  }

  /** Record the JSON cursor for the currently running cursor step. */
  setCursor(cursor: JsonValue): void {
    if (this.#stepName === null) {
      throw new LifecycleError(
        "resumable cursor can only be set inside stepWithCursor()"
      );
    }
    this.#cursors.set(this.#stepName, toJsonValue(cursor));
    this.#cursorsDirty = true;
  }

  /**
   * Persist the current step and optional cursor in a caller transaction.
   *
   * Commit the transaction before the step callback resolves, and let transaction
   * and commit errors reject the callback. Resolving it marks the step complete
   * even if an enclosing transaction later rolls back.
   */
  async checkpoint<Transaction>(
    options: ResumableCheckpointOptions<Transaction>
  ): Promise<JobRow> {
    if (this.#stepName === null) {
      throw new LifecycleError(
        "resumable checkpoint can only be set inside a resumable step"
      );
    }
    const pending = this.#pendingCursorStep();
    if (pending !== undefined) {
      throw new LifecycleError(
        `resumable checkpoint cannot precede saved cursor step ${JSON.stringify(pending)}`
      );
    }
    const cursors = new Map(this.#cursors);
    if (options.cursor !== undefined) {
      cursors.set(this.#stepName, toJsonValue(options.cursor));
    }
    const updated = await this.#client.jobs.update(
      this.#job.id,
      {
        metadata: {
          [STEP_KEY]: this.#stepName,
          [CURSOR_KEY]:
            cursors.size > 0 ? toJsonObject(Object.fromEntries(cursors)) : null,
        },
      },
      { tx: options.tx }
    );
    if (updated === null) {
      throw new LifecycleError(`running job ${this.#job.id} no longer exists`);
    }
    // The caller transaction owns this checkpoint, including its rollback.
    // Only subsequent progress belongs in deferred attempt metadata.
    this.#cursors.clear();
    for (const [name, cursor] of cursors) this.#cursors.set(name, cursor);
    this.#cursorsDirty = false;
    this.#completedStep = null;
    return updated;
  }

  /** @internal Resolve metadata to merge with the attempt completion. */
  finish(workerFailed: boolean): ResumableFinish {
    if (!workerFailed && !this.#resumeMatched && this.#failure === null) {
      this.#failure = new LifecycleError(
        `resumable step ${JSON.stringify(this.#resumeStep)} not found in worker`
      );
    }
    const pending = this.#pendingCursorStep();
    if (pending !== undefined) {
      if (!workerFailed && this.#failure === null) {
        this.#failure = new LifecycleError(
          `resumable cursor step ${JSON.stringify(pending)} not found in worker`
        );
      }
      // Preserve the original cursor-only checkpoint so restoring the missing
      // step can recover; later steps must not advance its skip boundary.
      return { error: this.#failure, metadata: {} };
    }
    if (!workerFailed && this.#failure === null) {
      return { error: null, metadata: {} };
    }
    return {
      error: this.#failure,
      metadata: this.#checkpointMetadata(),
    };
  }

  #begin(name: string, cursorStep: boolean): boolean {
    if (name.length === 0) {
      throw new ValidationError("resumable step name is empty");
    }
    if (this.#failure !== null) throw this.#failure;
    if (this.#stepName !== null) {
      this.#failure = new LifecycleError("resumable steps cannot be nested");
      throw this.#failure;
    }
    if (!cursorStep && this.#pendingCursorSteps.has(name)) {
      this.#failure = new LifecycleError(
        `resumable cursor step ${JSON.stringify(name)} requires a cursor step`
      );
      throw this.#failure;
    }
    if (this.#allNames.has(name)) {
      this.#failure = new ValidationError(
        `duplicate resumable step name ${JSON.stringify(name)}`
      );
      throw this.#failure;
    }
    this.#allNames.add(name);
    if (!this.#resumeMatched) {
      if (name !== this.#resumeStep) return false;
      this.#completedStep = name;
      this.#resumeMatched = true;
      if (!cursorStep || !this.#cursors.has(name)) return false;
    }
    this.#stepName = name;
    return true;
  }

  #checkpointMetadata(): JsonObject {
    const metadata: JsonObject = {};
    if (this.#completedStep !== null) metadata[STEP_KEY] = this.#completedStep;
    if (this.#cursorsDirty) {
      metadata[CURSOR_KEY] =
        this.#cursors.size > 0
          ? toJsonObject(Object.fromEntries(this.#cursors))
          : null;
    }
    return metadata;
  }

  #pendingCursorStep(): string | undefined {
    return [...this.#pendingCursorSteps].sort()[0];
  }
}

/** Construct an attempt-scoped resumable coordinator for first-party tooling. */
export function createResumable(client: Client, job: JobRow): Resumable {
  return new Resumable(client, job);
}

/** Finalize resumable state for first-party worker test helpers. */
export function finishResumable(
  resumable: Resumable,
  workerFailed: boolean
): ResumableFinish {
  return resumable.finish(workerFailed);
}

/**
 * A failed step's error, which fails the attempt. Like River for Go, it's the
 * step's own error, so the job records the step's message.
 */
function stepError(name: string, cause: unknown): Error {
  if (cause instanceof Error) return cause;
  return new LifecycleError(`resumable step ${JSON.stringify(name)} failed`, {
    cause,
  });
}
