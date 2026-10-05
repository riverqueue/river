/** Identifies the background task that caused a supervised runtime failure. */
export class BackgroundTaskError extends Error {
  readonly taskName: string;

  constructor(taskName: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "BackgroundTaskError";
    this.taskName = taskName;
  }
}

export interface SupervisedTaskOptions {
  /** Allow a finite task to resolve while the supervisor remains active. */
  allowCompletion?: boolean;
}

/**
 * Owns River background tasks and turns detached failures into one observable
 * runtime failure.
 */
export class TaskSupervisor {
  readonly completed: Promise<void>;
  readonly signal: AbortSignal;

  #abortController = new AbortController();
  #completedReject!: (reason: unknown) => void;
  #completedResolve!: () => void;
  #fatalError: BackgroundTaskError | undefined;
  #stopping = false;
  #tasks = new Map<string, Promise<void>>();

  constructor(parentSignal?: AbortSignal) {
    this.signal = this.#abortController.signal;
    this.completed = new Promise<void>((resolve, reject) => {
      this.#completedResolve = resolve;
      this.#completedReject = reject;
    });

    // Consumers observe the same promise, but attaching a rejection handler
    // here prevents a fatal task from becoming an unhandled rejection before
    // a RunHandle has a chance to await it.
    void this.completed.catch(() => undefined);

    if (parentSignal) {
      if (parentSignal.aborted) {
        this.#stopping = true;
        this.#abortController.abort(parentSignal.reason);
        this.#completedResolve();
      } else {
        parentSignal.addEventListener(
          "abort",
          () => void this.stop(parentSignal.reason),
          { once: true }
        );
      }
    }
  }

  get activeTaskNames(): readonly string[] {
    return [...this.#tasks.keys()].sort();
  }

  get stopping(): boolean {
    return this.#stopping;
  }

  /** Start a task whose entire lifetime is owned by this supervisor. */
  start(
    name: string,
    task: (signal: AbortSignal) => Promise<void>,
    options: SupervisedTaskOptions = {}
  ): void {
    if (name.length === 0) throw new TypeError("task name must not be empty");
    if (this.#tasks.has(name)) {
      throw new Error(`background task already exists: ${name}`);
    }
    if (this.#stopping || this.#fatalError !== undefined) {
      throw new Error("cannot start a task after supervisor shutdown");
    }

    let promise: Promise<void>;
    try {
      promise = Promise.resolve(task(this.signal));
    } catch (error: unknown) {
      promise = Promise.reject(error);
    }
    this.#tasks.set(name, promise);
    void promise.then(
      () => this.#taskResolved(name, options.allowCompletion === true),
      (error: unknown) => this.#taskRejected(name, error)
    );
  }

  /** Abort all tasks and wait for each one to settle. */
  async stop(
    reason: unknown = new Error("River runtime stopped")
  ): Promise<void> {
    if (this.#fatalError !== undefined) {
      await Promise.allSettled(this.#tasks.values());
      throw this.#fatalError;
    }

    if (!this.#stopping) {
      this.#stopping = true;
      this.#abortController.abort(reason);
    }

    await Promise.allSettled(this.#tasks.values());
    // A task can fail while the others settle.
    this.#throwIfFailed();
    this.#completedResolve();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.stop();
  }

  #fail(name: string, error: unknown): void {
    if (this.#fatalError !== undefined || this.#stopping) return;

    this.#fatalError = new BackgroundTaskError(
      name,
      `background task failed: ${name}`,
      { cause: error }
    );
    this.#abortController.abort(this.#fatalError);
    this.#completedReject(this.#fatalError);
  }

  #taskRejected(name: string, error: unknown): void {
    this.#tasks.delete(name);
    this.#fail(name, error);
    this.#resolveStopped();
  }

  #taskResolved(name: string, allowCompletion: boolean): void {
    this.#tasks.delete(name);
    if (!allowCompletion && !this.#stopping && !this.signal.aborted) {
      this.#fail(name, new Error("task exited before runtime shutdown"));
    }
    this.#resolveStopped();
  }

  #throwIfFailed(): void {
    if (this.#fatalError !== undefined) throw this.#fatalError;
  }

  #resolveStopped(): void {
    if (
      this.#stopping &&
      this.#fatalError === undefined &&
      this.#tasks.size === 0
    ) {
      this.#completedResolve();
    }
  }
}
