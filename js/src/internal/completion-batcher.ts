interface CompletionEntry<TItem, TResult> {
  accept: () => void;
  acceptReject: (reason: unknown) => void;
  accepted: boolean;
  acknowledged: boolean;
  item: TItem;
  key: string;
  persisted: boolean;
  released: boolean;
  serialKey: string;
  reject: (reason: unknown) => void;
  resolve: (result: TResult) => void;
}

/** How the batcher disposes of a batch whose persistence failed. */
export type CompletionFailureAction = "drop" | "requeue";

export interface CompletionBatcherOptions<TItem, TResult> {
  batchSize: number;
  flushIntervalMs: number;
  maxPendingItems?: number;
  /**
   * Observe completions dropped after a persistence failure. `count`
   * includes pending completions abandoned while draining.
   */
  onDrop?: (error: unknown, count: number) => void;
  /**
   * Decide what happens to a batch whose `persist` call rejected: `"requeue"`
   * returns it to the front of the queue for another attempt and `"drop"`
   * rejects its completions with {@link CompletionDroppedError}. When
   * omitted, every persistence failure is fatal to the batcher.
   */
  onPersistFailure?: (error: unknown, count: number) => CompletionFailureAction;
  persist: (
    items: readonly TItem[],
    signal: AbortSignal
  ) => Promise<ReadonlyMap<string, TResult>>;
}

/**
 * Rejection for a completion abandoned after persistence failed. The job row
 * is left untouched, so it stays `running` until the rescuer recovers it.
 */
export class CompletionDroppedError extends Error {
  constructor(cause: unknown) {
    super("completion was dropped after a persistence failure", { cause });
    this.name = "CompletionDroppedError";
  }
}

export interface CompletionSubmission<TResult> {
  /** Resolves once the bounded persistence queue owns the completion. */
  readonly accepted: Promise<void>;
  /** Releases queue ownership after post-commit observation settles. */
  readonly acknowledge: () => void;
  /** Resolves only after the completion is durably persisted. */
  readonly result: Promise<TResult>;
}

/**
 * Batches job outcomes with River's bounded two-way persistence policy.
 *
 * One query may run while another batch accumulates. A second concurrent query
 * starts only for a full batch. Partial batches wait until no query is active,
 * which bounds database pressure and preserves predictable hook/event timing.
 */
export class CompletionBatcher<TItem, TResult> {
  readonly batchSize: number;
  readonly flushIntervalMs: number;
  readonly maxPendingItems: number;

  #abortController = new AbortController();
  #acceptedCount = 0;
  #buffer: CompletionEntry<TItem, TResult>[] = [];
  #closePromise: Promise<void> | undefined;
  #closeResolve: (() => void) | undefined;
  #closing = false;
  #draining = false;
  /** The first fatal failure, boxed so even a thrown `undefined` counts. */
  #failure: { readonly error: unknown } | undefined;
  #flushRequested = false;
  #inFlight = 0;
  #inFlightSerialKeys = new Set<string>();
  #options: CompletionBatcherOptions<TItem, TResult>;
  #submittedKeys = new Set<string>();
  #timer: NodeJS.Timeout | undefined;
  #waiting: CompletionEntry<TItem, TResult>[] = [];

  constructor(options: CompletionBatcherOptions<TItem, TResult>) {
    if (!Number.isSafeInteger(options.batchSize) || options.batchSize < 1) {
      throw new RangeError(
        "completion batch size must be a positive safe integer"
      );
    }
    if (
      !Number.isSafeInteger(options.flushIntervalMs) ||
      options.flushIntervalMs < 0
    ) {
      throw new RangeError(
        "completion flush interval must be a non-negative safe integer"
      );
    }
    const maxPendingItems = options.maxPendingItems ?? options.batchSize * 4;
    if (!Number.isSafeInteger(maxPendingItems) || maxPendingItems < 1) {
      throw new RangeError(
        "completion maximum pending items must be a positive safe integer"
      );
    }

    this.batchSize = options.batchSize;
    this.flushIntervalMs = options.flushIntervalMs;
    this.maxPendingItems = maxPendingItems;
    this.#options = options;
  }

  get inFlightQueries(): number {
    return this.#inFlight;
  }

  get pendingItems(): number {
    return this.#acceptedCount;
  }

  enqueue(key: string, item: TItem, serialKey = key): Promise<TResult> {
    const submission = this.submit(key, item, serialKey);
    void submission.result.then(submission.acknowledge, submission.acknowledge);
    return submission.result;
  }

  /**
   * Submits a completion while exposing bounded acceptance separately from
   * durable persistence. Callers may release scarce worker capacity after
   * `accepted`, while a supervisor continues observing `result`.
   */
  submit(
    key: string,
    item: TItem,
    serialKey = key
  ): CompletionSubmission<TResult> {
    const invalid = this.#submissionError(key, serialKey);
    if (invalid !== undefined) return rejectedSubmission(invalid);

    let accept!: () => void;
    let acceptReject!: (reason: unknown) => void;
    let reject!: (reason: unknown) => void;
    let resolve!: (result: TResult) => void;
    const accepted = new Promise<void>((acceptedResolve, acceptedReject) => {
      accept = acceptedResolve;
      acceptReject = acceptedReject;
    });
    const result = new Promise<TResult>((resultResolve, resultReject) => {
      resolve = resultResolve;
      reject = resultReject;
    });
    // The runtime registers supervision immediately after acceptance. Keep a
    // synchronous persistence failure from becoming an unhandled rejection in
    // the small interval before that registration completes.
    void accepted.catch(() => undefined);
    void result.catch(() => undefined);

    const entry: CompletionEntry<TItem, TResult> = {
      accept,
      acceptReject,
      accepted: false,
      acknowledged: false,
      item,
      key,
      persisted: false,
      reject,
      released: false,
      resolve,
      serialKey,
    };
    this.#submittedKeys.add(key);
    this.#waiting.push(entry);
    this.#promoteWaiting();
    this.#advance();
    return {
      accepted,
      acknowledge: () => {
        if (entry.acknowledged) return;
        entry.acknowledged = true;
        if (entry.persisted) {
          this.#releaseEntry(entry);
          this.#advance();
        }
      },
      result,
    };
  }

  /** Rejects queued and in-flight completion waiters after a runtime failure. */
  abort(reason: unknown): void {
    this.#fail(reason);
  }

  /**
   * Stop requeueing failed batches. The next persistence failure drops its
   * batch together with every other completion not yet persisted, so a
   * shutdown during a database outage finishes in bounded time.
   */
  drain(): void {
    this.#draining = true;
  }

  /** Flush all accepted items and reject if persistence failed. */
  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;

    this.#closing = true;
    this.#draining = true;
    this.#flushRequested = true;
    this.#clearTimer();
    this.#closePromise = new Promise<void>((resolve) => {
      this.#closeResolve = resolve;
    });
    this.#advance();
    this.#resolveCloseIfDrained();
    return this.#closePromise.then(() => {
      if (this.#failure !== undefined) throw this.#failure.error;
    });
  }

  #advance(): void {
    this.#promoteWaiting();
    while (
      this.#eligibleCount(this.batchSize) >= this.batchSize &&
      this.#inFlight < 2
    ) {
      this.#dispatch(this.batchSize);
    }

    if (
      this.#buffer.length > 0 &&
      this.#inFlight === 0 &&
      (this.#flushRequested || this.#closing)
    ) {
      this.#dispatch(this.#buffer.length);
      this.#flushRequested = false;
    }

    if (this.#buffer.length > 0) this.#scheduleTimer();
    this.#resolveCloseIfDrained();
  }

  #clearTimer(): void {
    if (!this.#timer) return;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  #dispatch(count: number): void {
    const entries: CompletionEntry<TItem, TResult>[] = [];
    const remaining: CompletionEntry<TItem, TResult>[] = [];
    const selected = new Set<string>();
    for (const entry of this.#buffer) {
      if (
        entries.length >= count ||
        this.#inFlightSerialKeys.has(entry.serialKey) ||
        selected.has(entry.serialKey)
      ) {
        remaining.push(entry);
        continue;
      }
      selected.add(entry.serialKey);
      entries.push(entry);
    }
    if (entries.length === 0) return;
    this.#buffer = remaining;

    this.#clearTimer();
    this.#inFlight += 1;
    for (const entry of entries) {
      this.#inFlightSerialKeys.add(entry.serialKey);
    }
    const items = entries.map(({ item }) => item);
    let persistence: Promise<ReadonlyMap<string, TResult>>;
    try {
      persistence = Promise.resolve(
        this.#options.persist(items, this.#abortController.signal)
      );
    } catch (error: unknown) {
      persistence = Promise.reject(error);
    }

    void persistence
      .then(
        (results) => this.#settleBatch(entries, results),
        (error: unknown) => this.#handlePersistFailure(entries, error)
      )
      .catch((error: unknown) => {
        this.#fail(error);
        for (const entry of entries) entry.reject(this.#failure?.error);
      })
      .finally(() => {
        for (const entry of entries) {
          this.#inFlightSerialKeys.delete(entry.serialKey);
          if (this.#failure !== undefined) this.#releaseEntry(entry);
        }
        this.#inFlight -= 1;
        this.#advance();
      });
  }

  #settleBatch(
    entries: readonly CompletionEntry<TItem, TResult>[],
    results: ReadonlyMap<string, TResult>
  ): void {
    if (this.#failure !== undefined) {
      for (const entry of entries) entry.reject(this.#failure.error);
      return;
    }
    for (const entry of entries) {
      if (!results.has(entry.key)) {
        throw new Error(`completion result is missing key: ${entry.key}`);
      }
    }
    for (const entry of entries) {
      entry.persisted = true;
      entry.resolve(results.get(entry.key) as TResult);
      if (entry.acknowledged) this.#releaseEntry(entry);
    }
  }

  #handlePersistFailure(
    entries: readonly CompletionEntry<TItem, TResult>[],
    error: unknown
  ): void {
    if (this.#failure !== undefined) {
      for (const entry of entries) entry.reject(this.#failure.error);
      return;
    }
    const decide = this.#options.onPersistFailure;
    if (decide === undefined) throw error;
    if (this.#draining) {
      const pending = [...this.#buffer.splice(0), ...this.#waiting.splice(0)];
      this.#dropEntries([...entries, ...pending], error);
      return;
    }
    if (decide(error, entries.length) === "requeue") {
      this.#buffer.unshift(...entries);
      this.#flushRequested = true;
      return;
    }
    this.#dropEntries(entries, error);
  }

  #dropEntries(
    entries: readonly CompletionEntry<TItem, TResult>[],
    error: unknown
  ): void {
    if (entries.length === 0) return;
    const dropped = new CompletionDroppedError(error);
    for (const entry of entries) {
      entry.persisted = true;
      entry.acceptReject(dropped);
      entry.reject(dropped);
      if (entry.accepted) {
        this.#releaseEntry(entry);
      } else {
        this.#submittedKeys.delete(entry.key);
      }
    }
    this.#options.onDrop?.(error, entries.length);
  }

  #eligibleCount(limit: number): number {
    const selected = new Set<string>();
    for (const entry of this.#buffer) {
      if (
        this.#inFlightSerialKeys.has(entry.serialKey) ||
        selected.has(entry.serialKey)
      ) {
        continue;
      }
      selected.add(entry.serialKey);
      if (selected.size === limit) break;
    }
    return selected.size;
  }

  #fail(error: unknown): void {
    if (this.#failure !== undefined) return;

    this.#failure = { error };
    this.#closing = true;
    this.#clearTimer();
    this.#abortController.abort(error);
    for (const entry of this.#buffer.splice(0)) {
      entry.persisted = true;
      entry.reject(error);
      this.#releaseEntry(entry);
    }
    for (const entry of this.#waiting.splice(0)) {
      this.#submittedKeys.delete(entry.key);
      entry.acceptReject(error);
      entry.reject(error);
    }
  }

  #resolveCloseIfDrained(): void {
    if (
      !this.#closing ||
      this.#waiting.length > 0 ||
      this.#buffer.length > 0 ||
      this.#inFlight > 0 ||
      this.#acceptedCount > 0
    )
      return;
    this.#closeResolve?.();
  }

  #scheduleTimer(): void {
    if (this.#timer || this.#flushRequested || this.#closing) return;

    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#flushRequested = true;
      this.#advance();
    }, this.flushIntervalMs);
    this.#timer.unref();
  }

  #promoteWaiting(): void {
    while (
      this.#acceptedCount < this.maxPendingItems &&
      this.#waiting.length > 0 &&
      this.#failure === undefined
    ) {
      const entry = this.#waiting.shift() as CompletionEntry<TItem, TResult>;
      this.#acceptedCount += 1;
      entry.accepted = true;
      this.#buffer.push(entry);
      entry.accept();
    }
    if (this.#buffer.length > 0) this.#scheduleTimer();
  }

  #submissionError(key: string, serialKey: string): unknown {
    if (key.length === 0) return new TypeError("completion key is empty");
    if (this.#failure !== undefined) return this.#failure.error;
    if (this.#closing) return new Error("completion batcher is closing");
    if (this.#submittedKeys.has(key)) {
      return new Error(`duplicate completion key: ${key}`);
    }
    if (serialKey.length === 0) {
      return new TypeError("completion serial key is empty");
    }
    return undefined;
  }

  #releaseEntry(entry: CompletionEntry<TItem, TResult>): void {
    if (entry.released) return;
    entry.released = true;
    this.#acceptedCount -= 1;
    this.#submittedKeys.delete(entry.key);
  }
}

function rejectedSubmission<TResult>(
  error: unknown
): CompletionSubmission<TResult> {
  const accepted = Promise.reject(error);
  const result = Promise.reject<TResult>(error);
  void accepted.catch(() => undefined);
  void result.catch(() => undefined);
  return { accepted, acknowledge: () => undefined, result };
}
