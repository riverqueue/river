/**
 * Leased pool connections that fail fast once node-postgres reports a broken
 * socket, plus the abort helper the driver's pooled operations share.
 */
import type { Notification as NodePgNotification, PoolClient } from "pg";

/** A pool client with the listener overloads River attaches while leasing it. */
export type PgListeningClient = PoolClient & {
  off(event: "error", listener: (error: Error) => void): PgListeningClient;
  off(
    event: "notification",
    listener: (message: NodePgNotification) => void
  ): PgListeningClient;
  on(event: "error", listener: (error: Error) => void): PgListeningClient;
  on(
    event: "notification",
    listener: (message: NodePgNotification) => void
  ): PgListeningClient;
  once(event: "end", listener: () => void): PgListeningClient;
};

/**
 * A pool client checked out for one operation. Once node-postgres reports a
 * connection error, pending {@link PgClientLease.race} calls reject and the
 * socket is destroyed instead of being returned to the pool.
 */
export class PgClientLease {
  readonly client: PgListeningClient;
  readonly #failure: Promise<never>;
  #failureError: Error | undefined;
  #failureReject!: (error: Error) => void;
  #released = false;

  /** Lease `client`, calling `onFailure` once on its first connection error. */
  constructor(client: PoolClient, onFailure?: (error: Error) => void) {
    this.client = client;
    this.#failure = new Promise<never>((_resolve, reject) => {
      this.#failureReject = reject;
    });
    void this.#failure.catch(() => undefined);
    this.#onFailure = (error: Error) => {
      if (this.#failureError !== undefined) return;
      this.#failureError = error;
      onFailure?.(error);
      this.destroy();
      this.#failureReject(error);
    };
    this.client.on("error", this.#onFailure);
  }

  /** Whether the connection has reported an error. */
  get failed(): boolean {
    return this.#failureError !== undefined;
  }

  /** Discard the connection instead of returning it to the pool. */
  destroy(): void {
    if (this.#released) return;
    this.#released = true;
    // Keep the listener through forced socket teardown: node-postgres may emit
    // a second connection error after `release(true)`.
    this.client.once("end", () => this.client.off("error", this.#onFailure));
    this.client.release(true);
  }

  /** Settle with `operation`, or reject as soon as the connection fails. */
  async race<T>(operation: PromiseLike<T> | T): Promise<T> {
    if (this.#failureError !== undefined) throw this.#failureError;
    return Promise.race([Promise.resolve(operation), this.#failure]);
  }

  /** Return a healthy connection to the pool. */
  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.client.off("error", this.#onFailure);
    this.client.release();
  }

  readonly #onFailure: (error: Error) => void;
}

/**
 * Settle with `promise`, or reject with `signal.reason` once the signal
 * aborts. The underlying work keeps running; only the wait ends.
 */
export async function abortablePromise<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined
): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) throw signal.reason;

  let rejectAborted: ((reason: unknown) => void) | undefined;
  const aborted = new Promise<never>((_resolve, reject) => {
    rejectAborted = reject;
  });
  const abort = (): void => rejectAborted?.(signal.reason);
  signal.addEventListener("abort", abort, { once: true });

  try {
    return await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener("abort", abort);
  }
}
