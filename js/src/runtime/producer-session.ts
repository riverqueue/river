/**
 * A pilot's producer session for one queue generation: River claims
 * through it, tells it about configuration changes and finished attempts,
 * keeps it alive at the report interval, and shuts it down once the queue
 * drained, like River for Go's producer does with its pilot.
 */
import type { JobClaimParams, JobClaimResult } from "../driver.js";
import { ExtensionError } from "../errors.js";
import type { JobRow } from "../job.js";
import type {
  PilotDatabase,
  PilotProducer,
  ProducerConfiguration,
} from "../pilot.js";
import { checkClaimResult } from "./claim-result.js";
import type { RuntimeContext } from "./context.js";
import { describeError } from "./failures.js";
import { isContractViolation, runInterceptor } from "./pilot-operations.js";

/** Bound on one keep-alive, like River for Go's producer reports. */
const KEEP_ALIVE_TIMEOUT_MS = 10_000;
/** The most initial jitter before the first keep-alive. */
const KEEP_ALIVE_JITTER_MS = 1_000;
/** Sessions silent for longer than this are stale, as in River for Go. */
const STALE_PRODUCER_RETENTION_MS = 5 * 60_000;
/** Shutdown deadlines of River for Go's four shutdown attempts. */
const SHUTDOWN_TIMEOUTS_MS = [100, 500, 2_500, 12_500] as const;

/** A claim result that broke the session's contract. */
export class ClaimHandoffError extends ExtensionError {}

/** Wraps one {@link PilotProducer} with River's rules for calling it. */
export class ProducerSession {
  readonly #context: RuntimeContext;
  readonly #database: PilotDatabase<unknown>;
  readonly #producer: PilotProducer<unknown>;
  readonly #queue: string;
  readonly #reportIntervalMs: number;
  readonly #reports = new AbortController();
  #reporting: Promise<void> | undefined;
  #shutDown = false;

  constructor(options: {
    readonly context: RuntimeContext;
    readonly database: PilotDatabase<unknown>;
    readonly producer: PilotProducer<unknown>;
    readonly queue: string;
    readonly reportIntervalMs: number;
  }) {
    this.#context = options.context;
    this.#database = options.database;
    this.#producer = options.producer;
    this.#queue = options.queue;
    this.#reportIntervalMs = options.reportIntervalMs;
  }

  /** Whether the session claims jobs itself. */
  get claims(): boolean {
    return this.#producer.claim !== undefined;
  }

  /**
   * Claim through the session, checking the rows it hands off before River
   * works any. A {@link ClaimHandoffError} means the session broke its
   * contract, and the runtime must stop.
   */
  async claim(
    params: JobClaimParams,
    limit: number,
    retrySignal: AbortSignal,
    isActive: (id: bigint) => boolean
  ): Promise<JobClaimResult> {
    const claim = this.#producer.claim?.bind(this.#producer);
    if (claim === undefined) {
      return this.#context.driver.jobClaim(params, { signal: retrySignal });
    }
    let result: JobClaimResult;
    try {
      result = await runInterceptor<
        JobClaimResult,
        [options: { readonly tx: unknown }]
      >({
        invoke: (next) =>
          claim(
            Object.freeze({
              attemptedBy: params.attemptedBy,
              database: this.#database,
              kinds: params.kinds,
              limit,
              queue: this.#queue,
              retrySignal,
              signal: this.#context.runSignal,
            }),
            next
          ),
        mode: "optional",
        operation: "claim",
        replacement: (value) => value as JobClaimResult,
        snapshot: (value) => value,
        standard: async (options) => {
          const tx = (options as { readonly tx?: unknown } | undefined)?.tx;
          if (tx === undefined) {
            throw new ClaimHandoffError(
              "a producer's claim must pass its transaction to next({ tx })"
            );
          }
          return this.#context.driver.jobClaim(params, { tx });
        },
      });
    } catch (error: unknown) {
      if (error instanceof ClaimHandoffError) throw error;
      if (isContractViolation(error)) {
        throw new ClaimHandoffError((error as Error).message, { cause: error });
      }
      throw error;
    }
    return validateHandoff(
      result,
      this.#queue,
      params.attemptedBy,
      limit,
      isActive
    );
  }

  /**
   * Offer the session a new configuration. Throws, leaving the previous
   * one in place, when the session rejects it.
   */
  configurationChanged(configuration: ProducerConfiguration): void {
    if (this.#shutDown) return;
    this.#producer.configurationChanged?.(Object.freeze({ ...configuration }));
  }

  /** Report a claimed job's finished attempt, once. */
  jobFinished(job: JobRow): void {
    if (this.#shutDown) return;
    try {
      this.#producer.jobFinished?.(job);
    } catch (error: unknown) {
      this.#context.logger.error("River producer failed to finish a job", {
        error: describeError(error),
        jobId: job.id.toString(10),
        queue: this.#queue,
      });
    }
  }

  /** Release the session, with River for Go's four bounded attempts. */
  async shutdown(): Promise<void> {
    const shutdown = this.#producer.shutdown?.bind(this.#producer);
    this.#shutDown = true;
    if (shutdown === undefined) return;
    for (const [index, timeoutMs] of SHUTDOWN_TIMEOUTS_MS.entries()) {
      const timeout = this.#context.timer.timeout(
        timeoutMs,
        () =>
          new ExtensionError(
            `producer shutdown attempt timed out after ${timeoutMs} ms`
          )
      );
      try {
        // A deadline only aborts the signal; the attempt must settle before
        // River tries again.
        await shutdown({ signal: timeout.signal });
        return;
      } catch (error: unknown) {
        this.#context.logger.error("River producer shutdown failed", {
          attempt: index + 1,
          error: describeError(error),
          queue: this.#queue,
          timeoutMs,
        });
      } finally {
        timeout.dispose();
      }
    }
    this.#context.logger.warn(
      "River producer failed to shut down cleanly after all attempts",
      { queue: this.#queue }
    );
  }

  /** Start keep-alives: an initial jitter, then one per report interval. */
  startReports(): void {
    if (this.#producer.keepAlive === undefined) return;
    this.#reporting ??= this.#reportLoop(this.#reports.signal);
  }

  /** Stop keep-alives and wait for one in flight. */
  async stopReports(): Promise<void> {
    this.#reports.abort();
    await this.#reporting;
  }

  async #reportLoop(signal: AbortSignal): Promise<void> {
    const keepAlive = this.#producer.keepAlive?.bind(this.#producer);
    if (keepAlive === undefined) return;
    const timer = this.#context.timer;
    try {
      await timer.delay(
        Math.floor(this.#context.random() * KEEP_ALIVE_JITTER_MS),
        signal
      );
      while (!signal.aborted) {
        // Reports keep a fixed rate, like River for Go's ticker; a slow one
        // delays the next, and they never overlap.
        const startedAt = timer.now();
        const timeout = timer.timeout(
          KEEP_ALIVE_TIMEOUT_MS,
          () => new ExtensionError("producer keep-alive timed out")
        );
        try {
          await keepAlive({
            // One per report interval, so `AbortSignal.any` stays cheap, and
            // the signal still aborts with the session after the report.
            // eslint-disable-next-line no-restricted-properties
            signal: AbortSignal.any([signal, timeout.signal]),
            staleBefore: this.#context
              .now()
              .subtract({ milliseconds: STALE_PRODUCER_RETENTION_MS }),
          });
        } catch (error: unknown) {
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
          if (!signal.aborted) {
            this.#context.logger.error("River producer keep-alive failed", {
              error: describeError(error),
              queue: this.#queue,
            });
          }
        } finally {
          timeout.dispose();
        }
        await timer.delay(
          Math.max(0, this.#reportIntervalMs - (timer.now() - startedAt)),
          signal
        );
      }
    } catch (error: unknown) {
      if (!signal.aborted) throw error;
    }
  }
}

/**
 * Check rows a session claimed before River works any: at most `limit`,
 * each running in `queue`, owned by `attemptedBy`, listed once, and not
 * already being worked. A fallback row of a job River couldn't decode may
 * lack fields; those it has must match.
 */
function validateHandoff(
  result: unknown,
  queue: string,
  attemptedBy: string,
  limit: number,
  isActive: (id: bigint) => boolean
): JobClaimResult {
  // Annotated so each call narrows like a throw.
  const fail: (reason: string) => never = (reason) => {
    throw new ClaimHandoffError(
      `a producer's claim for queue ${JSON.stringify(queue)} returned ${reason}`,
      { details: { queue } }
    );
  };
  const { checked, rows } = checkClaimResult(result, fail);
  if (rows.length > limit) fail(`${rows.length} jobs for a limit of ${limit}`);
  const seen = new Set<bigint>();
  for (const { job, partial } of rows) {
    if (typeof job !== "object" || job === null) fail("a missing job row");
    const row = job as Partial<JobRow>;
    const id = row.id;
    if (typeof id !== "bigint") fail("a job without an ID");
    if (seen.has(id)) fail(`job ${id} twice`);
    seen.add(id);
    if (isActive(id)) fail(`job ${id}, which this client is already working`);
    const attempt = row.attempt;
    if (
      !(typeof attempt === "number" && attempt >= 1) &&
      !(partial && attempt === undefined)
    ) {
      fail(`job ${id}, which has no attempt`);
    }
    if (row.state !== "running" && !(partial && row.state === undefined)) {
      fail(`job ${id}, which isn't running`);
    }
    if (row.queue !== queue && !(partial && row.queue === "")) {
      fail(`job ${id} of another queue`);
    }
    const owner = row.attemptedBy?.at(-1);
    if (owner !== attemptedBy && !(partial && owner === undefined)) {
      fail(`job ${id}, which another client claimed`);
    }
  }
  return checked;
}
