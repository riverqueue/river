/**
 * Peer attempts: jobs that a running attempt, their coordinator, claims
 * and completes alongside its own job, such as a group of related jobs it
 * works together.
 *
 * River owns each peer from the commit of the claim that took it until its
 * outcome settles, under the attempt that claimed it. A peer moves from
 * `claimed` to `preparing` once an outcome for it is accepted, to
 * `submitted` once the completer accepts that outcome, and to `settled`
 * once the completer persisted it; an outcome that fails before the
 * completer accepts it returns the peer to `claimed`. When the coordinator ends,
 * River stops accepting its peer operations, waits for those it accepted,
 * and completes every peer still `claimed` with an outcome of its own.
 * Peers don't take producer slots, and their producer never hears about
 * them, like the other jobs of a multi-job result in River for Go.
 */
import type { JobClaimResult } from "../driver.js";
import { ExtensionError, LifecycleError, ValidationError } from "../errors.js";
import type { RiverErrorHandler, WorkAttemptResult } from "../extensions.js";
import type { JobRow } from "../job.js";
import type { JsonObject } from "../json.js";
import { toJsonObject } from "../json.js";
import type { PeerClaimContext, PeerOutcome, PilotDatabase } from "../pilot.js";
import type { WorkAttemptContext } from "../worker.js";
import { normalizeWorkAttemptResult } from "./attempt-result.js";
import type {
  CompletionPipeline,
  CompletionTracking,
} from "./completion-pipeline.js";
import type { RuntimeContext } from "./context.js";
import { checkClaimResult } from "./claim-result.js";
import { canonicalError, describeError } from "./failures.js";
import type { RetryPolicy } from "./settings.js";
import type { AnyWorkContext, WorkOutputState } from "./work-context.js";
import {
  errorHandlerContext,
  normalizeOutput,
  publishWorkResult,
  snapshotWorkMetadata,
  workAttemptState,
} from "./work-context.js";

/** Collaborators of {@link PeerAttempts}. */
export interface PeerAttemptsOptions {
  readonly completions: CompletionPipeline;
  /** The pilot's database, in whose transactions peers are claimed. */
  readonly database: PilotDatabase<unknown>;
  readonly errorHandler: RiverErrorHandler | undefined;
  /** Whether this client works an ordinary attempt of job `id`. */
  readonly isWorking: (id: bigint) => boolean;
  /** Apply the client's argument transformers to a claimed row. */
  readonly transformJobArgs: (row: JobRow) => JobRow;
  /** The retry policy of the worker registered for a kind, if any. */
  readonly workerRetryPolicy: (kind: string) => RetryPolicy | undefined;
}

type PeerState = "claimed" | "preparing" | "settled" | "submitted";

/** One peer a coordinator owns. */
interface Peer {
  /** The row as claimed, which identifies the peer's attempt. */
  readonly claimed: JobRow;
  readonly ledger: PeerLedger;
  /** The row completions persist and events report, after transforms. */
  row: JobRow;
  state: PeerState;
}

/** A peer and the outcome accepted for it. */
interface PeerSubmission {
  readonly peer: Peer;
  readonly result: WorkAttemptResult;
}

/** The peers of one coordinating attempt, and its operations on them. */
class PeerLedger {
  /** Set once the coordinator ended; no new operation starts. */
  closed = false;
  /** Set once River stopped tracking the peers; see `abandon`. */
  released = false;
  /** The coordinating attempt's context. */
  readonly coordinator: AnyWorkContext;
  /** Operations accepted and not yet settled. */
  readonly operations = new Set<Promise<unknown>>();
  /** Peers by job ID, including settled ones. */
  readonly peers = new Map<bigint, Peer>();

  constructor(coordinator: AnyWorkContext) {
    this.coordinator = coordinator;
  }

  /** Accept `operation`: the coordinator's end waits for it to settle. */
  track<T>(operation: Promise<T>): Promise<T> {
    this.operations.add(operation);
    const untrack = () => this.operations.delete(operation);
    void operation.then(untrack, untrack);
    return operation;
  }
}

/** Claims and completes the peers of this runtime's attempts. */
export class PeerAttempts {
  readonly #completions: CompletionPipeline;
  readonly #context: RuntimeContext;
  readonly #database: PilotDatabase<unknown>;
  /** States of attempts that ended; they take no peer operations. */
  readonly #ended = new WeakSet<object>();
  readonly #errorHandler: RiverErrorHandler | undefined;
  readonly #isWorking: (id: bigint) => boolean;
  /** Each coordinator's ledger, by the attempt's state. */
  readonly #ledgers = new WeakMap<object, PeerLedger>();
  /** Peers claimed or being claimed and not yet settled, by job ID. */
  readonly #owned = new Map<bigint, PeerLedger>();
  readonly #transformJobArgs: (row: JobRow) => JobRow;
  readonly #workerRetryPolicy: (kind: string) => RetryPolicy | undefined;

  constructor(context: RuntimeContext, options: PeerAttemptsOptions) {
    this.#completions = options.completions;
    this.#context = context;
    this.#database = options.database;
    this.#errorHandler = options.errorHandler;
    this.#isWorking = options.isWorking;
    this.#transformJobArgs = options.transformJobArgs;
    this.#workerRetryPolicy = options.workerRetryPolicy;
  }

  /**
   * Stop tracking the peers of the attempt whose state is `state`, leaving
   * any without a settled outcome to the rescuer. For an attempt that ends
   * without {@link finish}.
   */
  abandon(state: object): void {
    this.#ended.add(state);
    const ledger = this.#ledgers.get(state);
    if (ledger === undefined) return;
    ledger.closed = true;
    ledger.released = true;
    this.#ledgers.delete(state);
    for (const id of ledger.peers.keys()) {
      if (this.#owned.get(id) === ledger) this.#owned.delete(id);
    }
  }

  /**
   * Claim peers of `attempt` with `claim`, in a transaction River commits,
   * and return the rows River will track, after argument transforms. A row
   * River can't decode or transform is completed as a failure instead.
   */
  claim(
    attempt: AnyWorkContext,
    run: (context: PeerClaimContext<unknown>) => Promise<JobClaimResult>
  ): Promise<readonly JobRow[]> {
    let ledger: PeerLedger;
    try {
      ledger = this.#openLedger(attempt);
      if (typeof run !== "function") {
        throw new ValidationError("a peer claim requires a callback");
      }
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    return ledger.track(this.#claim(ledger, run));
  }

  /**
   * Complete peers of `attempt` through the ordinary completion pipeline,
   * resolving once every outcome persisted.
   */
  complete(
    attempt: AnyWorkContext,
    outcomes: readonly PeerOutcome[]
  ): Promise<void> {
    let ledger: PeerLedger;
    let submissions: readonly PeerSubmission[];
    try {
      ledger = this.#openLedger(attempt);
      submissions = reserve(ledger, outcomes);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    return ledger.track(this.#submit(ledger, submissions));
  }

  /**
   * End the peers of the attempt whose state is `state`, once the attempt
   * settled: refuse its new peer operations, wait for the accepted ones,
   * then complete each peer still without an outcome. A peer of an attempt
   * the runtime interrupted (`abortReason` a `LifecycleError`) is
   * interrupted too; any other such peer fails.
   */
  async finish(state: object, abortReason: unknown): Promise<void> {
    // The attempt takes no peer operation from now on, even while its own
    // outcome persists.
    this.#ended.add(state);
    const ledger = this.#ledgers.get(state);
    if (ledger === undefined) return;
    ledger.closed = true;
    while (ledger.operations.size > 0) {
      await Promise.allSettled([...ledger.operations]);
    }
    const missing = [...ledger.peers.values()].filter(
      (peer) => peer.state === "claimed"
    );
    if (missing.length > 0) {
      const result: WorkAttemptResult =
        abortReason instanceof LifecycleError
          ? { error: abortReason, status: "cancelled" }
          : {
              error: new ExtensionError(
                `the attempt of job ${ledger.coordinator.job.id} ended without an outcome for this job`
              ),
              status: "failed",
            };
      for (const peer of missing) peer.state = "preparing";
      try {
        await this.#submit(
          ledger,
          missing.map((peer) => ({ peer, result }))
        );
      } catch (error: unknown) {
        this.#context.logger.error(
          "River failed to complete peers their attempt left without an outcome",
          {
            error: describeError(error),
            jobId: ledger.coordinator.job.id.toString(10),
            peers: missing.length,
          }
        );
      }
    }
    this.abandon(state);
  }

  /** Whether job `id` is a peer this runtime owns or is claiming. */
  owns(id: bigint): boolean {
    return this.#owned.has(id);
  }

  async #claim(
    ledger: PeerLedger,
    run: (context: PeerClaimContext<unknown>) => Promise<JobClaimResult>
  ): Promise<readonly JobRow[]> {
    // A soft stop doesn't end a running coordinator's claims: it keeps
    // claiming until its attempt ends or is cancelled, and the stop waits
    // for the peers it claims.
    const signal = ledger.coordinator.signal;
    const reserved: bigint[] = [];
    let claimed: JobClaimResult;
    try {
      signal.throwIfAborted();
      claimed = await this.#database.transaction(
        async (tx) => {
          const result: unknown = await run(Object.freeze({ signal, tx }));
          const checked = this.#check(ledger, result);
          // Reserved until the commit settles, so no other claim of this
          // client can take the same jobs meanwhile.
          for (const row of checked.jobs) {
            this.#owned.set(row.id, ledger);
            reserved.push(row.id);
          }
          return checked;
        },
        { signal }
      );
    } catch (error: unknown) {
      for (const id of reserved) {
        if (this.#owned.get(id) === ledger) this.#owned.delete(id);
      }
      throw error;
    }

    if (ledger.released) {
      // River stopped tracking the attempt's peers while the claim ran;
      // its rows are left to the rescuer.
      for (const id of reserved) {
        if (this.#owned.get(id) === ledger) this.#owned.delete(id);
      }
      throw new LifecycleError("peer operations require a running attempt");
    }
    // The claim committed: every row is this coordinator's peer now, even
    // when the coordinator was cancelled meanwhile.
    const failures: PeerSubmission[] = [];
    const record = (job: JobRow): Peer => {
      const peer: Peer = { claimed: job, ledger, row: job, state: "claimed" };
      ledger.peers.set(job.id, peer);
      return peer;
    };
    const rows: JobRow[] = [];
    for (const job of claimed.jobs) {
      const peer = record(job);
      const decodeError = claimed.decodeErrors?.get(job.id);
      if (decodeError !== undefined) {
        failures.push({
          peer,
          result: {
            error: new Error(
              `job row couldn't be decoded: ${decodeError.message}`,
              { cause: decodeError }
            ),
            status: "failed",
          },
        });
        continue;
      }
      try {
        peer.row = this.#transformJobArgs(peer.claimed);
        rows.push(peer.row);
      } catch (error: unknown) {
        failures.push({ peer, result: { error, status: "failed" } });
      }
    }
    if (failures.length > 0) {
      for (const { peer } of failures) peer.state = "preparing";
      await this.#submit(ledger, failures);
    }
    return Object.freeze(rows);
  }

  /**
   * Check a peer claim's result before it commits: each row running, on an
   * attempt of this client, listed once, and neither the coordinator's own
   * job nor a job this client already works, owns, or finished at that
   * attempt.
   */
  #check(ledger: PeerLedger, result: unknown): JobClaimResult {
    const fail: (reason: string) => never = (reason) => {
      throw new ExtensionError(`a peer claim returned ${reason}`);
    };
    const { checked, rows } = checkClaimResult(result, fail);
    const seen = new Set<bigint>();
    const clientId = this.#context.clientId;
    for (const { job, partial } of rows) {
      if (typeof job !== "object" || job === null) fail("a missing job row");
      const row = job as Partial<JobRow>;
      const id = row.id;
      if (typeof id !== "bigint") fail("a job without an ID");
      if (seen.has(id)) fail(`job ${id} twice`);
      seen.add(id);
      if (id === ledger.coordinator.job.id) {
        fail(`job ${id}, the claiming attempt's own job`);
      }
      if (this.#owned.has(id)) {
        fail(`job ${id}, which this client already works as a peer`);
      }
      if (this.#isWorking(id)) {
        fail(`job ${id}, which this client already works`);
      }
      const attempt = row.attempt;
      if (typeof attempt === "number" && attempt >= 1) {
        const earlier = ledger.peers.get(id);
        if (earlier !== undefined && attempt <= earlier.claimed.attempt) {
          fail(`job ${id} at attempt ${attempt}, which already ended here`);
        }
      } else if (!(partial && attempt === undefined)) {
        fail(`job ${id}, which has no attempt`);
      }
      if (row.state !== "running" && !(partial && row.state === undefined)) {
        fail(`job ${id}, which isn't running`);
      }
      const owner = row.attemptedBy?.at(-1);
      if (owner !== clientId && !(partial && owner === undefined)) {
        fail(`job ${id}, which another client claimed`);
      }
    }
    return checked;
  }

  /**
   * The ledger of the running attempt `attempt`, which must be this
   * client's and must not have ended.
   */
  #openLedger(attempt: AnyWorkContext): PeerLedger {
    // JavaScript callers may pass anything.
    const context: unknown = attempt;
    if (typeof context !== "object" || context === null) {
      throw new ValidationError("peer operations require an attempt's context");
    }
    if (attempt.client !== this.#context.client) {
      throw new ValidationError("the attempt belongs to another River client");
    }
    const state = workAttemptState(attempt);
    if (state === undefined || !state.active || this.#ended.has(state)) {
      throw new LifecycleError("peer operations require a running attempt");
    }
    let ledger = this.#ledgers.get(state);
    if (ledger === undefined) {
      ledger = new PeerLedger(attempt);
      this.#ledgers.set(state, ledger);
    }
    if (ledger.closed) {
      throw new LifecycleError("peer operations require a running attempt");
    }
    return ledger;
  }

  /**
   * Hand peer outcomes to the completer, running the error handler and
   * adding the coordinator's metadata first, like the coordinator's own
   * outcome. An outcome that fails before reaching the completer returns
   * its peer to `claimed`. Rejects with the first failure once every
   * outcome settled.
   */
  async #submit(
    ledger: PeerLedger,
    submissions: readonly PeerSubmission[]
  ): Promise<void> {
    const coordinator = ledger.coordinator;
    const sharedMetadata = snapshotWorkMetadata(coordinator);
    const persisting: Promise<void>[] = [];
    let failure: { readonly error: unknown } | undefined;
    const settle = async () => {
      for (const settled of await Promise.allSettled(persisting.splice(0))) {
        if (settled.status === "rejected")
          failure ??= { error: settled.reason };
      }
    };
    for (const { peer, result } of submissions) {
      let prepared: WorkAttemptResult;
      try {
        prepared = await this.#prepare(
          coordinator,
          peer.row,
          normalizeWorkAttemptResult(result),
          sharedMetadata
        );
      } catch (error: unknown) {
        peer.state = "claimed";
        failure ??= { error };
        continue;
      }
      persisting.push(
        this.#persist(peer, prepared, coordinator.execution.startedAt)
      );
      if (persisting.length >= this.#completions.maxPendingItems) {
        await settle();
      }
    }
    await settle();
    if (failure !== undefined) throw failure.error;
  }

  /**
   * Hand one outcome to the completer. Once the completer accepts it, it
   * owns the outcome and retries it until it settles; a rejection after
   * that leaves the peer to the rescuer. Ownership ends as the outcome
   * persists, before its event, so the job can be claimed again at once.
   */
  async #persist(
    peer: Peer,
    result: WorkAttemptResult,
    startedAt: Temporal.Instant
  ): Promise<void> {
    const tracking: CompletionTracking = {
      accepted: () => {
        peer.state = "submitted";
      },
      persisted: () => {
        peer.state = "settled";
        if (this.#owned.get(peer.claimed.id) === peer.ledger) {
          this.#owned.delete(peer.claimed.id);
        }
      },
    };
    // Like River for Go, a failed peer retries on its worker's policy.
    const retryPolicy = this.#workerRetryPolicy(peer.row.kind);
    try {
      await (result.status === "cancelled"
        ? this.#completions.persistAbort(
            peer.row,
            result.error,
            startedAt,
            result,
            tracking,
            retryPolicy
          )
        : this.#completions.persistResult(
            peer.row,
            result,
            startedAt,
            tracking,
            retryPolicy
          ));
    } catch (error: unknown) {
      // Not accepted: the peer still needs an outcome.
      if (peer.state === "preparing") peer.state = "claimed";
      throw error;
    }
  }

  async #prepare(
    coordinator: AnyWorkContext,
    job: JobRow,
    value: WorkAttemptResult,
    sharedMetadata: JsonObject
  ): Promise<WorkAttemptResult> {
    let result = value;
    const outputState: WorkOutputState = {};
    const peerContext: WorkAttemptContext = {
      client: coordinator.client,
      execution: coordinator.execution,
      job,
      logger: coordinator.logger,
      recordOutput: (output) => {
        outputState.output = normalizeOutput(output);
      },
      setMetadata: coordinator.setMetadata,
      signal: coordinator.signal,
    };
    if (result.status === "failed" && this.#errorHandler !== undefined) {
      try {
        const decision = await this.#errorHandler(
          errorHandlerContext(peerContext),
          result.error
        );
        if (
          decision?.cancel !== undefined &&
          typeof decision.cancel !== "boolean"
        ) {
          throw new ValidationError("errorHandler cancel must be a boolean");
        }
        if (decision?.cancel === true) result = { ...result, cancel: true };
      } catch (error: unknown) {
        this.#context.logger.error("River error handler failed", {
          error: canonicalError(error, this.#context.now()).error,
        });
      }
    }
    if ("output" in outputState) {
      result = { ...result, output: outputState.output };
    }
    if (Object.keys(sharedMetadata).length > 0) {
      result = {
        ...result,
        metadata: toJsonObject({
          ...(result.metadata ?? {}),
          ...sharedMetadata,
        }),
      };
    }
    publishWorkResult(peerContext, result);
    return result;
  }
}

/**
 * Accept one outcome for each of `outcomes`' peers, all or none: each job
 * must be a peer of `ledger`, identified by its ID, attempt, and attempting
 * client, have no outcome yet, and appear once.
 */
function reserve(
  ledger: PeerLedger,
  outcomes: readonly PeerOutcome[]
): readonly PeerSubmission[] {
  // JavaScript callers may pass anything.
  const values: unknown = outcomes;
  if (!Array.isArray(values)) {
    throw new ValidationError("peer outcomes must be an array");
  }
  const submissions: PeerSubmission[] = [];
  const seen = new Set<Peer>();
  for (const outcome of values as unknown[]) {
    if (typeof outcome !== "object" || outcome === null) {
      throw new ValidationError("a peer outcome must be an object");
    }
    const { job, result } = outcome as Partial<PeerOutcome>;
    const id: unknown = (job as Partial<JobRow> | undefined)?.id;
    const peer = typeof id === "bigint" ? ledger.peers.get(id) : undefined;
    if (job === undefined || peer === undefined) {
      throw new ExtensionError(
        `job ${String(id)} isn't a peer of the attempt completing it`
      );
    }
    if (
      job.attempt !== peer.claimed.attempt ||
      !Array.isArray(job.attemptedBy) ||
      job.attemptedBy.at(-1) !== peer.claimed.attemptedBy.at(-1)
    ) {
      throw new ExtensionError(
        `job ${peer.claimed.id} attempt ${String(job.attempt)} isn't the peer attempt ${peer.claimed.attempt} this attempt owns`
      );
    }
    if (seen.has(peer)) {
      throw new ExtensionError(`job ${peer.claimed.id} has two outcomes`);
    }
    seen.add(peer);
    if (peer.state !== "claimed") {
      throw new ExtensionError(`job ${peer.claimed.id} already has an outcome`);
    }
    const value: unknown = result;
    if (typeof value !== "object" || value === null) {
      throw new ValidationError("a peer outcome requires a result");
    }
    // A snapshot, which River validates before handing it over.
    submissions.push({ peer, result: { ...(value as WorkAttemptResult) } });
  }
  for (const { peer } of submissions) peer.state = "preparing";
  return submissions;
}
