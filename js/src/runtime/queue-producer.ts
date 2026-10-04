/**
 * The queue producer: one claim loop per configured queue that fills free
 * worker capacity while respecting the fetch cooldown and poll interval, and
 * a control loop that heartbeats configured queues and applies persisted
 * pause and resume commands.
 *
 * Each queue runs as a generation, `starting → running → draining →
 * stopped`, which reserves the queue's name until it has fully stopped: a
 * second add of the name fails, and a re-add after a removal waits for the
 * old generation. With a pilot, each generation has a producer session.
 */
import type { JobClaimResult, QueueRow } from "../driver.js";
import { ExtensionError, LifecycleError, ValidationError } from "../errors.js";
import { validateQueueName } from "../identifiers.js";
import {
  abortableDelay,
  LinkedAbortSignal,
  raceWithAbort,
  unrefTimeout,
} from "../internal/abort.js";
import {
  measuredDuration,
  millisecondsToDuration,
} from "../internal/duration.js";
import { deepFreezeJson, jsonValuesEqual, toJsonObject } from "../json.js";
import type {
  PilotDatabase,
  PilotProducer,
  ProducerStartContext,
} from "../pilot.js";
import type { AttemptRunner } from "./attempt-runner.js";
import type { RuntimeContext } from "./context.js";
import { backOffAfterFailure, retryDatabaseOperation } from "./context.js";
import {
  copyQueueMetadataText,
  queueMetadataText,
} from "../internal/queue-metadata-text.js";
import { describeError, isPermanentRuntimeError } from "./failures.js";
import { ClaimHandoffError, ProducerSession } from "./producer-session.js";
import type {
  QueueRuntimeDiagnostics,
  QueueSettings,
  ResolvedQueue,
} from "./settings.js";

/** Configuration for a {@link QueueProducer}. */
export interface QueueProducerOptions {
  /** How often persisted queue controls are polled. */
  readonly controlPollIntervalMs: number;
  /** The kinds claims are limited to, or empty to claim every kind. */
  readonly fetchKinds: readonly string[];
  /** How often configured queues' rows are refreshed. */
  readonly heartbeatIntervalMs: number;
  /** The pilot's producer sessions, when it has any. */
  readonly pilot?: {
    readonly database: PilotDatabase<unknown>;
    /** How often sessions report themselves alive. */
    readonly reportIntervalMs: number;
    readonly startProducer: (
      context: ProducerStartContext<unknown>
    ) => Promise<PilotProducer<unknown>>;
  };
}

/** One generation of a configured queue. */
interface QueueRuntime {
  /** Aborts once the generation stops claiming. */
  readonly abort: AbortController;
  config: Required<QueueSettings>;
  /** Settles once the generation drained and its session shut down. */
  drained: Promise<void> | undefined;
  /** Serializes claims and configuration changes. */
  gate: Promise<void>;
  lastClaimStartedAtMs: number;
  paused: boolean;
  /**
   * The metadata last offered to the session, accepted or not, so a
   * rejected value is offered and logged once, like River for Go.
   */
  offered: QueueRow["metadata"] | undefined;
  /** The stored text of {@link offered}. */
  offeredText: string | undefined;
  /**
   * The queue's settings parsed by the client's pilot, replaced
   * together with `config`.
   */
  pilotSettings: unknown;
  /** The latest persisted queue row the session accepted. */
  queue: QueueRow | undefined;
  /** The removal of this generation, once one began. */
  removal: Promise<boolean> | undefined;
  session: ProducerSession | undefined;
  /** Settles once the generation started, or failed to. */
  readonly started: Promise<void>;
  state: "draining" | "running" | "starting" | "stopped";
  task: Promise<void>;
  wake: (() => void) | null;
}

/** Claims jobs for the configured queues and hands them to the runner. */
export class QueueProducer {
  readonly #context: RuntimeContext;
  readonly #controlPollIntervalMs: number;
  readonly #fetchKinds: readonly string[];
  readonly #heartbeatIntervalMs: number;
  readonly #pilot: QueueProducerOptions["pilot"];
  /** Every generation, from the start of its start to the end of its drain. */
  readonly #queues = new Map<string, QueueRuntime>();
  readonly #runner: AttemptRunner;
  #wakeControl: (() => void) | null = null;

  constructor(
    context: RuntimeContext,
    runner: AttemptRunner,
    options: QueueProducerOptions
  ) {
    this.#context = context;
    this.#controlPollIntervalMs = options.controlPollIntervalMs;
    this.#fetchKinds = options.fetchKinds;
    this.#heartbeatIntervalMs = options.heartbeatIntervalMs;
    this.#pilot = options.pilot;
    this.#runner = runner;
  }

  /**
   * Start a queue added, already validated, while the runtime runs. A
   * queue being removed is replaced once its old generation stopped.
   */
  async add(name: string, queue: ResolvedQueue): Promise<void> {
    const validatedName = validateQueueName(name);
    const existing = this.#queues.get(validatedName);
    if (existing !== undefined) {
      if (existing.removal === undefined) {
        throw new ValidationError(
          `queue ${validatedName} is already configured`
        );
      }
      await existing.removal.catch(() => undefined);
      if (this.#queues.has(validatedName)) {
        throw new ValidationError(
          `queue ${validatedName} is already configured`
        );
      }
    }
    await this.start(validatedName, queue, true);
  }

  /** Apply a queue command already committed by this client. */
  applyCommittedControl(queue: QueueRow): void {
    const runtime = this.#queues.get(queue.name);
    if (runtime === undefined) return;
    runtime.paused = queue.pausedAt !== null;
    runtime.wake?.();
  }

  /** Each running queue's effective configuration and pause state. */
  diagnostics(): Readonly<Record<string, QueueRuntimeDiagnostics>> {
    return Object.fromEntries(
      [...this.#queues]
        .filter(([, runtime]) => runtime.state !== "starting")
        .map(([name, runtime]) => [
          name,
          {
            fetchCooldown: millisecondsToDuration(
              runtime.config.fetchCooldownMs
            ),
            maxWorkers: runtime.config.maxWorkers,
            paused: runtime.paused,
            pollInterval: millisecondsToDuration(runtime.config.pollIntervalMs),
          },
        ])
    );
  }

  /** Reload one queue's persisted controls after a control notification. */
  async refresh(name: string): Promise<void> {
    const runtime = this.#queues.get(name);
    if (runtime === undefined) return;
    let queue: QueueRow | null;
    try {
      queue = await this.#context.driver.queueGet(name);
    } catch (error: unknown) {
      if (isPermanentRuntimeError(error)) throw error;
      // The queue control poll applies the change on its next pass.
      this.#context.logger.warn("River queue control refresh failed", {
        error: describeError(error),
        queue: name,
      });
      return;
    }
    if (queue === null) return;
    await this.#applyQueueControl(runtime, queue);
  }

  /** Reload every queue's persisted controls. */
  async refreshAll(): Promise<void> {
    await Promise.all(
      [...this.#queues.keys()].map((name) => this.refresh(name))
    );
  }

  /**
   * Stop claiming a queue, drain its attempts, and shut its session down.
   * The name stays reserved until then, and a second removal joins the
   * first.
   */
  remove(name: string): Promise<boolean> {
    let validatedName: string;
    try {
      validatedName = validateQueueName(name);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    const runtime = this.#queues.get(validatedName);
    if (runtime === undefined) return Promise.resolve(false);
    runtime.removal ??= (async () => {
      const ran = await runtime.started.then(
        () => true,
        () => false
      );
      await this.#drain(runtime, new LifecycleError("River queue was removed"));
      if (this.#queues.get(validatedName) === runtime) {
        this.#queues.delete(validatedName);
      }
      if (!ran) return false;
      await this.#context.emit({
        at: this.#context.now(),
        kind: "queue_removed",
        queueName: validatedName,
      });
      return true;
    })();
    return runtime.removal;
  }

  /**
   * Poll persisted queue controls until the runtime stops claiming,
   * refreshing each configured queue's heartbeat row on its interval.
   */
  async runControlLoop(): Promise<void> {
    const signal = this.#context.claimSignal;
    const upsert = this.#context.driver.runtimeQueueUpsert?.bind(
      this.#context.driver
    );
    if (upsert === undefined) {
      throw new LifecycleError(
        "runtime backend cannot persist configured queue heartbeats"
      );
    }
    let nextHeartbeatAt = 0;
    let failures = 0;
    try {
      while (!signal.aborted) {
        const nowMs = this.#context.now().epochMilliseconds;
        const heartbeat = nowMs >= nextHeartbeatAt;
        try {
          for (const [name, runtime] of this.#queues) {
            if (runtime.state !== "running") continue;
            // A stop ends the wait for a connection during an outage. A read
            // is simply abandoned; a heartbeat only stops waiting to start,
            // so it never commits after the runtime stopped.
            const queue = heartbeat
              ? await upsert(name, this.#context.now(), { signal })
              : await raceWithAbort(
                  this.#context.driver.queueGet(name),
                  signal
                );
            if (queue === null) continue;
            await this.#applyQueueControl(runtime, queue);
          }
        } catch (error: unknown) {
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
          if (signal.aborted) return;
          failures += 1;
          await backOffAfterFailure(
            this.#context,
            "queue control poll",
            error,
            failures,
            signal
          );
          continue;
        }
        failures = 0;
        if (heartbeat) {
          nextHeartbeatAt = nowMs + this.#heartbeatIntervalMs;
        }
        await this.#waitForQueueControl(signal);
      }
    } catch (error: unknown) {
      if (!signal.aborted) throw error;
    }
  }

  /**
   * Persist a queue, load its controls, start its producer session, and
   * start its claim loop. Claims begin only after the controls load, so a
   * paused queue never claims. The name is reserved from the start; a
   * failed start releases it.
   */
  async start(
    name: string,
    resolved: ResolvedQueue,
    emit: boolean
  ): Promise<void> {
    let settle!: (error?: unknown) => void;
    const started = new Promise<void>((resolve, reject) => {
      settle = (error) => {
        if (error === undefined) resolve();
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- the start's own failure
        else reject(error);
      };
    });
    void started.catch(() => undefined);
    const runtime: QueueRuntime = {
      abort: new AbortController(),
      config: resolved.config,
      drained: undefined,
      gate: Promise.resolve(),
      lastClaimStartedAtMs: Number.NEGATIVE_INFINITY,
      offered: undefined,
      offeredText: undefined,
      paused: false,
      pilotSettings: resolved.pilotSettings,
      queue: undefined,
      removal: undefined,
      session: undefined,
      started,
      state: "starting",
      task: Promise.resolve(),
      wake: null,
    };
    this.#queues.set(name, runtime);
    let queue: QueueRow;
    try {
      queue = await this.#upsert(name);
      runtime.queue = queue;
      runtime.paused = queue.pausedAt !== null;
      runtime.session = await this.#startSession(name, runtime, queue);
    } catch (error: unknown) {
      runtime.state = "stopped";
      if (this.#queues.get(name) === runtime) this.#queues.delete(name);
      settle(error);
      throw error;
    }
    runtime.state = "running";
    runtime.task = this.#context.guard(this.#queueLoop(name, runtime));
    this.#context.trackTask(runtime.task);
    runtime.session?.startReports();
    settle();
    if (emit) {
      await this.#context.emit({
        at: this.#context.now(),
        kind: "queue_added",
        queue,
      });
    }
  }

  /**
   * Stop claiming every queue, drain their attempts, stop their sessions'
   * reports, and shut the sessions down.
   */
  async drainAll(): Promise<void> {
    await Promise.all(
      [...this.#queues.values()].map((runtime) =>
        this.#drain(runtime, new LifecycleError("River runtime is stopping"))
      )
    );
  }

  /**
   * Replace a running queue's configuration, already validated, and reload
   * its controls.
   */
  async update(name: string, resolved: ResolvedQueue): Promise<void> {
    const validatedName = validateQueueName(name);
    const runtime = this.#queues.get(validatedName);
    if (runtime?.state !== "running" || runtime.removal !== undefined) {
      throw new ValidationError(`queue ${validatedName} is not configured`);
    }
    const queue = await this.#upsert(validatedName);
    await this.#withGate(runtime, () => {
      if (runtime.state !== "running") {
        throw new LifecycleError(`queue ${validatedName} is stopping`);
      }
      // The session validates the whole configuration before River
      // applies any of it.
      runtime.session?.configurationChanged({
        maxWorkers: resolved.config.maxWorkers,
        metadataText: queueMetadataText(queue),
        queue: frozenQueueRow(queue),
        settings: resolved.pilotSettings,
      });
      runtime.config = resolved.config;
      runtime.pilotSettings = resolved.pilotSettings;
      runtime.offered = queue.metadata;
      runtime.offeredText = queueMetadataText(queue);
      runtime.queue = queue;
      runtime.paused = queue.pausedAt !== null;
    });
    runtime.wake?.();
    await this.#context.emit({
      at: this.#context.now(),
      kind: "queue_reconfigured",
      queue,
    });
  }

  /** Wake one queue's claim loop, if it is waiting. */
  wake(name: string): void {
    this.#queues.get(name)?.wake?.();
  }

  /** Wake every queue's claim loop. */
  wakeAll(): void {
    for (const runtime of this.#queues.values()) runtime.wake?.();
  }

  /** Wake the queue control poll, if it is waiting. */
  wakeControl(): void {
    this.#wakeControl?.();
  }

  async #applyQueueControl(
    runtime: QueueRuntime,
    queue: QueueRow
  ): Promise<void> {
    this.#offerPersistedQueue(runtime, queue);
    const paused = queue.pausedAt !== null;
    if (paused === runtime.paused) return;
    runtime.paused = paused;
    runtime.wake?.();
    await this.#context.emit({
      at: this.#context.now(),
      kind: paused ? "queue_paused" : "queue_resumed",
      queue,
    });
  }

  async #queueLoop(queue: string, runtime: QueueRuntime): Promise<void> {
    const active = new Set<Promise<void>>();
    const attempts = new Set<Promise<void>>();
    let claimFailures = 0;
    let databaseLikelyHasMore = false;
    const link = new LinkedAbortSignal([
      this.#context.claimSignal,
      runtime.abort.signal,
    ]);
    const signal = link.signal;
    try {
      while (!signal.aborted) {
        // Match River's producer: one claim may fill every currently
        // available worker slot. Completion ownership remains independently
        // bounded, so a large worker pool cannot create an unbounded
        // persistence backlog.
        const capacity = runtime.config.maxWorkers - active.size;
        // Like River for Go's producer after a full fetch, claim again as
        // soon as any worker slot frees, subject to the fetch cooldown.
        if (databaseLikelyHasMore && capacity <= 0 && active.size > 0) {
          await Promise.race(active);
          continue;
        }
        if (!runtime.paused && capacity > 0) {
          await this.#waitForFetchCooldown(runtime, signal);
          // Slots that freed during the cooldown join this claim, as Go
          // sizes a fetch when it starts.
          const limit = runtime.config.maxWorkers - active.size;
          if (limit <= 0) continue;
          runtime.lastClaimStartedAtMs = performance.now();
          const claimStartedAtMs = runtime.lastClaimStartedAtMs;
          // Cancellations of jobs not yet worked here that arrive while
          // this claim runs apply to the attempts it starts.
          const cancellations = this.#runner.watchCancellations();
          let claimedCount = 0;
          try {
            let claim: JobClaimResult;
            try {
              const params = {
                attemptedBy: this.#context.clientId,
                // Without `fetchOnlyKnownKinds`, River claims every kind in a
                // configured queue. Unknown kinds consume an attempt and
                // persist a compatible execution error instead of remaining
                // stranded indefinitely.
                kinds: this.#fetchKinds,
                queues: [{ limit, name: queue }],
              };
              const session = runtime.session;
              claim = await this.#withGate(runtime, () => {
                // A claim queued behind a configuration change doesn't start
                // once the queue stopped claiming.
                signal.throwIfAborted();
                return session === undefined
                  ? // A stop ends a wait for a connection, never a started
                    // claim.
                    this.#context.driver.jobClaim(params, { signal })
                  : session.claim(params, limit, signal, (id) =>
                      this.#runner.isActive(id)
                    );
              });
            } catch (error: unknown) {
              // Rows a session claimed against its contract are left to the
              // rescuer, and the runtime stops, even when the claim raced a
              // stop or the queue's removal.
              if (error instanceof ClaimHandoffError) {
                this.#context.logger.error(
                  "River producer's claim broke its contract; the runtime stops",
                  { error: describeError(error), queue }
                );
                throw error;
              }
              // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
              if (signal.aborted) throw error;
              claimFailures += 1;
              await backOffAfterFailure(
                this.#context,
                "job claim",
                error,
                claimFailures,
                signal,
                { queue }
              );
              continue;
            }
            claimFailures = 0;
            this.#context.emitMetric({
              duration: measuredDuration(performance.now() - claimStartedAtMs),
              name: "job_get_available_duration",
              queue,
            });
            // Every claimed job, including one whose row couldn't be decoded,
            // is now running and needs an attempt, started in claim order
            // like River for Go. An undecodable job isn't worked; its attempt
            // fails with the decode error.
            const claimed = claim.jobs.map((job) => ({
              decodeError: claim.decodeErrors?.get(job.id),
              job,
            }));
            this.#context.emitMetric({
              count: claimed.length,
              name: "job_get_available_count",
              queue,
            });
            for (const { decodeError, job } of claimed) {
              let releaseCapacity!: () => void;
              const capacityReleased = new Promise<void>((resolve) => {
                let released = false;
                releaseCapacity = () => {
                  if (released) return;
                  released = true;
                  resolve();
                };
              });
              const execution = this.#runner.run(
                job,
                releaseCapacity,
                decodeError,
                cancellations.cancelled(job.id)
              );
              const attempt = execution.finally(() => {
                attempts.delete(attempt);
                releaseCapacity();
                // The attempt ended and its outcome went to the completer.
                runtime.session?.jobFinished(job);
              });
              attempts.add(attempt);
              void attempt.catch((error: unknown) => this.#context.fail(error));
              const slot = Promise.race([attempt, capacityReleased]).finally(
                () => {
                  active.delete(slot);
                }
              );
              void slot.catch(() => undefined);
              active.add(slot);
            }
            claimedCount = claimed.length;
            databaseLikelyHasMore = claimed.length === limit;
          } finally {
            cancellations.end();
          }
          if (databaseLikelyHasMore) continue;
          if (claimedCount > 0) {
            await this.#waitForQueue(runtime, signal);
            continue;
          }
        }

        databaseLikelyHasMore = false;
        if (active.size >= runtime.config.maxWorkers) {
          await Promise.race(active);
        } else {
          await this.#waitForQueue(runtime, signal);
        }
      }
    } catch (error: unknown) {
      if (!signal.aborted || error instanceof ClaimHandoffError) throw error;
    } finally {
      // Every attempt settles, and reports its finished job, before the
      // generation drains further; a failed attempt already failed the
      // runtime.
      await Promise.allSettled(attempts);
      link[Symbol.dispose]();
    }
  }

  /**
   * Drain a generation once: stop its claims, wait for its attempts, stop
   * its session's reports, and shut the session down.
   */
  #drain(runtime: QueueRuntime, reason: LifecycleError): Promise<void> {
    runtime.drained ??= (async () => {
      await runtime.started.catch(() => undefined);
      if (runtime.state === "stopped") return;
      runtime.state = "draining";
      runtime.abort.abort(reason);
      runtime.wake?.();
      // A failed loop already failed the runtime.
      await runtime.task.catch(() => undefined);
      const session = runtime.session;
      if (session !== undefined) {
        await session.stopReports().catch((error: unknown) => {
          this.#context.logger.warn("River producer reports failed", {
            error: describeError(error),
          });
        });
        await session.shutdown();
      }
      runtime.state = "stopped";
    })();
    return runtime.drained;
  }

  /**
   * Offer a running generation's session a persisted queue row whose
   * metadata changed, between claims. A session that rejects it keeps its
   * configuration, and River logs why.
   */
  #offerPersistedQueue(runtime: QueueRuntime, queue: QueueRow): void {
    const session = runtime.session;
    const previous = runtime.queue;
    if (session === undefined) {
      runtime.queue = queue;
      return;
    }
    // A row read before a later update applied is stale.
    if (previous !== undefined && isOlderRow(queue, previous)) return;
    const text = queueMetadataText(queue);
    if (
      previous !== undefined &&
      jsonValuesEqual(previous.metadata, queue.metadata) &&
      queueMetadataText(previous) === text
    ) {
      runtime.offered = previous.metadata;
      runtime.offeredText = text;
      const kept = { ...queue, metadata: previous.metadata };
      copyQueueMetadataText(queue, kept);
      runtime.queue = kept;
      return;
    }
    // A rejected value is offered, and logged, once.
    if (
      runtime.offered !== undefined &&
      jsonValuesEqual(runtime.offered, queue.metadata) &&
      runtime.offeredText === text
    ) {
      return;
    }
    runtime.offered = queue.metadata;
    runtime.offeredText = text;
    void this.#withGate(runtime, () => {
      if (runtime.state !== "running") return;
      if (runtime.queue !== undefined && isOlderRow(queue, runtime.queue)) {
        return;
      }
      try {
        session.configurationChanged({
          maxWorkers: runtime.config.maxWorkers,
          metadataText: text,
          queue: frozenQueueRow(queue),
          settings: runtime.pilotSettings,
        });
        runtime.queue = queue;
      } catch (error: unknown) {
        this.#context.logger.error(
          "River producer rejected the queue's persisted configuration",
          { error: describeError(error), queue: queue.name }
        );
      }
    });
  }

  /** Start a generation's producer session, when the pilot has one. */
  async #startSession(
    name: string,
    runtime: QueueRuntime,
    queue: QueueRow
  ): Promise<ProducerSession | undefined> {
    const pilot = this.#pilot;
    if (pilot === undefined) return undefined;
    const producer: unknown = await pilot.startProducer(
      Object.freeze({
        clientId: this.#context.clientId,
        database: pilot.database,
        maxWorkers: runtime.config.maxWorkers,
        metadataText: queueMetadataText(queue),
        queue,
        settings: runtime.pilotSettings,
        // One per queue generation, living as long as its session.
        // eslint-disable-next-line no-restricted-properties
        signal: AbortSignal.any([
          this.#context.claimSignal,
          runtime.abort.signal,
        ]),
      })
    );
    if (typeof producer !== "object" || producer === null) {
      throw new ExtensionError(
        `startProducer for queue ${JSON.stringify(name)} returned no producer`
      );
    }
    return new ProducerSession({
      context: this.#context,
      database: pilot.database,
      producer,
      queue: name,
      reportIntervalMs: pilot.reportIntervalMs,
    });
  }

  /** Run `operation` once no claim or configuration change is running. */
  #withGate<T>(
    runtime: QueueRuntime,
    operation: () => PromiseLike<T> | T
  ): Promise<T> {
    const run = runtime.gate.then(operation);
    runtime.gate = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  async #upsert(name: string) {
    const upsert = this.#context.driver.runtimeQueueUpsert?.bind(
      this.#context.driver
    );
    if (upsert === undefined) {
      throw new LifecycleError(
        "runtime backend cannot persist configured queue heartbeats"
      );
    }
    return retryDatabaseOperation(
      () => upsert(name, this.#context.now()),
      this.#context.claimSignal
    );
  }

  async #waitForFetchCooldown(
    runtime: QueueRuntime,
    signal: AbortSignal
  ): Promise<void> {
    const remaining =
      runtime.config.fetchCooldownMs -
      (performance.now() - runtime.lastClaimStartedAtMs);
    if (remaining > 0) await abortableDelay(Math.ceil(remaining), signal);
  }

  #waitForQueue(runtime: QueueRuntime, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const cancel = unrefTimeout(
        done,
        jitteredPollInterval(
          runtime.config.pollIntervalMs,
          this.#context.random
        )
      );
      const onAbort = () => {
        cleanup();
        reject(signal.reason);
      };
      function cleanup() {
        cancel();
        signal.removeEventListener("abort", onAbort);
        runtime.wake = null;
      }
      function done() {
        cleanup();
        resolve();
      }
      runtime.wake = done;
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  /** Wait for the queue control poll interval or an explicit wake-up. */
  #waitForQueueControl(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const cancel = unrefTimeout(done, this.#controlPollIntervalMs);
      const onAbort = () => {
        cleanup();
        reject(signal.reason);
      };
      const cleanup = () => {
        cancel();
        signal.removeEventListener("abort", onAbort);
        if (this.#wakeControl === done) this.#wakeControl = null;
      };
      function done() {
        cleanup();
        resolve();
      }
      this.#wakeControl = done;
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }
}

/**
 * A queue's poll interval plus a random jitter of up to a tenth of it, and
 * at least 10 ms, like River for Go's producer, so producers don't poll in
 * lockstep after a pause.
 */
function jitteredPollInterval(
  intervalMs: number,
  random: () => number
): number {
  return intervalMs + Math.floor(random() * Math.max(intervalMs / 10, 10));
}

/** A queue row the session can keep but not change. */
function frozenQueueRow(queue: QueueRow): QueueRow {
  const frozen = Object.freeze({
    ...queue,
    metadata: deepFreezeJson(toJsonObject(queue.metadata)),
  });
  copyQueueMetadataText(queue, frozen);
  return frozen;
}

/** Whether a queue row was written before another. */
function isOlderRow(queue: QueueRow, than: QueueRow): boolean {
  return Temporal.Instant.compare(queue.updatedAt, than.updatedAt) < 0;
}
