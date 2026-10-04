import { channel } from "node:diagnostics_channel";

import {
  type JobStuckError,
  SubscriptionLagError,
  ValidationError,
} from "./errors.js";
import type { JobRow } from "./job.js";
import type { QueueRow } from "./driver.js";
import type { LeaderTerm } from "./driver.js";
import type { MaintenanceServiceName } from "./services.js";
import type { EventLoopDelayObservation } from "./internal/event-loop-delay-monitor.js";

export type JobEventKind = JobEvent["kind"];

export type QueueEventKind = QueueEvent["kind"];

/** Fields shared by every River event. */
export interface RiverEventBase<Kind extends string = string> {
  /** When River observed the transition. */
  readonly at: Temporal.Instant;
  /** Discriminates the event type. */
  readonly kind: Kind;
}

/** A claimed job started an attempt. */
export interface JobStartedEvent extends RiverEventBase<"job_started"> {
  /** The job as of the transition. */
  readonly job: JobRow;
}

/** A job's attempt succeeded and the job is `completed`. */
export interface JobCompletedEvent extends RiverEventBase<"job_completed"> {
  /** The job as of the transition. */
  readonly job: JobRow;
}

/**
 * A job's attempt failed; the job is `retryable`, `available` (a
 * near-future retry), or `discarded` after its last attempt.
 */
export interface JobFailedEvent extends RiverEventBase<"job_failed"> {
  readonly error: unknown;
  /** The job as of the transition. */
  readonly job: JobRow;
}

/** A job was cancelled, by its handler or remotely. */
export interface JobCancelledEvent extends RiverEventBase<"job_cancelled"> {
  /** The attempt's error, when it failed as it was cancelled. */
  readonly error?: unknown;
  /** The job as of the transition. */
  readonly job: JobRow;
}

/** A job snoozed and will run again later without using an attempt. */
export interface JobSnoozedEvent extends RiverEventBase<"job_snoozed"> {
  /** The job as of the transition. */
  readonly job: JobRow;
}

/** A job's attempt stopped for shutdown and the job is available again. */
export interface JobInterruptedEvent extends RiverEventBase<"job_interrupted"> {
  /** The job as of the transition. */
  readonly job: JobRow;
}

/**
 * A finished attempt no longer owned its row (another process completed,
 * cancelled, or re-claimed the job), so its result was not persisted.
 */
export interface JobRaceEvent extends RiverEventBase<"job_race"> {
  /** The job as of the transition. */
  readonly job: JobRow;
}

/** An attempt stayed unsettled past its timeout plus stuck threshold. */
export interface JobStuckEvent extends RiverEventBase<"job_stuck"> {
  /** Which timeout and threshold the attempt exceeded. */
  readonly error: JobStuckError;
  /** The job as of the transition. */
  readonly job: JobRow;
}

export type JobEvent =
  | JobCancelledEvent
  | JobCompletedEvent
  | JobFailedEvent
  | JobInterruptedEvent
  | JobRaceEvent
  | JobSnoozedEvent
  | JobStartedEvent
  | JobStuckEvent;

export interface QueueEvent extends RiverEventBase<
  | "queue_added"
  | "queue_paused"
  | "queue_reconfigured"
  | "queue_resumed"
  | "queue_updated"
> {
  /** The queue as of the change. */
  readonly queue: QueueRow;
}

/** A queue was removed from this client. */
export interface QueueRemovedEvent extends RiverEventBase<"queue_removed"> {
  readonly queueName: string;
}

/** This client won or lost maintenance leadership. */
export interface LeaderEvent extends RiverEventBase<
  "leader_acquired" | "leader_lost"
> {
  /** The leadership term won or lost. */
  readonly leader: LeaderTerm;
}

/** A leader-owned maintenance service pass failed. */
export interface MaintenanceFailedEvent extends RiverEventBase<"maintenance_failed"> {
  /** The error the maintenance pass failed with. */
  readonly error: unknown;
  readonly service: MaintenanceServiceName;
}

/** A leader-owned maintenance service pass succeeded. */
export interface MaintenanceSucceededEvent extends RiverEventBase<"maintenance_succeeded"> {
  /** Rows the pass affected. */
  readonly count: number;
  readonly service: MaintenanceServiceName;
}

/** The event loop was delayed past the configured threshold. */
export interface EventLoopDelayEvent extends RiverEventBase<"runtime_event_loop_delay"> {
  /** The delay measured over the reporting interval. */
  readonly eventLoopDelay: EventLoopDelayObservation;
}

/**
 * A subscription dropped events because its consumer fell behind. Delivered
 * to every subscription regardless of its `kinds` filter.
 */
export interface SubscriptionLagEvent extends RiverEventBase<"subscription_lag"> {
  /** Events dropped since the previous lag event. */
  readonly dropped: number;
  /** Describes the lag, for logging. */
  readonly error: SubscriptionLagError;
}

/** Every observation River emits, discriminated by `kind`. */
export type RiverEvent =
  | EventLoopDelayEvent
  | JobEvent
  | LeaderEvent
  | MaintenanceFailedEvent
  | MaintenanceSucceededEvent
  | QueueEvent
  | QueueRemovedEvent
  | SubscriptionLagEvent;

export type RiverEventKind = RiverEvent["kind"];

/** Events a subscription filtered to `Kind` yields. */
export type SubscribedEvent<Kind extends RiverEventKind = RiverEventKind> =
  Extract<RiverEvent, { readonly kind: Kind }> | SubscriptionLagEvent;

/** Options for `client.subscribe`. */
export interface SubscribeOptions<
  Kind extends RiverEventKind = RiverEventKind,
> {
  /**
   * Maximum buffered events before the oldest are dropped and a
   * `subscription_lag` event reports how many. Defaults to 256.
   */
  readonly capacity?: number;
  /** Only deliver these kinds (plus `subscription_lag`). */
  readonly kinds?: readonly Kind[];
  /** Close the subscription when this signal aborts. */
  readonly signal?: AbortSignal;
}

interface SubscriptionWaiter<Event> {
  readonly resolve: (result: IteratorResult<Event>) => void;
}

/**
 * A bounded, explicitly disposable async iterable of events emitted after
 * their database transitions commit. Close it with `close()`, `using`,
 * `await using`, or by breaking out of a `for await` loop.
 */
export class EventSubscription<Event extends RiverEvent = RiverEvent>
  implements AsyncIterable<Event>, AsyncIterator<Event>, Disposable
{
  readonly #buffer: Event[] = [];
  readonly #capacity: number;
  readonly #kinds: ReadonlySet<RiverEventKind> | null;
  readonly #remove: () => void;
  readonly #signal: AbortSignal | undefined;
  readonly #waiters: SubscriptionWaiter<Event>[] = [];
  #closed = false;
  #dropped = 0;

  /** @internal Constructed by Client.subscribe. */
  constructor(remove: () => void, options: SubscribeOptions, capacity: number) {
    this.#remove = remove;
    this.#capacity = capacity;
    this.#kinds = options.kinds === undefined ? null : new Set(options.kinds);
    this.#signal = options.signal;
    options.signal?.addEventListener("abort", this.#onAbort, { once: true });
  }

  [Symbol.asyncIterator](): AsyncIterator<Event> {
    return this;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  /** Stop delivering events and release the subscription's listeners. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#dropped = 0;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#remove();
    this.#buffer.length = 0;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.resolve({ done: true, value: undefined });
    }
  }

  /** Wait for the next event, or `done` once closed. */
  next(): Promise<IteratorResult<Event>> {
    if (this.#dropped > 0) {
      const dropped = this.#dropped;
      this.#dropped = 0;
      return Promise.resolve({
        done: false,
        value: {
          at: Temporal.Now.instant(),
          dropped,
          error: new SubscriptionLagError(dropped),
          kind: "subscription_lag",
        } as Event,
      });
    }
    const event = this.#buffer.shift();
    if (event !== undefined) {
      return Promise.resolve({ done: false, value: event });
    }
    if (this.#closed) {
      return Promise.resolve({ done: true, value: undefined });
    }
    return new Promise((resolve) => this.#waiters.push({ resolve }));
  }

  /** Close the subscription; called when a `for await` loop exits early. */
  return(): Promise<IteratorResult<Event>> {
    this.close();
    return Promise.resolve({ done: true, value: undefined });
  }

  /** @internal */
  publish(event: RiverEvent): void {
    if (
      this.#closed ||
      (this.#kinds !== null && !this.#kinds.has(event.kind))
    ) {
      return;
    }
    const waiter = this.#waiters.shift();
    if (waiter !== undefined) {
      waiter.resolve({ done: false, value: event as Event });
      return;
    }
    if (this.#buffer.length === this.#capacity) {
      this.#buffer.shift();
      this.#dropped++;
    }
    this.#buffer.push(event as Event);
  }

  readonly #onAbort = () => this.close();
}

/** @internal Build a job event whose fields match its kind. */
export function jobEvent(
  kind: JobEventKind,
  at: Temporal.Instant,
  job: JobRow,
  error: unknown
): JobEvent {
  switch (kind) {
    case "job_cancelled":
      return error === undefined ? { at, job, kind } : { at, error, job, kind };
    case "job_failed":
      return { at, error, job, kind };
    case "job_stuck":
      return {
        at,
        error: error as JobStuckError,
        job,
        kind,
      };
    case "job_completed":
    case "job_interrupted":
    case "job_race":
    case "job_snoozed":
    case "job_started":
      return { at, job, kind };
  }
}

/** @internal Client-owned collection of bounded subscriptions. */
export class EventHub {
  readonly #subscriptions = new Set<EventSubscription>();

  close(): void {
    for (const subscription of [...this.#subscriptions]) subscription.close();
  }

  publish(event: RiverEvent): void {
    for (const subscription of this.#subscriptions) {
      subscription.publish(event);
    }
    riverEventChannel.publish(event);
  }

  subscribe<Kind extends RiverEventKind>(
    options: SubscribeOptions<Kind> = {}
  ): EventSubscription<SubscribedEvent<Kind>> {
    if (options.signal?.aborted) {
      throw options.signal.reason;
    }
    const capacity = options.capacity ?? 256;
    if (!Number.isSafeInteger(capacity) || capacity < 1) {
      throw new ValidationError(
        "subscription capacity must be a positive safe integer"
      );
    }
    const holder: { value?: EventSubscription<SubscribedEvent<Kind>> } = {};
    const subscription = new EventSubscription<SubscribedEvent<Kind>>(
      () => {
        if (holder.value !== undefined) {
          this.#subscriptions.delete(holder.value as EventSubscription);
        }
      },
      options,
      capacity
    );
    holder.value = subscription;
    this.#subscriptions.add(subscription as EventSubscription);
    return subscription;
  }
}

const riverEventChannel = channel("riverqueue:event");
