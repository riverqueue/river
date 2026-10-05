/**
 * The notification pump: supervises the backend's notification streams,
 * waking queues on inserts, applying queue controls and cancellations, and
 * forwarding leadership resignation requests. Notifications are hints;
 * polling remains the durable path.
 */
import type { RuntimeNotification } from "../driver.js";
import { LifecycleError } from "../errors.js";
import type { AttemptRunner } from "./attempt-runner.js";
import type { RuntimeContext } from "./context.js";
import { backOffAfterFailure } from "./context.js";
import { describeError } from "./failures.js";
import {
  notificationCancellation,
  notificationLeaderResigned,
  notificationQueue,
  notificationRequestsLeadershipResignation,
} from "./notification-payloads.js";
import type { QueueProducer } from "./queue-producer.js";

/** Configuration for a {@link NotificationPump}. */
export interface NotificationPumpOptions {
  /** Another client resigned leadership. */
  readonly leaderResigned: () => void;
  readonly producer: QueueProducer;
  /** Resign leadership when another client asks, if this client leads. */
  readonly resignLeadership: () => PromiseLike<unknown> | undefined;
  readonly runner: AttemptRunner;
}

/** Subscribes to backend notifications for the runtime's lifetime. */
export class NotificationPump {
  readonly #context: RuntimeContext;
  readonly #leaderResigned: () => void;
  readonly #producer: QueueProducer;
  readonly #resignLeadership: () => PromiseLike<unknown> | undefined;
  readonly #runner: AttemptRunner;

  constructor(context: RuntimeContext, options: NotificationPumpOptions) {
    this.#context = context;
    this.#leaderResigned = options.leaderResigned;
    this.#producer = options.producer;
    this.#resignLeadership = options.resignLeadership;
    this.#runner = options.runner;
  }

  /**
   * Start the supported streams as tracked background tasks, unless the
   * runtime relies on polling alone (`listen` false). Resolves once the
   * runtime notification stream first becomes ready, so no insert made after
   * startup can be missed. Like River for Go's notifier, which fails its
   * client's start when it can't connect and listen, rejects with the
   * stream's error if it fails or ends before then; later failures are
   * logged and resubscribed with backoff.
   */
  async start(listen: boolean): Promise<void> {
    if (!listen) return;
    const driver = this.#context.driver;
    if (driver.runtimeNotificationSubscribe !== undefined) {
      const ready = Promise.withResolvers<undefined>();
      const task = this.#context.guard(
        this.#notificationLoop(() => ready.resolve(undefined))
      );
      this.#context.trackTask(task);
      await Promise.race([
        ready.promise,
        task.then(() => {
          throw new LifecycleError(
            "runtime notification stream ended before becoming ready"
          );
        }),
      ]);
    } else if (driver.jobCancellationSubscribe !== undefined) {
      this.#context.trackTask(
        this.#context.guard(this.#remoteCancellationLoop())
      );
    }
  }

  async #handleNotification(notification: RuntimeNotification): Promise<void> {
    switch (notification.topic) {
      case "insert": {
        const queue = notificationQueue(notification.payload);
        if (queue === null) {
          this.#producer.wakeAll();
        } else {
          this.#producer.wake(queue);
        }
        return;
      }
      case "control": {
        const cancellation = notificationCancellation(notification.payload);
        if (cancellation !== null) {
          this.#runner.cancelAttempt(cancellation, this.#context.clientId);
        }
        const queueName = notificationQueue(notification.payload);
        if (queueName === "*") {
          await this.#producer.refreshAll();
        } else if (queueName !== null) {
          await this.#producer.refresh(queueName);
        }
        return;
      }
      case "leadership": {
        if (notificationRequestsLeadershipResignation(notification.payload)) {
          await this.#resignLeadership();
          return;
        }
        // Like River for Go's elector, a client ignores its own resignation.
        const leaderId = notificationLeaderResigned(notification.payload);
        if (leaderId !== null && leaderId !== this.#context.clientId) {
          this.#leaderResigned();
        }
        return;
      }
    }
  }

  async #notificationLoop(ready: () => void): Promise<void> {
    const subscribe = this.#context.driver.runtimeNotificationSubscribe?.bind(
      this.#context.driver
    );
    if (subscribe === undefined) return;
    let subscribed = false;
    await this.#superviseSubscription(
      "runtime notification stream",
      (signal, streamReady) =>
        subscribe(["control", "insert", "leadership"], signal, streamReady),
      (notification) => this.#handleNotification(notification),
      () => {
        if (subscribed) {
          this.#recoverMissedNotifications();
        } else {
          subscribed = true;
          ready();
        }
      },
      () => subscribed
    );
  }

  /**
   * Notifications are hints and are lost while a stream reconnects. Poll every
   * queue and its persisted controls immediately so nothing waits for the
   * next polling interval, and check running attempts for cancellations.
   */
  #recoverMissedNotifications(): void {
    this.#producer.wakeAll();
    this.#producer.wakeControl();
    this.#recoverMissedCancellations();
  }

  /**
   * Cancel attempts whose cancellation notice was lost while a stream
   * reconnected. A failed check is logged; the attempt then finishes as if
   * its notice never arrived, as it would before this recovery.
   */
  #recoverMissedCancellations(): void {
    const recovery = this.#runner
      .recoverCancellations(this.#context.claimSignal)
      .catch((error: unknown) => {
        this.#context.logger.warn(
          "River could not check running jobs for missed cancellations",
          { error: describeError(error) }
        );
      });
    this.#context.trackTask(recovery);
  }

  async #remoteCancellationLoop(): Promise<void> {
    const subscribe = this.#context.driver.jobCancellationSubscribe?.bind(
      this.#context.driver
    );
    if (subscribe === undefined) return;
    let subscribed = false;
    await this.#superviseSubscription(
      "remote cancellation stream",
      (signal, ready) => subscribe(this.#context.clientId, signal, ready),
      (notice) => {
        this.#runner.cancelAttempt(notice.id, notice.attemptedBy);
      },
      () => {
        if (subscribed) {
          this.#recoverMissedCancellations();
        } else {
          subscribed = true;
        }
      }
    );
  }

  /**
   * Supervise a backend notification stream. A stream that fails or ends is
   * logged and resubscribed with capped backoff; configuration errors remain
   * fatal, and so is any failure while `resubscribes` returns false.
   * `connected` runs after every (re)subscription becomes ready.
   */
  async #superviseSubscription<T>(
    name: string,
    subscribe: (signal: AbortSignal, ready: () => void) => AsyncIterable<T>,
    handle: (item: T) => Promise<void> | void,
    connected: () => void,
    resubscribes: () => boolean = () => true
  ): Promise<void> {
    const signal = this.#context.claimSignal;
    let failures = 0;
    while (!signal.aborted) {
      let failure: unknown;
      try {
        for await (const item of subscribe(signal, () => {
          failures = 0;
          connected();
        })) {
          await handle(item);
        }
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
        if (signal.aborted) return;
        failure = new LifecycleError(`${name} ended unexpectedly`);
      } catch (error: unknown) {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
        if (signal.aborted) return;
        failure = error;
      }
      if (!resubscribes()) throw failure;
      failures += 1;
      try {
        await backOffAfterFailure(
          this.#context,
          name,
          failure,
          failures,
          signal
        );
      } catch (error: unknown) {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
        if (signal.aborted) return;
        throw error;
      }
    }
  }
}
