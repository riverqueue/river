/**
 * Limits one client's insert notifications to one per queue per fetch
 * cooldown, like River for Go's per-client insert notification limiter.
 * Producers fetch a queue at most once per cooldown, so a notification sent
 * sooner would wake nothing new; a job whose notification is suppressed is
 * found by the producer's next fetch.
 */
export class InsertNotifyLimiter {
  readonly #cooldownMs: number;
  /** When each queue's last notification was allowed. */
  readonly #lastSent = new Map<string, number>();
  readonly #now: () => number;

  constructor(cooldownMs: number, now: () => number = () => performance.now()) {
    this.#cooldownMs = cooldownMs;
    this.#now = now;
  }

  /**
   * The queues among `queues`, each listed once, that are due an insert
   * notification, recording that one was sent for each of them now.
   */
  allow(queues: Iterable<string>): readonly string[] {
    const now = this.#now();
    const allowed: string[] = [];
    for (const queue of new Set(queues)) {
      const lastSent = this.#lastSent.get(queue);
      if (lastSent !== undefined && now - lastSent <= this.#cooldownMs) {
        continue;
      }
      this.#lastSent.set(queue, now);
      allowed.push(queue);
    }
    return allowed;
  }
}
