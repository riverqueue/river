/**
 * Delivers events to `onEvent` hooks in order, off the job-completion path.
 *
 * A slow hook doesn't hold worker or completion capacity: events queue up
 * to `capacity`, and only a full queue makes the emitter wait. Hooks run one
 * event at a time, so every hook still observes events in emission order.
 */
export class EventDispatcher<Event> {
  readonly #capacity: number;
  readonly #deliver: (event: Event) => Promise<void>;
  readonly #queue: Event[] = [];
  #draining: Promise<void> | null = null;
  #spaceWaiters: (() => void)[] = [];

  constructor(deliver: (event: Event) => Promise<void>, capacity: number) {
    this.#capacity = capacity;
    this.#deliver = deliver;
  }

  /** Events queued but not yet delivered. */
  get pending(): number {
    return this.#queue.length;
  }

  /** Queue an event, waiting only while the queue is full. */
  async enqueue(event: Event): Promise<void> {
    while (this.#queue.length >= this.#capacity) {
      await new Promise<void>((resolve) => this.#spaceWaiters.push(resolve));
    }
    this.#queue.push(event);
    this.#draining ??= this.#run();
  }

  /** Resolve once every queued event has been delivered. */
  async drain(): Promise<void> {
    while (this.#draining !== null) await this.#draining;
  }

  async #run(): Promise<void> {
    try {
      for (;;) {
        const event = this.#queue.shift();
        if (event === undefined) return;
        this.#spaceWaiters.shift()?.();
        await this.#deliver(event);
      }
    } finally {
      this.#draining = null;
    }
  }
}
