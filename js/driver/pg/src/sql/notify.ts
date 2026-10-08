/** `NOTIFY` delivery and supervised `LISTEN` subscriptions. */
import { LinkedAbortSignal, quoteIdentifier } from "riverqueue/unstable-driver";
import { Buffer } from "node:buffer";
import type { Notification as NodePgNotification } from "pg";
import { ConfigurationError } from "riverqueue";
import type { RuntimeNotification } from "riverqueue/unstable-driver";
import type { PgDatabase, PgQueryable } from "../database.js";
import { POSTGRES_IDENTIFIER_MAX_BYTES } from "../database.js";
import {
  configurationError,
  databaseError,
  unsupportedError,
} from "../errors.js";
import { PG_EXACT_TYPES } from "../exact-types.js";
import { abortablePromise, PgClientLease } from "../lease.js";
import type { PgNotification, PgOperationOptions } from "../types.js";

/** Notifications buffered per connection; beyond it the oldest is dropped. */
const NOTIFICATION_QUEUE_CAPACITY = 1_024;

/** Idle time after which a LISTEN connection is pinged, like River's Go notifier. */
const LISTENER_PING_INTERVAL_MS = 5_000;

/**
 * Limit on connecting a LISTEN connection and subscribing its channels, like
 * the listener timeout of River's Go notifier. A pooled connection whose
 * socket went half-open would otherwise hang the subscription indefinitely.
 */
const LISTENER_SETUP_TIMEOUT_MS = 10_000;

/** Returned by `NotificationQueue.shift` when a connection sat idle. */
const LISTENER_IDLE: unique symbol = Symbol("listener idle");

/**
 * Send River for Go's insert notification, `{"queue": "..."}`, for each of
 * `queues`.
 */
export function notifyInsert(
  db: PgDatabase,
  queues: readonly string[],
  options?: PgOperationOptions
): Promise<void> {
  return notifyMany(
    db,
    "river_insert",
    queues.map((queue) => `{"queue": ${JSON.stringify(queue)}}`),
    options
  );
}

/** Send one or more Postgres notifications on a River topic. */
export async function notifyMany(
  db: PgDatabase,
  topic: string,
  payloads: readonly string[],
  options?: PgOperationOptions
): Promise<void> {
  if (payloads.length === 0) return;
  // A server without LISTEN/NOTIFY, like YugabyteDB by default, gets none.
  if (!(await db.capabilities(options)).supportsListenNotify) return;
  await db.query(
    "notifyMany",
    `
      SELECT pg_notify(
        concat(coalesce($1::text, current_schema()), '.', $2::text), payload
      )
      FROM unnest($3::text[]) AS payload
    `,
    [db.schemaName, topic, payloads],
    options
  );
}

/**
 * Yield namespaced Postgres notifications from one LISTEN connection.
 *
 * Notifications are hints only: callers must retain polling because NOTIFY
 * is not durable. An idle connection is pinged every five seconds, like
 * River's Go notifier. Connecting and subscribing are bounded by a
 * ten-second timeout, and `ready` runs once the channels are subscribed.
 * The iteration ends when `signal` aborts, and fails with the error when
 * connecting, subscribing, a ping, or the connection fails; the caller
 * decides whether to subscribe again, as the runtime does with backoff,
 * logging each failure like River for Go's notifier.
 */
export async function* listen(
  db: PgDatabase,
  topics: readonly string[],
  signal: AbortSignal,
  ready?: () => void,
  options: {
    readonly pingIntervalMs?: number;
    readonly setupTimeoutMs?: number;
  } = {}
): AsyncGenerator<PgNotification> {
  if (db.pool === null) {
    throw unsupportedError(
      "listen",
      "Postgres LISTEN requires constructing PgDriver with a Pool"
    );
  }
  if (topics.length === 0 || signal.aborted) return;
  const pingIntervalMs = options.pingIntervalMs ?? LISTENER_PING_INTERVAL_MS;
  const setupTimeoutMs = options.setupTimeoutMs ?? LISTENER_SETUP_TIMEOUT_MS;
  const pool = db.pool;

  let lease: PgClientLease | undefined;
  let queue: NotificationQueue | undefined;
  const setupTimeout = new AbortController();
  const setupTimer = setTimeout(() => {
    setupTimeout.abort(
      databaseError(
        "listen",
        `Postgres LISTEN connection setup did not finish within ${setupTimeoutMs} ms`
      )
    );
  }, setupTimeoutMs);
  setupTimer.unref();
  const setupLink = new LinkedAbortSignal([signal, setupTimeout.signal]);
  try {
    const setupSignal = setupLink.signal;
    const connecting = pool.connect();
    let client: Awaited<typeof connecting>;
    try {
      client = await abortablePromise(connecting, setupSignal);
    } catch (error: unknown) {
      // Discard a connection that arrives after setup gave up on it.
      connecting.then(
        (late) => {
          late.release(true);
        },
        () => undefined
      );
      throw error;
    }
    lease = new PgClientLease(client, (error) => queue?.fail(error));
    const connected = lease;
    const listeningClient = connected.client;
    const setupQuery = <T>(query: Promise<T>): Promise<T> =>
      abortablePromise(connected.race(query), setupSignal);
    const schema =
      db.schemaName ?? (await setupQuery(currentSchema(listeningClient)));
    const channels = topics.map((topic) => namespacedChannel(schema, topic));
    for (const channel of channels) {
      await setupQuery(
        listeningClient.query(`LISTEN ${quoteIdentifier(channel)}`)
      );
    }
    clearTimeout(setupTimer);

    const notificationQueue = new NotificationQueue(signal);
    queue = notificationQueue;
    const channelToTopic = new Map(
      channels.map((channel, index) => [channel, topics[index] as string])
    );
    const onNotification = (message: NodePgNotification): void => {
      const topic = channelToTopic.get(message.channel);
      if (topic !== undefined) {
        notificationQueue.push({ payload: message.payload ?? "", topic });
      }
    };
    listeningClient.on("notification", onNotification);
    ready?.();
    try {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
      while (!signal.aborted) {
        const notification = await notificationQueue.shift(pingIntervalMs);
        if (notification === null) break;
        if (notification === LISTENER_IDLE) {
          await pingListener(connected, channels, pingIntervalMs);
          continue;
        }
        yield notification;
      }
    } finally {
      // The pool client is destroyed below. Keep its error listener until
      // it becomes unreachable because node-postgres may emit a second
      // connection error while tearing down a forcibly terminated socket.
      listeningClient.off("notification", onNotification);
      notificationQueue.dispose();
    }
  } catch (error) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
    if (signal.aborted && !(error instanceof ConfigurationError)) return;
    throw error;
  } finally {
    setupLink[Symbol.dispose]();
    clearTimeout(setupTimer);
    lease?.destroy();
  }
}

/** Adapt namespaced backend hints to the common runtime notification SPI. */
export async function* runtimeNotificationSubscribe(
  db: PgDatabase,
  topics: readonly RuntimeNotification["topic"][],
  signal: AbortSignal,
  ready: () => void
): AsyncGenerator<RuntimeNotification> {
  const backendTopics = topics.map(runtimeTopicName);
  for await (const notification of listen(db, backendTopics, signal, ready)) {
    yield {
      payload: notification.payload,
      topic: runtimeTopic(notification.topic),
    };
  }
}

/** Convert control-topic notifications into attempt-owner cancellation hints. */
export async function* jobCancellationSubscribe(
  db: PgDatabase,
  attemptedBy: string,
  signal: AbortSignal,
  ready?: () => void
): AsyncGenerator<{ attemptedBy: string; id: bigint }> {
  for await (const notification of listen(
    db,
    ["river_control"],
    signal,
    ready
  )) {
    let value: unknown;
    try {
      value = JSON.parse(notification.payload);
    } catch {
      continue;
    }
    if (
      typeof value !== "object" ||
      value === null ||
      !("action" in value) ||
      value.action !== "cancel" ||
      !("job_id" in value)
    ) {
      continue;
    }
    try {
      const id = exactCancellationID(notification.payload, value.job_id);
      if (id !== null) yield { attemptedBy, id };
    } catch {
      // Ignore malformed notifications; polling remains the durable path.
    }
  }
}

/**
 * A bounded, deduplicating buffer between a LISTEN connection's
 * notification events and the async generator that yields them.
 */
class NotificationQueue {
  readonly #onAbort = (): void => this.close();
  readonly #signal: AbortSignal;
  readonly #keys = new Set<string>();
  #head = 0;
  #values: PgNotification[] = [];
  #failure: Error | undefined;
  #waiting:
    | {
        reject: (error: Error) => void;
        resolve: (value: PgNotification | null) => void;
      }
    | undefined;

  constructor(signal: AbortSignal) {
    this.#signal = signal;
    signal.addEventListener("abort", this.#onAbort, { once: true });
  }

  close(): void {
    this.#waiting?.resolve(null);
    this.#waiting = undefined;
  }

  fail(error: Error): void {
    this.#failure = error;
    this.#waiting?.reject(error);
    this.#waiting = undefined;
  }

  dispose(): void {
    this.#signal.removeEventListener("abort", this.#onAbort);
  }

  push(value: PgNotification): void {
    if (this.#signal.aborted) return;
    if (this.#waiting !== undefined) {
      this.#waiting.resolve(value);
      this.#waiting = undefined;
      return;
    }
    const key = `${value.topic}\0${value.payload}`;
    if (this.#keys.has(key)) return;
    if (this.#values.length - this.#head >= NOTIFICATION_QUEUE_CAPACITY) {
      const dropped = this.#values[this.#head++];
      if (dropped !== undefined) {
        this.#keys.delete(`${dropped.topic}\0${dropped.payload}`);
      }
    }
    this.#keys.add(key);
    this.#values.push(value);
    this.#compact();
  }

  /**
   * Take the next notification, `null` once closed, or {@link LISTENER_IDLE}
   * when none arrives within `idleMs`.
   */
  shift(idleMs: number): Promise<PgNotification | null | typeof LISTENER_IDLE> {
    if (this.#failure !== undefined) return Promise.reject(this.#failure);
    if (this.#signal.aborted) return Promise.resolve(null);
    const value =
      this.#head < this.#values.length ? this.#values[this.#head++] : undefined;
    if (value !== undefined) {
      this.#keys.delete(`${value.topic}\0${value.payload}`);
      this.#compact();
      return Promise.resolve(value);
    }
    this.#values = [];
    this.#head = 0;
    return new Promise((resolve, reject) => {
      const idle = setTimeout(() => {
        this.#waiting = undefined;
        resolve(LISTENER_IDLE);
      }, idleMs);
      idle.unref();
      this.#waiting = {
        reject: (error) => {
          clearTimeout(idle);
          reject(error);
        },
        resolve: (notification) => {
          clearTimeout(idle);
          resolve(notification);
        },
      };
    });
  }

  #compact(): void {
    if (this.#head < 512 || this.#head * 2 < this.#values.length) return;
    this.#values = this.#values.slice(this.#head);
    this.#head = 0;
  }
}

async function currentSchema(client: PgQueryable): Promise<string> {
  const result = await client.query<{ schema: string }>({
    text: "SELECT current_schema()::text AS schema",
    types: PG_EXACT_TYPES,
  });
  const schema = result.rows[0]?.schema;
  if (typeof schema !== "string" || schema.length === 0) {
    throw databaseError(
      "listen",
      "Postgres returned no current schema for LISTEN"
    );
  }
  return schema;
}

function exactCancellationID(payload: string, decoded: unknown): bigint | null {
  if (typeof decoded === "string" && /^-?(?:0|[1-9]\d*)$/.test(decoded)) {
    return BigInt(decoded);
  }
  if (typeof decoded !== "number") return null;
  const match = /"job_id"\s*:\s*(-?(?:0|[1-9]\d*))(?=\s*[,}])/.exec(payload);
  return match?.[1] === undefined ? null : BigInt(match[1]);
}

function namespacedChannel(schema: string, topic: string): string {
  const channel = `${schema}.${topic}`;
  if (topic.length === 0 || topic.includes("\0")) {
    throw configurationError(
      "listen",
      "Postgres notification topics must be non-empty and contain no NUL byte"
    );
  }
  if (Buffer.byteLength(channel, "utf8") > POSTGRES_IDENTIFIER_MAX_BYTES) {
    throw configurationError(
      "listen",
      `Postgres notification channel must not exceed ${POSTGRES_IDENTIFIER_MAX_BYTES} bytes`
    );
  }
  return channel;
}

/**
 * Round-trip a query on an idle LISTEN connection. A dead or half-open
 * connection fails or times out, so the caller reconnects and re-listens.
 *
 * The ping repeats `LISTEN` for the first channel, which is a no-op for a
 * channel already listened on. Unlike `SELECT 1` it leaves the session's
 * `pg_stat_activity.query` showing `LISTEN`, which operators and tools use to
 * identify notification connections.
 */
async function pingListener(
  lease: PgClientLease,
  channels: readonly string[],
  timeoutMs: number
): Promise<void> {
  const channel = channels[0];
  if (channel === undefined) return;
  const timeout = new AbortController();
  const timer = setTimeout(
    () => timeout.abort(new Error("LISTEN ping timed out")),
    timeoutMs
  );
  timer.unref();
  try {
    await abortablePromise(
      lease.race(lease.client.query(`LISTEN ${quoteIdentifier(channel)}`)),
      timeout.signal
    );
  } catch (cause: unknown) {
    throw databaseError(
      "listen",
      "Postgres LISTEN connection did not answer a ping",
      cause
    );
  } finally {
    clearTimeout(timer);
  }
}

function runtimeTopic(topic: string): RuntimeNotification["topic"] {
  switch (topic) {
    case "river_control":
      return "control";
    case "river_insert":
      return "insert";
    case "river_leadership":
      return "leadership";
    default:
      throw databaseError(
        "runtimeNotificationSubscribe",
        `unknown River notification topic ${JSON.stringify(topic)}`
      );
  }
}

function runtimeTopicName(topic: RuntimeNotification["topic"]): string {
  switch (topic) {
    case "control":
      return "river_control";
    case "insert":
      return "river_insert";
    case "leadership":
      return "river_leadership";
  }
}
