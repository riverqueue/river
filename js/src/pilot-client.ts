/**
 * The client base class of first-party companion packages, exported only
 * from `riverqueue/unstable-driver`.
 */
import type { ClientDriver } from "./driver.js";
import type { Client } from "./client.js";
import type { ClientOptions } from "./options.js";
import { RiverClient } from "./client.js";
import { createPilotBinding } from "./internal/driver-registry.js";
import type { QueueConfig } from "./options.js";
import type { PilotFactory } from "./pilot.js";

/**
 * Options of a {@link PilotClient}: a client's options, with queues that
 * may use the keys its pilot owns.
 */
export type PilotClientOptions<
  Transaction,
  Config extends QueueConfig = QueueConfig,
> = Omit<ClientOptions<Transaction>, "queues"> & {
  /** Queues this client works, keyed by name. */
  readonly queues?: Readonly<Record<string, Config>>;
};
import type { RunHandle } from "./runtime.js";

/**
 * A River client with a pilot attached. A companion package's client
 * extends {@link PilotClient}, and its public declarations name only an
 * interface extending `Client`, so applications never see the pilot.
 *
 * `Config` is the queue configuration the run handle accepts, including the
 * keys the pilot owns.
 *
 * Because an intercepted operation runs statements on a caller's
 * transaction across its interceptor's awaits, this client's operations and
 * its pilot's statements on one caller transaction run one at a time, in
 * arrival order, like node-postgres runs a client's queries. One started
 * from inside another, such as from an interceptor, runs inside it.
 */
export interface PilotClient<
  Transaction = unknown,
  Config extends QueueConfig = QueueConfig,
> extends Client<Transaction> {
  start(): Promise<RunHandle<Config>>;
}

/**
 * Constructor of {@link PilotClient}. It is abstract: only a subclass can
 * call it, passing the factory of the client's pilot.
 *
 * River calls `createPilot` once, synchronously, with the driver's
 * database, then the pilot's `init`, before the constructor returns, so a
 * subclass's own fields aren't assigned yet when they run. The driver must
 * be a first-party driver that supports pilots: `PgDriver` constructed with
 * a `Pool`, or `SqliteDriver`.
 */
export type PilotClientConstructor = abstract new <
  Transaction,
  Config extends QueueConfig = QueueConfig,
>(
  driver: ClientDriver<Transaction, "runtime">,
  options: PilotClientOptions<Transaction, Config>,
  createPilot: PilotFactory<Transaction>
) => PilotClient<Transaction, Config>;

abstract class PilotClientImplementation<
  Transaction,
> extends RiverClient<Transaction> {
  protected constructor(
    driver: ClientDriver<Transaction, "runtime">,
    options: PilotClientOptions<Transaction>,
    createPilot: PilotFactory<Transaction>
  ) {
    if (new.target === PilotClientImplementation) {
      throw new TypeError("PilotClient is abstract; extend it");
    }
    super(driver, options, createPilotBinding(createPilot));
  }
}

/** The base class of a client with a pilot; see {@link PilotClient}. */
export const PilotClient: PilotClientConstructor =
  PilotClientImplementation as unknown as PilotClientConstructor;
