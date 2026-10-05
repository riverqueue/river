/**
 * Private records of first-party driver instances and of the pilot bindings
 * `PilotClient` passes to River's client constructor. Nothing here is
 * readable through a package entry point.
 */
import type { ClientDriver } from "../driver.js";
import { ConfigurationError, ValidationError } from "../errors.js";
import type {
  DriverMigrationTarget,
  DriverRecord,
  PilotFactory,
} from "../pilot.js";

const drivers = new WeakMap<object, DriverRecord<unknown>>();

/**
 * Register what River needs to know privately about a first-party driver
 * instance. A driver registers itself once, from its constructor.
 *
 * @throws {ConfigurationError} when `handle` is already registered.
 */
export function registerDriver<Transaction>(
  handle: ClientDriver<Transaction>,
  record: DriverRecord<Transaction>
): void {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (typeof handle !== "object" || handle === null) {
    throw new ValidationError("a River driver must be an object");
  }
  if (drivers.has(handle)) {
    throw new ConfigurationError("this River driver is already registered");
  }
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (record.capability !== "insert" && record.capability !== "runtime") {
    throw new ValidationError(
      'a River driver record needs capability "insert" or "runtime"'
    );
  }
  if (record.database !== undefined && record.capability !== "runtime") {
    throw new ValidationError(
      "only a runtime driver can provide a pilot database"
    );
  }
  const operations: unknown = record.operations;
  if (
    typeof operations !== "object" ||
    operations === null ||
    typeof (operations as { jobInsert?: unknown }).jobInsert !== "function" ||
    typeof (operations as { jobInsertMany?: unknown }).jobInsertMany !==
      "function"
  ) {
    throw new ValidationError(
      "a River driver record needs operations that insert jobs"
    );
  }
  if (typeof record.backend !== "string" || record.backend.length === 0) {
    throw new ValidationError("a River driver record needs a backend name");
  }
  const migration = record.migration;
  if (migration !== undefined && !isMigrationTarget(migration)) {
    throw new ValidationError(
      "a River driver's migration target needs { pool, schema }, " +
        "{ client, schema }, or { database }"
    );
  }
  const frozen: DriverRecord<Transaction> = Object.freeze({
    backend: record.backend,
    capability: record.capability,
    operations: record.operations,
    ...(record.database === undefined
      ? {}
      : { database: Object.freeze(record.database) }),
    ...(migration === undefined
      ? {}
      : { migration: Object.freeze({ ...migration }) }),
  });
  drivers.set(handle, frozen);
}

/** The record `handle` registered, if any. */
export function driverRecord<Transaction>(
  handle: object
): DriverRecord<Transaction> | undefined {
  return drivers.get(handle) as DriverRecord<Transaction> | undefined;
}

/**
 * The connection `createMigrator` migrates for a registered driver, or
 * undefined when `handle` isn't one or its driver can't migrate.
 */
export function driverMigrationTarget(
  handle: unknown
): DriverMigrationTarget | undefined {
  if (typeof handle !== "object" || handle === null) return undefined;
  return drivers.get(handle)?.migration;
}

function isMigrationTarget(target: unknown): target is DriverMigrationTarget {
  if (typeof target !== "object" || target === null) return false;
  const connection =
    "database" in target
      ? target.database
      : "pool" in target
        ? target.pool
        : "client" in target
          ? target.client
          : undefined;
  if (typeof connection !== "object" || connection === null) return false;
  if ("database" in target) return true;
  const schema = (target as { schema?: unknown }).schema;
  return schema === undefined || typeof schema === "string";
}

const pilotBindings = new WeakMap<object, PilotFactory<never>>();

/** A private token that makes River's client constructor attach a pilot. */
export function createPilotBinding<Transaction>(
  createPilot: PilotFactory<Transaction>
): object {
  const binding = Object.freeze(Object.create(null) as object);
  pilotBindings.set(binding, createPilot as unknown as PilotFactory<never>);
  return binding;
}

/**
 * The pilot factory behind a binding from {@link createPilotBinding}, or
 * undefined for anything else, such as an extra constructor argument passed
 * from untyped JavaScript.
 */
export function pilotFactory<Transaction>(
  binding: unknown
): PilotFactory<Transaction> | undefined {
  if (typeof binding !== "object" || binding === null) return undefined;
  return pilotBindings.get(binding) as PilotFactory<Transaction> | undefined;
}
