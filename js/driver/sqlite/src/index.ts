import type { DatabaseSync } from "node:sqlite";

export { SqliteDriver } from "./driver.js";
export { transaction, type SqliteTransactionOptions } from "./scope.js";
export type { SqliteDriverOptions } from "./types.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** Application handles on a SQLite driver's database with a transaction open. */
    "@riverqueue/driver-sqlite": DatabaseSync;
  }
}
