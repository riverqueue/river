import type { ClientBase } from "pg";

export { PgDriver } from "./driver.js";
export type { PgDriverOptions } from "./types.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** node-postgres clients (`pg.Client` or a pool's `PoolClient`). */
    "@riverqueue/driver-pg": ClientBase;
  }
}
