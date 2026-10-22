import type { PoolConnection } from "mysql2/promise";

export { MySqlDriver } from "./driver.js";
export type { MySqlDriverOptions } from "./driver.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** A mysql2 connection with an open transaction. */
    "@riverqueue/driver-mysql": PoolConnection;
  }
}
