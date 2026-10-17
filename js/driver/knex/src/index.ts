import type { Knex } from "knex";

export { KnexDriver } from "./driver.js";
export type { KnexDriverOptions } from "./driver.js";
export { knexTransactionAdapter } from "./transaction.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** Knex transactions on Postgres. */
    "@riverqueue/driver-knex": Knex.Transaction;
  }
}
