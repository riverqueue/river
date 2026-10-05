import type { PrismaClientLike } from "./driver.js";

export { PrismaDriver } from "./driver.js";
export type {
  PrismaClientLike,
  PrismaDriverOptions,
  PrismaTransactionOptions,
} from "./driver.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** Prisma interactive-transaction clients. */
    "@riverqueue/driver-prisma": PrismaClientLike;
  }
}
