import type { PrismaTransactionClientLike } from "./transaction.js";

export { PrismaDriver } from "./driver.js";
export type {
  PrismaClientLike,
  PrismaDriverOptions,
  PrismaTransactionOptions,
} from "./driver.js";
export { prismaTransactionAdapter } from "./transaction.js";
export type { PrismaTransactionClientLike } from "./transaction.js";

declare module "riverqueue" {
  interface RiverTransactionRegistry {
    /** Prisma interactive-transaction clients. */
    "@riverqueue/driver-prisma": PrismaTransactionClientLike;
  }
}
