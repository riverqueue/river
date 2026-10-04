import type { Pool, PoolClient } from "pg";
import { Client, type InsertClient, type InsertManyItem } from "riverqueue";
import { PrismaDriver, type PrismaClientLike } from "@riverqueue/driver-prisma";

import { invalidParams, methodNotFound, notFound, rejected } from "./errors.js";
import { normalizeJob } from "./normalize.js";
import { requiredNonEmptyString, requiredRecord } from "./params.js";
import {
  POSTGRES_CONFORMANCE_APPLICATION_NAME,
  parseInsert,
  schemaOption,
  uniqueKeyResult,
  type ParsedInsert,
} from "./pg-adapter.js";
import type { InsertOnlyProfile } from "./profile.js";

/**
 * The raw-query surface of a Prisma client, served by node-postgres. River's
 * Prisma driver only calls `$queryRawUnsafe`, and a `PoolClient` inside
 * `BEGIN` stands in for a Prisma interactive-transaction client.
 */
class PgPrismaClient implements PrismaClientLike {
  readonly #queryable: Pool | PoolClient;

  constructor(queryable: Pool | PoolClient) {
    this.#queryable = queryable;
  }

  async $queryRawUnsafe<T = unknown>(
    query: string,
    ...values: unknown[]
  ): Promise<T> {
    const result = await this.#queryable.query(query, values);
    return result.rows as T;
  }
}

/**
 * A root Prisma client served by a node-postgres pool, whose
 * `$transaction` stands in for Prisma's interactive transactions.
 */
class PgPrismaRootClient extends PgPrismaClient {
  readonly #pool: Pool;

  constructor(pool: Pool) {
    super(pool);
    this.#pool = pool;
  }

  async $transaction<R>(
    callback: (tx: PrismaClientLike) => Promise<R>
  ): Promise<R> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      try {
        const result = await callback(new PgPrismaClient(client));
        await client.query("COMMIT");
        return result;
      } catch (error: unknown) {
        await client.query("ROLLBACK");
        throw error;
      }
    } finally {
      client.release();
    }
  }
}

interface Transaction {
  readonly client: PoolClient;
  readonly prisma: PgPrismaClient;
}

/**
 * `insert-only-v1` over PostgreSQL through `@riverqueue/driver-prisma`,
 * River's insertion-only driver. The Go reference migrates, observes, and
 * works every job the adapter inserts.
 */
export class InsertOnlyConformanceAdapter {
  readonly #pool: Pool;
  readonly #prisma: PgPrismaRootClient;
  readonly #profile: InsertOnlyProfile;
  readonly #transactions = new Map<string, Transaction>();

  constructor(pool: Pool, profile: InsertOnlyProfile) {
    this.#pool = pool;
    this.#prisma = new PgPrismaRootClient(pool);
    this.#profile = profile;
  }

  async close(): Promise<void> {
    for (const [handle, transaction] of [...this.#transactions]) {
      this.#transactions.delete(handle);
      try {
        await transaction.client.query("ROLLBACK");
      } finally {
        transaction.client.release();
      }
    }
  }

  async dispatch(
    method: string,
    params: Record<string, unknown>
  ): Promise<unknown> {
    if (!this.#profile.methods.includes(method)) throw methodNotFound(method);
    this.#profile.params.check(method, params);
    switch (method) {
      case "handshake":
        return {
          adapter_version: this.#profile.adapterVersion,
          application_name: POSTGRES_CONFORMANCE_APPLICATION_NAME,
          backend: this.#profile.backend,
          capabilities: this.#profile.capabilities,
          implementation: "javascript",
          implementation_version: this.#profile.implementationVersion,
          methods: this.#profile.methods,
          migration_lines: {
            [this.#profile.migrationLine]: this.#profile.latestMigration,
          },
          profile: this.#profile.name,
          protocol_revision: this.#profile.protocolRevision,
        };
      case "insert":
        return this.#insert(parseInsert(params));
      case "insert_many":
        return this.#insertMany(params);
      case "tx_begin":
        await this.#begin(requiredNonEmptyString(params, "handle"));
        return {};
      case "tx_commit":
      case "tx_rollback":
        await this.#finish(
          requiredNonEmptyString(params, "handle"),
          method === "tx_commit" ? "COMMIT" : "ROLLBACK"
        );
        return {};
      case "tx_insert":
        return this.#insert(
          parseInsert(requiredRecord(params, "job")),
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "tx_insert_many":
        return this.#insertMany(
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "unique_key":
        return uniqueKeyResult(params);
    }
    throw methodNotFound(method);
  }

  async #begin(handle: string): Promise<void> {
    if (this.#transactions.has(handle)) {
      throw rejected(`transaction ${JSON.stringify(handle)} already exists`);
    }
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
    } catch (error: unknown) {
      client.release(true);
      throw error;
    }
    this.#transactions.set(handle, {
      client,
      prisma: new PgPrismaClient(client),
    });
  }

  #client(schema: string | undefined): InsertClient<PrismaClientLike> {
    return new Client(new PrismaDriver(this.#prisma, schemaOption(schema)));
  }

  async #finish(handle: string, action: "COMMIT" | "ROLLBACK"): Promise<void> {
    const transaction = this.#transaction(handle);
    this.#transactions.delete(handle);
    try {
      await transaction.client.query(action);
    } finally {
      transaction.client.release();
    }
  }

  async #insert(
    parsed: ParsedInsert,
    transaction?: Transaction
  ): Promise<unknown> {
    const result = await this.#client(parsed.schema).insert(
      parsed.definition,
      parsed.args,
      {
        ...parsed.options,
        ...(transaction === undefined ? {} : { tx: transaction.prisma }),
      }
    );
    return normalizeJob(result.job);
  }

  async #insertMany(
    params: Record<string, unknown>,
    transaction?: Transaction
  ): Promise<unknown> {
    const jobs = params.jobs;
    if (!Array.isArray(jobs)) throw invalidParams("jobs must be an array");
    if (jobs.length === 0) throw rejected("no jobs to insert");
    const parsed = jobs.map((job: unknown, index) => {
      if (job === null || typeof job !== "object" || Array.isArray(job)) {
        throw invalidParams(`jobs[${index}] must be an object`);
      }
      return parseInsert(job as Record<string, unknown>);
    });
    const schemas = new Set(parsed.map(({ schema }) => schema ?? ""));
    if (schemas.size !== 1) throw rejected("batch jobs must use one schema");
    const items: InsertManyItem[] = parsed.map((job) => ({
      args: job.args,
      job: job.definition,
      options: job.options,
    }));
    const results = await this.#client(parsed[0]?.schema).insertMany(
      items,
      transaction === undefined ? {} : { tx: transaction.prisma }
    );
    return {
      results: results.map((result) => ({
        job: normalizeJob(result.job),
        unique_skipped_as_duplicate: result.status === "duplicate",
      })),
    };
  }

  #transaction(handle: string): Transaction {
    const transaction = this.#transactions.get(handle);
    if (transaction === undefined) {
      throw notFound(`transaction ${JSON.stringify(handle)} not found`);
    }
    return transaction;
  }
}
