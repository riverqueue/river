import { createHash } from "node:crypto";

import type { Driver, DriverOptions, JobInsertParams } from "./driver.js";
import type { InsertOpts, UniqueOpts } from "./insert-opts.js";
import type { JobArgs, JobRow, JobState } from "./job.js";
import {
  JOB_STATE_AVAILABLE,
  JOB_STATE_COMPLETED,
  JOB_STATE_PENDING,
  JOB_STATE_RETRYABLE,
  JOB_STATE_RUNNING,
  JOB_STATE_SCHEDULED,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
} from "./job.js";
import { uniqueBitmaskFromStates } from "./unique-bitmask.js";

const TAG_RE = /^\w[\w-]+\w$/;

const DEFAULT_UNIQUE_STATES: JobState[] = [
  JOB_STATE_AVAILABLE,
  JOB_STATE_COMPLETED,
  JOB_STATE_PENDING,
  JOB_STATE_RETRYABLE,
  JOB_STATE_RUNNING,
  JOB_STATE_SCHEDULED,
];

const REQUIRED_UNIQUE_STATES: JobState[] = [
  JOB_STATE_AVAILABLE,
  JOB_STATE_PENDING,
  JOB_STATE_RUNNING,
  JOB_STATE_SCHEDULED,
];

/** Result of a single job insertion. */
export interface InsertResult {
  /** The inserted job row (or existing row if unique-skipped). */
  job: JobRow;

  /** True if insertion was skipped due to an existing unique job. */
  uniqueSkippedAsDuplicated: boolean;
}

/**
 * Pairs job args with per-job insertion options for use with `insertMany`.
 *
 * Example:
 *
 *     await client.insertMany([
 *       new InsertManyParams(new SortArgs(["b"]), { maxAttempts: 5 }),
 *       new SortArgs(["a"]),  // raw job args use defaults
 *     ]);
 */
export class InsertManyParams {
  readonly args: JobArgs;
  readonly insertOpts?: InsertOpts;

  constructor(args: JobArgs, insertOpts?: InsertOpts) {
    this.args = args;
    this.insertOpts = insertOpts;
  }
}

/** Options for constructing a River Client. */
export interface ClientOpts {
  /**
   * A non-default PostgreSQL schema where River tables are located. All
   * table references in database queries will use this as a prefix.
   *
   * Defaults to empty, which causes queries to use the Postgres `search_path`.
   */
  schema?: string;
}

const SCHEMA_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Client for River that inserts jobs. Unlike the Go River client, this one
 * can only insert jobs — job execution is handled by a Go River server.
 *
 * Used in conjunction with a driver:
 *
 *     import { Client } from "riverqueue";
 *     import { PgDriver } from "@riverqueue/driver-pg";
 *
 *     const client = new Client(new PgDriver(pool));
 *     await client.insert(new SortArgs(["whale", "tiger"]));
 *
 * To use a non-default schema:
 *
 *     const client = new Client(new PgDriver(pool), { schema: "private" });
 */
export class Client<TTx = unknown> {
  private driver: Driver<TTx>;
  private schemaPrefix: string; // differs from `schema` in that it's the full prefix used in queries (e.g. `"my_schema".` or "")

  constructor(driver: Driver<TTx>, opts?: ClientOpts) {
    this.driver = driver;

    if (opts?.schema) {
      if (!SCHEMA_NAME_RE.test(opts.schema)) {
        throw new Error(
          `invalid schema name: ${JSON.stringify(opts.schema)} (must match ${SCHEMA_NAME_RE})`
        );
      }
      this.schemaPrefix = `"${opts.schema}".`;
    } else {
      this.schemaPrefix = "";
    }
  }

  /**
   * Insert a single job for work. Options include standard insertion options
   * and an optional `tx` for running within a transaction.
   */
  async insert(
    args: JobArgs,
    opts?: InsertOpts & { tx?: TTx }
  ): Promise<InsertResult> {
    const params = this.makeInsertParams(args, opts ?? {});
    const [job, uniqueSkipped] = await this.driver.jobInsert(
      params,
      this.driverOptions(opts?.tx)
    );
    return { job, uniqueSkippedAsDuplicated: uniqueSkipped };
  }

  /**
   * Insert many jobs in a single batch operation. Accepts an array of
   * `JobArgs` or `InsertManyParams` (which pairs args with per-job options).
   * Pass `tx` to run the entire batch within a transaction.
   */
  async insertMany(
    args: (JobArgs | InsertManyParams)[],
    opts?: { tx?: TTx }
  ): Promise<InsertResult[]> {
    const allParams = args.map((arg) => {
      if (arg instanceof InsertManyParams) {
        return this.makeInsertParams(arg.args, arg.insertOpts || {});
      }
      return this.makeInsertParams(arg, {});
    });

    // Deduplicate by unique key within the batch. PostgreSQL aborts a
    // multi-row INSERT ... ON CONFLICT DO UPDATE if two rows conflict on
    // the same unique key, so we must only send the first occurrence to the
    // database and mark subsequent duplicates ourselves.
    const { dedupedParams, resultMapping } =
      this.deduplicateByUniqueKey(allParams);

    const dbResults = await this.driver.jobInsertMany(
      dedupedParams,
      this.driverOptions(opts?.tx)
    );

    return resultMapping.map((mapping) => {
      if ("duplicateOf" in mapping) {
        const [job] = dbResults[mapping.duplicateOf] as [JobRow, boolean];
        return { job, uniqueSkippedAsDuplicated: true };
      }
      const [job, uniqueSkipped] = dbResults[mapping.index] as [
        JobRow,
        boolean,
      ];
      return { job, uniqueSkippedAsDuplicated: uniqueSkipped };
    });
  }

  private deduplicateByUniqueKey(params: JobInsertParams[]): {
    dedupedParams: JobInsertParams[];
    resultMapping: ({ index: number } | { duplicateOf: number })[];
  } {
    const uniqueKeyToIndex = new Map<string, number>();
    const dedupedParams: JobInsertParams[] = [];
    const resultMapping: ({ index: number } | { duplicateOf: number })[] = [];

    for (const p of params) {
      if (p.uniqueKey) {
        const hexKey = Buffer.from(p.uniqueKey).toString("hex");
        const existing = uniqueKeyToIndex.get(hexKey);
        if (existing !== undefined) {
          resultMapping.push({ duplicateOf: existing });
          continue;
        }
        uniqueKeyToIndex.set(hexKey, dedupedParams.length);
      }
      resultMapping.push({ index: dedupedParams.length });
      dedupedParams.push(p);
    }

    return { dedupedParams, resultMapping };
  }

  private driverOptions(tx?: TTx): DriverOptions<TTx> {
    return { schemaPrefix: this.schemaPrefix, tx };
  }

  private makeInsertParams(
    args: JobArgs,
    insertOpts: InsertOpts
  ): JobInsertParams {
    if (!args.kind) {
      throw new Error("args must have a non-empty kind");
    }

    const encodedArgs = this.encodeArgs(args);

    const argsInsertOpts: InsertOpts = args.insertOpts || {};

    const scheduledAt = insertOpts.scheduledAt || argsInsertOpts.scheduledAt;

    const params: JobInsertParams = {
      encodedArgs,
      kind: args.kind,
      maxAttempts:
        insertOpts.maxAttempts ||
        argsInsertOpts.maxAttempts ||
        MAX_ATTEMPTS_DEFAULT,
      priority:
        insertOpts.priority || argsInsertOpts.priority || PRIORITY_DEFAULT,
      queue: insertOpts.queue || argsInsertOpts.queue || QUEUE_DEFAULT,
      scheduledAt: scheduledAt || new Date(),
      state: scheduledAt ? JOB_STATE_SCHEDULED : JOB_STATE_AVAILABLE,
      tags: this.validateTags(insertOpts.tags || argsInsertOpts.tags || []),
      uniqueKey: null,
      uniqueStates: null,
    };

    const uniqueOpts = insertOpts.uniqueOpts || argsInsertOpts.uniqueOpts;
    if (uniqueOpts && this.hasUniqueConstraints(uniqueOpts)) {
      const [uniqueKey, uniqueStates] = this.makeUniqueKeyAndBitmask(
        params,
        uniqueOpts
      );
      params.uniqueKey = uniqueKey;
      params.uniqueStates = uniqueStates;
    }

    return params;
  }

  private hasUniqueConstraints(uniqueOpts: UniqueOpts): boolean {
    return !!(
      uniqueOpts.byArgs ||
      uniqueOpts.byPeriod ||
      uniqueOpts.byQueue ||
      uniqueOpts.byState ||
      uniqueOpts.excludeKind
    );
  }

  private encodeArgs(args: JobArgs): string {
    // If toJSON() is defined, JSON.stringify will call it automatically,
    // giving the implementation full control over serialization.
    const argsAny = args as unknown as Record<string, unknown>;
    if (typeof argsAny.toJSON === "function") {
      return JSON.stringify(args);
    }

    // Otherwise, serialize all properties except non-data fields.
    const obj = { ...argsAny };
    delete obj.kind;
    delete obj.insertOpts;
    return JSON.stringify(obj);
  }

  private makeUniqueKeyAndBitmask(
    params: JobInsertParams,
    uniqueOpts: UniqueOpts
  ): [Uint8Array, string] {
    // It's extremely important here that this unique key format and algorithm
    // match the one in the main River library _exactly_. Don't change them
    // unless they're updated everywhere.
    let uniqueKeyStr = "";

    if (!uniqueOpts.excludeKind) {
      uniqueKeyStr += `&kind=${params.kind}`;
    }

    if (uniqueOpts.byArgs) {
      const parsedArgs = JSON.parse(params.encodedArgs) as Record<
        string,
        unknown
      >;
      let filteredArgs: Record<string, unknown>;

      if (Array.isArray(uniqueOpts.byArgs)) {
        filteredArgs = {};
        for (const key of uniqueOpts.byArgs) {
          if (key in parsedArgs) {
            filteredArgs[key] = parsedArgs[key];
          }
        }
      } else {
        filteredArgs = parsedArgs;
      }

      // Sort keys for deterministic output matching other River clients.
      const sortedArgs: Record<string, unknown> = {};
      for (const key of Object.keys(filteredArgs).sort()) {
        sortedArgs[key] = filteredArgs[key];
      }
      uniqueKeyStr += `&args=${JSON.stringify(sortedArgs)}`;
    }

    if (uniqueOpts.byPeriod) {
      const lowerBound = this.truncateTime(
        params.scheduledAt,
        uniqueOpts.byPeriod
      );
      uniqueKeyStr += `&period=${this.formatTimeUTC(lowerBound)}`;
    }

    if (uniqueOpts.byQueue) {
      uniqueKeyStr += `&queue=${params.queue}`;
    }

    const uniqueKey = createHash("sha256").update(uniqueKeyStr).digest();
    const states = this.validateUniqueStates(
      uniqueOpts.byState || DEFAULT_UNIQUE_STATES
    );
    const uniqueStates = uniqueBitmaskFromStates(states);

    return [new Uint8Array(uniqueKey), uniqueStates];
  }

  private truncateTime(time: Date, intervalSeconds: number): Date {
    const epochSeconds = time.getTime() / 1000;
    return new Date(
      Math.floor(epochSeconds / intervalSeconds) * intervalSeconds * 1000
    );
  }

  private formatTimeUTC(date: Date): string {
    const pad = (n: number) => n.toString().padStart(2, "0");
    return (
      `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}` +
      `T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}Z`
    );
  }

  private validateTags(tags: string[]): string[] {
    for (const tag of tags) {
      if (tag.length > 255) {
        throw new Error("tags should be 255 characters or less");
      }
      if (!TAG_RE.test(tag)) {
        throw new Error(`tag should match regex ${TAG_RE}`);
      }
    }
    return tags;
  }

  private validateUniqueStates(states: JobState[]): JobState[] {
    for (const required of REQUIRED_UNIQUE_STATES) {
      if (!states.includes(required)) {
        throw new Error(`byState should include required state '${required}'`);
      }
    }
    return states;
  }
}
