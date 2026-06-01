import { describe, it, expect, beforeEach } from "vitest";
import { Client, InsertManyParams } from "./client.js";
import type { Driver, DriverOptions, JobInsertParams } from "./driver.js";
import type { JobArgs, JobRow } from "./job.js";
import {
  JOB_STATE_AVAILABLE,
  JOB_STATE_SCHEDULED,
  JobArgsObject,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
} from "./job.js";

// Stub driver that records insert params instead of hitting a database.
class FakeDriver implements Driver {
  insertedParams: JobInsertParams[] = [];
  lastOptions?: DriverOptions;

  async jobInsert(
    params: JobInsertParams,
    options?: DriverOptions
  ): Promise<[JobRow, boolean]> {
    this.insertedParams.push(params);
    this.lastOptions = options;
    return [fakeJobRow(params), false];
  }

  async jobInsertMany(
    params: JobInsertParams[],
    options?: DriverOptions
  ): Promise<[JobRow, boolean][]> {
    this.insertedParams.push(...params);
    this.lastOptions = options;
    return params.map((p) => [fakeJobRow(p), false]);
  }
}

function fakeJobRow(params: JobInsertParams): JobRow {
  return {
    id: 1,
    args: JSON.parse(params.encodedArgs) as Record<string, unknown>,
    attempt: 0,
    attemptedAt: null,
    attemptedBy: null,
    createdAt: new Date(),
    errors: null,
    finalizedAt: null,
    kind: params.kind,
    maxAttempts: params.maxAttempts,
    metadata: {},
    priority: params.priority,
    queue: params.queue,
    scheduledAt: params.scheduledAt,
    state: params.state,
    tags: params.tags,
    uniqueKey: params.uniqueKey,
    uniqueStates: null,
  };
}

class SortArgs implements JobArgs {
  kind = "sort";

  constructor(public strings: string[]) {}

  toJSON() {
    return { strings: this.strings };
  }
}

describe("Client", () => {
  let driver: FakeDriver;
  let client: Client;

  beforeEach(() => {
    driver = new FakeDriver();
    client = new Client(driver);
  });

  describe("insert", () => {
    it("inserts a job with defaults", async () => {
      const result = await client.insert(new SortArgs(["b", "a"]));

      expect(result.uniqueSkippedAsDuplicated).toBe(false);
      expect(result.job.kind).toBe("sort");

      const params = driver.insertedParams[0]!;
      expect(params.kind).toBe("sort");
      expect(params.encodedArgs).toBe('{"strings":["b","a"]}');
      expect(params.maxAttempts).toBe(MAX_ATTEMPTS_DEFAULT);
      expect(params.priority).toBe(PRIORITY_DEFAULT);
      expect(params.queue).toBe(QUEUE_DEFAULT);
      expect(params.state).toBe(JOB_STATE_AVAILABLE);
      expect(params.tags).toEqual([]);
      expect(params.uniqueKey).toBeNull();
      expect(params.uniqueStates).toBeNull();
    });

    it("respects insert opts", async () => {
      const future = new Date(Date.now() + 60_000);
      await client.insert(new SortArgs(["a"]), {
        maxAttempts: 5,
        priority: 3,
        queue: "high",
        scheduledAt: future,
        tags: ["tag1"],
      });

      const params = driver.insertedParams[0]!;
      expect(params.maxAttempts).toBe(5);
      expect(params.priority).toBe(3);
      expect(params.queue).toBe("high");
      expect(params.scheduledAt).toBe(future);
      expect(params.state).toBe(JOB_STATE_SCHEDULED);
      expect(params.tags).toEqual(["tag1"]);
    });

    it("uses JobArgsObject", async () => {
      await client.insert(
        new JobArgsObject("email", { to: "user@example.com" })
      );

      const params = driver.insertedParams[0]!;
      expect(params.kind).toBe("email");
      expect(params.encodedArgs).toBe('{"to":"user@example.com"}');
    });

    it("strips kind and insertOpts from args without toJSON", async () => {
      const args: JobArgs = {
        kind: "plain",
        insertOpts: { maxAttempts: 3 },
      };
      // Add a data property at runtime
      (args as unknown as Record<string, unknown>).data = "hello";

      await client.insert(args);

      const params = driver.insertedParams[0]!;
      expect(JSON.parse(params.encodedArgs)).toEqual({ data: "hello" });
      expect(params.maxAttempts).toBe(3);
    });
  });

  describe("insert with uniqueOpts", () => {
    it("generates unique key when constraints are set", async () => {
      await client.insert(new SortArgs(["a"]), {
        uniqueOpts: { byQueue: true },
      });

      const params = driver.insertedParams[0]!;
      expect(params.uniqueKey).not.toBeNull();
      expect(params.uniqueStates).not.toBeNull();
    });

    it("does not generate unique key for empty uniqueOpts", async () => {
      await client.insert(new SortArgs(["a"]), {
        uniqueOpts: {},
      });

      const params = driver.insertedParams[0]!;
      expect(params.uniqueKey).toBeNull();
      expect(params.uniqueStates).toBeNull();
    });

    it("does not generate unique key for args-level empty uniqueOpts", async () => {
      class ArgsWithEmptyUnique implements JobArgs {
        kind = "with_empty_unique";
        insertOpts = { uniqueOpts: {} };

        toJSON() {
          return {};
        }
      }

      await client.insert(new ArgsWithEmptyUnique());

      const params = driver.insertedParams[0]!;
      expect(params.uniqueKey).toBeNull();
      expect(params.uniqueStates).toBeNull();
    });
  });

  describe("insertMany", () => {
    it("inserts multiple jobs", async () => {
      const results = await client.insertMany([
        new SortArgs(["b"]),
        new SortArgs(["a"]),
      ]);

      expect(results).toHaveLength(2);
      expect(driver.insertedParams).toHaveLength(2);
      expect(driver.insertedParams[0]!.encodedArgs).toBe('{"strings":["b"]}');
      expect(driver.insertedParams[1]!.encodedArgs).toBe('{"strings":["a"]}');
    });

    it("supports InsertManyParams with per-job opts", async () => {
      await client.insertMany([
        new InsertManyParams(new SortArgs(["a"]), { maxAttempts: 5 }),
        new SortArgs(["b"]),
      ]);

      expect(driver.insertedParams[0]!.maxAttempts).toBe(5);
      expect(driver.insertedParams[1]!.maxAttempts).toBe(MAX_ATTEMPTS_DEFAULT);
    });
  });

  describe("validation", () => {
    it("rejects empty kind", async () => {
      await expect(client.insert({ kind: "" })).rejects.toThrow(
        "args must have a non-empty kind"
      );
    });

    it("rejects tags over 255 characters", async () => {
      await expect(
        client.insert(new SortArgs(["a"]), { tags: ["x".repeat(256)] })
      ).rejects.toThrow("255 characters");
    });

    it("rejects tags with invalid characters", async () => {
      await expect(
        client.insert(new SortArgs(["a"]), { tags: ["bad tag!"] })
      ).rejects.toThrow("tag should match regex");
    });

    it("rejects unique states missing required states", async () => {
      await expect(
        client.insert(new SortArgs(["a"]), {
          uniqueOpts: {
            byArgs: true,
            byState: [JOB_STATE_AVAILABLE],
          },
        })
      ).rejects.toThrow("byState should include required state");
    });

    it("rejects invalid schema names", () => {
      expect(() => new Client(driver, { schema: "bad schema" })).toThrow(
        "invalid schema name"
      );
      expect(() => new Client(driver, { schema: "has;semicolon" })).toThrow(
        "invalid schema name"
      );
      expect(() => new Client(driver, { schema: "1starts" })).toThrow(
        "invalid schema name"
      );
    });
  });

  describe("schema", () => {
    it("passes empty schema prefix by default", async () => {
      await client.insert(new SortArgs(["a"]));
      expect(driver.lastOptions?.schemaPrefix).toBe("");
    });

    it("passes schema prefix when configured", async () => {
      const schemaClient = new Client(driver, { schema: "private" });
      await schemaClient.insert(new SortArgs(["a"]));
      expect(driver.lastOptions?.schemaPrefix).toBe('"private".');
    });

    it("passes schema prefix to insertMany", async () => {
      const schemaClient = new Client(driver, { schema: "custom" });
      await schemaClient.insertMany([new SortArgs(["a"])]);
      expect(driver.lastOptions?.schemaPrefix).toBe('"custom".');
    });
  });
});
