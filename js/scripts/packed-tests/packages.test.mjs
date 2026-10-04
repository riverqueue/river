import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import process from "node:process";
import { describe, it } from "node:test";
import { fileURLToPath, URL } from "node:url";
import { promisify } from "node:util";

import * as migrate from "@riverqueue/migrate";
import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createTestClient } from "@riverqueue/test";
import * as river from "riverqueue";
import * as unstable from "riverqueue/unstable-driver";

const execFileAsync = promisify(execFile);
const require = createRequire(import.meta.url);
const consumerDirectory = fileURLToPath(new URL("..", import.meta.url));

const SATELLITES = [
  "@riverqueue/cli",
  "@riverqueue/driver-pg",
  "@riverqueue/driver-prisma",
  "@riverqueue/driver-sqlite",
  "@riverqueue/migrate",
  "@riverqueue/test",
  "@riverqueue/worker-threads",
];

describe("packed package graph", () => {
  it("resolves exactly one riverqueue from every satellite package", () => {
    const root = require.resolve("riverqueue");
    for (const name of SATELLITES) {
      const fromPackage = createRequire(require.resolve(name));
      assert.equal(
        fromPackage.resolve("riverqueue"),
        root,
        `${name} resolves the application's riverqueue`
      );
    }
  });

  it("shares River error classes across packages", async () => {
    assert.equal(migrate.MigrationError, river.MigrationError);
    assert.throws(
      () => migrate.createMigrator({}),
      (error) =>
        error instanceof river.MigrationError &&
        error instanceof river.RiverError &&
        error.code === "migration"
    );

    const { client } = createTestClient();
    await assert.rejects(
      client.jobs.get(1n),
      (error) =>
        error instanceof river.UnsupportedCapabilityError &&
        error instanceof river.RiverError
    );

    assert.throws(
      () => river.toJsonObject({ value: 1n }),
      (error) =>
        error instanceof river.JsonValueError &&
        error instanceof river.ValidationError &&
        error instanceof river.RiverError &&
        error.path === "$.value"
    );
  });

  it("loads the same ESM instance through require(esm)", async () => {
    const required = require("riverqueue");
    assert.equal(required.Client, river.Client);
    assert.equal(required.RiverError, river.RiverError);
    const requiredUnstable = require("riverqueue/unstable-driver");
    assert.equal(requiredUnstable.buildUniqueKey, unstable.buildUniqueKey);
    for (const name of SATELLITES) {
      assert.ok(Object.keys(require(name)).length > 0, `${name} requires`);
    }
  });

  it("works from a CommonJS module file", async () => {
    const path = join(consumerDirectory, "packed-require-check.cjs");
    await writeFile(
      path,
      `const river = require("riverqueue");
const { MigrationError } = require("@riverqueue/migrate");
if (MigrationError !== river.MigrationError) throw new Error("two River copies");
const exact = river.parseJson('{"id":9223372036854775807}');
process.stdout.write(JSON.stringify(exact));
`
    );
    const { stdout } = await execFileAsync(process.execPath, [path]);
    assert.equal(stdout, '{"id":9223372036854775807}');
  });

  it("exposes the unstable driver SPI only on its subpath", async () => {
    assert.equal(typeof unstable.buildUniqueKey, "function");
    assert.equal(typeof unstable.decodeJobListCursor, "function");
    assert.equal(typeof unstable.encodeJobListCursor, "function");
    assert.equal("buildUniqueKey" in river, false);
    assert.equal("uniqueBitmaskFromStates" in river, false);
    await assert.rejects(
      import("riverqueue/dist/index.js"),
      (error) => error.code === "ERR_PACKAGE_PATH_NOT_EXPORTED"
    );
  });

  it("keeps driver objects opaque", () => {
    const driver = SqliteDriver.memory();
    try {
      assert.deepEqual(Reflect.ownKeys(driver), []);
      assert.deepEqual(
        new Set(Reflect.ownKeys(SqliteDriver.prototype)),
        new Set(["close", "connect", "constructor", Symbol.dispose])
      );
      for (const name of ["jobInsert", "jobClaim", "database", "backend"]) {
        assert.equal(name in driver, false, name);
      }
      assert.ok(new river.Client(driver) instanceof river.Client);
      assert.throws(
        () => new river.Client({ jobInsert() {}, jobInsertMany() {} }),
        river.ConfigurationError
      );
    } finally {
      driver.close();
    }
  });

  it("keeps the pilot client base off the stable entry point", async () => {
    assert.equal(typeof unstable.PilotClient, "function");
    assert.equal(typeof unstable.registerDriver, "function");
    assert.equal("PilotClient" in river, false);
    assert.equal("registerDriver" in river, false);
    const declarations = await readFile(
      join(dirname(require.resolve("riverqueue")), "index.d.ts"),
      "utf8"
    );
    assert.equal(/Pilot/.test(declarations), false);
  });

  it("attaches a pilot through a PilotClient subclass", async () => {
    class CompanionClient extends unstable.PilotClient {
      constructor(driver, options = {}) {
        // The pilot is created inside River's constructor, before `this`
        // exists, so it records what it receives elsewhere.
        const pilot = {};
        super(driver, options, (database) => ({
          init: (host) => {
            pilot.host = host;
            pilot.database = database;
          },
        }));
        this.companion = true;
        this.host = pilot.host;
        this.database = pilot.database;
      }
    }
    const driver = SqliteDriver.memory();
    try {
      assert.throws(
        () => new unstable.PilotClient(driver, {}, () => ({})),
        TypeError
      );
      const client = new CompanionClient(driver);
      assert.ok(client instanceof river.Client);
      assert.equal(client.companion, true);
      assert.equal(client.database.backend, "sqlite");
      assert.equal(client.host.client, client);
      const job = river.defineJob({ kind: "packed_pilot" });
      const [inserted] = await client.host
        .insertPrepared([
          {
            encodedArgs: "{}",
            kind: job.kind,
            maxAttempts: 25,
            metadata: {},
            priority: 1,
            queue: "default",
            scheduledAt: Temporal.Now.instant(),
            state: "available",
            tags: [],
            uniqueKey: null,
            uniqueStates: null,
          },
        ])
        .catch((error) => [error]);
      // The in-memory database has no River tables, so the insert reaches
      // SQLite and fails there.
      assert.ok(inserted instanceof river.DatabaseOperationError);
    } finally {
      driver.close();
    }
  });

  it("computes River unique keys identically through the packed SPI", () => {
    const params = {
      args: river.toJsonObject({ b: 2, a: { z: 1, y: 2 } }),
      kind: "packed_unique",
      queue: "default",
      scheduledAt: Temporal.Instant.from("2026-09-01T12:34:56Z"),
    };
    const [key] = unstable.buildUniqueKey(params, { byArgs: true });
    const reordered = {
      ...params,
      args: river.toJsonObject({ a: { z: 1, y: 2 }, b: 2 }),
    };
    const [same] = unstable.buildUniqueKey(reordered, { byArgs: true });
    assert.deepEqual(same, key);
    assert.equal(key.length, 32);
  });
});
