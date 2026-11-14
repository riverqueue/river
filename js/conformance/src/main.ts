// River for JavaScript's conformance adapter: answers the harness's
// JSON-RPC requests, one per line on stdin, with one response per line on
// stdout. See conformance/protocol in the repository for the contract.

import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { DatabaseSync } from "node:sqlite";

import { PgDriver } from "@riverqueue/driver-pg";
import { SqliteDriver, transaction } from "@riverqueue/driver-sqlite";
import pg from "pg";
import { parseJsonObject, type JsonObject, type JsonValue } from "riverqueue";

import { Adapter, type Backend, type OpenTransaction } from "./adapter.js";
import { CODE, ProtocolError, rejected } from "./protocol.js";

const { version } = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8")
) as { version: string };

function postgresBackend(url: string): Backend<pg.ClientBase> {
  const pool = new pg.Pool({
    application_name: process.env.RIVER_CONFORMANCE_APPLICATION_NAME,
    connectionString: url,
    max: 10,
  });
  // Fault scenarios terminate idle connections, which the pool then drops.
  pool.on("error", () => undefined);
  const drivers = new Map<string, PgDriver>();
  return {
    async begin() {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
      } catch (error: unknown) {
        client.release(true);
        throw error;
      }
      return {
        async end(commit) {
          try {
            await client.query(commit ? "COMMIT" : "ROLLBACK");
            client.release();
          } catch (error: unknown) {
            client.release(true);
            throw error;
          }
        },
        tx: client,
      };
    },
    close: () => pool.end(),
    driver(schema) {
      let driver = drivers.get(schema);
      if (driver === undefined) {
        driver = new PgDriver(pool, schema === "" ? {} : { schema });
        drivers.set(schema, driver);
      }
      return driver;
    },
    name: "postgres",
  };
}

function sqliteBackend(path: string): Backend<DatabaseSync> {
  // Several processes share the file, so it's in WAL mode. River opens a
  // connection of its own; this one only holds `tx_begin`'s transactions.
  const database = new DatabaseSync(path, { timeout: 5_000 });
  database.exec("PRAGMA journal_mode = WAL");
  const driver = new SqliteDriver(database);
  return {
    async begin(): Promise<OpenTransaction<DatabaseSync>> {
      // Each transaction gets a handle of its own, held open until
      // `tx_end` decides how the transaction ends.
      const handle = driver.connect({ timeout: 5_000 });
      const decision = Promise.withResolvers<boolean>();
      const opened = Promise.withResolvers<DatabaseSync>();
      const rollback = new Error("rolled back");
      const finished = transaction(handle, async (tx) => {
        opened.resolve(tx);
        if (!(await decision.promise)) throw rollback;
      }).finally(() => handle.close());
      finished.catch((error: unknown) => opened.reject(error));
      return {
        async end(commit) {
          decision.resolve(commit);
          await finished.catch((error: unknown) => {
            if (error !== rollback) throw error;
          });
        },
        tx: await opened.promise,
      };
    },
    async close() {
      driver.close();
      database.close();
    },
    driver(schema) {
      if (schema !== "") throw rejected("SQLite databases have no schemas");
      return driver;
    },
    name: "sqlite",
  };
}

/** Answer one request line. */
async function respond(
  adapter: Adapter<unknown>,
  line: string
): Promise<object> {
  let request: JsonObject;
  try {
    request = parseJsonObject(line);
  } catch (error: unknown) {
    return errorResponse(null, CODE.parseError, error);
  }
  const id = request.id ?? null;
  if (request.jsonrpc !== "2.0" || typeof request.method !== "string") {
    return errorResponse(
      id,
      CODE.invalidRequest,
      "invalid JSON-RPC 2.0 request"
    );
  }
  try {
    const result = await adapter.handle(request.method, request.params);
    return { id, jsonrpc: "2.0", result };
  } catch (error: unknown) {
    const code = error instanceof ProtocolError ? error.code : CODE.rejected;
    return errorResponse(id, code, error);
  }
}

function errorResponse(id: JsonValue, code: number, error: unknown): object {
  const message = error instanceof Error ? error.message : String(error);
  return { error: { code, message }, id, jsonrpc: "2.0" };
}

async function main(): Promise<void> {
  const url = process.env.RIVER_CONFORMANCE_DATABASE_URL ?? "";
  if (url === "") throw new Error("RIVER_CONFORMANCE_DATABASE_URL is required");
  const driver = process.env.RIVER_CONFORMANCE_DRIVER;
  let backend: Backend<unknown>;
  switch (driver) {
    case "postgres":
      backend = postgresBackend(url);
      break;
    case "sqlite":
      backend = sqliteBackend(url);
      break;
    default:
      throw new Error(
        `unsupported RIVER_CONFORMANCE_DRIVER ${JSON.stringify(driver)}`
      );
  }

  const adapter = new Adapter(backend, version);
  try {
    // Requests are sequential: each is answered before the next is read.
    for await (const line of createInterface({
      crlfDelay: Infinity,
      input: process.stdin,
    })) {
      if (line.trim() === "") continue;
      process.stdout.write(`${JSON.stringify(await respond(adapter, line))}\n`);
    }
  } finally {
    await adapter.shutdown();
    await backend.close();
  }
}

main().then(
  () => process.exit(0),
  (error: unknown) => {
    console.error("River JavaScript conformance adapter:", error);
    process.exit(1);
  }
);
