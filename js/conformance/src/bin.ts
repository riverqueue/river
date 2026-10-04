#!/usr/bin/env node
import { DatabaseSync } from "node:sqlite";
import pg from "pg";
import { assertRuntimeSupport } from "riverqueue";

import { PortableSqliteAdapter } from "./adapter.js";
import { InsertOnlyConformanceAdapter } from "./insert-only-adapter.js";
import {
  POSTGRES_CONFORMANCE_APPLICATION_NAME,
  PostgresConformanceAdapter,
} from "./pg-adapter.js";
import {
  loadInsertOnlyProfile,
  loadPortableStorageProfile,
  loadPostgresFullProfile,
  loadSqliteRuntimeProfile,
} from "./profile.js";
import { serveJsonRpc } from "./rpc.js";

/** Longer than the adapters' own ten-second graceful stop timeout. */
const SHUTDOWN_TIMEOUT_MS = 15_000;

async function main(): Promise<void> {
  assertRuntimeSupport();
  const backend = process.env.RIVER_CONFORMANCE_DATABASE_KIND ?? "sqlite";
  if (backend === "postgres") {
    await runPostgres();
    return;
  }
  if (backend !== "sqlite") {
    throw new Error(
      `unsupported conformance backend ${JSON.stringify(backend)}`
    );
  }
  await runSqlite();
}

async function runPostgres(): Promise<void> {
  const requestedProfile =
    process.env.RIVER_CONFORMANCE_PROFILE ?? "postgres-full-v1";
  if (
    requestedProfile !== "postgres-full-v1" &&
    requestedProfile !== "insert-only-v1"
  ) {
    throw new Error(
      `PostgreSQL adapter cannot use profile ${JSON.stringify(requestedProfile)}`
    );
  }
  const databaseURL = requiredDatabaseURL();
  const pool = new pg.Pool({
    application_name: POSTGRES_CONFORMANCE_APPLICATION_NAME,
    connectionString: databaseURL,
    max: 32,
  });
  // The fault-injection contract deliberately terminates idle application
  // backends. node-postgres surfaces those expected failures on the Pool.
  pool.on("error", () => undefined);
  const adapter =
    requestedProfile === "insert-only-v1"
      ? new InsertOnlyConformanceAdapter(pool, await loadInsertOnlyProfile())
      : new PostgresConformanceAdapter(pool, await loadPostgresFullProfile());
  try {
    await serveJsonRpc(process.stdin, process.stdout, (method, params) =>
      adapter.dispatch(method, params)
    );
  } finally {
    await closeWithin(async () => {
      await adapter.close();
      await pool.end();
    });
  }
}

async function runSqlite(): Promise<void> {
  const requestedProfile =
    process.env.RIVER_CONFORMANCE_PROFILE ?? "portable-storage-v1";
  if (
    requestedProfile !== "portable-storage-v1" &&
    requestedProfile !== "sqlite-runtime-v1"
  ) {
    throw new Error(
      `SQLite adapter cannot use profile ${JSON.stringify(requestedProfile)}`
    );
  }
  const databasePath = requiredDatabaseURL();

  const profile =
    requestedProfile === "sqlite-runtime-v1"
      ? await loadSqliteRuntimeProfile()
      : await loadPortableStorageProfile();
  const database = new DatabaseSync(databasePath);
  const adapter = new PortableSqliteAdapter(database, profile);
  try {
    await serveJsonRpc(process.stdin, process.stdout, (method, params) =>
      adapter.dispatch(method, params)
    );
  } finally {
    await closeWithin(async () => {
      await adapter.close();
      database.close();
    });
  }
}

/**
 * Bound shutdown after the harness closes standard input. A worker that
 * ignores cancellation (the `ignored_cancel` behavior) never settles, so a
 * graceful close could otherwise keep the process, and the harness waiting
 * for it to exit, alive forever.
 */
async function closeWithin(close: () => Promise<void>): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  const timedOut = new Promise<true>((resolve) => {
    timer = setTimeout(resolve, SHUTDOWN_TIMEOUT_MS, true);
  });
  try {
    if (await Promise.race([close().then(() => false), timedOut])) {
      process.stderr.write(
        `riverqueue conformance adapter did not shut down within ${SHUTDOWN_TIMEOUT_MS} ms; exiting\n`
      );
      process.exit(1);
    }
  } finally {
    clearTimeout(timer);
  }
}

function requiredDatabaseURL(): string {
  const databaseURL = process.env.RIVER_CONFORMANCE_DATABASE_URL;
  if (databaseURL === undefined || databaseURL.length === 0) {
    throw new Error("RIVER_CONFORMANCE_DATABASE_URL is required");
  }
  return databaseURL;
}

main().catch((error: unknown) => {
  process.stderr.write(
    `riverqueue conformance adapter failed: ${
      error instanceof Error ? error.message : String(error)
    }\n`
  );
  process.exitCode = 1;
});
