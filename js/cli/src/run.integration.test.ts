import { randomUUID } from "node:crypto";
import { PassThrough } from "node:stream";

import type { Migration } from "@riverqueue/migrate";
import pg from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { run, type RunOptions } from "./run.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";

async function invoke(
  argv: readonly string[],
  options: Pick<RunOptions, "migrationLines"> = {}
) {
  let stderr = "";
  let stdout = "";
  const exitCode = await run(argv, {
    ...options,
    stderr: { write: (chunk: string) => (stderr += chunk) },
    stdin: Object.assign(new PassThrough(), { isTTY: false }),
    stdout: { write: (chunk: string) => (stdout += chunk) },
  });
  return { exitCode, stderr, stdout };
}

describe("riverqueue with PostgreSQL", () => {
  let pool: pg.Pool;
  let schema: string;

  beforeEach(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    schema = `river_cli_${randomUUID().replaceAll("-", "")}`;
    await pool.query(`CREATE SCHEMA "${schema}"`);
  });

  afterEach(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await pool.end();
  });

  function database(...argv: string[]): string[] {
    return [...argv, "--database-url", TEST_DATABASE_URL, "--schema", schema];
  }

  it("migrates a schema up and down", async () => {
    await expect(invoke(database("validate"))).resolves.toEqual({
      exitCode: 1,
      stderr: "unapplied migrations: 1, 2, 3, 4, 5, 6, 7, 8\n",
      stdout: "",
    });

    const up = await invoke(
      database("migrate-up", "--statement-timeout", "1m")
    );
    expect(up).toMatchObject({ exitCode: 0, stderr: "" });
    expect(up.stdout).toMatch(
      /^applied migration 001 \[up\] create_river_migration +\[[\d.]+m?s\]\n/
    );
    await expect(invoke(database("validate"))).resolves.toEqual({
      exitCode: 0,
      stderr: "",
      stdout: "",
    });
    const list = await invoke(database("migrate-list"));
    expect(list.stdout).toMatch(/\n\* 008 \S+\n$/);
    const tables = await pool.query(
      "SELECT 1 FROM information_schema.tables " +
        "WHERE table_schema = $1 AND table_name = 'river_job'",
      [schema]
    );
    expect(tables.rows).toHaveLength(1);

    const down = await invoke(
      database("migrate-down", "--target-version", "0", "--show-sql")
    );
    expect(down.exitCode).toBe(0);
    expect(down.stdout).toContain(`DROP TABLE "${schema}".river_migration`);
    const remaining = await pool.query(
      "SELECT 1 FROM information_schema.tables WHERE table_schema = $1",
      [schema]
    );
    expect(remaining.rows).toEqual([]);
  });

  it("migrates an additional line", async () => {
    const migrations: readonly Migration[] = [
      {
        downSql: "DROP TABLE /* TEMPLATE: schema */extra_widget;",
        name: "create_widget",
        upSql: "CREATE TABLE /* TEMPLATE: schema */extra_widget (id bigint);",
        version: 1,
      },
    ];
    const migrationLines = {
      extra: (backend: string) => (backend === "postgres" ? migrations : []),
    };
    const extra = (...argv: string[]) =>
      invoke(database(...argv, "--line", "extra"), { migrationLines });
    await invoke(database("migrate-up"));

    await expect(extra("migrate-up")).resolves.toMatchObject({
      exitCode: 0,
      stderr: "",
    });
    await expect(extra("migrate-list")).resolves.toMatchObject({
      stdout: "* 001 create_widget\n",
    });
    const rows = await pool.query(
      `SELECT line, version::text FROM "${schema}".river_migration WHERE line = 'extra'`
    );
    expect(rows.rows).toEqual([{ line: "extra", version: "1" }]);

    await expect(extra("migrate-down")).resolves.toMatchObject({
      exitCode: 0,
    });
    const after = await pool.query(
      `SELECT 1 FROM "${schema}".river_migration WHERE line = 'extra'`
    );
    expect(after.rows).toEqual([]);
    await expect(invoke(database("validate"))).resolves.toMatchObject({
      exitCode: 0,
    });
  });

  it("benchmarks a migrated schema after emptying River's tables", async () => {
    expect((await invoke(database("migrate-up"))).exitCode).toBe(0);
    await pool.query(
      `INSERT INTO "${schema}".river_queue (name, updated_at) ` +
        "VALUES ('bench_sentinel', now())"
    );

    const result = await invoke(
      database(
        "bench",
        "--yes",
        "--num-total-jobs",
        "300",
        "--max-workers",
        "50",
        "--max-connections",
        "10",
        "--statement-timeout",
        "1m"
      )
    );

    expect(result.stderr).toBe(
      `bench: emptying "${schema}"."river_job", "${schema}"."river_leader", ` +
        `"${schema}"."river_queue", "${schema}"."river_notification"\n`
    );
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(
      /^bench: total jobs worked \[ +300 \], total jobs inserted \[ +300 \], overall job\/sec \[ +[\d.]+ \], p95 \[ +[\d.]+s \], running [\d.]+s$/m
    );
    const queues = await pool.query<{ name: string }>(
      `SELECT name FROM "${schema}".river_queue ORDER BY name`
    );
    expect(queues.rows.map(({ name }) => name)).not.toContain("bench_sentinel");
    const jobs = await pool.query<{
      args: unknown;
      kind: string;
      state: string;
    }>(
      `SELECT args, kind, state FROM "${schema}".river_job ORDER BY id LIMIT 2`
    );
    expect(jobs.rows).toEqual([
      { args: { num: 1 }, kind: "benchmark", state: "completed" },
      { args: { num: 2 }, kind: "benchmark", state: "completed" },
    ]);
    expect(process.listenerCount("SIGINT")).toBe(0);
  });

  it("refuses to benchmark an unmigrated schema", async () => {
    const result = await invoke(database("bench", "--yes", "-n", "1"));

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(
      "the database is not fully migrated (unapplied migrations: 1, 2, 3, 4, 5, 6, 7, 8); run riverqueue migrate-up first"
    );
  });
});
