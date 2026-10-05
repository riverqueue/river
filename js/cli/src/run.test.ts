import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";

import type { Migration, MigrationBackend } from "@riverqueue/migrate";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { parseDuration } from "./options.js";
import { run, type RunOptions } from "./run.js";

interface Invocation {
  exitCode: number;
  stderr: string;
  stdout: string;
}

async function invoke(
  argv: readonly string[],
  options: {
    interactive?: boolean;
    migrationLines?: RunOptions["migrationLines"];
    program?: string;
  } = {}
): Promise<Invocation> {
  let stderr = "";
  let stdout = "";
  const stdin = Object.assign(new PassThrough(), {
    isTTY: options.interactive ?? false,
  });
  const exitCode = await run(argv, {
    ...(options.migrationLines === undefined
      ? {}
      : { migrationLines: options.migrationLines }),
    ...(options.program === undefined ? {} : { program: options.program }),
    stderr: {
      isTTY: options.interactive ?? false,
      write: (chunk: string) => (stderr += chunk),
    },
    stdin,
    stdout: { write: (chunk: string) => (stdout += chunk) },
  });
  return { exitCode, stderr, stdout };
}

describe("help and version", () => {
  it.each([[[]], [["--help"]], [["-h"]], [["help"]]])(
    "prints program help for %j",
    async (argv) => {
      const result = await invoke(argv);

      expect(result).toMatchObject({ exitCode: 0, stderr: "" });
      for (const command of [
        "bench",
        "migrate-down",
        "migrate-get",
        "migrate-list",
        "migrate-up",
        "validate",
        "version",
      ]) {
        expect(result.stdout).toContain(`  ${command} `);
      }
      expect(result.stdout).toContain("riverqueue <command> [flags]");
    }
  );

  it.each([
    [["migrate-up", "--help"]],
    [["migrate-up", "-h"]],
    [["help", "migrate-up"]],
  ])("prints command help for %j", async (argv) => {
    const result = await invoke(argv);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("riverqueue migrate-up [flags]");
    expect(result.stdout).toMatch(
      /--target-version VERSION\s+Version to end at/
    );
    expect(result.stdout).not.toContain("--num-total-jobs");
  });

  it("describes durations without Go jargon", async () => {
    const result = await invoke(["bench", "--help"]);

    expect(result.stdout.replaceAll(/\s+/g, " ")).toContain(
      "such as 30s, 5m, or 1h30m"
    );
    expect(result.stdout).not.toMatch(/go-style/i);
    for (const line of result.stdout.split("\n")) {
      expect(line.length).toBeLessThanOrEqual(80);
    }
  });

  it.each([[["--version"]], [["version"]]])(
    "prints versions for %j",
    async (argv) => {
      const result = await invoke(argv);

      expect(result.exitCode).toBe(0);
      expect(result.stdout).toMatch(
        /^riverqueue version \d+\.\d+\.\d+\S*\nNode\.js v\d+/
      );
    }
  );
});

describe("argument errors", () => {
  it.each([
    [
      ["migrate-up", "--bogus-flag"],
      "unknown flag: --bogus-flag",
      "migrate-up",
    ],
    [["migrate-up", "-x"], "unknown flag: -x", "migrate-up"],
    [
      ["migrate-up", "--num-total-jobs", "10"],
      "unknown flag: --num-total-jobs",
      "migrate-up",
    ],
    [
      ["migrate-up", "--max-steps"],
      "flag --max-steps requires a value",
      "migrate-up",
    ],
    [
      ["migrate-up", "--max-steps", "--dry-run"],
      "flag --max-steps requires a value; " +
        'write --max-steps=VALUE to pass a value that starts with "-"',
      "migrate-up",
    ],
    [
      ["migrate-up", "--dry-run=yes"],
      "flag --dry-run does not take a value",
      "migrate-up",
    ],
    [["migrate-up", "extra"], 'unexpected argument: "extra"', "migrate-up"],
    [
      ["migrate-up", "--max-steps", "two"],
      '--max-steps must be a non-negative integer; received "two"',
      "migrate-up",
    ],
    [
      ["migrate-down", "--target-version=-1"],
      '--target-version must be a non-negative integer; received "-1"',
      "migrate-down",
    ],
    [["unknown"], 'unknown command: "unknown"', undefined],
    [["--bogus"], "unknown flag: --bogus", undefined],
    [["help", "nope"], 'unknown command: "nope"', undefined],
  ])("rejects %j", async (argv, message, command) => {
    const result = await invoke(argv);

    expect(result.exitCode).toBe(1);
    expect(result.stdout).toBe("");
    const prefix =
      command === undefined ? "riverqueue" : `riverqueue ${command}`;
    expect(result.stderr).toBe(
      `${prefix}: ${message}\n` +
        `Run "riverqueue${command === undefined ? "" : ` ${command}`} --help" for usage.\n`
    );
  });

  it.each([
    [
      ["migrate-up", "--database-url", "mysql://localhost/river"],
      "--database-url must start with postgres://",
    ],
    [
      ["migrate-up", "--database-url", "sqlite://"],
      "a SQLite --database-url needs a path",
    ],
    [
      ["migrate-up", "--database-url", "sqlite::memory:"],
      "--database-url must start with",
    ],
    [
      [
        "migrate-up",
        "--database-url",
        "sqlite://:memory:",
        "--schema",
        "river",
      ],
      "--schema only applies to PostgreSQL",
    ],
    [
      [
        "migrate-up",
        "--database-url",
        "sqlite://:memory:",
        "--statement-timeout",
        "5s",
      ],
      "--statement-timeout only applies to PostgreSQL",
    ],
    [
      ["migrate-up", "--database-url", "sqlite://:memory:", "--line", "mian"],
      "migration line does not exist: mian (available lines: main)",
    ],
    [
      [
        "migrate-up",
        "--database-url",
        "postgres://localhost/river",
        "--statement-timeout",
        "10",
      ],
      "--statement-timeout must be a duration",
    ],
  ])("rejects database arguments %j", async (argv, message) => {
    const result = await invoke(argv);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(message);
  });

  describe("without --database-url", () => {
    let pgDatabase: string | undefined;

    beforeEach(() => {
      pgDatabase = process.env.PGDATABASE;
      delete process.env.PGDATABASE;
    });

    afterEach(() => {
      if (pgDatabase !== undefined) process.env.PGDATABASE = pgDatabase;
    });

    it("requires a URL unless PGDATABASE is set", async () => {
      const result = await invoke(["migrate-list"]);

      expect(result.exitCode).toBe(1);
      expect(result.stderr).toContain(
        "--database-url is required unless PGDATABASE"
      );
    });
  });
});

describe("bench guards", () => {
  it.each([
    [
      ["bench"],
      "--database-url is required because bench empties River's tables",
    ],
    [
      ["bench", "--database-url", "postgres://localhost/river"],
      "pass --yes to confirm emptying River's tables",
    ],
    [
      ["bench", "--database-url", "sqlite:///tmp/river.db", "--yes"],
      "only PostgreSQL databases can be benchmarked",
    ],
    [
      [
        "bench",
        "--database-url",
        "postgres://localhost/river",
        "--duration",
        "1s",
        "-n",
        "5",
      ],
      "pass at most one of --duration and --num-total-jobs",
    ],
    [
      [
        "bench",
        "--database-url",
        "postgres://localhost/river",
        "--duration",
        "90",
      ],
      '--duration must be a duration such as 500ms, 30s, 5m, or 1h30m; received "90"',
    ],
    [
      ["bench", "--database-url", "postgres://localhost/river", "--dry-run"],
      "unknown flag: --dry-run",
    ],
  ])("rejects %j before touching a database", async (argv, message) => {
    const result = await invoke(argv);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(message);
    expect(result.stdout).toBe("");
  });

  it("never reads DATABASE_URL", async () => {
    const previous = process.env.DATABASE_URL;
    process.env.DATABASE_URL = "postgres://localhost/river";
    try {
      const result = await invoke(["bench", "--yes"]);

      expect(result.exitCode).toBe(1);
      expect(result.stderr).toContain("--database-url is required");
    } finally {
      if (previous === undefined) delete process.env.DATABASE_URL;
      else process.env.DATABASE_URL = previous;
    }
  });
});

describe("parseDuration", () => {
  it.each([
    ["500ms", 500],
    ["30s", 30_000],
    ["1.5s", 1_500],
    ["5m", 300_000],
    ["1h30m", 5_400_000],
    ["2m0.5s", 120_500],
  ])("parses %s", (value, expected) => {
    expect(parseDuration("bench", "--duration", value)).toBe(expected);
  });

  it.each(["", "30", "0s", "1d", "s", "1s ", "-1s", "0.1ms"])(
    "rejects %j",
    (value) => {
      expect(() => parseDuration("bench", "--duration", value)).toThrow(
        "--duration must be a duration such as"
      );
    }
  );
});

describe("migrate-get", () => {
  it("prints selected versions with schema-qualified SQL", async () => {
    const result = await invoke([
      "migrate-get",
      "--version",
      "1,2",
      "--up",
      "--schema",
      'my"schema',
    ]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(/^-- River main migration 001 \[up\]\n/);
    expect(result.stdout).toContain("\n\n-- River main migration 002 [up]\n");
    expect(result.stdout).toContain(
      'CREATE TABLE "my""schema".river_migration'
    );
    expect(result.stdout).not.toContain("TEMPLATE");
  });

  it("prints every down migration newest first, excluding versions", async () => {
    const result = await invoke([
      "migrate-get",
      "--all",
      "--down",
      "--exclude-version",
      "1",
      "--exclude-version=8",
    ]);

    const headers = [
      ...result.stdout.matchAll(/^-- River main migration (\d+)/gm),
    ].map((match) => match[1]);
    expect(headers).toEqual(["007", "006", "005", "004", "003", "002"]);
  });

  it("prints SQLite SQL when the URL selects SQLite", async () => {
    const postgres = await invoke(["migrate-get", "--version", "2", "--up"]);
    const sqlite = await invoke([
      "migrate-get",
      "--version",
      "2",
      "--up",
      "--database-url",
      "sqlite://",
    ]);

    expect(sqlite.exitCode).toBe(0);
    expect(sqlite.stdout).not.toBe(postgres.stdout);
    expect(postgres.stdout).toContain("CREATE TYPE");
    expect(sqlite.stdout).not.toContain("CREATE TYPE");
  });

  it.each([
    [["migrate-get", "--up"], "pass exactly one of --all or --version"],
    [
      ["migrate-get", "--all", "--version", "1", "--up"],
      "pass exactly one of --all or --version",
    ],
    [["migrate-get", "--all"], "pass exactly one of --up or --down"],
    [
      ["migrate-get", "--all", "--up", "--down"],
      "pass exactly one of --up or --down",
    ],
    [
      ["migrate-get", "--version", "9", "--up"],
      "migration 9 does not exist (available versions: 1, 2, 3, 4, 5, 6, 7, 8)",
    ],
    [
      ["migrate-get", "--version", "1,x", "--up"],
      '--version must be a positive integer; received "x"',
    ],
    [
      [
        "migrate-get",
        "--all",
        "--up",
        "--database-url",
        "sqlite://",
        "--schema",
        "s",
      ],
      "--schema only applies to PostgreSQL",
    ],
  ])("rejects %j", async (argv, message) => {
    const result = await invoke(argv);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(message);
  });
});

describe("SQLite migrations", () => {
  let directory: string;
  let url: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "riverqueue-cli-"));
    url = `sqlite://${join(directory, "river.db")}`;
  });

  afterEach(() => {
    rmSync(directory, { force: true, recursive: true });
  });

  it("migrates up, lists, validates, and migrates down", async () => {
    const missing = await invoke(["validate", "--database-url", url]);
    expect(missing).toEqual({
      exitCode: 1,
      stderr: "unapplied migrations: 1, 2, 3, 4, 5, 6, 7, 8\n",
      stdout: "",
    });

    const dryRun = await invoke([
      "migrate-up",
      "--database-url",
      url,
      "--dry-run",
      "--show-sql",
      "--max-steps",
      "1",
    ]);
    expect(dryRun.exitCode).toBe(0);
    expect(dryRun.stdout).toContain(
      "migration 001 [up] create_river_migration [dry run]\n" +
        "-".repeat(80) +
        "\n-- River main migration 001 [up]\nCREATE TABLE river_migration"
    );

    const up = await invoke(["migrate-up", "--database-url", url]);
    expect(up.exitCode).toBe(0);
    expect(up.stdout.trim().split("\n")).toHaveLength(8);
    expect(up.stdout).toMatch(
      /^applied migration 001 \[up\] create_river_migration\s+\[\d+\.\d+m?s\]$/m
    );

    const list = await invoke(["migrate-list", "--database-url", url]);
    expect(list.stdout).toContain(
      "  007 notification_outbox_sqlite_jsonb_and_sql_cleanup\n* 008 "
    );
    await expect(invoke(["validate", "--database-url", url])).resolves.toEqual({
      exitCode: 0,
      stderr: "",
      stdout: "",
    });
    await expect(
      invoke(["migrate-up", "--database-url", url])
    ).resolves.toMatchObject({
      exitCode: 0,
      stdout: "no migrations to apply\n",
    });

    const down = await invoke(["migrate-down", "--database-url", url]);
    expect(down.stdout).toMatch(/^applied migration 008 \[down\] /);
    const toFive = await invoke([
      "migrate-down",
      "--database-url",
      url,
      "--target-version",
      "5",
      "--max-steps",
      "3",
    ]);
    expect(toFive.stdout).toMatch(
      /^applied migration 007 \[down\] .*\napplied migration 006 \[down\] bulk_unique .*\nno more migrations to apply\n$/
    );
    const all = await invoke([
      "migrate-down",
      "--database-url",
      url,
      "--target-version",
      "0",
    ]);
    expect(all.stdout.trim().split("\n")).toHaveLength(5);
    const empty = await invoke(["migrate-list", "--database-url", url]);
    expect(empty.stdout).toMatch(/^001 create_river_migration\n/);
  });

  it("reports migration failures with their cause", async () => {
    const result = await invoke([
      "migrate-down",
      "--database-url",
      url,
      "--target-version",
      "3",
    ]);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toBe(
      "riverqueue migrate-down: cannot migrate down to version 3 because it is not applied\n"
    );
  });
});

describe("embedding", () => {
  const EXTRA_MIGRATIONS: readonly Migration[] = [
    {
      downSql: "DROP TABLE /* TEMPLATE: schema */extra_widget;",
      name: "create_widget",
      upSql:
        "CREATE TABLE /* TEMPLATE: schema */extra_widget (id INTEGER PRIMARY KEY);",
      version: 1,
    },
    {
      downSql:
        "ALTER TABLE /* TEMPLATE: schema */extra_widget DROP COLUMN name;",
      name: "add_widget_name",
      upSql:
        "ALTER TABLE /* TEMPLATE: schema */extra_widget ADD COLUMN name TEXT;",
      version: 2,
    },
  ];

  let backends: MigrationBackend[];
  let directory: string;
  let url: string;
  let migrationLines: RunOptions["migrationLines"];

  beforeEach(() => {
    backends = [];
    directory = mkdtempSync(join(tmpdir(), "riverqueue-cli-lines-"));
    url = `sqlite://${join(directory, "river.db")}`;
    migrationLines = {
      extra: (backend) => {
        backends.push(backend);
        return EXTRA_MIGRATIONS;
      },
      other: () => EXTRA_MIGRATIONS,
    };
  });

  afterEach(() => {
    rmSync(directory, { force: true, recursive: true });
  });

  it("migrates an additional line selected with --line", async () => {
    const extra = (...argv: string[]) =>
      invoke([...argv, "--database-url", url, "--line", "extra"], {
        migrationLines,
      });
    await invoke(["migrate-up", "--database-url", url]);

    const up = await extra("migrate-up");
    expect(up).toMatchObject({ exitCode: 0, stderr: "" });
    expect(up.stdout).toMatch(
      /^applied migration 001 \[up\] create_widget +\[.*\]\napplied migration 002 \[up\] add_widget_name +\[.*\]\n$/
    );
    await expect(extra("migrate-list")).resolves.toMatchObject({
      exitCode: 0,
      stdout: "  001 create_widget\n* 002 add_widget_name\n",
    });
    await expect(extra("validate")).resolves.toMatchObject({ exitCode: 0 });
    const down = await extra("migrate-down", "--target-version", "0");
    expect(down.stdout).toMatch(
      /^applied migration 002 \[down\] add_widget_name .*\napplied migration 001 \[down\] create_widget /
    );
    await expect(extra("validate")).resolves.toMatchObject({
      exitCode: 1,
      stderr: "unapplied migrations: 1, 2\n",
    });
    // The main line is untouched.
    await expect(
      invoke(["validate", "--database-url", url], { migrationLines })
    ).resolves.toMatchObject({ exitCode: 0 });
    expect(new Set(backends)).toEqual(new Set(["sqlite"]));
  });

  it("prints an additional line's SQL for each dialect", async () => {
    const postgres = await invoke(
      [
        "migrate-get",
        "--line",
        "extra",
        "--version",
        "1",
        "--up",
        "--schema",
        "s",
      ],
      { migrationLines }
    );
    const sqlite = await invoke(
      [
        "migrate-get",
        "--line",
        "extra",
        "--all",
        "--down",
        "--database-url",
        "sqlite://",
      ],
      { migrationLines }
    );

    expect(postgres.stdout).toBe(
      "-- River extra migration 001 [up]\n" +
        'CREATE TABLE "s".extra_widget (id INTEGER PRIMARY KEY);\n'
    );
    expect(sqlite.stdout).toBe(
      "-- River extra migration 002 [down]\n" +
        "ALTER TABLE extra_widget DROP COLUMN name;\n\n" +
        "-- River extra migration 001 [down]\n" +
        "DROP TABLE extra_widget;\n"
    );
    expect(backends).toEqual(["postgres", "sqlite"]);
  });

  it("rejects an unknown line, listing the known ones", async () => {
    const result = await invoke(
      ["migrate-list", "--database-url", url, "--line", "missing"],
      { migrationLines }
    );

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(
      "migration line does not exist: missing (available lines: main, extra, other)"
    );
  });

  it("keeps River's main line", async () => {
    const result = await invoke(["version"], {
      migrationLines: { main: () => EXTRA_MIGRATIONS },
    });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toBe(
      "riverqueue: migrationLines cannot replace River's main line\n"
    );
  });

  it("uses the program name in help, version, and errors", async () => {
    const program = "extension";

    const help = await invoke(["--help"], { program });
    const commandHelp = await invoke(["migrate-get", "--help"], { program });
    const version = await invoke(["--version"], { program });
    const unknown = await invoke(["nope"], { program });
    const usage = await invoke(["migrate-up", "--bogus"], { program });

    expect(help.stdout).toContain("extension <command> [flags]");
    expect(help.stdout).toContain('Run "extension <command> --help"');
    expect(commandHelp.stdout).toContain("extension migrate-get [flags]");
    expect(commandHelp.stdout).toContain(
      "  extension migrate-get --version 3 --up > river_3.up.sql"
    );
    expect(commandHelp.stdout).not.toContain("riverqueue");
    expect(version.stdout).toMatch(/^extension version /);
    expect(unknown.stderr).toBe(
      'extension: unknown command: "nope"\nRun "extension --help" for usage.\n'
    );
    expect(usage.stderr).toBe(
      "extension migrate-up: unknown flag: --bogus\n" +
        'Run "extension migrate-up --help" for usage.\n'
    );
  });
});
