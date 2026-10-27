import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { after, before, describe, it } from "node:test";
import { fileURLToPath, pathToFileURL, URL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const bin = fileURLToPath(
  new URL("../node_modules/.bin/riverqueue", import.meta.url)
);

function cliEnvironment() {
  // The parent test runner preloads Temporal for library tests. Standalone
  // CLI processes must supply their own implementation on Node 24.
  return { ...process.env, NO_COLOR: "1", NODE_OPTIONS: "", PGDATABASE: "" };
}

function importCli(beforeImport, afterImport = "") {
  return execFileAsync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
import assert from "node:assert/strict";
process.argv = [process.execPath, ${JSON.stringify(bin)}, "--help"];
${beforeImport}
await import(${JSON.stringify(pathToFileURL(bin).href)});
${afterImport}
`,
    ],
    { env: cliEnvironment() }
  );
}

function riverqueue(...args) {
  return execFileAsync(bin, args, { env: cliEnvironment() });
}

describe("packed riverqueue CLI", () => {
  let directory;

  before(async () => {
    directory = await mkdtemp(join(tmpdir(), "riverqueue-packed-cli-"));
  });

  after(async () => {
    await rm(directory, { force: true, recursive: true });
  });

  it("initializes Temporal only in the executable when the global is absent", async () => {
    const { stdout, stderr } = await importCli(
      "delete globalThis.Temporal;",
      `assert.equal(typeof globalThis.Temporal.Instant.from, "function");
assert.equal(Temporal.Instant.from("2026-10-26T00:00:00Z").toString(), "2026-10-26T00:00:00Z");`
    );
    assert.match(stdout, /Commands:/);
    assert.equal(stderr, "");
  });

  it("preserves native or application-provided Temporal in the executable", async () => {
    const { stdout } = await importCli(
      `const original = globalThis.Temporal ?? (await import("@js-temporal/polyfill")).Temporal;
Object.defineProperty(globalThis, "Temporal", { value: original, configurable: false, writable: false });`,
      "assert.equal(globalThis.Temporal, original);"
    );
    assert.match(stdout, /Commands:/);
  });

  it("reports initialization errors without a stack trace", async () => {
    await assert.rejects(importCli("globalThis.Temporal = {};"), (error) => {
      assert.equal(error.code, 1);
      assert.equal(error.stdout, "");
      assert.match(
        error.stderr,
        /^riverqueue: River requires the Temporal API/
      );
      assert.match(error.stderr, /#temporal-on-nodejs-24/);
      assert.equal(error.stderr.trim().split("\n").length, 1);
      return true;
    });
  });

  it("reports its version without an application preload", async () => {
    const { stdout } = await riverqueue("--version");
    assert.match(stdout, /^riverqueue version \d+\.\d+\.\d+/);
  });

  it("prints help for the command set without an application preload", async () => {
    const { stdout } = await riverqueue("--help");
    for (const command of [
      "bench",
      "migrate-down",
      "migrate-get",
      "migrate-list",
      "migrate-up",
      "validate",
      "version",
    ]) {
      assert.match(stdout, new RegExp(`\\b${command}\\b`), command);
    }
  });

  it("migrates and validates a SQLite database without an application preload", async () => {
    const databaseUrl = `sqlite://${join(directory, "river.db")}`;
    await assert.rejects(
      riverqueue("validate", "--database-url", databaseUrl),
      (error) => error.code !== 0
    );
    const { stdout: applied } = await riverqueue(
      "migrate-up",
      "--database-url",
      databaseUrl
    );
    assert.match(applied, /applied migration 001/);
    await riverqueue("validate", "--database-url", databaseUrl);
    const { stdout: listed } = await riverqueue(
      "migrate-list",
      "--database-url",
      databaseUrl
    );
    assert.doesNotMatch(listed, /not applied/i);
  });

  it("fails with a usage error for an unknown command", async () => {
    await assert.rejects(riverqueue("no-such-command"), (error) => {
      assert.notEqual(error.code, 0);
      assert.match(`${error.stderr}${error.stdout}`, /no-such-command/);
      return true;
    });
  });
});
