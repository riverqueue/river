import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { after, before, describe, it } from "node:test";
import { fileURLToPath, URL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const bin = fileURLToPath(
  new URL("../node_modules/.bin/riverqueue", import.meta.url)
);

function riverqueue(...args) {
  return execFileAsync(bin, args, {
    env: { ...process.env, NO_COLOR: "1", PGDATABASE: "" },
  });
}

describe("packed riverqueue CLI", () => {
  let directory;

  before(async () => {
    directory = await mkdtemp(join(tmpdir(), "riverqueue-packed-cli-"));
  });

  after(async () => {
    await rm(directory, { force: true, recursive: true });
  });

  it("reports its version", async () => {
    const { stdout } = await riverqueue("--version");
    assert.match(stdout, /^riverqueue version \d+\.\d+\.\d+/);
  });

  it("prints help for the command set", async () => {
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

  it("migrates and validates a SQLite database with bundled migrations", async () => {
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
