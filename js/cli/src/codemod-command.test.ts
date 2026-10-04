import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { loadTypeScript } from "./codemod-command.js";
import { run } from "./run.js";

const LEGACY = `import { Client, type JobArgs } from "riverqueue";

declare const client: Client;

class SortArgs implements JobArgs {
  kind = "sort";
  constructor(public strings: string[]) {}
}

const result = await client.insert(new SortArgs(["b", "a"]));
console.log(result.job.id + 1);
`;

const MIGRATED = `import { Client, defineJob } from "riverqueue";

declare const client: Client;

const sort = defineJob<{ strings: string[] }>()({
  kind: "sort",
});

const result = await client.insert(sort, { strings: ["b", "a"] });
// TODO(riverqueue-0.1): \`JobRow.id\` is now a \`bigint\`; review number annotations, arithmetic, and JSON serialization
console.log(result.job.id + 1);
`;

async function invoke(argv: readonly string[]): Promise<{
  exitCode: number;
  stderr: string;
  stdout: string;
}> {
  let stderr = "";
  let stdout = "";
  const exitCode = await run(argv, {
    stderr: { write: (chunk: string) => (stderr += chunk) },
    stdin: Object.assign(new PassThrough(), { isTTY: false }),
    stdout: { write: (chunk: string) => (stdout += chunk) },
  });
  return { exitCode, stderr, stdout };
}

describe("codemod-0.1", () => {
  let directory: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "riverqueue-codemod-"));
  });

  afterEach(() => {
    rmSync(directory, { force: true, recursive: true });
  });

  function file(name: string, contents: string): string {
    const path = join(directory, name);
    mkdirSync(join(path, ".."), { recursive: true });
    writeFileSync(path, contents);
    return path;
  }

  it("describes itself in help", async () => {
    const program = await invoke(["--help"]);
    const command = await invoke(["codemod-0.1", "--help"]);

    expect(program.stdout).toMatch(
      /codemod-0\.1\s+Migrate source code from riverqueue 0\.1/
    );
    expect(command.exitCode).toBe(0);
    expect(command.stdout).toContain(
      "riverqueue codemod-0.1 [flags] <file | directory | glob>..."
    );
    expect(command.stdout).toMatch(/--check\s+Exit with status 1/);
    expect(command.stdout).toMatch(/--write\s+Write the migrated files/);
  });

  it("reports changes without writing by default", async () => {
    const path = file("app.ts", LEGACY);

    const result = await invoke(["codemod-0.1", path]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("would rewrite ");
    expect(result.stdout).toContain("1 file to rewrite, 0 unchanged.");
    expect(result.stdout).toContain("(lines as rewritten):");
    expect(result.stdout).toMatch(/app\.ts:11: `JobRow\.id` is now a `bigint`/);
    expect(readFileSync(path, "utf8")).toBe(LEGACY);
  });

  it("writes changes and then passes --check", async () => {
    const path = file("app.ts", LEGACY);

    const check = await invoke(["codemod-0.1", "--check", path]);
    const write = await invoke(["codemod-0.1", "--write", path]);
    const recheck = await invoke(["codemod-0.1", "--check", path]);

    expect(check.exitCode).toBe(1);
    expect(write).toMatchObject({ exitCode: 0, stderr: "" });
    expect(write.stdout).toContain("rewrote ");
    expect(write.stdout).toContain("1 file rewritten, 0 unchanged.");
    expect(readFileSync(path, "utf8")).toBe(MIGRATED);
    expect(recheck.exitCode).toBe(0);
    expect(recheck.stdout).toContain("0 files to rewrite, 1 unchanged.");
    // Remaining sites are still listed for review.
    expect(recheck.stdout).toMatch(/app\.ts:11: `JobRow\.id`/);
  });

  it("expands directories and globs, skipping node_modules and declarations", async () => {
    const app = file("src/app.ts", LEGACY);
    const script = file("src/legacy.mjs", "export const answer = 42;\n");
    file("src/types.d.ts", LEGACY);
    file("src/node_modules/dep/index.ts", LEGACY);
    file("src/notes.md", "new SortArgs()\n");

    const fromDirectory = await invoke(["codemod-0.1", join(directory, "src")]);
    const fromGlob = await invoke([
      "codemod-0.1",
      join(directory, "src/**/*.ts"),
    ]);

    expect(fromDirectory.stdout).toContain("1 file to rewrite, 1 unchanged.");
    expect(fromGlob.stdout).toContain("1 file to rewrite, 0 unchanged.");
    for (const result of [fromDirectory, fromGlob]) {
      expect(result.stdout).toContain(join("src", "app.ts"));
      expect(result.stdout).not.toContain("node_modules");
    }
    expect(readFileSync(app, "utf8")).toBe(LEGACY);
    expect(readFileSync(script, "utf8")).toBe("export const answer = 42;\n");
  });

  it.each([
    [[], "pass at least one file, directory, or glob"],
    [
      ["--check", "--write", "app.ts"],
      "--check and --write cannot be combined",
    ],
    [["missing.ts"], 'no such file or directory: "missing.ts"'],
    [["README.md"], '"README.md" is not a .ts'],
    [["nothing/**/*.ts"], 'no source files match "nothing/**/*.ts"'],
  ])("rejects %j", async (argv, message) => {
    file("README.md", "# readme\n");
    const relativeArgv = argv.map((arg) =>
      arg.startsWith("-") ? arg : join(directory, arg)
    );

    const result = await invoke(["codemod-0.1", ...relativeArgv]);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain(
      message.replace(/"(.*)"/, (_match, name: string) =>
        JSON.stringify(join(directory, name))
      )
    );
    expect(result.stderr).toContain('Run "riverqueue codemod-0.1 --help"');
  });

  it("keeps rejecting positional arguments for other commands", async () => {
    const result = await invoke(["migrate-list", "extra"]);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain('unexpected argument: "extra"');
  });
});

describe("loadTypeScript", () => {
  it("loads the compiler API", () => {
    const ts = loadTypeScript([import.meta.dirname]);

    expect(typeof ts.createSourceFile).toBe("function");
  });

  it("explains how to install TypeScript when it is missing", () => {
    const directory = mkdtempSync(join(tmpdir(), "riverqueue-no-ts-"));
    try {
      expect(() => loadTypeScript([directory])).toThrow(
        /needs the typescript package, version 5 or 6.*npm install --save-dev typescript@6/
      );
    } finally {
      rmSync(directory, { force: true, recursive: true });
    }
  });

  it("skips a TypeScript without the compiler API", () => {
    const directory = mkdtempSync(join(tmpdir(), "riverqueue-native-ts-"));
    try {
      const packageDirectory = join(directory, "node_modules", "typescript");
      mkdirSync(packageDirectory, { recursive: true });
      writeFileSync(
        join(packageDirectory, "package.json"),
        JSON.stringify({
          main: "version.cjs",
          name: "typescript",
          version: "7.0.0",
        })
      );
      writeFileSync(
        join(packageDirectory, "version.cjs"),
        'module.exports = { version: "7.0.0" };\n'
      );

      expect(() => loadTypeScript([directory])).toThrow(
        /found typescript@7\.0\.0 without the compiler API/
      );
      expect(
        typeof loadTypeScript([directory, import.meta.dirname]).createSourceFile
      ).toBe("function");
    } finally {
      rmSync(directory, { force: true, recursive: true });
    }
  });
});
