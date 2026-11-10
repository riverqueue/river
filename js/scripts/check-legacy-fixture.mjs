import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  assertNoDeniedContent,
  assertPortableArchivePath,
  deniedSubstrings,
} from "./package-guard.mjs";

const execFileAsync = promisify(execFile);
const compilers = ["typescript", "typescript-next"];
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const fixtureRoot = resolve(repositoryRoot, "fixtures/migration-0.1");
const originalRoot = join(fixtureRoot, "original");
const manifest = JSON.parse(
  await readFile(join(originalRoot, "manifest.json"), "utf8")
);
const archive = join(fixtureRoot, manifest.npm.file);
const archiveBytes = await readFile(archive);

assert.equal(hash(archiveBytes, "sha1", "hex"), manifest.npm.sha1);
assert.equal(hash(archiveBytes, "sha256", "hex"), manifest.npm.sha256);
assert.equal(
  `sha512-${hash(archiveBytes, "sha512", "base64")}`,
  manifest.npm.integrity
);
for (const [relativePath, expected] of Object.entries(
  manifest.source.documents
)) {
  const contents = await readFile(join(originalRoot, relativePath));
  assert.equal(hash(contents, "sha256", "hex"), expected, relativePath);
}

const { stdout: archiveListing } = await execFileAsync("tar", [
  "-tzf",
  archive,
]);
const archiveFiles = archiveListing.trim().split("\n");
const denied = deniedSubstrings(repositoryRoot);
for (const file of archiveFiles) {
  assert.ok(file.startsWith("package/"), `${file} is inside package/`);
  assertPortableArchivePath(`legacy archive ${file}`, file);
  assertNoDeniedContent(`legacy archive ${file}`, file, denied);
}
const { stdout: packageJsonText } = await execFileAsync("tar", [
  "-xOf",
  archive,
  "package/package.json",
]);
const packageJson = JSON.parse(packageJsonText);
assert.equal(packageJson.name, "riverqueue");
assert.equal(packageJson.version, manifest.version);
// The pinned 0.1.0 archive predates the switch to MPL-2.0.
assert.equal(packageJson.license, "LGPL-3.0-or-later");

const temporaryDirectory = await mkdtemp(join(tmpdir(), "riverqueue-0.1-"));
try {
  const packageDirectory = join(
    temporaryDirectory,
    "node_modules",
    "riverqueue"
  );
  await mkdir(packageDirectory, { recursive: true });
  await execFileAsync("tar", [
    "-xzf",
    archive,
    "--strip-components=1",
    "-C",
    packageDirectory,
  ]);
  await copyFile(
    join(fixtureRoot, "before.ts.txt"),
    join(temporaryDirectory, "consumer.ts")
  );
  await writeFile(
    join(temporaryDirectory, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          exactOptionalPropertyTypes: true,
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          skipLibCheck: true,
          strict: true,
          target: "ES2024",
          types: [],
        },
        files: ["consumer.ts"],
      },
      null,
      2
    )}\n`
  );
  for (const compiler of compilers) {
    await execFileAsync(process.execPath, [
      resolve(repositoryRoot, "node_modules", compiler, "bin", "tsc"),
      "--project",
      join(temporaryDirectory, "tsconfig.json"),
    ]);
  }
} finally {
  await rm(temporaryDirectory, { force: true, recursive: true });
}

process.stdout.write(
  `validated riverqueue@${manifest.version} archive, docs, and TS6/TS-next consumer\n`
);

function hash(contents, algorithm, encoding) {
  return createHash(algorithm).update(contents).digest(encoding);
}
