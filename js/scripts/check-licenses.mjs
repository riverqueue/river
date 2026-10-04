import { execFile } from "node:child_process";
import { dirname, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageDirectories = [
  ".",
  "migrate",
  "driver/pg",
  "driver/prisma",
  "driver/sqlite",
  "worker-threads",
  "test",
  "cli",
];
const allowed = [
  "0BSD",
  "Apache-2.0",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "ISC",
  "LGPL-3.0-or-later",
  "MIT",
  "PostgreSQL",
].join(";");
const checker = resolve(
  repositoryRoot,
  "node_modules/.bin/license-checker-rseidelsohn"
);

for (const directory of packageDirectories) {
  const packageRoot = resolve(repositoryRoot, directory);
  try {
    await execFileAsync(checker, [
      "--production",
      "--onlyAllow",
      allowed,
      "--excludePrivatePackages",
      "--start",
      packageRoot,
      "--summary",
    ]);
  } catch (error) {
    if (error.stdout) process.stderr.write(error.stdout);
    if (error.stderr) process.stderr.write(error.stderr);
    throw error;
  }
}

process.stdout.write(
  `validated production dependency licenses for ${packageDirectories.length} packages\n`
);
