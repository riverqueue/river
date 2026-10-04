// Copy the repository license to the root of the package being built, where
// the registry and license tooling look for it. The copies are generated build
// output ignored by Git; the repository root LICENSE is the only source.

import { copyFile } from "node:fs/promises";
import { resolve } from "node:path";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const packageRoot = resolve(process.cwd());

if (packageRoot === repositoryRoot) {
  throw new Error("the repository root already contains LICENSE");
}
await copyFile(
  resolve(repositoryRoot, "LICENSE"),
  resolve(packageRoot, "LICENSE")
);
