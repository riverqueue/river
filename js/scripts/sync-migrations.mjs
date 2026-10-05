// Mirrors River's canonical PostgreSQL and SQLite migrations, which live in
// the Go drivers of the River repository this workspace is part of, into
// `migrate/migrations` with a manifest of SHA-256 digests. `--check` fails
// instead of writing when the mirror or its manifest differs from River's
// sources.
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const riverRoot = resolve(repositoryRoot, "..");
const targetRoot = resolve(repositoryRoot, "migrate/migrations");
const check = process.argv.includes("--check");
const sources = {
  postgres: "riverdriver/riverpgxv5/migration/main",
  sqlite: "riverdriver/riversqlite/migration/main",
};

const manifest = {
  backends: {},
  format: 1,
  sources,
};

for (const [backend, sourceRelative] of Object.entries(sources)) {
  const sourceDirectory = resolve(riverRoot, sourceRelative);
  const targetDirectory = resolve(targetRoot, backend, "main");
  const sourceFiles = (await readdir(sourceDirectory))
    .filter((file) => /^\d{3}_.+\.(?:up|down)\.sql$/.test(file))
    .sort();

  if (sourceFiles.length === 0) {
    throw new Error(`no canonical migrations found in ${sourceDirectory}`);
  }

  const entries = [];
  for (const file of sourceFiles) {
    const contents = await readFile(resolve(sourceDirectory, file));
    entries.push({
      file,
      sha256: createHash("sha256").update(contents).digest("hex"),
    });

    const target = resolve(targetDirectory, file);
    if (check) {
      let existing;
      try {
        existing = await readFile(target);
      } catch (error) {
        throw new Error(`generated migration is missing: ${target}`, {
          cause: error,
        });
      }
      if (!existing.equals(contents)) {
        throw new Error(
          `generated migration differs from canonical source: ${target}`
        );
      }
    } else {
      await mkdir(targetDirectory, { recursive: true });
      await writeFile(target, contents);
    }
  }

  for (const existing of await readdir(targetDirectory)) {
    if (existing.endsWith(".sql") && !sourceFiles.includes(existing)) {
      if (check) {
        throw new Error(
          `generated migration has no canonical source: ${resolve(targetDirectory, existing)}`
        );
      }
      await rm(resolve(targetDirectory, existing));
    }
  }

  manifest.backends[backend] = entries;
}

const encodedManifest = `${JSON.stringify(manifest, null, 2)}\n`;
const manifestPath = resolve(targetRoot, "manifest.json");
if (check) {
  const existingManifest = await readFile(manifestPath, "utf8");
  if (existingManifest !== encodedManifest) {
    throw new Error(`generated migration manifest is stale: ${manifestPath}`);
  }
} else {
  await mkdir(targetRoot, { recursive: true });
  await writeFile(manifestPath, encodedManifest);
}
