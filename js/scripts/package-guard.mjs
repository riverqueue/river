// Guards shared by the archive checks so published tarballs and pinned
// fixtures cannot carry machine-local paths or maintainer-denied names.

import assert from "node:assert/strict";
import { homedir } from "node:os";
import { posix } from "node:path";
import process from "node:process";

/**
 * Build the list of case-insensitive substrings that must never appear in a
 * packed path or packed text file.
 *
 * The list always includes the local checkout and home directory so builds
 * cannot leak absolute machine paths. Maintainers can extend it without
 * committing the extra names by setting `RIVER_PACKAGE_DENYLIST` to a comma-
 * or newline-separated list.
 */
export function deniedSubstrings(repositoryRoot, env = process.env) {
  const configured = (env.RIVER_PACKAGE_DENYLIST ?? "")
    .split(/[,\n]/u)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  // Match local directories as path prefixes so a short home directory such
  // as `/root` cannot collide with ordinary words.
  const localDirectories = [repositoryRoot, homedir()]
    .filter((directory) => directory.length > 1)
    .map((directory) => `${directory.replace(/[\\/]+$/u, "")}/`);
  return [...new Set([...localDirectories, ...configured])].map((entry) =>
    entry.toLowerCase()
  );
}

/** Assert that text contains none of the denied substrings. */
export function assertNoDeniedContent(label, text, denied) {
  const lowered = text.toLowerCase();
  for (const entry of denied) {
    assert.ok(
      !lowered.includes(entry),
      `${label} contains a denied local or private path`
    );
  }
}

/**
 * Assert that an archive-relative path is portable: relative, POSIX
 * separators, and never escaping the archive root.
 */
export function assertPortableArchivePath(label, path) {
  assert.ok(
    !path.startsWith("/") && !path.includes("\\") && !/^[a-z]:/iu.test(path),
    `${label} is not a portable relative path: ${JSON.stringify(path)}`
  );
  const normalized = posix.normalize(path);
  assert.ok(
    normalized !== ".." && !normalized.startsWith("../"),
    `${label} escapes the archive root: ${JSON.stringify(path)}`
  );
}
