// Checks and renders the JavaScript coverage matrix for River's shared
// conformance scenarios.
//
// Every scenario in River's PostgreSQL and SQLite catalogs must map, in
// `conformance/scenario-coverage.json`, to the JavaScript-native tests that
// cover the same behavior, to an explicit `gap` note, or to a
// `not_applicable` reason (for example, a tier that only means something with
// several engines on one database). The check fails on an unmapped or stale
// scenario ID, on a reference to a test that does not exist, and on a
// Markdown matrix that no longer matches the mapping.
//
// Catalogs are read from the River repository this workspace lives in, so the
// matrix always describes the scenarios the JavaScript adapter must pass.
//
//   node scripts/scenario-coverage.mjs          # check
//   node scripts/scenario-coverage.mjs --write  # render
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";

import * as prettier from "prettier";

const execFileAsync = promisify(execFile);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const riverRoot = resolve(repositoryRoot, "..");
const mappingPath = join(repositoryRoot, "conformance/scenario-coverage.json");
const matrixPath = join(repositoryRoot, "docs/conformance-coverage.md");

// River's scenario catalogs and the profile each one belongs to.
const CATALOGS = [
  { backend: "PostgreSQL", file: "core.json", profile: "postgres-full-v1" },
  {
    backend: "PostgreSQL",
    file: "insert-only.json",
    profile: "insert-only-v1",
  },
  {
    backend: "SQLite",
    file: "sqlite-storage.json",
    profile: "portable-storage-v1",
  },
  {
    backend: "SQLite",
    file: "sqlite-runtime.json",
    profile: "sqlite-runtime-v1",
  },
];

// Coverage statuses, in the order the matrix reports them.
const STATUSES = ["covered", "partial", "gap", "not applicable"];

// Vitest configurations whose tests may be cited as evidence.
const VITEST_CONFIGS = ["vitest.config.ts", "vitest.integration.config.ts"];

const { values: options } = parseArgs({
  // `pnpm run coverage:scenarios[:write] -- ...` forwards the separator,
  // after `--write` for the write script.
  args: process.argv.slice(2).filter((arg) => arg !== "--"),
  options: {
    write: { default: false, type: "boolean" },
  },
});

const catalogs = await loadCatalogs();
const mapping = JSON.parse(await readFile(mappingPath, "utf8"));
const tests = await listTests();
const problems = validate(catalogs, mapping, tests);
if (problems.length > 0) fail(problems);

const matrix = await renderMatrix(catalogs, sortMapping(mapping));
if (options.write) {
  await writeFile(
    mappingPath,
    `${JSON.stringify(sortMapping(mapping), null, 2)}\n`
  );
  await writeFile(matrixPath, matrix);
  process.stdout.write(
    `wrote ${relative(repositoryRoot, matrixPath)} for ${catalogs.length} scenarios\n`
  );
} else {
  const current = await readFile(matrixPath, "utf8").catch(() => "");
  if (current !== matrix) {
    fail([
      `${relative(repositoryRoot, matrixPath)} is stale; run pnpm run coverage:scenarios:write`,
    ]);
  }
  if (JSON.stringify(sortMapping(mapping)) !== JSON.stringify(mapping)) {
    fail([
      `${relative(repositoryRoot, mappingPath)} is not sorted by scenario ID; run pnpm run coverage:scenarios:write`,
    ]);
  }
  process.stdout.write(`${summaryLine(catalogs, mapping)}\n`);
}

async function loadCatalogs() {
  const scenarios = [];
  for (const catalog of CATALOGS) {
    const path = join(riverRoot, "conformance/scenarios", catalog.file);
    const parsed = JSON.parse(await readFile(path, "utf8"));
    for (const scenario of parsed.scenarios) {
      scenarios.push({
        backend: catalog.backend,
        id: scenario.name,
        profile: catalog.profile,
        tier: scenario.tier,
      });
    }
  }
  return scenarios;
}

/** Every Vitest test as `path > describe > ... > name`, per configuration. */
async function listTests() {
  const directory = await mkdtemp(join(tmpdir(), "riverqueue-scenarios-"));
  const names = new Set();
  try {
    for (const config of VITEST_CONFIGS) {
      const output = join(directory, `${config}.json`);
      try {
        await execFileAsync(
          join(repositoryRoot, "node_modules", ".bin", "vitest"),
          ["list", "--config", config, `--json=${output}`],
          {
            cwd: repositoryRoot,
            // Suites that skip without a database must still be listed, so
            // give them a placeholder. Listing collects tests without
            // running hooks, so nothing connects.
            env: {
              ...process.env,
              TEST_DATABASE_URL:
                process.env.TEST_DATABASE_URL ??
                "postgres://scenario-coverage.invalid/listing",
            },
            maxBuffer: 16 * 1024 * 1024,
          }
        );
      } catch (error) {
        fail([
          `vitest list --config ${config} failed:`,
          String(error.stderr ?? error.message).trim(),
        ]);
      }
      for (const test of JSON.parse(await readFile(output, "utf8"))) {
        names.add(`${relative(repositoryRoot, test.file)} > ${test.name}`);
      }
    }
  } finally {
    await rm(directory, { force: true, recursive: true });
  }
  return names;
}

function validate(scenarios, mapping, tests) {
  const problems = [];
  const entries = mapping.scenarios ?? {};
  const known = new Set(scenarios.map(({ id }) => id));
  for (const { id, profile } of scenarios) {
    if (!Object.hasOwn(entries, id)) {
      problems.push(`${id} (${profile}) has no JavaScript coverage entry`);
    }
  }
  for (const [id, entry] of Object.entries(entries)) {
    if (!known.has(id)) {
      problems.push(`${id} is not a scenario in River's catalogs (stale ID)`);
      continue;
    }
    const keys = Object.keys(entry).sort().join(",");
    if (
      !["gap", "gap,tests", "not_applicable", "tests"].includes(keys) ||
      (entry.tests !== undefined &&
        (!Array.isArray(entry.tests) || entry.tests.length === 0)) ||
      (entry.gap !== undefined && !nonEmptyString(entry.gap)) ||
      (entry.not_applicable !== undefined &&
        !nonEmptyString(entry.not_applicable))
    ) {
      problems.push(
        `${id} must have non-empty "tests", "gap", both, or only "not_applicable"`
      );
      continue;
    }
    for (const test of entry.tests ?? []) {
      if (!tests.has(test)) {
        problems.push(`${id} cites a test that does not exist: ${test}`);
      }
    }
    if (new Set(entry.tests ?? []).size !== (entry.tests ?? []).length) {
      problems.push(`${id} cites a test more than once`);
    }
  }
  return problems;
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function sortMapping(mapping) {
  const scenarios = {};
  for (const id of Object.keys(mapping.scenarios).sort()) {
    const entry = mapping.scenarios[id];
    scenarios[id] = Object.fromEntries(
      Object.keys(entry)
        .sort()
        .map((key) => [
          key,
          key === "tests" ? [...entry.tests].sort() : entry[key],
        ])
    );
  }
  return { ...mapping, scenarios };
}

function status(entry) {
  if (entry.not_applicable !== undefined) return "not applicable";
  if (entry.tests === undefined) return "gap";
  return entry.gap === undefined ? "covered" : "partial";
}

function countStatuses(scenarios, mapping) {
  const counts = Object.fromEntries(STATUSES.map((name) => [name, 0]));
  for (const { id } of scenarios) counts[status(mapping.scenarios[id])]++;
  return counts;
}

function summaryLine(scenarios, mapping) {
  const counts = countStatuses(scenarios, mapping);
  return `${scenarios.length} scenarios: ${STATUSES.map(
    (name) => `${counts[name]} ${name}`
  ).join(", ")}`;
}

async function renderMatrix(scenarios, mapping) {
  const lines = [
    "<!-- Generated by scripts/scenario-coverage.mjs from",
    "     conformance/scenario-coverage.json. Do not edit by hand. -->",
    "",
    "# Conformance scenario coverage",
    "",
    "River's shared conformance suite owns one executable scenario for each",
    "stable ID below, and the JavaScript adapter must pass all of them. This",
    "matrix records, for each scenario, the JavaScript-native tests in this",
    "repository that cover the same behavior, so a regression surfaces in",
    "`pnpm test` or `pnpm run test:integration` before it reaches the",
    "cross-language harness. Scenario IDs come from River's catalogs in",
    "`conformance/scenarios`.",
    "",
    "- **covered**: JavaScript-native tests exercise the behavior.",
    "- **partial**: tests exist, but part of the behavior is only checked by",
    "  the shared scenario; the note says which.",
    "- **gap**: only the shared scenario checks the behavior.",
    "- **not applicable**: the scenario needs several engines or a",
    "  harness-level measurement, so a single-implementation test would add",
    "  nothing.",
    "",
    "| Profile | Scenarios | Covered | Partial | Gap | Not applicable |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const catalog of CATALOGS) {
    const rows = scenarios.filter(({ profile }) => profile === catalog.profile);
    const counts = countStatuses(rows, mapping);
    lines.push(
      `| ${catalog.backend} \`${catalog.profile}\` | ${rows.length} | ${STATUSES.map(
        (name) => counts[name]
      ).join(" | ")} |`
    );
  }
  lines.push("");
  for (const catalog of CATALOGS) {
    const rows = scenarios
      .filter(({ profile }) => profile === catalog.profile)
      .sort((left, right) => left.id.localeCompare(right.id));
    lines.push(`## ${catalog.backend}: \`${catalog.profile}\``, "");
    for (const { id, tier } of rows) {
      const entry = mapping.scenarios[id];
      lines.push(`- **\`${id}\`** (${tier}, ${status(entry)})`);
      for (const test of entry.tests ?? []) {
        lines.push(`  - ${formatTest(test)}`);
      }
      if (entry.gap !== undefined) lines.push(`  - Gap: ${entry.gap}`);
      if (entry.not_applicable !== undefined) {
        lines.push(`  - ${entry.not_applicable}`);
      }
    }
    lines.push("");
  }
  const config = await prettier.resolveConfig(matrixPath);
  return prettier.format(lines.join("\n"), {
    ...config,
    filepath: matrixPath,
  });
}

function formatTest(test) {
  const [file, ...names] = test.split(" > ");
  return `\`${file}\`: ${names.join(" › ")}`;
}

function fail(messages) {
  for (const message of messages) process.stderr.write(`${message}\n`);
  process.exit(1);
}
