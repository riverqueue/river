import { glob, readFile, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import {
  dirname,
  extname,
  isAbsolute,
  join,
  relative,
  resolve,
} from "node:path";
import { fileURLToPath } from "node:url";

import {
  migrateSources,
  SOURCE_EXTENSIONS,
  TODO_MARKER,
  type CodemodFileResult,
  type TypeScriptApi,
} from "./codemod.js";
import { writeLine, type Command, type CommandContext } from "./command.js";
import { booleanValue, UsageError, type OptionValues } from "./options.js";

const COMMAND = "codemod-0.1";
const SKIPPED_DIRECTORIES = new Set([".git", "node_modules"]);

/** `riverqueue codemod-0.1`: rewrite sources written for `riverqueue@0.1`. */
export const codemodCommand: Command = {
  description: `
Migrate TypeScript and JavaScript sources written for riverqueue 0.1 to the
current API. Pass files, directories, or glob patterns; directories are
searched for .ts, .tsx, .mts, .cts, .js, .jsx, .mjs, and .cjs files outside
node_modules. Without --write the command only reports what it would change.

It converts JobArgs classes whose args are exactly their constructor
parameter properties into defineJob definitions and rewrites their
construction in insert and insertMany calls, along with JobArgsObject,
InsertManyParams, uniqueOpts (a byPeriod in seconds becomes { seconds: N }),
uniqueSkippedAsDuplicated, and the JOB_STATE_* constants. Anything that
needs judgment, such as bigint job IDs and Temporal timestamps, gets a
"${TODO_MARKER}" comment and is listed at the end.

Requires the typescript package (5.x or 6.x) installed in the project. Run
your formatter and type checker afterwards.`,
  name: COMMAND,
  options: {
    check: {
      description:
        "Exit with status 1 if any file would change, without writing",
      type: "boolean",
    },
    write: {
      description: "Write the migrated files in place",
      type: "boolean",
    },
  },
  positionals: "<file | directory | glob>...",
  summary: "Migrate source code from riverqueue 0.1",
  run: runCodemod,
};

async function runCodemod(
  values: OptionValues,
  context: CommandContext,
  positionals: readonly string[]
): Promise<number> {
  const check = booleanValue(values, "check");
  const write = booleanValue(values, "write");
  if (check && write) {
    throw new UsageError("--check and --write cannot be combined", COMMAND);
  }
  if (positionals.length === 0) {
    throw new UsageError("pass at least one file, directory, or glob", COMMAND);
  }

  const cwd = process.cwd();
  const paths = await expandPaths(cwd, positionals);
  const ts = loadTypeScript([cwd, dirname(fileURLToPath(import.meta.url))]);
  const sources = await Promise.all(
    paths.map(async (path) => ({ path, text: await readFile(path, "utf8") }))
  );
  const results = migrateSources(ts, sources);

  const display = (path: string): string => relative(cwd, path) || path;
  let changed = 0;
  const failed = new Set<CodemodFileResult>();
  for (const result of results) {
    if (result.error !== undefined) {
      failed.add(result);
      writeLine(
        context.stderr,
        `${context.program} ${COMMAND}: skipped ${display(result.path)}: ${result.error}`
      );
      continue;
    }
    if (!result.changed) continue;
    const original = sources.find(({ path }) => path === result.path);
    if (
      original !== undefined &&
      syntaxErrorCount(ts, result.path, result.output) >
        syntaxErrorCount(ts, result.path, original.text)
    ) {
      failed.add(result);
      writeLine(
        context.stderr,
        `${context.program} ${COMMAND}: skipped ${display(result.path)}: the rewrite would not parse; please report this file`
      );
      continue;
    }
    changed++;
    if (write) await writeFile(result.path, result.output);
    writeLine(
      context.stdout,
      `${write ? "rewrote" : "would rewrite"} ${display(result.path)}`
    );
  }

  const unchanged = results.length - changed - failed.size;
  writeLine(
    context.stdout,
    `${plural(changed, "file")} ${write ? "rewritten" : "to rewrite"}, ${unchanged} unchanged.`
  );
  printSites(
    context,
    results.filter((result) => !failed.has(result)),
    display,
    write
  );
  if (failed.size > 0) return 1;
  return check && changed > 0 ? 1 : 0;
}

function printSites(
  context: CommandContext,
  results: readonly CodemodFileResult[],
  display: (path: string) => string,
  written: boolean
): void {
  const sites = results.flatMap((result) =>
    result.sites.map((site) => ({ ...site, path: display(result.path) }))
  );
  if (sites.length === 0) return;
  writeLine(context.stdout);
  writeLine(
    context.stdout,
    `${plural(sites.length, "site")} to review, marked "${TODO_MARKER}"` +
      (written ? ":" : " (lines as rewritten):")
  );
  for (const site of sites) {
    writeLine(context.stdout, `  ${site.path}:${site.line}: ${site.message}`);
  }
}

function plural(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

/** Resolve file, directory, and glob arguments to sorted absolute paths. */
async function expandPaths(
  cwd: string,
  patterns: readonly string[]
): Promise<string[]> {
  const paths = new Set<string>();
  for (const pattern of patterns) {
    if (/[*?[\]{}]/.test(pattern)) {
      let matched = false;
      for await (const match of glob(pattern, { cwd, exclude: isSkipped })) {
        const path = resolve(cwd, match);
        if (isSourceFile(path) && (await stat(path)).isFile()) {
          paths.add(path);
          matched = true;
        }
      }
      if (!matched) {
        throw new UsageError(
          `no source files match ${JSON.stringify(pattern)}`,
          COMMAND
        );
      }
      continue;
    }
    const path = isAbsolute(pattern) ? pattern : resolve(cwd, pattern);
    const stats = await stat(path).catch(() => {
      throw new UsageError(
        `no such file or directory: ${JSON.stringify(pattern)}`,
        COMMAND
      );
    });
    if (stats.isDirectory()) {
      for await (const match of glob(`**/*{${SOURCE_EXTENSIONS.join(",")}}`, {
        cwd: path,
        exclude: isSkipped,
      })) {
        const file = join(path, match);
        if (isSourceFile(file)) paths.add(file);
      }
    } else if (isSourceFile(path)) {
      paths.add(path);
    } else {
      throw new UsageError(
        `${JSON.stringify(pattern)} is not a ${SOURCE_EXTENSIONS.join(", ")} file`,
        COMMAND
      );
    }
  }
  return [...paths].sort();
}

function isSkipped(path: string): boolean {
  return path.split(/[\\/]/).some((part) => SKIPPED_DIRECTORIES.has(part));
}

function isSourceFile(path: string): boolean {
  return (
    SOURCE_EXTENSIONS.includes(extname(path).toLowerCase()) &&
    !/\.d\.[cm]?ts$/i.test(path)
  );
}

/**
 * Load the `typescript` compiler API from the first of `directories` that
 * resolves a version providing it. The project's own TypeScript comes first
 * so the codemod parses with the version the project compiles with.
 */
export function loadTypeScript(directories: readonly string[]): TypeScriptApi {
  const found: string[] = [];
  for (const directory of directories) {
    let loaded: unknown;
    try {
      loaded = createRequire(join(directory, "package.json"))("typescript");
    } catch {
      continue;
    }
    if (isTypeScriptApi(loaded)) return loaded;
    const version =
      typeof loaded === "object" &&
      loaded !== null &&
      "version" in loaded &&
      typeof loaded.version === "string"
        ? loaded.version
        : "unknown";
    found.push(`typescript@${version} without the compiler API`);
  }
  const detail = found.length === 0 ? "" : ` (found ${found.join(", ")})`;
  throw new Error(
    `${COMMAND} needs the typescript package, version 5 or 6, for its ` +
      `compiler API${detail}; install it in the project being migrated, ` +
      `for example with "npm install --save-dev typescript@6"`
  );
}

function isTypeScriptApi(value: unknown): value is TypeScriptApi {
  if (typeof value !== "object" || value === null) return false;
  const api = value as Partial<Record<keyof TypeScriptApi, unknown>>;
  const major = Number.parseInt(String(api.version), 10);
  return (
    major >= 5 &&
    typeof api.createSourceFile === "function" &&
    typeof api.getModifiers === "function" &&
    typeof api.isSatisfiesExpression === "function" &&
    typeof api.transpileModule === "function"
  );
}

/** Count syntax errors the TypeScript parser reports for `text`. */
function syntaxErrorCount(
  ts: TypeScriptApi,
  path: string,
  text: string
): number {
  const { diagnostics = [] } = ts.transpileModule(text, {
    compilerOptions: {
      allowJs: true,
      jsx: ts.JsxEmit.Preserve,
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ESNext,
    },
    fileName: path,
    reportDiagnostics: true,
  });
  return diagnostics.length;
}
