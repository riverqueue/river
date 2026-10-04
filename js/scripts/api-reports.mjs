// Generate reviewable API reports from the built declarations of every
// published entry point, in the spirit of API Extractor's `.api.md` files.
//
// API Extractor itself cannot analyze these packages: its bundled compiler
// (TypeScript 5.9 as of 7.59) rejects the `ES2025` target and fails on the
// global `Temporal` namespace from TypeScript 6's `esnext.temporal` library.
// This script uses the workspace's own compiler instead.
//
// Each report lists every exported declaration with its TSDoc, including the
// documentation on class and interface members, so documentation changes show
// up in review diffs. Generation fails when an exported declaration refers to
// a type declared in the same package that the entry point does not export (a
// "forgotten export"), unless the name is allow-listed below with a reason.

import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

import prettier from "prettier";
import ts from "typescript";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const check = process.argv.includes("--check");
const prettierConfig =
  (await prettier.resolveConfig(resolve(repositoryRoot, "package.json"))) ?? {};

/**
 * Types that exported declarations may reference without the entry point
 * exporting them. Every entry needs a reason, and stale entries fail.
 *
 * @type {Record<string, Record<string, string>>}
 */
const intentionallyUnexported = {
  "@riverqueue/test": {
    WorkOnceResultBase:
      "Fields shared by every `WorkOnceResult` variant; name the union instead.",
  },
  riverqueue: {
    JsonCompatibleProperty:
      "Type-level helper of `JsonCompatible`; never named by applications.",
    SchemaCheck:
      "Type-level helper that rejects schemas whose input is not JSON-compatible.",
    SchemaJobInput:
      "Type-level helper that extracts a schema's input type for `defineJob`.",
    WorkAttemptResultBase:
      "Fields shared by every `WorkAttemptResult` variant; name the union instead.",
    WorkerRegistrationBase:
      "Fields shared by the exported worker registration interfaces.",
  },
  "riverqueue/unstable-driver": {
    ResumableFinish:
      "Result of the internal resumable completion step used by first-party runtimes.",
  },
};

// Each report covers one entry point. `alsoSee` names sibling entry points of
// the same package whose exports the report may reference: the unstable
// driver subpath builds on the root API, so it need not re-export it, while
// the stable root must never depend on a type only the unstable subpath
// exports.
const reports = [
  {
    entry: "dist/index.d.ts",
    name: "riverqueue",
    output: "etc/riverqueue.api.md",
  },
  {
    alsoSee: ["riverqueue"],
    entry: "dist/unstable-driver.d.ts",
    name: "riverqueue/unstable-driver",
    output: "etc/riverqueue.unstable-driver.api.md",
  },
  {
    entry: "cli/dist/index.d.ts",
    name: "@riverqueue/cli",
    output: "cli/etc/cli.api.md",
  },
  {
    entry: "driver/pg/dist/index.d.ts",
    name: "@riverqueue/driver-pg",
    output: "driver/pg/etc/driver-pg.api.md",
  },
  {
    entry: "driver/prisma/dist/index.d.ts",
    name: "@riverqueue/driver-prisma",
    output: "driver/prisma/etc/driver-prisma.api.md",
  },
  {
    entry: "driver/sqlite/dist/index.d.ts",
    name: "@riverqueue/driver-sqlite",
    output: "driver/sqlite/etc/driver-sqlite.api.md",
  },
  {
    entry: "migrate/dist/index.d.ts",
    name: "@riverqueue/migrate",
    output: "migrate/etc/migrate.api.md",
  },
  {
    entry: "test/dist/index.d.ts",
    name: "@riverqueue/test",
    output: "test/etc/test.api.md",
  },
  {
    entry: "worker-threads/dist/index.d.ts",
    name: "@riverqueue/worker-threads",
    output: "worker-threads/etc/worker-threads.api.md",
  },
];

const program = ts.createProgram({
  rootNames: reports.map(({ entry }) => resolve(repositoryRoot, entry)),
  options: {
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    skipLibCheck: true,
    target: ts.ScriptTarget.ES2025,
    types: ["node"],
  },
});
const checker = program.getTypeChecker();
const exportsByReport = new Map(
  reports.map((report) => [report.name, entryExports(report.entry)])
);

const failures = [];
for (const report of reports) {
  const exported = exportsByReport.get(report.name);
  const visible = new Set(
    [report.name, ...(report.alsoSee ?? [])].flatMap((name) =>
      [...exportsByReport.get(name).values()].map(({ target }) => target)
    )
  );
  const packageDist = `${await realpath(
    dirname(resolve(repositoryRoot, report.entry))
  )}${sep}`;
  const allowed = intentionallyUnexported[report.name] ?? {};
  const { forgotten, unexported } = findUnexportedReferences(
    exported,
    visible,
    packageDist,
    allowed
  );
  for (const [name, referencedBy] of forgotten) {
    failures.push(
      `${report.name}: \`${name}\` is referenced by ${[...referencedBy]
        .sort()
        .map((referrer) => `\`${referrer}\``)
        .join(", ")} but not exported`
    );
  }
  for (const name of Object.keys(allowed)) {
    if (!unexported.has(name)) {
      failures.push(
        `${report.name}: stale allow-list entry \`${name}\` is exported or no longer referenced`
      );
    }
  }

  const section = (name, symbol, note = []) => [
    `## \`${name}\``,
    "",
    ...note,
    "```ts",
    uniqueNodes(symbol.getDeclarations() ?? [])
      .map(reportableNode)
      .map(declarationText)
      .join("\n\n"),
    "```",
    "",
  ];
  const body = await prettier.format(
    [
      `# API report: \`${report.name}\``,
      "",
      "<!-- Generated by scripts/api-reports.mjs. Do not edit directly. -->",
      "",
      "This report contains declarations and TSDoc for names exported by the",
      "package entry point. Private members, unexported implementation",
      "declarations, and file layout are omitted.",
      "",
      ...[...exported]
        .sort(([left], [right]) => left.localeCompare(right))
        .flatMap(([name, { target }]) => section(name, target)),
      ...(unexported.size === 0
        ? []
        : [
            "# Referenced but unexported",
            "",
            "Exported declarations refer to these types, which the entry point",
            "intentionally does not export.",
            "",
            ...[...unexported]
              .sort(([left], [right]) => left.localeCompare(right))
              .flatMap(([name, symbol]) =>
                section(name, symbol, [allowed[name], ""])
              ),
          ]),
    ].join("\n"),
    { ...prettierConfig, parser: "markdown" }
  );
  const output = resolve(repositoryRoot, report.output);
  if (check) {
    const existing = await readFile(output, "utf8").catch(() => null);
    if (existing !== body) {
      failures.push(
        `API declaration report is stale: ${report.output} (run \`pnpm run api:report\`)`
      );
    }
  } else {
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, body);
  }
}

if (failures.length > 0) {
  process.stderr.write(`${failures.map((line) => `- ${line}`).join("\n")}\n`);
  process.stderr.write(
    "Export forgotten types from the entry point (types only when possible) " +
      "or allow-list them with a reason in scripts/api-reports.mjs.\n"
  );
  process.exitCode = 1;
}

// Map each exported name to its resolved declaration symbol.
function entryExports(entryRelative) {
  const entry = resolve(repositoryRoot, entryRelative);
  const source = program.getSourceFile(entry);
  if (source === undefined) {
    throw new Error(`declaration entry missing: ${entry}`);
  }
  const moduleSymbol = checker.getSymbolAtLocation(source);
  if (moduleSymbol === undefined) {
    throw new Error(`declaration entry is not a module: ${entry}`);
  }
  return new Map(
    checker.getExportsOfModule(moduleSymbol).map((exported) => {
      const target = resolveAlias(checker, exported);
      if ((target.getDeclarations() ?? []).length === 0) {
        throw new Error(`no declaration found for exported ${exported.name}`);
      }
      return [exported.name, { target }];
    })
  );
}

// Walk the declarations reachable from the exports. A referenced symbol
// declared in this package must be visible from the entry point; allow-listed
// ones are collected so the report can show them, and their own references
// are checked in turn.
function findUnexportedReferences(exported, visible, packageDist, allowed) {
  const forgotten = new Map();
  const unexported = new Map();
  const pending = [...exported].map(([name, { target }]) => [name, target]);
  const inPackage = (declaration) =>
    ts.sys
      .realpath(declaration.getSourceFile().fileName)
      .startsWith(packageDist);
  while (pending.length > 0) {
    const [referrer, symbol] = pending.pop();
    for (const node of symbol.getDeclarations() ?? []) {
      for (const referenced of referencedSymbols(reportableNode(node))) {
        if (
          referenced === symbol ||
          visible.has(referenced) ||
          !(referenced.getDeclarations() ?? []).some(inPackage)
        ) {
          continue;
        }
        if (Object.hasOwn(allowed, referenced.name)) {
          if (!unexported.has(referenced.name)) {
            unexported.set(referenced.name, referenced);
            pending.push([referenced.name, referenced]);
          }
          continue;
        }
        const referencedBy = forgotten.get(referenced.name) ?? new Set();
        referencedBy.add(referrer);
        forgotten.set(referenced.name, referencedBy);
      }
    }
  }
  return { forgotten, unexported };
}

function resolveAlias(checker, symbol) {
  return (symbol.flags & ts.SymbolFlags.Alias) === 0
    ? symbol
    : checker.getAliasedSymbol(symbol);
}

// Report whole variable statements so `declare const` keeps its keyword.
function reportableNode(node) {
  return ts.isVariableDeclaration(node) ? node.parent.parent : node;
}

// Symbols named by type positions inside a reported declaration, excluding
// private class members, which are not part of the public surface.
function referencedSymbols(root) {
  const symbols = new Set();
  const visit = (node) => {
    if (isPrivateMember(node)) return;
    let name;
    if (ts.isTypeReferenceNode(node)) name = node.typeName;
    else if (ts.isExpressionWithTypeArguments(node)) name = node.expression;
    else if (ts.isTypeQueryNode(node)) name = node.exprName;
    else if (ts.isImportTypeNode(node)) name = node.qualifier;
    if (name !== undefined) {
      const symbol = checker.getSymbolAtLocation(
        ts.isPropertyAccessExpression(name) ? name.name : name
      );
      if (symbol !== undefined) {
        const target = resolveAlias(checker, symbol);
        if ((target.flags & ts.SymbolFlags.TypeParameter) === 0) {
          symbols.add(target);
        }
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(root);
  return symbols;
}

function isPrivateMember(node) {
  if (!ts.isClassElement(node)) return false;
  if (node.name !== undefined && ts.isPrivateIdentifier(node.name)) return true;
  const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : [];
  return (modifiers ?? []).some(
    (modifier) => modifier.kind === ts.SyntaxKind.PrivateKeyword
  );
}

// The declaration's source text with its own TSDoc comment and without
// private class members. Member TSDoc is part of the text and kept.
function declarationText(node) {
  const sourceFile = node.getSourceFile();
  const text = sourceFile.text;
  const removed = ts.isClassDeclaration(node)
    ? node.members
        .filter(isPrivateMember)
        .map((member) => [member.getFullStart(), member.end])
    : [];
  let body = "";
  let position = node.getStart(sourceFile);
  for (const [start, end] of removed) {
    body += text.slice(position, start);
    position = end;
  }
  body += text.slice(position, node.end);
  const documentation = (
    ts.getLeadingCommentRanges(text, node.getFullStart()) ?? []
  )
    .filter(
      (range) =>
        range.kind === ts.SyntaxKind.MultiLineCommentTrivia &&
        text.startsWith("/**", range.pos)
    )
    .at(-1);
  return documentation === undefined
    ? body
    : `${text.slice(documentation.pos, documentation.end)}\n${body}`;
}

function uniqueNodes(nodes) {
  const seen = new Set();
  return nodes.filter((node) => {
    const key = `${node.getSourceFile().fileName}:${node.pos}:${node.end}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
