import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";

import ts from "typescript";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const readmes = [
  "README.md",
  ...(await readdir(resolve(repositoryRoot, "docs")))
    .filter((name) => name.endsWith(".md"))
    .sort()
    .map((name) => `docs/${name}`),
  "cli/README.md",
  "driver/pg/README.md",
  "driver/prisma/README.md",
  "driver/sqlite/README.md",
  "migrate/README.md",
  "test/README.md",
  "worker-threads/README.md",
];
const config = ts.readConfigFile(
  resolve(repositoryRoot, "tsconfig.base.json"),
  ts.sys.readFile
);
if (config.error !== undefined) fail([config.error]);
const parsed = ts.parseJsonConfigFileContent(
  config.config,
  ts.sys,
  repositoryRoot
);

let checked = 0;
for (const readme of readmes) {
  const markdown = await readFile(resolve(repositoryRoot, readme), "utf8");
  // Each ```ts block compiles as its own module. A block fenced as
  // ```ts continued extends the previous block, for prose that interleaves
  // one example. A block fenced as ```ts file=name.ts is a module that the
  // README's other blocks can import as "./name.js", for examples that span
  // several files. An HTML comment `<!-- ts-setup ... -->` directly before a
  // fence supplies declarations (such as an application's `mailer`) that
  // the prose assumes but readers need not see. A block fenced as
  // ```ts ignore is not checked.
  const programs = [];
  const directory = resolve(
    repositoryRoot,
    ".readme-snippets",
    readme.replaceAll("/", "-")
  );
  const modules = new Map();
  for (const match of markdown.matchAll(
    /^(?:<!-- ts-setup\n([\s\S]*?)^-->\n\n?)?```(?:ts|typescript)(?: (continued)| (ignore)| file=([\w.-]+\.ts))?\n([\s\S]*?)^```$/gm
  )) {
    const [, setup, continued, ignored, file, snippet] = match;
    // ```ts ignore marks code that is deliberately not current River, such
    // as the 0.1 API in the migration guide.
    if (ignored !== undefined) continue;
    const code = setup === undefined ? snippet : `${setup}\n${snippet}`;
    checked += 1;
    if (file !== undefined) {
      const fileName = resolve(directory, file);
      if (modules.has(fileName)) {
        throw new Error(`${readme} repeats snippet file ${file}`);
      }
      modules.set(fileName, code);
    } else if (continued !== undefined && programs.length > 0) {
      programs[programs.length - 1] += `\n${code}`;
    } else {
      programs.push(code);
    }
  }
  for (const [index, source] of programs.entries()) {
    checkProgram(
      directory,
      new Map([
        ...modules,
        [resolve(directory, `${index}.ts`), `${source}\nexport {};\n`],
      ]),
      readme
    );
  }
  if (programs.length === 0 && modules.size > 0) {
    checkProgram(directory, modules, readme);
  }
}

function checkProgram(directory, virtualFiles, readme) {
  const options = {
    ...parsed.options,
    noEmit: true,
    rootDir: undefined,
    outDir: undefined,
    paths: {
      "@riverqueue/cli": [resolve(repositoryRoot, "cli/dist/index.d.ts")],
      "@riverqueue/driver-pg": [
        resolve(repositoryRoot, "driver/pg/dist/index.d.ts"),
      ],
      "@riverqueue/driver-prisma": [
        resolve(repositoryRoot, "driver/prisma/dist/index.d.ts"),
      ],
      "@riverqueue/driver-sqlite": [
        resolve(repositoryRoot, "driver/sqlite/dist/index.d.ts"),
      ],
      "@riverqueue/migrate": [
        resolve(repositoryRoot, "migrate/dist/index.d.ts"),
      ],
      "@riverqueue/test": [resolve(repositoryRoot, "test/dist/index.d.ts")],
      "@riverqueue/worker-threads": [
        resolve(repositoryRoot, "worker-threads/dist/index.d.ts"),
      ],
      riverqueue: [resolve(repositoryRoot, "dist/index.d.ts")],
      "riverqueue/unstable-driver": [
        resolve(repositoryRoot, "dist/unstable-driver.d.ts"),
      ],
    },
  };
  const host = ts.createCompilerHost(options);
  const getSourceFile = host.getSourceFile.bind(host);
  const fileExists = host.fileExists.bind(host);
  const readVirtualFile = host.readFile.bind(host);
  const directoryExists = host.directoryExists?.bind(host);
  host.directoryExists = (directoryName) =>
    resolve(directoryName) === directory ||
    (directoryExists?.(directoryName) ?? true);
  host.fileExists = (fileName) =>
    virtualFiles.has(fileName) || fileExists(fileName);
  host.readFile = (fileName) =>
    virtualFiles.get(fileName) ?? readVirtualFile(fileName);
  host.getSourceFile = (fileName, languageVersion, onError, shouldCreate) =>
    virtualFiles.has(fileName)
      ? ts.createSourceFile(
          fileName,
          virtualFiles.get(fileName),
          languageVersion,
          true
        )
      : getSourceFile(fileName, languageVersion, onError, shouldCreate);

  const program = ts.createProgram({
    host,
    options,
    rootNames: [...virtualFiles.keys()],
  });
  const diagnostics = ts.getPreEmitDiagnostics(program);
  if (diagnostics.length > 0) {
    process.stderr.write(`README TypeScript snippets failed: ${readme}\n`);
    fail(diagnostics);
  }
}

process.stdout.write(
  `typechecked ${checked} TypeScript snippets from ${readmes.length} Markdown files\n`
);

function fail(diagnostics) {
  process.stderr.write(
    ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (fileName) => fileName,
      getCurrentDirectory: () => repositoryRoot,
      getNewLine: () => "\n",
    })
  );
  process.exit(1);
}
