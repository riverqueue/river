/**
 * Source-to-source migration from the `riverqueue@0.1.0` insert-only client
 * to the current job-definition API.
 *
 * The codemod rewrites only patterns with one unambiguous meaning: argument
 * classes whose serialized shape is exactly their constructor parameter
 * properties, `JobArgsObject`, `InsertManyParams`, renamed option types,
 * the `uniqueOpts` rename, `byPeriod` seconds, `uniqueSkippedAsDuplicated`,
 * and the `JOB_STATE_*` constants. Everything else gets a `TODO(riverqueue-0.1):` comment so the
 * remaining sites line up with the compiler errors they produce. It never
 * guesses at ID arithmetic, timestamp handling, or string formatting.
 *
 * The TypeScript compiler API is passed in rather than imported so the CLI
 * can use whichever compatible `typescript` the project being migrated has
 * installed. Syntax kinds differ between TypeScript versions, so this module
 * reads every kind from that instance.
 */

import { dirname, extname, resolve } from "node:path";

import type * as TS from "typescript";

import { SourceEdits } from "./codemod-edits.js";

/** The `typescript` module namespace. */
export type TypeScriptApi = typeof TS;

/** Prefix of every comment the codemod leaves at a site it cannot rewrite. */
export const TODO_MARKER = "TODO(riverqueue-0.1):";

/** One file to migrate. */
export interface CodemodSource {
  /** Absolute path, used for parsing mode and relative import resolution. */
  readonly path: string;
  /** Current contents. */
  readonly text: string;
}

/** A site left for manual review, marked with {@link TODO_MARKER}. */
interface CodemodSite {
  /** One-based line in the migrated text of the code the comment marks. */
  readonly line: number;
  /** The comment's text after the marker. */
  readonly message: string;
}

export interface CodemodFileResult {
  /** Whether {@link CodemodFileResult.output} differs from the input. */
  readonly changed: boolean;
  /** Migrated contents. */
  readonly output: string;
  /**
   * Why the file could not be migrated, if it could not; `output` is then
   * the unchanged input.
   */
  readonly error?: string;
  /** Absolute path of the file. */
  readonly path: string;
  /** Every marked site in the migrated text, including earlier runs' marks. */
  readonly sites: readonly CodemodSite[];
}

const RIVER_MODULE = "riverqueue";
const JOB_ROW_TIMESTAMPS = new Set([
  "attemptedAt",
  "createdAt",
  "finalizedAt",
  "scheduledAt",
]);
const JOB_STATE_PREFIX = "JOB_STATE_";
const CLIENT_SCHEMA_MESSAGE =
  "the `schema` client option moved to the driver: `new PgDriver(pool, { schema })`";
const BY_PERIOD_MESSAGE =
  "`byPeriod` was a number of seconds and is now a duration; check the value";
const UNIQUE_OPTIONS_MESSAGE =
  "`unique.byPeriod` is now a duration such as `{ seconds: 60 }`, not a number of seconds";
/** 0.1 exports replaced by a rewrite; the import is dropped once unused. */
const REPLACED_EXPORTS = new Set([
  "InsertManyParams",
  "JobArgs",
  "JobArgsObject",
]);
/** 0.1 exports with no direct replacement, and what to do instead. */
const REMOVED_EXPORTS: ReadonlyMap<string, string> = new Map([
  ["Driver", "drivers now implement `riverqueue/unstable-driver`"],
  ["DriverOptions", "drivers now implement `riverqueue/unstable-driver`"],
  ["JobInsertParams", "drivers now implement `riverqueue/unstable-driver`"],
  ["uniqueBitmaskFromStates", "it moved to `riverqueue/unstable-driver`"],
  ["uniqueBitmaskToStates", "it moved to `riverqueue/unstable-driver`"],
]);
/**
 * 0.1 exports under a new name. Imports keep their local name, so the
 * code using them needs no change.
 */
const RENAMED_EXPORTS: ReadonlyMap<string, string> = new Map([
  ["ClientOpts", "ClientOptions"],
  ["InsertOpts", "InsertOptions"],
  ["UniqueOpts", "UniqueOptions"],
]);
const RESERVED_WORDS = new Set(
  (
    "arguments await break case catch class const continue debugger default " +
    "delete do else enum eval export extends false finally for function if " +
    "implements import in instanceof interface let new null package private " +
    "protected public return static super switch this throw true try typeof " +
    "undefined var void while with yield"
  ).split(" ")
);
/** Extensions of the files the codemod parses. */
export const SOURCE_EXTENSIONS: readonly string[] = [
  ".ts",
  ".tsx",
  ".mts",
  ".cts",
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
];

/**
 * Migrate a set of files together. Argument classes are resolved across the
 * set, so `new SortArgs(...)` in one file is rewritten against the class
 * declared in another when both are processed.
 */
export function migrateSources(
  ts: TypeScriptApi,
  sources: readonly CodemodSource[]
): CodemodFileResult[] {
  const files = sources.map((source) => parseFile(ts, source));
  const byPath = new Map(files.map((file) => [file.path, file]));
  const project = new Project(ts, files, byPath);
  return files.map((file) => {
    try {
      return new FileMigration(ts, project, file).run();
    } catch (error: unknown) {
      return {
        changed: false,
        error: error instanceof Error ? error.message : String(error),
        output: file.text,
        path: file.path,
        sites: [],
      };
    }
  });
}

interface ParsedFile {
  /** Job argument classes declared at the top level, by class name. */
  readonly classes: Map<string, JobClass>;
  readonly isTypeScript: boolean;
  readonly newline: string;
  readonly path: string;
  /** Local names bound by named imports from `riverqueue` to imported names. */
  readonly riverImports: Map<string, string>;
  /** Local name of `import * as name from "riverqueue"`, if any. */
  readonly riverNamespace: string | undefined;
  readonly sourceFile: TS.SourceFile;
  /** Identifier texts that name bindings or references in the file. */
  readonly taken: Set<string>;
  readonly text: string;
}

interface JobParam {
  readonly comments: readonly string[];
  readonly name: string;
  readonly optional: boolean;
  readonly type: string | undefined;
}

interface ConvertibleJobClass {
  readonly className: string;
  readonly convertible: true;
  readonly declaration: TS.ClassDeclaration;
  readonly defaults: TS.Expression | undefined;
  readonly defaultsComments: readonly string[];
  readonly file: ParsedFile;
  /** Source text of the `kind` literal, keeping its quote style. */
  readonly kind: string;
  readonly kindComments: readonly string[];
  readonly memberIndent: string;
  readonly params: readonly JobParam[];
}

interface UnconvertibleJobClass {
  readonly className: string;
  readonly convertible: false;
  readonly declaration: TS.ClassDeclaration;
  readonly file: ParsedFile;
  readonly reason: string;
}

type JobClass = ConvertibleJobClass | UnconvertibleJobClass;

/** How an exported name reaches a job class, and whether it changes. */
interface ResolvedExport {
  readonly jobClass: JobClass;
  /** Whether the exported name follows the class name to its definition name. */
  readonly renamed: boolean;
}

function parseFile(ts: TypeScriptApi, source: CodemodSource): ParsedFile {
  const extension = extname(source.path).toLowerCase();
  const scriptKind =
    extension === ".tsx"
      ? ts.ScriptKind.TSX
      : extension === ".jsx"
        ? ts.ScriptKind.JSX
        : [".js", ".mjs", ".cjs"].includes(extension)
          ? ts.ScriptKind.JS
          : ts.ScriptKind.TS;
  const sourceFile = ts.createSourceFile(
    source.path,
    source.text,
    ts.ScriptTarget.Latest,
    true,
    scriptKind
  );
  const riverImports = new Map<string, string>();
  let riverNamespace: string | undefined;
  for (const statement of sourceFile.statements) {
    if (!isRiverImport(ts, statement)) continue;
    const bindings = statement.importClause?.namedBindings;
    if (bindings === undefined) continue;
    if (ts.isNamespaceImport(bindings)) {
      riverNamespace = bindings.name.text;
    } else {
      for (const element of bindings.elements) {
        riverImports.set(
          element.name.text,
          (element.propertyName ?? element.name).text
        );
      }
    }
  }
  const taken = new Set<string>();
  const collect = (node: TS.Node): void => {
    if (ts.isIdentifier(node) && !isPropertyName(ts, node)) {
      taken.add(node.text);
    }
    ts.forEachChild(node, collect);
  };
  collect(sourceFile);

  const file: ParsedFile = {
    classes: new Map(),
    isTypeScript:
      scriptKind === ts.ScriptKind.TS || scriptKind === ts.ScriptKind.TSX,
    newline: source.text.includes("\r\n") ? "\r\n" : "\n",
    path: source.path,
    riverImports,
    riverNamespace,
    sourceFile,
    taken,
    text: source.text,
  };
  for (const statement of sourceFile.statements) {
    if (
      ts.isClassDeclaration(statement) &&
      statement.name !== undefined &&
      implementsJobArgs(ts, file, statement)
    ) {
      file.classes.set(
        statement.name.text,
        analyzeClass(ts, file, statement, statement.name.text)
      );
    }
  }
  return file;
}

function isRiverImport(
  ts: TypeScriptApi,
  node: TS.Node
): node is TS.ImportDeclaration {
  return (
    ts.isImportDeclaration(node) &&
    ts.isStringLiteral(node.moduleSpecifier) &&
    node.moduleSpecifier.text === RIVER_MODULE
  );
}

/** Whether `identifier` names a property rather than a binding or reference. */
function isPropertyName(ts: TypeScriptApi, identifier: TS.Identifier): boolean {
  const parent = identifier.parent;
  return (
    ((ts.isPropertyAccessExpression(parent) ||
      ts.isPropertyAssignment(parent) ||
      ts.isPropertyDeclaration(parent) ||
      ts.isPropertySignature(parent) ||
      ts.isMethodDeclaration(parent) ||
      ts.isMethodSignature(parent) ||
      ts.isGetAccessorDeclaration(parent) ||
      ts.isSetAccessorDeclaration(parent) ||
      ts.isEnumMember(parent)) &&
      parent.name === identifier) ||
    (ts.isQualifiedName(parent) && parent.right === identifier) ||
    ((ts.isImportSpecifier(parent) || ts.isExportSpecifier(parent)) &&
      parent.propertyName === identifier) ||
    (ts.isBindingElement(parent) && parent.propertyName === identifier)
  );
}

/** Resolve `expression` to the name it imports from `riverqueue`, if any. */
function riverName(
  ts: TypeScriptApi,
  file: ParsedFile,
  expression: TS.Node
): string | undefined {
  if (ts.isIdentifier(expression)) {
    return file.riverImports.get(expression.text);
  }
  if (
    ts.isPropertyAccessExpression(expression) &&
    ts.isIdentifier(expression.expression) &&
    expression.expression.text === file.riverNamespace
  ) {
    return expression.name.text;
  }
  return undefined;
}

function implementsJobArgs(
  ts: TypeScriptApi,
  file: ParsedFile,
  declaration: TS.ClassDeclaration
): boolean {
  return (declaration.heritageClauses ?? []).some(
    (clause) =>
      clause.token === ts.SyntaxKind.ImplementsKeyword &&
      clause.types.some(
        (type) => riverName(ts, file, type.expression) === "JobArgs"
      )
  );
}

function analyzeClass(
  ts: TypeScriptApi,
  file: ParsedFile,
  declaration: TS.ClassDeclaration,
  className: string
): JobClass {
  const unconvertible = (reason: string): UnconvertibleJobClass => ({
    className,
    convertible: false,
    declaration,
    file,
    reason,
  });
  const modifiers = ts.getModifiers(declaration) ?? [];
  if (
    modifiers.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword)
  ) {
    return unconvertible("it is a default export");
  }
  if (
    modifiers.some(
      (modifier) =>
        modifier.kind === ts.SyntaxKind.AbstractKeyword ||
        modifier.kind === ts.SyntaxKind.DeclareKeyword
    ) ||
    (ts.getDecorators(declaration) ?? []).length > 0
  ) {
    return unconvertible("it is abstract, declared, or decorated");
  }
  if (declaration.typeParameters !== undefined) {
    return unconvertible("it has type parameters");
  }
  const heritage = declaration.heritageClauses ?? [];
  if (
    heritage.some(
      (clause) =>
        clause.token === ts.SyntaxKind.ExtendsKeyword ||
        clause.types.length !== 1
    )
  ) {
    return unconvertible("it extends a class or implements other interfaces");
  }

  let kind: string | undefined;
  let kindComments: readonly string[] = [];
  let memberIndent: string | undefined;
  let defaults: TS.Expression | undefined;
  let defaultsComments: readonly string[] = [];
  let params: JobParam[] = [];
  let toJson: TS.MethodDeclaration | undefined;
  let sawConstructor = false;
  for (const member of declaration.members) {
    const name =
      member.name !== undefined && ts.isIdentifier(member.name)
        ? member.name.text
        : undefined;
    if (
      ts.isPropertyDeclaration(member) &&
      (name === "kind" || name === "insertOpts") &&
      !(ts.getModifiers(member) ?? []).some(
        (modifier) => modifier.kind === ts.SyntaxKind.StaticKeyword
      ) &&
      (ts.getDecorators(member) ?? []).length === 0
    ) {
      if (name === "kind") {
        const literal = stringLiteral(ts, member.initializer);
        if (literal === undefined) {
          return unconvertible("its `kind` is not a string literal");
        }
        kind = literal.getText();
        kindComments = leadingComments(ts, file.text, member);
        memberIndent = lineIndent(file.text, member.getStart());
      } else if (member.initializer !== undefined) {
        defaults = member.initializer;
        defaultsComments = leadingComments(ts, file.text, member);
      }
      continue;
    }
    if (ts.isConstructorDeclaration(member) && !sawConstructor) {
      sawConstructor = true;
      if (member.body === undefined || member.body.statements.length > 0) {
        return unconvertible("its constructor has a body");
      }
      const parsed = parseParameters(ts, file, member.parameters);
      if (typeof parsed === "string") return unconvertible(parsed);
      params = parsed;
      continue;
    }
    if (
      ts.isMethodDeclaration(member) &&
      name === "toJSON" &&
      toJson === undefined
    ) {
      toJson = member;
      continue;
    }
    if (ts.isSemicolonClassElement(member)) continue;
    return unconvertible(
      "it has members other than `kind`, `insertOpts`, and constructor parameter properties"
    );
  }
  if (kind === undefined) {
    return unconvertible("it has no `kind` property initialized to a string");
  }
  if (toJson !== undefined && !isIdentityToJson(ts, toJson, params)) {
    return unconvertible("its `toJSON` does not return exactly its parameters");
  }
  return {
    className,
    convertible: true,
    declaration,
    defaults,
    defaultsComments,
    file,
    kind,
    kindComments,
    memberIndent: memberIndent ?? "  ",
    params,
  };
}

function parseParameters(
  ts: TypeScriptApi,
  file: ParsedFile,
  parameters: TS.NodeArray<TS.ParameterDeclaration>
): JobParam[] | string {
  const params: JobParam[] = [];
  for (const parameter of parameters) {
    const modifiers = ts.getModifiers(parameter) ?? [];
    const isProperty = modifiers.some((modifier) =>
      [
        ts.SyntaxKind.PublicKeyword,
        ts.SyntaxKind.PrivateKeyword,
        ts.SyntaxKind.ProtectedKeyword,
        ts.SyntaxKind.ReadonlyKeyword,
      ].includes(modifier.kind)
    );
    if (!isProperty || !ts.isIdentifier(parameter.name)) {
      return "a constructor parameter is not a parameter property";
    }
    if (
      parameter.dotDotDotToken !== undefined ||
      parameter.initializer !== undefined ||
      (ts.getDecorators(parameter) ?? []).length > 0
    ) {
      return "a constructor parameter is a rest, default, or decorated parameter";
    }
    if (parameter.type === undefined && file.isTypeScript) {
      return `constructor parameter \`${parameter.name.text}\` has no type annotation`;
    }
    params.push({
      comments: leadingComments(ts, file.text, parameter),
      name: parameter.name.text,
      optional: parameter.questionToken !== undefined,
      type: parameter.type?.getText(),
    });
  }
  return params;
}

/** Whether `toJSON` returns `{ a: this.a, ... }` for exactly `params`. */
function isIdentityToJson(
  ts: TypeScriptApi,
  method: TS.MethodDeclaration,
  params: readonly JobParam[]
): boolean {
  const statements = method.body?.statements ?? [];
  const statement = statements[0];
  if (
    method.parameters.length > 0 ||
    statements.length !== 1 ||
    statement === undefined ||
    !ts.isReturnStatement(statement) ||
    statement.expression === undefined
  ) {
    return false;
  }
  const expression = skipParentheses(ts, statement.expression);
  if (!ts.isObjectLiteralExpression(expression)) return false;
  const names = new Set<string>();
  for (const property of expression.properties) {
    if (
      !ts.isPropertyAssignment(property) ||
      !ts.isIdentifier(property.name) ||
      !ts.isPropertyAccessExpression(property.initializer) ||
      property.initializer.expression.kind !== ts.SyntaxKind.ThisKeyword ||
      property.initializer.name.text !== property.name.text
    ) {
      return false;
    }
    names.add(property.name.text);
  }
  return (
    names.size === params.length && params.every(({ name }) => names.has(name))
  );
}

/** The string literal `expression` is, ignoring `as const` and `satisfies`. */
function stringLiteral(
  ts: TypeScriptApi,
  expression: TS.Expression | undefined
): TS.NoSubstitutionTemplateLiteral | TS.StringLiteral | undefined {
  if (expression === undefined) return undefined;
  const current = skipOuterExpressions(ts, expression);
  return ts.isStringLiteral(current) ||
    ts.isNoSubstitutionTemplateLiteral(current)
    ? current
    : undefined;
}

function skipParentheses(
  ts: TypeScriptApi,
  expression: TS.Expression
): TS.Expression {
  let current = expression;
  while (ts.isParenthesizedExpression(current)) current = current.expression;
  return current;
}

/** Skip parentheses, `as const`, and `satisfies` around `expression`. */
function skipOuterExpressions(
  ts: TypeScriptApi,
  expression: TS.Expression
): TS.Expression {
  let current = skipParentheses(ts, expression);
  while (
    (ts.isAsExpression(current) && current.type.getText() === "const") ||
    ts.isSatisfiesExpression(current)
  ) {
    current = skipParentheses(ts, current.expression);
  }
  return current;
}

/** Full text of the comments immediately before `node`. */
function leadingComments(
  ts: TypeScriptApi,
  text: string,
  node: TS.Node
): string[] {
  return (ts.getLeadingCommentRanges(text, node.getFullStart()) ?? []).map(
    (range) => text.slice(range.pos, range.end)
  );
}

function lineStart(text: string, position: number): number {
  return text.lastIndexOf("\n", position - 1) + 1;
}

function lineIndent(text: string, position: number): string {
  const start = lineStart(text, position);
  return /^[ \t]*/.exec(text.slice(start))?.[0] ?? "";
}

/** Cross-file knowledge: job classes, their definition names, and exports. */
class Project {
  readonly #byPath: ReadonlyMap<string, ParsedFile>;
  readonly #definitionNames = new Map<ConvertibleJobClass, string>();
  readonly #files: readonly ParsedFile[];
  readonly #ts: TypeScriptApi;

  constructor(
    ts: TypeScriptApi,
    files: readonly ParsedFile[],
    byPath: ReadonlyMap<string, ParsedFile>
  ) {
    this.#ts = ts;
    this.#files = files;
    this.#byPath = byPath;
    for (const file of files) {
      for (const jobClass of file.classes.values()) {
        if (!jobClass.convertible) continue;
        const base =
          definitionBaseName(jobClass.className) ||
          `${camelCase(jobClass.kind.slice(1, -1))}Job`;
        const name = claimName(file.taken, base, [`${base}Job`]);
        this.#definitionNames.set(jobClass, name);
      }
    }
  }

  /** The module-level name of a converted class's definition. */
  definitionName(jobClass: ConvertibleJobClass): string {
    const name = this.#definitionNames.get(jobClass);
    if (name === undefined) {
      throw new Error(
        `internal codemod error: no name for ${jobClass.className}`
      );
    }
    return name;
  }

  /** Resolve an import or re-export of `name` from `specifier` in `from`. */
  resolveImport(
    from: ParsedFile,
    specifier: string,
    name: string
  ): ResolvedExport | undefined {
    const target = this.#resolveModule(from.path, specifier);
    if (target !== undefined) return this.#resolveExport(target, name, 0);
    if (specifier.startsWith(".") || specifier === RIVER_MODULE) {
      return undefined;
    }
    // A path alias or package name this codemod cannot resolve: match an
    // exported class of the same name when exactly one processed file has one.
    const matches = this.#files.flatMap((file) => {
      const jobClass = file.classes.get(name);
      return jobClass !== undefined &&
        hasModifier(
          this.#ts,
          jobClass.declaration,
          this.#ts.SyntaxKind.ExportKeyword
        )
        ? [jobClass]
        : [];
    });
    const [only] = matches;
    return matches.length === 1 && only !== undefined
      ? { jobClass: only, renamed: true }
      : undefined;
  }

  #resolveExport(
    file: ParsedFile,
    name: string,
    depth: number
  ): ResolvedExport | undefined {
    const ts = this.#ts;
    if (depth > 16) return undefined;
    const declared = file.classes.get(name);
    if (
      declared !== undefined &&
      hasModifier(ts, declared.declaration, ts.SyntaxKind.ExportKeyword)
    ) {
      return { jobClass: declared, renamed: true };
    }
    for (const statement of file.sourceFile.statements) {
      if (!ts.isExportDeclaration(statement) || statement.isTypeOnly) continue;
      const module =
        statement.moduleSpecifier !== undefined &&
        ts.isStringLiteral(statement.moduleSpecifier)
          ? statement.moduleSpecifier.text
          : undefined;
      const clause = statement.exportClause;
      if (clause === undefined) {
        if (module === undefined) continue;
        const target = this.#resolveModule(file.path, module);
        const resolved =
          target === undefined
            ? undefined
            : this.#resolveExport(target, name, depth + 1);
        if (resolved !== undefined) return resolved;
        continue;
      }
      if (!ts.isNamedExports(clause)) continue;
      for (const element of clause.elements) {
        if (element.name.text !== name || element.isTypeOnly) continue;
        const local = (element.propertyName ?? element.name).text;
        const aliased = element.propertyName !== undefined;
        let resolved: ResolvedExport | undefined;
        if (module === undefined) {
          const jobClass = file.classes.get(local);
          resolved =
            jobClass === undefined ? undefined : { jobClass, renamed: true };
        } else {
          const target = this.#resolveModule(file.path, module);
          resolved =
            target === undefined
              ? undefined
              : this.#resolveExport(target, local, depth + 1);
        }
        if (resolved !== undefined) {
          return {
            jobClass: resolved.jobClass,
            renamed: resolved.renamed && !aliased,
          };
        }
      }
    }
    return undefined;
  }

  #resolveModule(from: string, specifier: string): ParsedFile | undefined {
    if (!specifier.startsWith(".")) return undefined;
    const base = resolve(dirname(from), specifier);
    const extension = extname(base);
    const stem = base.slice(0, base.length - extension.length);
    const candidates = [
      base,
      ...(
        {
          ".cjs": [".cts"],
          ".js": [".ts", ".tsx"],
          ".jsx": [".tsx"],
          ".mjs": [".mts"],
        }[extension] ?? []
      ).map((replacement) => stem + replacement),
      ...SOURCE_EXTENSIONS.map((candidate) => base + candidate),
      ...SOURCE_EXTENSIONS.map((candidate) =>
        resolve(base, `index${candidate}`)
      ),
    ];
    for (const candidate of candidates) {
      const file = this.#byPath.get(candidate);
      if (file !== undefined) return file;
    }
    return undefined;
  }
}

function hasModifier(
  ts: TypeScriptApi,
  node: TS.HasModifiers,
  kind: TS.SyntaxKind
): boolean {
  return (ts.getModifiers(node) ?? []).some(
    (modifier) => modifier.kind === kind
  );
}

/** `SortArgs` becomes `sort`, `URLFetchArgs` becomes `urlFetch`. */
function definitionBaseName(className: string): string {
  const base = className.replace(/Args$/, "");
  const leading = /^[A-Z]+(?=[A-Z][a-z]|\d|$)/.exec(base)?.[0];
  if (leading !== undefined) {
    return leading.toLowerCase() + base.slice(leading.length);
  }
  return base.charAt(0).toLowerCase() + base.slice(1);
}

/** `send_email` becomes `sendEmail`. */
function camelCase(kind: string): string {
  const words = kind.split(/[^A-Za-z0-9]+/).filter((word) => word !== "");
  const joined = words
    .map((word, index) =>
      index === 0
        ? word.charAt(0).toLowerCase() + word.slice(1)
        : word.charAt(0).toUpperCase() + word.slice(1)
    )
    .join("");
  return joined === "" || /^\d/.test(joined) ? `job${joined}` : joined;
}

/** Take the first free name among `base`, `fallbacks`, and numbered forms. */
function claimName(
  taken: Set<string>,
  base: string,
  fallbacks: readonly string[] = []
): string {
  const free = (name: string): boolean =>
    !taken.has(name) && !RESERVED_WORDS.has(name);
  let name = [base, ...fallbacks].find(free);
  const last = fallbacks.at(-1) ?? base;
  for (let suffix = 2; name === undefined; suffix++) {
    if (free(`${last}${suffix}`)) name = `${last}${suffix}`;
  }
  taken.add(name);
  return name;
}

/** Where a `new` expression sits relative to insertion calls. */
type InsertPosition = "element" | "insert" | "other" | "params";

/** The pieces of a legacy job argument expression. */
interface JobArgsParts {
  readonly args: string;
  readonly definition: string;
}

interface NamedImportDeclaration {
  readonly bindings: TS.NamedImports;
  readonly clause: TS.ImportClause;
  readonly statement: TS.ImportDeclaration;
}

interface PendingTodo {
  readonly message: string;
  readonly node: TS.Node;
  /**
   * Skip the comment when an existing marker comment above the line already
   * mentions this text, for messages whose wording depends on which files a
   * run processed.
   */
  readonly unlessMentioned: string | undefined;
}

/** Rewrites one file against the shared {@link Project}. */
class FileMigration {
  readonly #edits: SourceEdits;
  readonly #file: ParsedFile;
  /** Hoisted unchecked definitions for `JobArgsObject` kinds. */
  readonly #hoisted = new Map<
    string,
    { readonly literal: string; readonly name: string }
  >();
  /** Local names bound to job classes, and their definition names here. */
  readonly #localClasses = new Map<
    string,
    { readonly definition: string | undefined; readonly jobClass: JobClass }
  >();
  /** Object literals known to be insert options. */
  readonly #optionObjects = new Set<TS.Node>();
  readonly #project: Project;
  /** Import and export specifiers to rename, with their new text. */
  readonly #specifierRenames = new Map<TS.Node, string>();
  readonly #todos: PendingTodo[] = [];
  readonly #ts: TypeScriptApi;
  /** Object literals known to be unique options. */
  readonly #uniqueObjects = new Set<TS.Node>();
  #usesDefineJob = false;
  #usesJobState = false;

  constructor(ts: TypeScriptApi, project: Project, file: ParsedFile) {
    this.#ts = ts;
    this.#project = project;
    this.#file = file;
    this.#edits = new SourceEdits(file.text);
  }

  get #isRiverFile(): boolean {
    return (
      this.#file.riverImports.size > 0 ||
      this.#file.riverNamespace !== undefined ||
      this.#localClasses.size > 0
    );
  }

  run(): CodemodFileResult {
    this.#bindClasses();
    this.#collectOptionObjects(this.#file.sourceFile);
    this.#visit(this.#file.sourceFile);
    this.#flagRemainingReferences();
    const keptImports = this.#rewriteImports();
    this.#insertHoisted(keptImports);
    this.#insertTodos();
    const output = this.#edits.apply();
    return {
      changed: output !== this.#file.text,
      output,
      path: this.#file.path,
      sites: collectSites(output),
    };
  }

  /** Bind local and imported class names, and plan specifier renames. */
  #bindClasses(): void {
    const ts = this.#ts;
    const file = this.#file;
    for (const [name, jobClass] of file.classes) {
      this.#localClasses.set(name, {
        definition: jobClass.convertible
          ? this.#project.definitionName(jobClass)
          : undefined,
        jobClass,
      });
    }
    for (const statement of file.sourceFile.statements) {
      if (
        ts.isImportDeclaration(statement) &&
        ts.isStringLiteral(statement.moduleSpecifier) &&
        statement.importClause?.namedBindings !== undefined &&
        ts.isNamedImports(statement.importClause.namedBindings)
      ) {
        for (const element of statement.importClause.namedBindings.elements) {
          const imported = (element.propertyName ?? element.name).text;
          const resolved = this.#project.resolveImport(
            file,
            statement.moduleSpecifier.text,
            imported
          );
          if (resolved === undefined) continue;
          const { jobClass } = resolved;
          if (!jobClass.convertible) {
            this.#localClasses.set(element.name.text, {
              definition: undefined,
              jobClass,
            });
            continue;
          }
          const definition = this.#project.definitionName(jobClass);
          // eslint-disable-next-line @typescript-eslint/no-deprecated -- phaseModifier is unavailable in TypeScript 5, which the codemod supports
          if (element.isTypeOnly || statement.importClause.isTypeOnly) {
            // A type-only import cannot construct the class; its remaining
            // type references are flagged.
            this.#localClasses.set(element.name.text, { definition, jobClass });
            continue;
          }
          const exported = resolved.renamed ? definition : imported;
          let local = element.name.text;
          if (element.propertyName === undefined && resolved.renamed) {
            file.taken.delete(local);
            local = claimName(file.taken, definition, [`${definition}Job`]);
          }
          this.#localClasses.set(element.name.text, {
            definition: local,
            jobClass,
          });
          const text = exported === local ? local : `${exported} as ${local}`;
          if (text !== element.getText()) {
            this.#specifierRenames.set(element, text);
          }
        }
      }
      if (
        ts.isExportDeclaration(statement) &&
        !statement.isTypeOnly &&
        statement.exportClause !== undefined &&
        ts.isNamedExports(statement.exportClause)
      ) {
        const module =
          statement.moduleSpecifier !== undefined &&
          ts.isStringLiteral(statement.moduleSpecifier)
            ? statement.moduleSpecifier.text
            : undefined;
        for (const element of statement.exportClause.elements) {
          if (element.isTypeOnly) continue;
          const local = (element.propertyName ?? element.name).text;
          let jobClass: JobClass | undefined;
          let renamed = true;
          if (module === undefined) {
            jobClass = file.classes.get(local);
          } else {
            const resolved = this.#project.resolveImport(file, module, local);
            jobClass = resolved?.jobClass;
            renamed = resolved?.renamed ?? false;
          }
          if (jobClass === undefined || !jobClass.convertible) continue;
          const definition = this.#project.definitionName(jobClass);
          const source = module === undefined || renamed ? definition : local;
          const exported =
            element.propertyName === undefined ? source : element.name.text;
          const text =
            source === exported ? source : `${source} as ${exported}`;
          if (text !== element.getText()) {
            this.#specifierRenames.set(element, text);
          }
        }
      }
    }
  }

  #collectOptionObjects(node: TS.Node): void {
    const ts = this.#ts;
    const addOptions = (expression: TS.Expression | undefined): void => {
      if (expression === undefined) return;
      const object = skipParentheses(ts, expression);
      if (ts.isObjectLiteralExpression(object)) this.#optionObjects.add(object);
    };
    if (ts.isCallExpression(node) && calleeName(ts, node) === "insert") {
      const [, second, third] = node.arguments;
      addOptions(node.arguments.length === 2 ? second : third);
    }
    if (
      ts.isNewExpression(node) &&
      riverName(ts, this.#file, node.expression) === "InsertManyParams"
    ) {
      addOptions(node.arguments?.[1]);
    }
    if (
      ts.isPropertyDeclaration(node) &&
      ts.isIdentifier(node.name) &&
      node.name.text === "insertOpts" &&
      ts.isClassDeclaration(node.parent) &&
      node.parent.name !== undefined &&
      this.#file.classes.has(node.parent.name.text)
    ) {
      addOptions(node.initializer);
    }
    const typed = this.#declaredRiverType(node);
    if (typed?.type === "InsertOpts") addOptions(typed.expression);
    if (typed?.type === "UniqueOpts") {
      const object = skipParentheses(ts, typed.expression);
      if (ts.isObjectLiteralExpression(object)) this.#uniqueObjects.add(object);
    }
    ts.forEachChild(node, (child) => this.#collectOptionObjects(child));
  }

  /** `const x: InsertOpts = {...}` and `{...} satisfies InsertOpts`. */
  #declaredRiverType(
    node: TS.Node
  ): { readonly expression: TS.Expression; readonly type: string } | undefined {
    const ts = this.#ts;
    let typeNode: TS.TypeNode | undefined;
    let expression: TS.Expression | undefined;
    if (ts.isVariableDeclaration(node)) {
      typeNode = node.type;
      expression = node.initializer;
    } else if (ts.isSatisfiesExpression(node) || ts.isAsExpression(node)) {
      typeNode = node.type;
      expression = node.expression;
    }
    if (
      typeNode === undefined ||
      expression === undefined ||
      !ts.isTypeReferenceNode(typeNode)
    ) {
      return undefined;
    }
    const typeName = typeNode.typeName;
    const name = ts.isIdentifier(typeName)
      ? this.#file.riverImports.get(typeName.text)
      : ts.isIdentifier(typeName.left) &&
          typeName.left.text === this.#file.riverNamespace
        ? typeName.right.text
        : undefined;
    return name === undefined ? undefined : { expression, type: name };
  }

  /** Post-order traversal, so inner rewrites exist before outer ones. */
  #visit(node: TS.Node): void {
    const ts = this.#ts;
    ts.forEachChild(node, (child) => this.#visit(child));
    if (ts.isClassDeclaration(node)) this.#visitClass(node);
    else if (ts.isNewExpression(node)) this.#visitNew(node);
    else if (ts.isPropertyAccessExpression(node))
      this.#visitPropertyAccess(node);
    else if (
      ts.isPrefixUnaryExpression(node) &&
      node.operator === ts.SyntaxKind.ExclamationToken
    ) {
      this.#visitNegation(node);
    } else if (
      ts.isPropertyAssignment(node) ||
      ts.isShorthandPropertyAssignment(node)
    ) {
      this.#visitProperty(node);
    } else if (ts.isIdentifier(node)) this.#visitIdentifier(node);
    else if (
      ts.isBindingElement(node) &&
      (node.propertyName ?? node.name).getText() === "uniqueSkippedAsDuplicated"
    ) {
      this.#todo(
        node,
        'replace `uniqueSkippedAsDuplicated` with `status === "duplicate"`'
      );
    }
  }

  #visitClass(node: TS.ClassDeclaration): void {
    if (node.name === undefined) return;
    const jobClass = this.#file.classes.get(node.name.text);
    if (jobClass?.declaration !== node) return;
    if (!jobClass.convertible) {
      this.#todo(
        node,
        `convert \`${jobClass.className}\` to \`defineJob\` by hand: ${jobClass.reason}`
      );
      return;
    }
    const definition = this.#project.definitionName(jobClass);
    this.#edits.replace(
      node.getStart(),
      node.getEnd(),
      this.#definitionText(jobClass, definition)
    );
  }

  #definitionText(jobClass: ConvertibleJobClass, name: string): string {
    const ts = this.#ts;
    const newline = this.#file.newline;
    const indent = lineIndent(this.#file.text, jobClass.declaration.getStart());
    const member = jobClass.memberIndent;
    const exported = hasModifier(
      ts,
      jobClass.declaration,
      ts.SyntaxKind.ExportKeyword
    )
      ? "export "
      : "";
    const head = `${exported}const ${name} = ${this.#defineJobReference()}`;
    let call: string;
    if (jobClass.params.length === 0) {
      call = `${head}({`;
    } else {
      const property = (param: JobParam): string =>
        `${param.name}${param.optional ? "?" : ""}: ${param.type ?? "unknown"}`;
      const inline = `${head}<{ ${jobClass.params.map(property).join("; ")} }>()({`;
      const multiline =
        jobClass.params.some((param) => param.comments.length > 0) ||
        inline.includes("\n") ||
        indent.length + inline.length > 80;
      call = multiline
        ? [
            `${head}<{`,
            ...jobClass.params.flatMap((param) => [
              ...param.comments.map((comment) => member + comment),
              `${member}${property(param)};`,
            ]),
            `${indent}}>()({`,
          ].join(newline)
        : inline;
    }
    const lines = [
      call,
      ...jobClass.kindComments.map((comment) => member + comment),
      `${member}kind: ${jobClass.kind},`,
    ];
    if (jobClass.defaults !== undefined) {
      lines.push(
        ...jobClass.defaultsComments.map((comment) => member + comment),
        `${member}defaults: ${this.#render(jobClass.defaults)},`
      );
    }
    lines.push(`${indent}});`);
    return lines.join(newline);
  }

  #defineJobReference(): string {
    for (const [local, imported] of this.#file.riverImports) {
      if (imported === "defineJob") return local;
    }
    if (
      this.#file.riverNamespace !== undefined &&
      !this.#hasNamedRiverImport()
    ) {
      return `${this.#file.riverNamespace}.defineJob`;
    }
    this.#usesDefineJob = true;
    return "defineJob";
  }

  #jobStateReference(): string {
    for (const [local, imported] of this.#file.riverImports) {
      if (imported === "JOB_STATE") return local;
    }
    if (
      this.#file.riverNamespace !== undefined &&
      !this.#hasNamedRiverImport()
    ) {
      return `${this.#file.riverNamespace}.JOB_STATE`;
    }
    this.#usesJobState = true;
    return "JOB_STATE";
  }

  #hasNamedRiverImport(): boolean {
    const ts = this.#ts;
    return this.#file.sourceFile.statements.some(
      (statement) =>
        isRiverImport(ts, statement) &&
        statement.importClause?.namedBindings !== undefined &&
        ts.isNamedImports(statement.importClause.namedBindings)
    );
  }

  #visitNew(node: TS.NewExpression): void {
    const ts = this.#ts;
    const position = this.#insertPosition(node);
    const river = riverName(ts, this.#file, node.expression);
    if (river === "InsertManyParams") {
      this.#rewriteInsertManyParams(node);
      return;
    }
    if (river === "Client") {
      this.#rewriteClientSchema(node);
      return;
    }
    if (position === "params") return; // The InsertManyParams rewrite uses it.
    const parts = this.#jobArgsParts(node, position !== "other");
    if (parts === undefined) {
      if (
        position !== "other" &&
        this.#isRiverFile &&
        ts.isIdentifier(node.expression)
      ) {
        const name = `\`${node.expression.text}\``;
        this.#todo(
          node,
          `${name} was not converted; insert a job definition with a plain args object`,
          name
        );
      }
      return;
    }
    if (typeof parts === "string") {
      this.#todo(node, parts);
      return;
    }
    switch (position) {
      case "element":
        this.#replace(
          node,
          `{ job: ${parts.definition}, args: ${parts.args} }`
        );
        return;
      case "insert":
        this.#replace(node, `${parts.definition}, ${parts.args}`);
        return;
      case "other":
        this.#todo(
          node,
          `pass \`${parts.definition}\` and the args object \`${abbreviate(parts.args)}\` to insert or insertMany`
        );
        return;
    }
  }

  /**
   * The definition and args object a legacy job expression becomes, a TODO
   * message when it is legacy but cannot be converted, or undefined when it
   * is not a legacy job expression. Without `hoist`, a `JobArgsObject`
   * kind is described rather than given a module-level definition.
   */
  #jobArgsParts(
    node: TS.Expression,
    hoist = true
  ): JobArgsParts | string | undefined {
    const ts = this.#ts;
    if (!ts.isNewExpression(node)) return undefined;
    const args = node.arguments ?? [];
    if (riverName(ts, this.#file, node.expression) === "JobArgsObject") {
      const [kindArgument, objectArgument] = args;
      const kind = stringLiteral(ts, kindArgument);
      if (kind === undefined || args.some((arg) => ts.isSpreadElement(arg))) {
        return "define this `JobArgsObject` kind with `defineJob({ kind })` and insert a plain args object";
      }
      return {
        args:
          objectArgument === undefined ? "{}" : this.#render(objectArgument),
        definition: hoist
          ? this.#hoist(kind)
          : `defineJob({ kind: ${kind.getText()} })`,
      };
    }
    if (!ts.isIdentifier(node.expression)) return undefined;
    const bound = this.#localClasses.get(node.expression.text);
    if (bound === undefined) return undefined;
    const { jobClass } = bound;
    if (!jobClass.convertible || bound.definition === undefined) {
      return `\`${jobClass.className}\` could not be converted automatically; insert a job definition with a plain args object`;
    }
    if (args.some((arg) => ts.isSpreadElement(arg))) {
      return `spell out the \`${jobClass.className}\` arguments as a plain args object for \`${bound.definition}\``;
    }
    const properties = jobClass.params.flatMap((param, index) => {
      const arg = args[index];
      if (arg === undefined) return [];
      const text = this.#render(arg);
      return [text === param.name ? text : `${param.name}: ${text}`];
    });
    return {
      args: properties.length === 0 ? "{}" : `{ ${properties.join(", ")} }`,
      definition: bound.definition,
    };
  }

  #insertPosition(node: TS.NewExpression): InsertPosition {
    const ts = this.#ts;
    const parent = node.parent;
    if (
      ts.isCallExpression(parent) &&
      parent.arguments[0] === node &&
      calleeName(ts, parent) === "insert"
    ) {
      return "insert";
    }
    if (
      ts.isArrayLiteralExpression(parent) &&
      ts.isCallExpression(parent.parent) &&
      parent.parent.arguments[0] === parent &&
      calleeName(ts, parent.parent) === "insertMany"
    ) {
      return "element";
    }
    if (
      ts.isNewExpression(parent) &&
      parent.arguments?.[0] === node &&
      riverName(ts, this.#file, parent.expression) === "InsertManyParams"
    ) {
      return "params";
    }
    return "other";
  }

  #rewriteInsertManyParams(node: TS.NewExpression): void {
    const [argsNode, optionsNode] = node.arguments ?? [];
    const parts =
      argsNode === undefined ? undefined : this.#jobArgsParts(argsNode);
    if (parts === undefined || typeof parts === "string") {
      this.#todo(
        node,
        "replace `InsertManyParams` with a `{ job, args, options }` item built from a job definition"
      );
      return;
    }
    const options =
      optionsNode === undefined
        ? ""
        : `, options: ${this.#render(optionsNode)}`;
    this.#replace(
      node,
      `{ job: ${parts.definition}, args: ${parts.args}${options} }`
    );
  }

  /** `new Client(new PgDriver(pool), { schema })` moves the schema. */
  #rewriteClientSchema(node: TS.NewExpression): void {
    const ts = this.#ts;
    const [driver, options] = node.arguments ?? [];
    if (options === undefined) return;
    const object = skipParentheses(ts, options);
    if (!ts.isObjectLiteralExpression(object)) {
      this.#todo(node, CLIENT_SCHEMA_MESSAGE);
      return;
    }
    const names = object.properties.map((property) =>
      property.name !== undefined && ts.isIdentifier(property.name)
        ? property.name.text
        : undefined
    );
    if (!names.includes("schema")) return;
    const pool =
      driver !== undefined &&
      ts.isNewExpression(driver) &&
      ts.isIdentifier(driver.expression) &&
      driver.expression.text === "PgDriver" &&
      driver.arguments?.length === 1
        ? driver.arguments[0]
        : undefined;
    const onlySchema =
      names.length === 1 &&
      object.properties.every(
        (property) =>
          ts.isPropertyAssignment(property) ||
          ts.isShorthandPropertyAssignment(property)
      );
    if (driver === undefined || pool === undefined || !onlySchema) {
      this.#todo(node, CLIENT_SCHEMA_MESSAGE);
      return;
    }
    this.#edits.replace(
      driver.getStart(),
      options.getEnd(),
      `new PgDriver(${this.#render(pool)}, ${this.#render(options)})`
    );
  }

  #visitPropertyAccess(node: TS.PropertyAccessExpression): void {
    const ts = this.#ts;
    const name = node.name.text;
    if (name === "uniqueSkippedAsDuplicated") {
      if (
        ts.isPrefixUnaryExpression(node.parent) &&
        node.parent.operator === ts.SyntaxKind.ExclamationToken
      ) {
        return;
      }
      this.#replaceStatus(node, node, "===", "duplicate");
      return;
    }
    if (name.startsWith(JOB_STATE_PREFIX)) {
      const state = name.slice(JOB_STATE_PREFIX.length).toLowerCase();
      if (riverName(ts, this.#file, node) === name) {
        this.#replace(node, `${this.#file.riverNamespace}.JOB_STATE.${state}`);
      }
      return;
    }
    if (
      !this.#isRiverFile ||
      !ts.isPropertyAccessExpression(node.expression) ||
      node.expression.name.text !== "job"
    ) {
      return;
    }
    if (name === "id" && !isStringConversion(ts, node)) {
      this.#todo(
        node,
        "`JobRow.id` is now a `bigint`; review number annotations, arithmetic, and JSON serialization"
      );
    } else if (JOB_ROW_TIMESTAMPS.has(name)) {
      this.#todo(
        node,
        `\`JobRow.${name}\` is now a \`Temporal.Instant\`, not a \`Date\``
      );
    }
  }

  #visitNegation(node: TS.PrefixUnaryExpression): void {
    const ts = this.#ts;
    const operand = node.operand;
    if (
      ts.isPropertyAccessExpression(operand) &&
      operand.name.text === "uniqueSkippedAsDuplicated"
    ) {
      // `!undefined` is true, so an optional chain must not compare equal.
      if (operand.questionDotToken === undefined) {
        this.#replaceStatus(node, operand, "===", "inserted");
      } else {
        this.#replaceStatus(node, operand, "!==", "duplicate");
      }
    }
  }

  #replaceStatus(
    node: TS.Expression,
    access: TS.PropertyAccessExpression,
    operator: "!==" | "===",
    status: "duplicate" | "inserted"
  ): void {
    const receiver = this.#render(access.expression);
    const dot = access.questionDotToken === undefined ? "." : "?.";
    const comparison = `${receiver}${dot}status ${operator} "${status}"`;
    this.#replace(
      node,
      needsParentheses(this.#ts, node) ? `(${comparison})` : comparison
    );
  }

  #visitProperty(
    node: TS.PropertyAssignment | TS.ShorthandPropertyAssignment
  ): void {
    const ts = this.#ts;
    if (!ts.isIdentifier(node.name)) return;
    const name = node.name.text;
    const object = node.parent;
    if (name === "byPeriod" && this.#uniqueObjects.has(object)) {
      this.#rewriteByPeriod(node);
      return;
    }
    if (name === "byArgs" && this.#uniqueObjects.has(object)) {
      const value = ts.isPropertyAssignment(node)
        ? node.initializer
        : undefined;
      if (
        value === undefined ||
        !(
          value.kind === ts.SyntaxKind.TrueKeyword ||
          ts.isArrayLiteralExpression(value)
        )
      ) {
        this.#todo(
          node,
          "`byArgs` takes `true` or a list of fields; omit it instead of passing false"
        );
      }
      return;
    }
    if (name !== "uniqueOpts") return;
    if (!this.#optionObjects.has(object)) {
      if (this.#isRiverFile) {
        this.#todo(
          node,
          "if these are River insert options, rename `uniqueOpts` to `unique` (`byPeriod` is now a duration)"
        );
      }
      return;
    }
    if (ts.isShorthandPropertyAssignment(node)) {
      this.#replace(node, "unique: uniqueOpts");
      this.#todo(node, UNIQUE_OPTIONS_MESSAGE);
      return;
    }
    const value = skipOuterExpressions(ts, node.initializer);
    if (ts.isObjectLiteralExpression(value)) {
      // The post-order walk visited this object's properties before it was
      // known to hold unique options, so rewrite them now.
      if (!this.#uniqueObjects.has(value)) {
        this.#uniqueObjects.add(value);
        for (const property of value.properties) {
          if (
            ts.isPropertyAssignment(property) ||
            ts.isShorthandPropertyAssignment(property)
          ) {
            this.#visitProperty(property);
          }
        }
      }
    } else {
      this.#todo(node, UNIQUE_OPTIONS_MESSAGE);
    }
    this.#replace(node.name, "unique");
  }

  #rewriteByPeriod(
    node: TS.PropertyAssignment | TS.ShorthandPropertyAssignment
  ): void {
    const ts = this.#ts;
    if (ts.isShorthandPropertyAssignment(node)) {
      this.#replace(node, "byPeriod: { seconds: byPeriod }");
      this.#todo(node, BY_PERIOD_MESSAGE);
      return;
    }
    const value = skipParentheses(ts, node.initializer);
    if (isNumericConstant(ts, value)) {
      this.#replace(
        node.initializer,
        `{ seconds: ${this.#render(node.initializer)} }`
      );
      return;
    }
    if (
      ts.isObjectLiteralExpression(value) ||
      isTemporalExpression(ts, value)
    ) {
      return;
    }
    this.#replace(
      node.initializer,
      `{ seconds: ${this.#render(node.initializer)} }`
    );
    this.#todo(node, BY_PERIOD_MESSAGE);
  }

  #visitIdentifier(node: TS.Identifier): void {
    const ts = this.#ts;
    const imported = this.#file.riverImports.get(node.text);
    if (
      imported === undefined ||
      !imported.startsWith(JOB_STATE_PREFIX) ||
      isPropertyName(ts, node) ||
      ts.isImportSpecifier(node.parent) ||
      ts.isExportSpecifier(node.parent)
    ) {
      return;
    }
    const state = imported.slice(JOB_STATE_PREFIX.length).toLowerCase();
    if (ts.isShorthandPropertyAssignment(node.parent)) {
      this.#replace(
        node.parent,
        `${node.text}: ${this.#jobStateReference()}.${state}`
      );
    } else {
      this.#replace(node, `${this.#jobStateReference()}.${state}`);
    }
  }

  /** Flag references to removed APIs that no rewrite consumed. */
  #flagRemainingReferences(): void {
    const ts = this.#ts;
    for (const reference of this.#remainingReferences()) {
      const { node, local } = reference;
      if (ts.isNewExpression(node.parent) && node.parent.expression === node) {
        continue; // #visitNew already rewrote or flagged it.
      }
      if (ts.isExpressionWithTypeArguments(node.parent)) {
        continue; // The unconvertible class itself is flagged.
      }
      const imported = this.#file.riverImports.get(local);
      if (imported !== undefined) {
        this.#todo(node, removedReferenceMessage(imported));
        continue;
      }
      const bound = this.#localClasses.get(local);
      if (bound?.definition !== undefined) {
        this.#todo(
          node,
          `\`${bound.jobClass.className}\` is now the \`${bound.definition}\` job definition; pass it with a plain args object`
        );
      }
    }
  }

  /** Identifier references to replaced APIs outside every rewritten range. */
  #remainingReferences(): {
    readonly local: string;
    readonly node: TS.Identifier;
  }[] {
    const watched = new Set<string>();
    for (const [local, imported] of this.#file.riverImports) {
      if (
        REPLACED_EXPORTS.has(imported) ||
        imported.startsWith(JOB_STATE_PREFIX)
      ) {
        watched.add(local);
      }
    }
    for (const [local, bound] of this.#localClasses) {
      if (bound.jobClass.convertible) watched.add(local);
    }
    return this.#references(watched, true).filter(
      ({ node }) => !this.#ts.isExportSpecifier(node.parent)
    );
  }

  /**
   * References to `names` as bindings or values, other than their import
   * specifiers, optionally skipping those inside rewritten ranges.
   */
  #references(
    names: ReadonlySet<string>,
    afterEdits: boolean
  ): { local: string; node: TS.Identifier }[] {
    const ts = this.#ts;
    const references: { local: string; node: TS.Identifier }[] = [];
    const visit = (node: TS.Node): void => {
      if (
        ts.isIdentifier(node) &&
        names.has(node.text) &&
        !isPropertyName(ts, node) &&
        !ts.isImportSpecifier(node.parent) &&
        !(
          ts.isExportSpecifier(node.parent) &&
          node.parent.parent.parent.moduleSpecifier !== undefined
        ) &&
        !(ts.isClassDeclaration(node.parent) && node.parent.name === node) &&
        !(afterEdits && this.#edits.intersects(node.getStart(), node.getEnd()))
      ) {
        references.push({ local: node.text, node });
      }
      ts.forEachChild(node, visit);
    };
    visit(this.#file.sourceFile);
    return references;
  }

  /**
   * Rewrite `riverqueue` import lists and job-class specifiers. Returns the
   * import declarations that remain, for placing hoisted definitions.
   */
  #rewriteImports(): TS.ImportDeclaration[] {
    const ts = this.#ts;
    const kept: TS.ImportDeclaration[] = [];
    const riverImports: NamedImportDeclaration[] = [];
    for (const statement of this.#file.sourceFile.statements) {
      if (ts.isImportDeclaration(statement)) {
        const clause = statement.importClause;
        const bindings = clause?.namedBindings;
        if (
          isRiverImport(ts, statement) &&
          clause !== undefined &&
          bindings !== undefined &&
          ts.isNamedImports(bindings)
        ) {
          riverImports.push({ bindings, clause, statement });
          continue;
        }
        kept.push(statement);
        if (bindings !== undefined && ts.isNamedImports(bindings)) {
          this.#renameSpecifiers(bindings.elements);
        }
      } else if (
        ts.isExportDeclaration(statement) &&
        statement.exportClause !== undefined &&
        ts.isNamedExports(statement.exportClause)
      ) {
        this.#renameSpecifiers(statement.exportClause.elements);
      }
    }

    const hasValueImport = (name: string): boolean =>
      riverImports.some(
        ({ bindings, clause }) =>
          // eslint-disable-next-line @typescript-eslint/no-deprecated -- phaseModifier is unavailable in TypeScript 5, which the codemod supports
          !clause.isTypeOnly &&
          bindings.elements.some(
            (element) =>
              !element.isTypeOnly &&
              (element.propertyName ?? element.name).text === name
          )
      );
    const additions: string[] = [];
    if (this.#usesDefineJob && !hasValueImport("defineJob")) {
      additions.push("defineJob");
    }
    if (this.#usesJobState && !hasValueImport("JOB_STATE")) {
      additions.push("JOB_STATE");
    }
    const target =
      // eslint-disable-next-line @typescript-eslint/no-deprecated -- phaseModifier is unavailable in TypeScript 5, which the codemod supports
      riverImports.find(({ clause }) => !clause.isTypeOnly) ?? riverImports[0];
    const locals = new Set(this.#file.riverImports.keys());
    const countReferences = (afterEdits: boolean): Map<string, number> => {
      const counts = new Map<string, number>();
      for (const { local } of this.#references(locals, afterEdits)) {
        counts.set(local, (counts.get(local) ?? 0) + 1);
      }
      return counts;
    };
    const referencedBefore = countReferences(false);
    const referencedAfter = countReferences(true);

    for (const declaration of riverImports) {
      const { bindings, clause, statement } = declaration;
      let specifiers: string[] = [];
      let changed = false;
      for (const element of bindings.elements) {
        const imported = (element.propertyName ?? element.name).text;
        const removed = REMOVED_EXPORTS.get(imported);
        if (removed !== undefined) {
          this.#todo(
            statement,
            `\`${imported}\` is no longer exported: ${removed}`
          );
        }
        // Drop replaced APIs, and anything whose every use was rewritten.
        const local = element.name.text;
        if (
          (referencedAfter.get(local) ?? 0) === 0 &&
          (REPLACED_EXPORTS.has(imported) ||
            imported.startsWith(JOB_STATE_PREFIX) ||
            (referencedBefore.get(local) ?? 0) > 0)
        ) {
          changed = true;
          continue;
        }
        const renamed = RENAMED_EXPORTS.get(imported);
        if (renamed !== undefined) {
          const type = element.isTypeOnly ? "type " : "";
          specifiers.push(`${type}${renamed} as ${local}`);
          changed = true;
          continue;
        }
        specifiers.push(element.getText());
      }
      const toValue =
        // eslint-disable-next-line @typescript-eslint/no-deprecated -- phaseModifier is unavailable in TypeScript 5, which the codemod supports
        declaration === target && clause.isTypeOnly && additions.length > 0;
      if (declaration === target && additions.length > 0) {
        if (toValue) {
          specifiers = specifiers.map((specifier) =>
            /^type\s/.test(specifier) ? specifier : `type ${specifier}`
          );
        }
        specifiers = insertSorted(specifiers, additions);
        changed = true;
      }
      if (!changed) {
        kept.push(statement);
      } else if (specifiers.length === 0 && clause.name === undefined) {
        this.#removeLine(statement);
      } else {
        kept.push(statement);
        this.#replaceImportList(declaration, specifiers, toValue);
      }
    }

    if (
      target === undefined &&
      additions.length > 0 &&
      this.#file.riverNamespace === undefined
    ) {
      const newline = this.#file.newline;
      const statement = `import { ${additions.join(", ")} } from "${RIVER_MODULE}";`;
      const last = kept.at(-1);
      if (last === undefined) {
        this.#edits.insert(0, statement + newline);
      } else {
        this.#edits.insert(last.getEnd(), newline + statement);
      }
    }
    return kept;
  }

  #renameSpecifiers(
    elements: readonly (TS.ExportSpecifier | TS.ImportSpecifier)[]
  ): void {
    for (const element of elements) {
      const text = this.#specifierRenames.get(element);
      if (text !== undefined) this.#replace(element, text);
    }
  }

  /** Replace an import's `{ ... }` list, as a value import when `toValue`. */
  #replaceImportList(
    { bindings, clause, statement }: NamedImportDeclaration,
    specifiers: readonly string[],
    toValue: boolean
  ): void {
    const text = this.#file.text;
    const newline = this.#file.newline;
    const start = toValue ? clause.getStart() : bindings.getStart();
    const prefix =
      toValue && clause.name !== undefined ? `${clause.name.text}, ` : "";
    const inline =
      specifiers.length === 0 ? "{}" : `{ ${specifiers.join(", ")} }`;
    const line =
      text.slice(lineStart(text, statement.getStart()), start) +
      prefix +
      inline +
      text.slice(bindings.getEnd(), statement.getEnd());
    const indent = lineIndent(text, statement.getStart());
    const list =
      line.length <= 80
        ? inline
        : [
            "{",
            ...specifiers.map((specifier) => `${indent}  ${specifier},`),
            `${indent}}`,
          ].join(newline);
    this.#edits.replace(start, bindings.getEnd(), prefix + list);
  }

  #removeLine(node: TS.Node): void {
    const text = this.#file.text;
    const end = text.indexOf("\n", node.getEnd());
    this.#edits.replace(
      lineStart(text, node.getStart()),
      end === -1 ? node.getEnd() : end + 1,
      ""
    );
  }

  #hoist(kind: TS.NoSubstitutionTemplateLiteral | TS.StringLiteral): string {
    const existing = this.#hoisted.get(kind.text);
    if (existing !== undefined) return existing.name;
    const name = claimName(this.#file.taken, `${camelCase(kind.text)}Job`);
    this.#hoisted.set(kind.text, { literal: kind.getText(), name });
    this.#defineJobReference();
    return name;
  }

  #insertHoisted(keptImports: readonly TS.ImportDeclaration[]): void {
    if (this.#hoisted.size === 0) return;
    const newline = this.#file.newline;
    const reference = this.#defineJobReference();
    const definitions = [...this.#hoisted.values()].map(
      ({ literal, name }) =>
        `const ${name} = ${reference}({ kind: ${literal} });`
    );
    const last = [...keptImports]
      .sort((left, right) => left.getEnd() - right.getEnd())
      .at(-1);
    if (last === undefined) {
      this.#edits.insert(0, definitions.join(newline) + newline + newline);
    } else {
      this.#edits.insert(
        last.getEnd(),
        newline + newline + definitions.join(newline)
      );
    }
  }

  #todo(node: TS.Node, message: string, unlessMentioned?: string): void {
    this.#todos.push({ message, node, unlessMentioned });
  }

  /** Insert each pending TODO above its line, skipping existing copies. */
  #insertTodos(): void {
    const text = this.#file.text;
    const newline = this.#file.newline;
    const byPosition = new Map<number, PendingTodo[]>();
    for (const todo of this.#todos) {
      const position = this.#commentPosition(todo.node);
      const todos = byPosition.get(position) ?? [];
      if (!todos.some(({ message }) => message === todo.message)) {
        todos.push(todo);
      }
      byPosition.set(position, todos);
    }
    for (const [position, todos] of byPosition) {
      const existing = commentBlockAbove(text, position).filter((line) =>
        line.startsWith(`// ${TODO_MARKER} `)
      );
      const indent = lineIndent(text, position);
      const fresh = todos
        .filter(
          ({ message, unlessMentioned }) =>
            !existing.includes(`// ${TODO_MARKER} ${message}`) &&
            (unlessMentioned === undefined ||
              !existing.some((line) => line.includes(unlessMentioned)))
        )
        .map(({ message }) => message);
      if (fresh.length === 0) continue;
      this.#edits.insert(
        position,
        fresh
          .map((message) => `${indent}// ${TODO_MARKER} ${message}${newline}`)
          .join("")
      );
    }
  }

  /**
   * The start of the line to put a comment above `node`: its own line,
   * unless that would land inside a rewritten range, a template literal, or
   * JSX text, in which case an enclosing line.
   */
  #commentPosition(node: TS.Node): number {
    const ts = this.#ts;
    const text = this.#file.text;
    let position = lineStart(text, node.getStart());
    for (;;) {
      const edit = this.#edits.containing(position);
      if (edit !== undefined) {
        position = lineStart(text, edit.start);
        continue;
      }
      let enclosing: TS.Node | undefined;
      for (
        let current: TS.Node | undefined = node;
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- a source file's parent is undefined at runtime
        current !== undefined;
        current = current.parent
      ) {
        if (
          (ts.isTemplateExpression(current) ||
            ts.isNoSubstitutionTemplateLiteral(current) ||
            ts.isJsxElement(current) ||
            ts.isJsxFragment(current) ||
            ts.isJsxSelfClosingElement(current)) &&
          current.getStart() < position &&
          position < current.getEnd()
        ) {
          enclosing = current;
        }
      }
      if (enclosing === undefined) return position;
      position = lineStart(text, enclosing.getStart());
    }
  }

  #render(node: TS.Node): string {
    return this.#edits.render(node.getStart(), node.getEnd());
  }

  #replace(node: TS.Node, text: string): void {
    this.#edits.replace(node.getStart(), node.getEnd(), text);
  }
}

function calleeName(
  ts: TypeScriptApi,
  call: TS.CallExpression
): string | undefined {
  return ts.isPropertyAccessExpression(call.expression)
    ? call.expression.name.text
    : undefined;
}

/** `id.toString()`, `String(id)`, and `${id}` read a bigint like a number. */
function isStringConversion(ts: TypeScriptApi, node: TS.Expression): boolean {
  const parent = node.parent;
  return (
    ts.isTemplateSpan(parent) ||
    (ts.isPropertyAccessExpression(parent) &&
      parent.name.text === "toString" &&
      ts.isCallExpression(parent.parent)) ||
    (ts.isCallExpression(parent) &&
      ts.isIdentifier(parent.expression) &&
      parent.expression.text === "String")
  );
}

/** A numeric literal, or integer arithmetic (`+`, `-`, `*`) on them. */
function isNumericConstant(ts: TypeScriptApi, node: TS.Expression): boolean {
  const expression = skipParentheses(ts, node);
  if (ts.isNumericLiteral(expression)) return true;
  return (
    ts.isBinaryExpression(expression) &&
    [
      ts.SyntaxKind.AsteriskToken,
      ts.SyntaxKind.MinusToken,
      ts.SyntaxKind.PlusToken,
    ].includes(expression.operatorToken.kind) &&
    isNumericConstant(ts, expression.left) &&
    isNumericConstant(ts, expression.right)
  );
}

function isTemporalExpression(ts: TypeScriptApi, node: TS.Expression): boolean {
  let current: TS.Expression = node;
  while (
    ts.isCallExpression(current) ||
    ts.isPropertyAccessExpression(current)
  ) {
    current = current.expression;
  }
  return ts.isIdentifier(current) && current.text === "Temporal";
}

/** Whether a comparison replacing `node` needs parentheses in its context. */
function needsParentheses(ts: TypeScriptApi, node: TS.Node): boolean {
  const parent = node.parent;
  if (ts.isCallExpression(parent) || ts.isNewExpression(parent)) {
    return parent.expression === node;
  }
  if (
    ts.isParenthesizedExpression(parent) ||
    ts.isIfStatement(parent) ||
    ts.isWhileStatement(parent) ||
    ts.isDoStatement(parent) ||
    ts.isForStatement(parent) ||
    ts.isReturnStatement(parent) ||
    ts.isExpressionStatement(parent) ||
    ts.isVariableDeclaration(parent) ||
    ts.isPropertyAssignment(parent) ||
    ts.isArrowFunction(parent) ||
    ts.isArrayLiteralExpression(parent) ||
    ts.isTemplateSpan(parent) ||
    ts.isJsxExpression(parent) ||
    // `===` binds tighter than `?:`, so every operand position is safe.
    ts.isConditionalExpression(parent)
  ) {
    return false;
  }
  if (ts.isBinaryExpression(parent)) {
    return ![
      ts.SyntaxKind.AmpersandAmpersandToken,
      ts.SyntaxKind.BarBarToken,
      ts.SyntaxKind.QuestionQuestionToken,
      ts.SyntaxKind.CommaToken,
      ts.SyntaxKind.EqualsToken,
    ].includes(parent.operatorToken.kind);
  }
  return true;
}

/** Add `additions` to a specifier list, alphabetically when it is sorted. */
function insertSorted(
  specifiers: readonly string[],
  additions: readonly string[]
): string[] {
  const key = (specifier: string): string =>
    specifier.replace(/^type\s+/, "").toLowerCase();
  const sorted = specifiers.every(
    (specifier, index) =>
      index === 0 || key(specifiers[index - 1] ?? "") <= key(specifier)
  );
  const result = [...specifiers];
  for (const addition of additions) {
    if (!sorted) {
      result.push(addition);
      continue;
    }
    const index = result.findIndex(
      (specifier) => key(specifier) > key(addition)
    );
    result.splice(index === -1 ? result.length : index, 0, addition);
  }
  return result;
}

/** The comment lines directly above the line starting at `position`. */
function commentBlockAbove(text: string, position: number): string[] {
  const lines: string[] = [];
  let end = position - 1;
  while (end > 0) {
    const start = lineStart(text, end);
    const line = text.slice(start, end).replace(/\r$/, "").trim();
    if (!line.startsWith("//")) break;
    lines.push(line);
    end = start - 1;
  }
  return lines;
}

/** Every marker comment in `text`, with the line of the code it marks. */
function collectSites(text: string): CodemodSite[] {
  const lines = text.split("\n");
  const sites: CodemodSite[] = [];
  const pattern = `// ${TODO_MARKER} `;
  lines.forEach((line, index) => {
    const at = line.indexOf(pattern);
    if (at === -1 || line.slice(0, at).trim() !== "") return;
    let target = index + 1;
    while (
      target < lines.length &&
      (lines[target] ?? "").trim().startsWith("//")
    ) {
      target++;
    }
    sites.push({
      line: target + 1,
      message: line
        .slice(at + pattern.length)
        .replace(/\r$/, "")
        .trim(),
    });
  });
  return sites;
}

function removedReferenceMessage(imported: string): string {
  if (imported === "JobArgs") {
    return "`JobArgs` was removed; accept a `JobDefinition` and its args, or an `InsertManyItem`";
  }
  if (imported.startsWith(JOB_STATE_PREFIX)) {
    const state = imported.slice(JOB_STATE_PREFIX.length).toLowerCase();
    return `\`${imported}\` was removed; use \`JOB_STATE.${state}\``;
  }
  return `\`${imported}\` was removed; use a job definition with a plain args object`;
}

/** A single-line, bounded rendering of code for a message. */
function abbreviate(code: string): string {
  const flat = code.replaceAll(/\s+/g, " ");
  return flat.length <= 60 ? flat : `${flat.slice(0, 57)}...`;
}
