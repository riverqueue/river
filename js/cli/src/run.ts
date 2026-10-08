import { readFileSync } from "node:fs";

import {
  MIGRATION_LINE_MAIN,
  type Migration,
  type MigrationBackend,
} from "@riverqueue/migrate";

import { benchCommand } from "./bench.js";
import { codemodCommand } from "./codemod-command.js";
import { writeLine, type Command, type CommandContext } from "./command.js";
import {
  migrateDownCommand,
  migrateGetCommand,
  migrateListCommand,
  migrateUpCommand,
  validateCommand,
} from "./migrate-commands.js";
import { formatHelp, parseOptions, UsageError } from "./options.js";

/**
 * Options for {@link run}. Each stream defaults to the process's own.
 *
 * A package that ships its own migration line can embed this command line
 * with its lines added, as River's Go CLI allows:
 *
 * ```ts
 * import { run } from "@riverqueue/cli";
 *
 * process.exitCode = await run(process.argv.slice(2), {
 *   migrationLines: { extension: (backend) => extensionMigrations(backend) },
 *   program: "extension",
 * });
 * ```
 */
export interface RunOptions {
  /**
   * Additional migration lines, by name, that the migration commands select
   * with `--line`. Each returns the line's migrations for a backend,
   * versioned from 1. River's bundled `main` line is always available and
   * cannot be replaced.
   */
  readonly migrationLines?: Readonly<
    Record<string, (backend: MigrationBackend) => readonly Migration[]>
  >;
  /**
   * Program name shown in help, version output, and error messages.
   * Defaults to `riverqueue`.
   */
  readonly program?: string;
  /** Receives errors, warnings, and prompts. */
  readonly stderr?: {
    readonly isTTY?: boolean;
    write(chunk: string): unknown;
  };
  /** Answers `bench`'s confirmation prompt when it is a terminal. */
  readonly stdin?: NodeJS.ReadableStream & { readonly isTTY?: boolean };
  /** Receives command output. */
  readonly stdout?: { write(chunk: string): unknown };
}

const PROGRAM = "riverqueue";

const versionCommand: Command = {
  description: "Print the CLI and Node.js versions.",
  name: "version",
  options: {},
  summary: "Print version information",
  run: async (_values, context) => {
    printVersion(context);
    return 0;
  },
};

const COMMANDS: ReadonlyMap<string, Command> = new Map(
  [
    benchCommand,
    codemodCommand,
    migrateDownCommand,
    migrateGetCommand,
    migrateListCommand,
    migrateUpCommand,
    validateCommand,
    versionCommand,
  ].map((command) => [command.name, command])
);

/**
 * Run the `riverqueue` command line with `argv`, the arguments after the
 * program name, and resolve to the process exit code.
 *
 * Errors are reported on `stderr` rather than thrown. `bench` handles
 * `SIGINT` and `SIGTERM` while it runs so it can stop cleanly and print its
 * summary, and removes its handlers when it finishes.
 *
 * ```ts
 * process.exitCode = await run(process.argv.slice(2));
 * ```
 */
export async function run(
  argv: readonly string[],
  options: RunOptions = {}
): Promise<number> {
  const context: CommandContext = {
    env: process.env,
    migrationLines: options.migrationLines ?? {},
    program: options.program ?? PROGRAM,
    stderr: options.stderr ?? process.stderr,
    stdin: options.stdin ?? process.stdin,
    stdout: options.stdout ?? process.stdout,
  };
  const [name, ...rest] = argv;
  let command: Command | undefined;
  try {
    if (Object.hasOwn(context.migrationLines, MIGRATION_LINE_MAIN)) {
      throw new Error(
        `migrationLines cannot replace River's ${MIGRATION_LINE_MAIN} line`
      );
    }
    if (name === undefined || name === "--help" || name === "-h") {
      context.stdout.write(programHelp(context.program));
      return 0;
    }
    if (name === "--version") {
      printVersion(context);
      return 0;
    }
    if (name === "help") {
      return printCommandHelp(context, rest);
    }
    command = COMMANDS.get(name);
    if (command === undefined) {
      throw new UsageError(
        name.startsWith("-")
          ? `unknown flag: ${name}`
          : `unknown command: ${JSON.stringify(name)}`
      );
    }

    const { help, positionals, values } = parseOptions(
      command.name,
      command.options,
      rest,
      command.positionals !== undefined
    );
    if (help) {
      context.stdout.write(commandHelp(context.program, command));
      return 0;
    }
    return await command.run(values, context, positionals);
  } catch (error: unknown) {
    reportError(context, error, command?.name);
    return 1;
  }
}

function commandHelp(program: string, command: Command): string {
  return formatHelp({
    description: command.description.replaceAll("{program}", program),
    options: command.options,
    usage: `${program} ${command.name} [flags]${
      command.positionals === undefined ? "" : ` ${command.positionals}`
    }`,
  });
}

function describeError(error: unknown): string {
  const messages: string[] = [];
  let current: unknown = error;
  while (current !== undefined && messages.length < 5) {
    if (current instanceof Error) {
      const message = current.message;
      if (message !== "" && !messages.includes(message)) messages.push(message);
      current = current.cause;
    } else {
      // eslint-disable-next-line @typescript-eslint/no-base-to-string -- reports any thrown value
      messages.push(String(current));
      break;
    }
  }
  return messages.join(": ");
}

function printCommandHelp(
  context: CommandContext,
  names: readonly string[]
): number {
  const [name, ...extra] = names;
  if (name === undefined) {
    context.stdout.write(programHelp(context.program));
    return 0;
  }
  const command = COMMANDS.get(name);
  if (command === undefined || extra.length > 0) {
    throw new UsageError(
      command === undefined
        ? `unknown command: ${JSON.stringify(name)}`
        : `unexpected argument: ${JSON.stringify(extra[0])}`
    );
  }
  context.stdout.write(commandHelp(context.program, command));
  return 0;
}

function printVersion(context: CommandContext): void {
  writeLine(context.stdout, `${context.program} version ${packageVersion()}`);
  writeLine(context.stdout, `Node.js ${process.version}`);
}

function packageVersion(): string {
  try {
    const manifest: unknown = JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8")
    );
    if (
      typeof manifest === "object" &&
      manifest !== null &&
      "version" in manifest &&
      typeof manifest.version === "string"
    ) {
      return manifest.version;
    }
  } catch {
    // Fall through to an unknown version.
  }
  return "(unknown)";
}

function programHelp(program: string): string {
  const commands = [...COMMANDS.values()].sort((left, right) =>
    left.name.localeCompare(right.name)
  );
  const width = Math.max(...commands.map(({ name }) => name.length)) + 2;
  return formatHelp({
    description: `
Command-line tools for River, the job queue for Postgres and SQLite.

Commands:
${commands.map(({ name, summary }) => `  ${name.padEnd(width)}${summary}`).join("\n")}

Commands that use a database take --database-url. Postgres commands other
than bench also read the standard PG* environment variables when PGDATABASE
is set. Run "${program} <command> --help" for a command's flags, and
"${program} --version" for version information.`,
    usage: `${program} <command> [flags]`,
  });
}

function reportError(
  context: CommandContext,
  error: unknown,
  commandName: string | undefined
): void {
  const { program } = context;
  const prefix =
    commandName === undefined ? program : `${program} ${commandName}`;
  writeLine(context.stderr, `${prefix}: ${describeError(error)}`);
  if (error instanceof UsageError) {
    const helpCommand = error.command ?? commandName;
    writeLine(
      context.stderr,
      helpCommand === undefined
        ? `Run "${program} --help" for usage.`
        : `Run "${program} ${helpCommand} --help" for usage.`
    );
  }
}
