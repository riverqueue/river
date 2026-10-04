import type { Migration, MigrationBackend } from "@riverqueue/migrate";

import type { OptionSpecs, OptionValues } from "./options.js";

/** A destination for command output, such as `process.stdout`. */
export interface OutputStream {
  write(chunk: string): unknown;
}

/** A source of interactive input, such as `process.stdin`. */
type InputStream = NodeJS.ReadableStream & { readonly isTTY?: boolean };

/** Returns an additional migration line's migrations for a backend. */
type MigrationLineProvider = (
  backend: MigrationBackend
) => readonly Migration[];

/** Process facilities a command runs with. */
export interface CommandContext {
  readonly env: NodeJS.ProcessEnv;
  /** Migration lines beyond River's bundled main line, by name. */
  readonly migrationLines: Readonly<Record<string, MigrationLineProvider>>;
  /** Program name shown in help and messages, such as `riverqueue`. */
  readonly program: string;
  readonly stderr: OutputStream & { readonly isTTY?: boolean };
  readonly stdin: InputStream;
  readonly stdout: OutputStream;
}

/** One `riverqueue` subcommand. */
export interface Command {
  /**
   * Longer help text shown by `riverqueue <command> --help`, in which
   * `{program}` stands for the program name.
   */
  readonly description: string;
  readonly name: string;
  readonly options: OptionSpecs;
  /**
   * Usage placeholder for positional arguments, such as `<file>...`. Commands
   * without it reject positional arguments.
   */
  readonly positionals?: string;
  /** One-line summary shown in the command list. */
  readonly summary: string;
  /** Run the command and resolve to the process exit code. */
  run(
    values: OptionValues,
    context: CommandContext,
    positionals: readonly string[]
  ): Promise<number>;
}

export function writeLine(stream: OutputStream, line = ""): void {
  stream.write(`${line}\n`);
}
