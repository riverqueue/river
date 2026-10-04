import { parseArgs } from "node:util";

/** One command-line flag accepted by a command. */
export interface OptionSpec {
  /** One-line help text. */
  readonly description: string;
  /** Whether the flag may repeat. Repeated values are collected in order. */
  readonly multiple?: boolean;
  /** Single-letter alias, such as `n` for `-n`. */
  readonly short?: string;
  /** `boolean` flags take no value; `string` flags require one. */
  readonly type: "boolean" | "string";
  /** Placeholder shown in help for the value, such as `URL`. */
  readonly valueName?: string;
}

/** Flags keyed by long name, without the leading `--`. */
export type OptionSpecs = Readonly<Record<string, OptionSpec>>;

/** Parsed flag values keyed by long name. */
export type OptionValues = Readonly<
  Record<string, boolean | readonly (boolean | string)[] | string | undefined>
>;

/** A mistake in how a command was invoked, reported with a usage hint. */
export class UsageError extends Error {
  /** Command whose help the error should point to, if any. */
  readonly command: string | undefined;

  constructor(message: string, command?: string) {
    super(message);
    this.name = "UsageError";
    this.command = command;
  }
}

const HELP_OPTION: OptionSpec = {
  description: "Show help",
  short: "h",
  type: "boolean",
};

/**
 * Parse `argv` against a command's flags with `node:util` `parseArgs` in
 * strict mode. Every command also accepts `-h` and `--help`. Positional
 * arguments are rejected unless `allowPositionals` is set.
 */
export function parseOptions(
  command: string,
  specs: OptionSpecs,
  argv: readonly string[],
  allowPositionals = false
): {
  help: boolean;
  positionals: readonly string[];
  values: OptionValues;
} {
  const options: Record<
    string,
    { multiple?: boolean; short?: string; type: "boolean" | "string" }
  > = { help: toParseArgsOption(HELP_OPTION) };
  for (const [name, spec] of Object.entries(specs)) {
    options[name] = toParseArgsOption(spec);
  }

  let parsed: ReturnType<typeof parseArgs>;
  try {
    parsed = parseArgs({
      allowPositionals,
      args: [...argv],
      options,
      strict: true,
    });
  } catch (error: unknown) {
    throw new UsageError(parseErrorMessage(error), command);
  }
  const { help, ...values } = parsed.values;
  return { help: help === true, positionals: parsed.positionals, values };
}

const HELP_WIDTH = 80;

/**
 * Render help text: the description, a usage line, and the flags with
 * descriptions wrapped to 80 columns.
 */
export function formatHelp(sections: {
  readonly description: string;
  readonly options?: OptionSpecs;
  readonly usage: string;
}): string {
  const lines = [
    sections.description.trim(),
    "",
    "Usage:",
    `  ${sections.usage}`,
  ];
  if (sections.options !== undefined) {
    const rows = [
      ...Object.entries(sections.options),
      ["help", HELP_OPTION] as const,
    ]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([name, spec]) => {
        const short = spec.short === undefined ? "    " : `-${spec.short}, `;
        const value =
          spec.type === "string" ? ` ${spec.valueName ?? "VALUE"}` : "";
        return [`  ${short}--${name}${value}`, spec.description] as const;
      });
    const width = Math.max(...rows.map(([flag]) => flag.length)) + 2;
    lines.push("", "Flags:");
    for (const [flag, description] of rows) {
      const wrapped = wrap(description, HELP_WIDTH - width);
      lines.push(`${flag.padEnd(width)}${wrapped[0] ?? ""}`);
      for (const continuation of wrapped.slice(1)) {
        lines.push(`${" ".repeat(width)}${continuation}`);
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

function wrap(text: string, width: number): string[] {
  const lines: string[] = [];
  let current = "";
  for (const word of text.split(/\s+/)) {
    if (current !== "" && current.length + 1 + word.length > width) {
      lines.push(current);
      current = word;
    } else {
      current = current === "" ? word : `${current} ${word}`;
    }
  }
  if (current !== "") lines.push(current);
  return lines;
}

/** Read an optional string flag. */
export function stringValue(
  values: OptionValues,
  name: string
): string | undefined {
  const value = values[name];
  return typeof value === "string" ? value : undefined;
}

/** Read a boolean flag, defaulting to `false`. */
export function booleanValue(values: OptionValues, name: string): boolean {
  return values[name] === true;
}

/** Read every value of a repeatable string flag. */
export function stringValues(
  values: OptionValues,
  name: string
): readonly string[] {
  const value = values[name];
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

/** Parse an optional flag as a non-negative (0) or positive (1) integer. */
export function integerValue(
  command: string,
  values: OptionValues,
  name: string,
  minimum: 0 | 1
): number | undefined {
  const value = stringValue(values, name);
  return value === undefined
    ? undefined
    : parseInteger(command, `--${name}`, value, minimum);
}

/** Parse `value` as a non-negative (0) or positive (1) decimal integer. */
export function parseInteger(
  command: string,
  flag: string,
  value: string,
  minimum: 0 | 1
): number {
  const parsed = /^\s*-?\d+\s*$/.test(value) ? Number(value) : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    const kind = minimum === 0 ? "a non-negative" : "a positive";
    throw new UsageError(
      `${flag} must be ${kind} integer; received ${JSON.stringify(value)}`,
      command
    );
  }
  return parsed;
}

const DURATION_UNITS_MS: Readonly<Record<string, number>> = {
  h: 3_600_000,
  m: 60_000,
  ms: 1,
  s: 1_000,
};

/**
 * Parse a duration such as `30s`, `5m`, `1h30m`, or `1.5s` into whole
 * milliseconds.
 */
export function parseDuration(
  command: string,
  flag: string,
  value: string
): number {
  const segment = /(\d+(?:\.\d+)?)(ms|h|m|s)/y;
  let total = 0;
  let offset = 0;
  while (offset < value.length) {
    segment.lastIndex = offset;
    const match = segment.exec(value);
    const amount = match?.[1];
    const unit = match?.[2];
    if (amount === undefined || unit === undefined) break;
    total += Number(amount) * (DURATION_UNITS_MS[unit] ?? Number.NaN);
    offset = segment.lastIndex;
  }
  const milliseconds = Math.round(total);
  if (
    value.length === 0 ||
    offset !== value.length ||
    !Number.isSafeInteger(milliseconds) ||
    milliseconds < 1
  ) {
    throw new UsageError(
      `${flag} must be a duration such as 500ms, 30s, 5m, or 1h30m; ` +
        `received ${JSON.stringify(value)}`,
      command
    );
  }
  return milliseconds;
}

function parseErrorMessage(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const code = (error as { code?: unknown }).code;
  const option = /'(-[^' ]+)/.exec(error.message)?.[1];
  switch (code) {
    case "ERR_PARSE_ARGS_UNKNOWN_OPTION":
      return option === undefined ? error.message : `unknown flag: ${option}`;
    case "ERR_PARSE_ARGS_INVALID_OPTION_VALUE":
      if (option === undefined) return error.message;
      if (error.message.includes("does not take")) {
        return `flag ${option} does not take a value`;
      }
      if (error.message.includes("ambiguous")) {
        return (
          `flag ${option} requires a value; ` +
          `write ${option}=VALUE to pass a value that starts with "-"`
        );
      }
      return `flag ${option} requires a value`;
    case "ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL": {
      const argument = /'([^']*)'/.exec(error.message)?.[1];
      return argument === undefined
        ? error.message
        : `unexpected argument: ${JSON.stringify(argument)}`;
    }
    default:
      return error.message;
  }
}

function toParseArgsOption(spec: OptionSpec): {
  multiple?: boolean;
  short?: string;
  type: "boolean" | "string";
} {
  return {
    type: spec.type,
    ...(spec.multiple === true ? { multiple: true } : {}),
    ...(spec.short === undefined ? {} : { short: spec.short }),
  };
}
