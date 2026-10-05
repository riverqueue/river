import {
  ConfigurationError,
  PayloadValidationError,
  RiverError,
  type PayloadValidationPhase,
} from "./errors.js";
import type {
  InsertOptions,
  NormalizedInsertOptions,
} from "./insert-options.js";
import { normalizeInsertOptions } from "./insert-options.js";
import { isUserSpecifiedIdOrKind } from "./identifiers.js";
import type { ExactJsonNumber, JsonObject, JsonValue } from "./json.js";
import { toJsonObject } from "./json.js";

declare const definitionTypes: unique symbol;

/**
 * The structural contract implemented by Standard Schema validators such as
 * Zod, Valibot, and ArkType. See https://standardschema.dev.
 */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": {
    readonly types?:
      | {
          readonly input: Input;
          readonly output: Output;
        }
      | undefined;
    readonly validate: (
      value: unknown
    ) =>
      PromiseLike<StandardSchemaResult<Output>> | StandardSchemaResult<Output>;
    readonly vendor: string;
    readonly version: 1;
  };
}

/** Successful or failed Standard Schema validation. */
export type StandardSchemaResult<Output> =
  | { readonly issues?: undefined; readonly value: Output }
  | { readonly issues: readonly StandardSchemaIssue[] };

/** The Standard Schema issue fields River retains for diagnostics. */
export interface StandardSchemaIssue {
  readonly message: string;
  readonly path?:
    ReadonlyArray<PropertyKey | { readonly key: PropertyKey }> | undefined;
}

/** Input type accepted by a Standard Schema validator. */
export type StandardSchemaInput<Schema> =
  Schema extends StandardSchemaV1<infer Input, unknown> ? Input : never;

/** Output type produced by a Standard Schema validator. */
export type StandardSchemaOutput<Schema> =
  Schema extends StandardSchemaV1<unknown, infer Output> ? Output : never;

/**
 * `T` itself when every value it describes can be stored as River job JSON,
 * otherwise a type that `T` is not assignable to.
 *
 * Unlike `T extends JsonObject`, this accepts interfaces (which have no
 * implicit index signature) and optional properties (River omits properties
 * whose value is `undefined`, like `JSON.stringify`). It rejects `Date`,
 * `bigint`, functions, symbols, `Map`/`Set`, class instances, and `unknown`.
 */
export type JsonCompatible<T> = [T] extends [JsonValue]
  ? T
  : T extends boolean | ExactJsonNumber | null | number | string
    ? T
    : T extends bigint | symbol | undefined | ((...args: never[]) => unknown)
      ? never
      : T extends readonly unknown[]
        ? { [Index in keyof T]: JsonCompatible<T[Index]> }
        : T extends object
          ? { [Key in keyof T]: JsonCompatibleProperty<T[Key]> }
          : never;

/** Object properties may also be `undefined`, which River omits. */
type JsonCompatibleProperty<T> = T extends undefined
  ? undefined
  : JsonCompatible<T>;

/** Readable compile-time error carried by an invalid job definition. */
export interface JobDefinitionTypeError<Message extends string> {
  readonly "~riverTypeError": Message;
}

/**
 * The producer input type for a definition whose decoder returns `Args`:
 * `Args` itself when it is a JSON object, otherwise a type error asking for an
 * explicit producer type.
 */
export type DecodedJobInput<Args> = [Args] extends [JsonCompatible<Args>]
  ? Args extends readonly unknown[]
    ? JobDefinitionTypeError<"job args must be a JSON object, not an array">
    : Args extends object
      ? Args
      : JobDefinitionTypeError<"job args must be a JSON object">
  : JobDefinitionTypeError<"decode() returns values that are not JSON; declare the producer input with defineJob<Input>()({ ... })">;

type SchemaJobInput<Schema> =
  unknown extends StandardSchemaInput<Schema>
    ? JsonObject
    : StandardSchemaInput<Schema>;

type SchemaCheck<Schema> = [SchemaJobInput<Schema>] extends [
  JsonCompatible<SchemaJobInput<Schema>>,
]
  ? SchemaJobInput<Schema> extends readonly unknown[]
    ? JobDefinitionTypeError<"schema input must be a JSON object, not an array">
    : unknown
  : JobDefinitionTypeError<"schema input must be JSON (no Date, bigint, undefined, Map, or class values); validate the persisted JSON shape and convert in the worker">;

export interface JobDefinitionOptions<Kind extends string = string> {
  /** Insertion defaults below call-site options and above client defaults. */
  readonly defaults?: InsertOptions;
  /** Stable persisted job kind shared by every language that works this job. */
  readonly kind: Kind;
  /**
   * Other kinds this job's worker also works, like River for Go's
   * `JobArgsWithKindAliases`. To rename a kind safely, make the new name the
   * `kind` and the old one an alias: jobs are inserted under the new kind,
   * while jobs already stored under the old one are still worked. Remove the
   * alias once those have finished, retries included.
   */
  readonly kindAliases?: readonly string[];
}

/** A job definition whose arguments are validated by a Standard Schema. */
export interface SchemaJobDefinitionConfig<
  Schema extends StandardSchemaV1,
  Kind extends string = string,
> extends JobDefinitionOptions<Kind> {
  readonly decode?: never;
  /**
   * Standard Schema validator for the persisted JSON arguments. River runs it
   * when inserting and again before working, because another producer (an
   * older deploy or another language) may have inserted the job.
   */
  readonly schema: Schema;
}

/** A job definition whose arguments are validated by an explicit decoder. */
export interface DecoderJobDefinitionConfig<
  Args,
  Kind extends string = string,
> extends JobDefinitionOptions<Kind> {
  /**
   * Validate the persisted JSON arguments and return the worker's args.
   * Throw to reject them. River calls it when inserting and before working.
   */
  readonly decode: (value: JsonObject) => Args | PromiseLike<Args>;
  readonly schema?: never;
}

/** A job definition without runtime validation. */
export interface UncheckedJobDefinitionConfig<
  Kind extends string = string,
> extends JobDefinitionOptions<Kind> {
  readonly decode?: never;
  readonly schema?: never;
}

/**
 * Immutable identity and type information for one job kind.
 *
 * `Input` is what producers pass to `client.insert`; `Args` is what the worker
 * receives after validation. Definitions contain no client, pool, or handler,
 * so web producers can import them without worker dependencies.
 */
export interface JobDefinition<
  Input extends object = object,
  Args = unknown,
  Kind extends string = string,
> {
  /** Insertion defaults below call-site options and above client defaults. */
  readonly defaults: Readonly<NormalizedInsertOptions>;
  /** Stable persisted job kind. */
  readonly kind: Kind;
  /** Other kinds its worker also works; see {@link JobDefinitionOptions.kindAliases}. */
  readonly kindAliases?: readonly string[];
  /** Type-only marker carrying the definition's input and args types. */
  readonly [definitionTypes]?: {
    readonly args: Args;
    readonly input: Input;
  };
}

/** Worker args produced by a job definition. */
export type JobDefinitionArgs<Definition extends JobDefinition> =
  Definition extends JobDefinition<object, infer Args> ? Args : never;

/** Producer input accepted by a job definition. */
export type JobDefinitionInput<Definition extends JobDefinition> =
  Definition extends JobDefinition<infer Input> ? Input : never;

interface DefinitionInternals {
  readonly decode?: (
    value: JsonObject,
    phase: PayloadValidationPhase
  ) => Promise<unknown>;
}

const definitionInternals = new WeakMap<object, DefinitionInternals>();

/**
 * Define a job validated by a Standard Schema (Zod, Valibot, ArkType, ...).
 *
 * Producers pass the schema's input type; workers receive its output type.
 * The schema's input must be JSON: River persists exactly what the producer
 * passed so other languages can read it and unique hashes stay stable.
 *
 * @example
 * ```ts
 * import * as v from "valibot";
 *
 * export const sendEmail = defineJob({
 *   kind: "send_email",
 *   schema: v.object({ to: v.pipe(v.string(), v.email()) }),
 * });
 * ```
 */
export function defineJob<
  const Schema extends StandardSchemaV1,
  const Kind extends string,
>(
  config: SchemaJobDefinitionConfig<Schema & SchemaCheck<Schema>, Kind>
): JobDefinition<
  SchemaJobInput<Schema> extends object ? SchemaJobInput<Schema> : never,
  StandardSchemaOutput<Schema>,
  Kind
>;

/**
 * Define a job validated by an explicit decoder.
 *
 * The decoder receives the untrusted persisted JSON object and returns the
 * worker's args. Producers insert values of the decoder's return type, which
 * must therefore be JSON; to insert a different type, declare it with
 * `defineJob<Input>()({ ... })`.
 *
 * @example
 * ```ts
 * export const resizeImage = defineJob({
 *   kind: "resize_image",
 *   decode(value) {
 *     if (typeof value.url !== "string") throw new TypeError("url required");
 *     return { url: value.url };
 *   },
 * });
 * ```
 */
export function defineJob<Args, const Kind extends string>(
  config: DecoderJobDefinitionConfig<Args, Kind>
): JobDefinition<DecodedJobInput<Awaited<Args>>, Awaited<Args>, Kind>;

/**
 * Declare a job's producer input type explicitly, then define it.
 *
 * With a decoder, workers receive the decoder's return type. Without one,
 * workers receive `JsonObject`: a type argument alone never makes persisted
 * input from another producer appear validated.
 *
 * @example
 * ```ts
 * interface ReportInput {
 *   reportId: string;
 * }
 *
 * export const buildReport = defineJob<ReportInput>()({
 *   kind: "build_report",
 *   decode(value) {
 *     if (typeof value.reportId !== "string") {
 *       throw new TypeError("reportId must be a string");
 *     }
 *     return { reportId: value.reportId, requestedAt: Temporal.Now.instant() };
 *   },
 * });
 * ```
 */
export function defineJob<Input extends object = JsonObject>(): [
  Input,
] extends [JsonCompatible<Input>]
  ? DefineJobWithInput<Input>
  : JobDefinitionTypeError<"the declared producer input must be JSON (no Date, bigint, undefined, Map, or class values)">;

/**
 * Define a job without runtime validation. Producers insert any JSON object
 * and workers receive `JsonObject` args.
 */
export function defineJob<const Kind extends string>(
  config: UncheckedJobDefinitionConfig<Kind>
): JobDefinition<JsonObject, JsonObject, Kind>;

export function defineJob(
  config?:
    | DecoderJobDefinitionConfig<unknown>
    | SchemaJobDefinitionConfig<StandardSchemaV1>
    | UncheckedJobDefinitionConfig
): JobDefinition | DefineJobWithInput<JsonObject> {
  if (config === undefined) {
    return createDefinition as DefineJobWithInput<JsonObject>;
  }
  return createDefinition(config);
}

/** Second step of `defineJob<Input>()`, with the producer type fixed. */
export interface DefineJobWithInput<Input extends object> {
  /** Define a job whose decoder returns the worker's args. */
  <Args, const Kind extends string>(
    config: DecoderJobDefinitionConfig<Args, Kind>
  ): JobDefinition<Input, Awaited<Args>, Kind>;
  /** Define a job whose workers receive unvalidated `JsonObject` args. */
  <const Kind extends string>(
    config: UncheckedJobDefinitionConfig<Kind>
  ): JobDefinition<Input, JsonObject, Kind>;
}

function createDefinition(
  config:
    | DecoderJobDefinitionConfig<unknown>
    | SchemaJobDefinitionConfig<StandardSchemaV1>
    | UncheckedJobDefinitionConfig
): JobDefinition {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (config === null || typeof config !== "object") {
    throw new ConfigurationError("job definition must be an object");
  }
  validateKind(config.kind);
  const kindAliases = normalizeKindAliases(config.kind, config.kindAliases);

  const definition: JobDefinition = {
    defaults: normalizeInsertOptions(config.defaults),
    kind: config.kind,
    kindAliases,
  };

  let decode: DefinitionInternals["decode"];
  if (config.schema !== undefined) {
    const schema = config.schema;
    if (
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      schema === null ||
      typeof schema !== "object" ||
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      typeof schema["~standard"]?.validate !== "function"
    ) {
      throw new ConfigurationError(
        "job schema must implement Standard Schema (a `~standard.validate` function)"
      );
    }
    decode = async (value, phase) => {
      const result = await schema["~standard"].validate(value);
      if (result.issues !== undefined) {
        throw payloadError(config.kind, phase, result.issues);
      }
      return result.value;
    };
  } else if (config.decode !== undefined) {
    if (typeof config.decode !== "function") {
      throw new ConfigurationError("job decode must be a function");
    }
    const configDecode = config.decode;
    decode = async (value, phase) => {
      try {
        return await configDecode(value);
      } catch (cause: unknown) {
        if (cause instanceof RiverError) throw cause;
        throw new PayloadValidationError(
          config.kind,
          phase,
          `invalid payload for job kind ${JSON.stringify(config.kind)}: ${
            cause instanceof Error ? cause.message : String(cause)
          }`,
          { cause }
        );
      }
    };
  }

  definitionInternals.set(definition, decode === undefined ? {} : { decode });
  return Object.freeze(definition);
}

/** Validate insertion input and return the exact JSON object to persist. */
export async function prepareJobInput<Definition extends JobDefinition>(
  definition: Definition,
  input: JobDefinitionInput<Definition>
): Promise<JsonObject> {
  const persisted = toJsonObject(input);
  const decode = requireInternals(definition).decode;
  if (decode !== undefined) await decode(persisted, "insert");
  return persisted;
}

/** Validate persisted args with a definition and return the worker's args. */
export async function decodeJobArgs<Definition extends JobDefinition>(
  definition: Definition,
  value: unknown
): Promise<JobDefinitionArgs<Definition>> {
  const persisted = toJsonObject(value);
  const decode = requireInternals(definition).decode;
  return (
    decode === undefined ? persisted : await decode(persisted, "work")
  ) as JobDefinitionArgs<Definition>;
}

/** Whether a value was created by {@link defineJob}. */
export function isJobDefinition(value: unknown): value is JobDefinition {
  return (
    value !== null &&
    typeof value === "object" &&
    definitionInternals.has(value)
  );
}

function payloadError(
  kind: string,
  phase: PayloadValidationPhase,
  issues: readonly StandardSchemaIssue[]
): PayloadValidationError {
  const details = issues.map((issue) => ({
    message: issue.message,
    ...(issue.path === undefined
      ? {}
      : {
          path: issue.path.map((part) =>
            typeof part === "object" ? String(part.key) : String(part)
          ),
        }),
  }));
  const first = details[0];
  const summary =
    first === undefined
      ? ""
      : `: ${first.path === undefined ? "" : `${first.path.join(".")}: `}${first.message}`;
  return new PayloadValidationError(
    kind,
    phase,
    `invalid payload for job kind ${JSON.stringify(kind)}${summary}`,
    { details: { issues: details } }
  );
}

function requireInternals(definition: JobDefinition): DefinitionInternals {
  const internals = definitionInternals.get(definition);
  if (internals === undefined) {
    throw new ConfigurationError(
      "job definition was not created by defineJob (or came from a second copy of the riverqueue package)"
    );
  }
  return internals;
}

function normalizeKindAliases(
  kind: string,
  aliases: readonly string[] | undefined
): readonly string[] {
  if (aliases === undefined) return Object.freeze([]);
  // Validates untyped JavaScript input.
  const value: unknown = aliases;
  if (!Array.isArray(value)) {
    throw new ConfigurationError("job kindAliases must be an array of kinds");
  }
  const seen = new Set<string>([kind]);
  for (const alias of aliases) {
    validateKind(alias);
    if (seen.has(alias)) {
      throw new ConfigurationError(
        `job kind alias ${JSON.stringify(alias)} repeats a kind of the same job`
      );
    }
    seen.add(alias);
  }
  return Object.freeze([...aliases]);
}

function validateKind(kind: string): void {
  if (typeof kind !== "string" || !isUserSpecifiedIdOrKind(kind)) {
    throw new ConfigurationError(
      "job kind must be at least 2 characters, start with a letter, number, or underscore, and contain only letters, numbers, and _-[]<>/.·:+"
    );
  }
  if (kind.startsWith("river_internal_")) {
    throw new ConfigurationError(
      'job kinds beginning with "river_internal_" are reserved'
    );
  }
}
