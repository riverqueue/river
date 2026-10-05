import type { JobDefinition } from "./job-definition.js";
import { ValidationError } from "./errors.js";
import { PluginPayloads } from "./internal/plugin-payloads.js";
import type { RiverPlugin } from "./extensions.js";
import type { ExactJsonNumber, JsonObject } from "./json.js";
import {
  deepFreezeJson,
  jsonValuesEqual,
  parseJsonObject,
  toJsonObject,
} from "./json.js";

declare const jobArgsTransformPluginBrand: unique symbol;

/** A recursively immutable JSON object supplied to an argument transformer. */
export interface ReadonlyJsonObject {
  readonly [key: string]: ReadonlyJsonValue;
}

/** A recursively immutable JSON value supplied to an argument transformer. */
export type ReadonlyJsonValue =
  | boolean
  | ExactJsonNumber
  | null
  | number
  | ReadonlyJsonObject
  | readonly ReadonlyJsonValue[]
  | string;

/** Immutable insertion input for an exact-version argument transformer. */
export interface JobArgsInsertTransformInput {
  readonly args: ReadonlyJsonObject;
  /**
   * The job definition the caller inserted (the same object identity), or
   * undefined for an insertion without one.
   */
  readonly definition: JobDefinition | undefined;
  readonly encodedArgs: string;
  readonly kind: string;
}

/** Validated insertion output from an exact-version argument transformer. */
export interface JobArgsInsertTransformOutput {
  readonly args: ReadonlyJsonObject;
  readonly encodedArgs: string;
}

interface TransformedJobArgs {
  readonly args: JsonObject;
  readonly encodedArgs: string;
}

/** Immutable persisted input for an exact-version argument transformer. */
export interface JobArgsReadTransformInput {
  readonly args: ReadonlyJsonObject;
  readonly kind: string;
}

/**
 * Transforms job arguments as they are written to and read from the
 * database, for extensions that store arguments in another form. Extensions
 * using it must pin the exact `riverqueue` version.
 *
 * Insertion transformations run in plugin order. Read transformations run in
 * reverse order so independently composed codecs unwrap in the natural order.
 * Insert middleware and hooks observe storage-shaped arguments. Immediate
 * insert results are unwrapped to preserve their typed input contract; query
 * and administrative operations continue to expose storage-shaped rows.
 * Omitting `onInsert` is a read-only migration mode; in that mode `onRead`
 * must pass through the plaintext rows this client continues to insert.
 */
export interface JobArgsTransformer {
  readonly name: string;
  readonly onInsert?: (
    input: JobArgsInsertTransformInput
  ) => JobArgsInsertTransformOutput;
  readonly onRead: (input: JobArgsReadTransformInput) => ReadonlyJsonObject;
}

/** Opaque River plugin produced by {@link createJobArgsTransformPlugin}. */
export interface JobArgsTransformPlugin extends RiverPlugin {
  readonly [jobArgsTransformPluginBrand]: true;
}

/** Each plugin's transformer, kept off the plugin object. */
const transformers = new PluginPayloads<Readonly<JobArgsTransformer>>(
  "riverqueue.job-args-transform-plugin",
  "job argument transform plugin"
);

/**
 * Create a plugin that transforms job arguments as they are written to and
 * read from the database.
 */
export function createJobArgsTransformPlugin(
  transformer: JobArgsTransformer
): JobArgsTransformPlugin {
  const normalized = normalizeTransformer(transformer);
  const plugin = {
    name: normalized.name,
  } as JobArgsTransformPlugin;
  transformers.set(plugin, normalized);
  return Object.freeze(plugin);
}

/** @internal Preserve an opaque transformer while snapshotting plugins. */
export function cloneJobArgsTransformPlugin(
  source: RiverPlugin,
  target: RiverPlugin
): void {
  transformers.copy(source, target);
}

/** @internal Return whether this is a matched argument-transform plugin. */
export function isJobArgsTransformPlugin(plugin: object): boolean {
  return transformers.get(plugin) !== undefined;
}

/** @internal Return only exact transformers, preserving plugin order. */
export function getJobArgsTransformers(
  plugins: readonly RiverPlugin[] | undefined
): readonly Readonly<JobArgsTransformer>[] {
  return transformers.list(plugins);
}

/** @internal Apply insertion transformations after plaintext uniqueness. */
export function transformJobArgsForInsert(
  configured: readonly Readonly<JobArgsTransformer>[],
  definition: JobDefinition | undefined,
  kind: string,
  args: JsonObject,
  encodedArgs: string
): TransformedJobArgs {
  if (configured.length === 0) {
    return { args: deepFreezeJson(args), encodedArgs };
  }

  let currentArgs = args;
  let currentEncodedArgs = encodedArgs;
  for (const transformer of configured) {
    if (transformer.onInsert === undefined) continue;
    const input = Object.freeze({
      args: immutableJsonObject(currentArgs),
      definition,
      encodedArgs: currentEncodedArgs,
      kind,
    });
    const output = transformer.onInsert(input);
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
    if (output === null || typeof output !== "object") {
      throw new ValidationError(
        `job argument transformer ${JSON.stringify(transformer.name)} returned an invalid insertion result`
      );
    }
    const outputArgs = toJsonObject(output.args);
    if (typeof output.encodedArgs !== "string") {
      throw new ValidationError(
        `job argument transformer ${JSON.stringify(transformer.name)} returned a non-string encodedArgs`
      );
    }
    let parsed: JsonObject;
    try {
      parsed = parseJsonObject(output.encodedArgs);
    } catch (cause: unknown) {
      throw new ValidationError(
        `job argument transformer ${JSON.stringify(transformer.name)} returned invalid encodedArgs`,
        { cause }
      );
    }
    if (!jsonValuesEqual(outputArgs, parsed)) {
      throw new ValidationError(
        `job argument transformer ${JSON.stringify(transformer.name)} returned mismatched args and encodedArgs`
      );
    }
    currentArgs = outputArgs;
    currentEncodedArgs = output.encodedArgs;
  }
  return {
    args: deepFreezeJson(currentArgs),
    encodedArgs: currentEncodedArgs,
  };
}

/** @internal Apply read transformations in reverse plugin order. */
export function transformJobArgsForRead(
  configured: readonly Readonly<JobArgsTransformer>[],
  kind: string,
  args: JsonObject
): JsonObject {
  if (configured.length === 0) return args;

  let currentArgs = args;
  for (let index = configured.length - 1; index >= 0; index -= 1) {
    const transformer = configured[index];
    if (transformer === undefined) continue;
    currentArgs = toJsonObject(
      transformer.onRead(
        Object.freeze({ args: immutableJsonObject(currentArgs), kind })
      )
    );
  }
  return currentArgs;
}

function immutableJsonObject(value: JsonObject): ReadonlyJsonObject {
  return deepFreezeJson(toJsonObject(value));
}

function normalizeTransformer(
  transformer: JobArgsTransformer
): Readonly<JobArgsTransformer> {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (transformer === null || typeof transformer !== "object") {
    throw new ValidationError("job argument transformer must be an object");
  }
  if (typeof transformer.name !== "string" || transformer.name.length === 0) {
    throw new ValidationError(
      "job argument transformer name must be a non-empty string"
    );
  }
  if (
    transformer.onInsert !== undefined &&
    typeof transformer.onInsert !== "function"
  ) {
    throw new ValidationError(
      "job argument transformer onInsert must be a function"
    );
  }
  if (typeof transformer.onRead !== "function") {
    throw new ValidationError(
      "job argument transformer onRead must be a function"
    );
  }
  return Object.freeze({
    name: transformer.name,
    ...(transformer.onInsert === undefined
      ? {}
      : { onInsert: transformer.onInsert }),
    onRead: transformer.onRead,
  });
}
