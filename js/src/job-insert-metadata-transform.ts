import { ValidationError } from "./errors.js";
import { PluginPayloads } from "./internal/plugin-payloads.js";
import type { RiverPlugin } from "./extensions.js";
import type { JobDefinition } from "./job-definition.js";
import type { ReadonlyJsonObject } from "./job-args-transform.js";
import type { JsonObject } from "./json.js";
import { deepFreezeJson, toJsonObject } from "./json.js";

declare const jobInsertMetadataTransformPluginBrand: unique symbol;

/** Plaintext insertion input for a matched-version metadata transformer. */
export interface JobInsertMetadataTransformInput {
  readonly args: ReadonlyJsonObject;
  /**
   * The job definition the caller inserted (the same object identity), or
   * undefined for an insertion without one. Extensions may key per-definition
   * behavior off it, for example with a `WeakMap`.
   */
  readonly definition: JobDefinition | undefined;
  readonly kind: string;
  readonly metadata: ReadonlyJsonObject;
  /** Whether the job will be inserted `pending` so far. */
  readonly pending: boolean;
  readonly queue: string;
}

/** What a metadata transformer returns for one insertion. */
export interface JobInsertMetadataTransformResult {
  /** The job's complete metadata after this transformer. */
  readonly metadata: ReadonlyJsonObject;
  /**
   * Insert the job `pending` instead of available or scheduled, like the
   * `pending` insert option. A transformer can only set it, never clear it.
   */
  readonly pending?: true;
}

/**
 * Adjusts an insertion's metadata (and optionally makes it `pending`) before
 * uniqueness is computed and before argument transforms run. Transformers
 * run in plugin order on every insertion path.
 */
export interface JobInsertMetadataTransformer {
  readonly name: string;
  readonly onInsert: (
    input: JobInsertMetadataTransformInput
  ) => JobInsertMetadataTransformResult;
}

/** The combined result of every configured metadata transformer. */
export interface TransformedInsertMetadata {
  readonly metadata: JsonObject;
  readonly pending: boolean;
}

/** Opaque plugin produced by {@link createJobInsertMetadataTransformPlugin}. */
export interface JobInsertMetadataTransformPlugin extends RiverPlugin {
  readonly [jobInsertMetadataTransformPluginBrand]: true;
}

/** Each plugin's transformer, kept off the plugin object. */
const transformers = new PluginPayloads<Readonly<JobInsertMetadataTransformer>>(
  "riverqueue.job-insert-metadata-transform-plugin",
  "job insert metadata transform plugin"
);

/**
 * Create a plugin that adds or rewrites a job's metadata at insert time,
 * before argument transforms run, and may insert the job as `pending`.
 */
export function createJobInsertMetadataTransformPlugin(
  transformer: JobInsertMetadataTransformer
): JobInsertMetadataTransformPlugin {
  const normalized = normalizeTransformer(transformer);
  const plugin = { name: normalized.name } as JobInsertMetadataTransformPlugin;
  transformers.set(plugin, normalized);
  return Object.freeze(plugin);
}

/** @internal Preserve an opaque transformer while snapshotting plugins. */
export function cloneJobInsertMetadataTransformPlugin(
  source: RiverPlugin,
  target: RiverPlugin
): void {
  transformers.copy(source, target);
}

/** @internal Return only exact transformers, preserving plugin order. */
export function getJobInsertMetadataTransformers(
  plugins: readonly RiverPlugin[] | undefined
): readonly Readonly<JobInsertMetadataTransformer>[] {
  return transformers.list(plugins);
}

/** @internal Return whether this is a matched metadata-transform plugin. */
export function isJobInsertMetadataTransformPlugin(plugin: object): boolean {
  return transformers.get(plugin) !== undefined;
}

/** @internal Apply metadata transforms while arguments are still plaintext. */
export function transformJobInsertMetadata(
  configured: readonly Readonly<JobInsertMetadataTransformer>[],
  definition: JobDefinition | undefined,
  kind: string,
  args: JsonObject,
  metadata: JsonObject,
  queue: string,
  pending: boolean
): TransformedInsertMetadata {
  let current = metadata;
  let currentPending = pending;
  for (const transformer of configured) {
    const input = Object.freeze({
      args: immutableJsonObject(args),
      definition,
      kind,
      metadata: immutableJsonObject(current),
      pending: currentPending,
      queue,
    });
    const name = JSON.stringify(transformer.name);
    const result = transformer.onInsert(input);
    if (
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      result === null ||
      typeof result !== "object" ||
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-boolean-literal-compare, @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      (result.pending !== undefined && result.pending !== true)
    ) {
      throw new ValidationError(
        `job insert metadata transformer ${name} must return { metadata, pending? }`
      );
    }
    try {
      current = toJsonObject(result.metadata);
    } catch (cause: unknown) {
      throw new ValidationError(
        `job insert metadata transformer ${name} returned invalid metadata`,
        { cause }
      );
    }
    if (result.pending === true) currentPending = true;
  }
  return { metadata: deepFreezeJson(current), pending: currentPending };
}

function immutableJsonObject(value: JsonObject): ReadonlyJsonObject {
  return deepFreezeJson(toJsonObject(value));
}

function normalizeTransformer(
  transformer: JobInsertMetadataTransformer
): Readonly<JobInsertMetadataTransformer> {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (transformer === null || typeof transformer !== "object") {
    throw new ValidationError(
      "job insert metadata transformer must be an object"
    );
  }
  if (typeof transformer.name !== "string" || transformer.name.length === 0) {
    throw new ValidationError(
      "job insert metadata transformer name must be non-empty"
    );
  }
  if (typeof transformer.onInsert !== "function") {
    throw new ValidationError(
      "job insert metadata transformer onInsert must be a function"
    );
  }
  return Object.freeze({
    name: transformer.name,
    onInsert: transformer.onInsert,
  });
}
