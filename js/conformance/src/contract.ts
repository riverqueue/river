/**
 * Parameter strictness derived from River's adapter contract. Every method's
 * `params` schema is closed (`additionalProperties: false`), and adapters
 * must reject parameters their method does not declare, nested ones
 * included, with `invalid_params` instead of silently ignoring them.
 */
import { invalidParams } from "./errors.js";

/** A JSON Schema fragment as it appears in the contract. */
type Schema = Readonly<Record<string, unknown>>;

/** The parts of `contract.json` the params checker reads. */
export interface ContractParamsSource {
  readonly $defs?: Readonly<Record<string, Schema>>;
  readonly methods: readonly {
    readonly name: string;
    readonly params?: Schema;
  }[];
}

/** Rejects request params the contract does not declare. */
export class ContractParams {
  readonly #definitions: ReadonlyMap<string, Schema>;
  readonly #methods: ReadonlyMap<string, Schema>;

  constructor(contract: ContractParamsSource) {
    this.#definitions = new Map(Object.entries(contract.$defs ?? {}));
    this.#methods = new Map(
      contract.methods.flatMap(({ name, params }) =>
        params === undefined ? [] : [[name, params] as const]
      )
    );
  }

  /**
   * Throw `invalid_params` when `params` has a key that the method's closed
   * schema, or any closed schema nested in it, does not declare. Methods
   * outside the contract are not checked; the profile gate rejects them.
   */
  check(method: string, params: Record<string, unknown>): void {
    const schema = this.#methods.get(method);
    if (schema !== undefined) this.#checkValue(schema, params, "params");
  }

  #checkValue(schema: Schema, value: unknown, location: string): void {
    const reference = schema.$ref;
    if (typeof reference === "string") {
      const definition = reference.startsWith("#/$defs/")
        ? this.#definitions.get(reference.slice("#/$defs/".length))
        : undefined;
      // References to other schema files describe results, which the
      // adapter produces rather than receives.
      if (definition !== undefined) {
        this.#checkValue(definition, value, location);
      }
      return;
    }
    if (Array.isArray(value)) {
      const items = schema.items;
      if (isSchema(items)) {
        value.forEach((item: unknown, index) => {
          this.#checkValue(items, item, `${location}[${index}]`);
        });
      }
      return;
    }
    if (value === null || typeof value !== "object") return;
    const properties = isSchema(schema.properties)
      ? schema.properties
      : undefined;
    for (const [key, child] of Object.entries(value)) {
      const childSchema = properties?.[key];
      if (isSchema(childSchema)) {
        this.#checkValue(childSchema, child, `${location}.${key}`);
      } else if (schema.additionalProperties === false) {
        throw invalidParams(`unknown parameter ${location}.${key}`);
      }
    }
  }
}

function isSchema(value: unknown): value is Schema {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
