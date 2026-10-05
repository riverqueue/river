/**
 * Private payloads of one kind of plugin, kept off the plugin objects so
 * code holding a plugin can neither read nor copy its payload.
 */
import { ConfigurationError } from "../errors.js";

/**
 * The payload of each plugin of one kind. Each plugin also carries a
 * payload-free `Symbol.for` brand shared by every installed copy of
 * riverqueue, so a copy can tell a plugin it has no payload for, such as
 * one from another copy or a spread copy of a plugin, from a plugin of
 * another kind, and reject it instead of silently ignoring it.
 */
export class PluginPayloads<Payload> {
  readonly #brand: symbol;
  readonly #kind: string;
  readonly #payloads = new WeakMap<object, Payload>();

  /**
   * `brandKey` is the `Symbol.for` key of this kind's brand, and `kind`
   * names the kind in errors.
   */
  constructor(brandKey: string, kind: string) {
    this.#brand = Symbol.for(brandKey);
    this.#kind = kind;
  }

  /** Give `target`, a snapshot of `source`, the same payload. */
  copy(source: object, target: object): void {
    const payload = this.get(source);
    if (payload !== undefined) this.set(target, payload);
  }

  /**
   * The payload of `plugin`, or undefined for a plugin of another kind.
   *
   * @throws {ConfigurationError} when `plugin` carries this kind's brand
   * but this copy of riverqueue has no payload for it.
   */
  get(plugin: object): Payload | undefined {
    const payload = this.#payloads.get(plugin);
    if (payload === undefined && Object.hasOwn(plugin, this.#brand)) {
      const name = (plugin as { readonly name?: unknown }).name;
      throw new ConfigurationError(
        `${this.#kind} ${JSON.stringify(name)} comes from another installed ` +
          "copy of riverqueue; check `npm ls riverqueue`"
      );
    }
    return payload;
  }

  /** The payloads of the plugins of this kind, in plugin order. */
  list(plugins: readonly object[] | undefined): readonly Payload[] {
    if (plugins === undefined) return [];
    return plugins.flatMap((plugin) => {
      const payload = this.get(plugin);
      return payload === undefined ? [] : [payload];
    });
  }

  /** Brand `plugin` and attach its payload. */
  set(plugin: object, payload: Payload): void {
    this.#payloads.set(plugin, payload);
    // Enumerable, so a spread copy of the plugin keeps it.
    Object.defineProperty(plugin, this.#brand, {
      configurable: false,
      enumerable: true,
      value: true,
      writable: false,
    });
  }
}
