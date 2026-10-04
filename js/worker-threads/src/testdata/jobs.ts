/**
 * Job definitions shared by the worker-thread tests and their thread-side
 * handlers. Handlers import this module for types only.
 */
import { defineJob, isExactJsonNumber } from "riverqueue";
import type { ExactJsonNumber, JsonObject } from "riverqueue";

/** Args accepted by {@link testJob}. */
export interface TestArgs {
  readonly id?: ExactJsonNumber | number;
  readonly milliseconds?: number;
  readonly value?: string;
}

/** Args decoded by {@link richJob}, which structured clone must carry. */
export interface RichArgs {
  readonly at: Date;
  readonly big: bigint;
  readonly bytes: Uint8Array;
  readonly lookup: Map<string, number>;
  readonly tags: Set<string>;
}

/** The ordinary job used by most worker-thread tests. */
export const testJob = defineJob({
  kind: "test",
  decode(value: JsonObject): TestArgs {
    const { id, milliseconds, value: text } = value;
    if (id !== undefined && typeof id !== "number" && !isExactJsonNumber(id)) {
      throw new TypeError("id must be a number");
    }
    if (milliseconds !== undefined && typeof milliseconds !== "number") {
      throw new TypeError("milliseconds must be a number");
    }
    if (text !== undefined && typeof text !== "string") {
      throw new TypeError("value must be a string");
    }
    return value;
  },
});

/** A job whose decoded args are not River JSON. */
export const richJob = defineJob<{ at: string; big: string }>()({
  kind: "test_rich",
  decode(value: JsonObject): RichArgs {
    const { at, big } = value;
    if (typeof at !== "string" || typeof big !== "string") {
      throw new TypeError("at and big must be strings");
    }
    return {
      at: new Date(at),
      big: BigInt(big),
      bytes: new Uint8Array([1, 2, 3]),
      lookup: new Map([["one", 1]]),
      tags: new Set(["a", "b"]),
    };
  },
});
