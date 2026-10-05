import {
  ConfigurationError,
  isExactJsonNumber,
  stringifyJson,
} from "riverqueue";

import type { EncodedArgs } from "./protocol.js";

/**
 * Encode a job's decoded args for a worker thread.
 *
 * Args in River's JSON domain cross as text, which preserves exact JSON
 * numbers that structured clone rejects. Other args must survive structured
 * clone unchanged: primitives including `bigint`, plain objects and arrays,
 * `Date`, `Map`, `Set`, and `Uint8Array`. Anything that would arrive as a
 * different type, such as a class instance whose prototype would be dropped,
 * is rejected so a handler's argument types stay truthful.
 */
export function encodeArgs(kind: string, args: unknown): EncodedArgs {
  let text: string | undefined;
  try {
    text = stringifyJson(args);
  } catch {
    // Not River JSON; fall back to a checked structured clone below.
  }
  if (text !== undefined) return { encoding: "json", text };

  const problem = cloneProblem(args, "$", new Set());
  if (problem !== undefined) {
    throw new ConfigurationError(
      `decoded args for job kind ${JSON.stringify(kind)} cannot cross the ` +
        `worker-thread boundary: ${problem.path} ${problem.reason}`,
      { details: { kind, path: problem.path } }
    );
  }
  return { encoding: "clone", value: args };
}

interface CloneProblem {
  readonly path: string;
  readonly reason: string;
}

function cloneProblem(
  value: unknown,
  path: string,
  seen: Set<object>
): CloneProblem | undefined {
  switch (typeof value) {
    case "bigint":
    case "boolean":
    case "number":
    case "string":
    case "undefined":
      return undefined;
    case "function":
      return { path, reason: "is a function" };
    case "symbol":
      return { path, reason: "is a symbol" };
  }
  if (value === null || typeof value !== "object" || seen.has(value)) {
    return undefined;
  }
  seen.add(value);

  if (isExactJsonNumber(value)) {
    return {
      path,
      reason:
        "is an exact JSON number, which only crosses in args that are " +
        "entirely River JSON",
    };
  }
  const prototype: unknown = Object.getPrototypeOf(value);
  if (prototype === Map.prototype) {
    let index = 0;
    for (const [key, entry] of value as Map<unknown, unknown>) {
      const problem =
        cloneProblem(key, `${path}.<key ${index}>`, seen) ??
        cloneProblem(entry, `${path}.<value ${index}>`, seen);
      if (problem !== undefined) return problem;
      index++;
    }
    return undefined;
  }
  if (prototype === Set.prototype) {
    let index = 0;
    for (const entry of value as Set<unknown>) {
      const problem = cloneProblem(entry, `${path}.<value ${index}>`, seen);
      if (problem !== undefined) return problem;
      index++;
    }
    return undefined;
  }
  if (prototype === Date.prototype || prototype === Uint8Array.prototype) {
    return undefined;
  }
  if (
    Array.isArray(value)
      ? prototype !== Array.prototype
      : prototype !== Object.prototype && prototype !== null
  ) {
    const name = (value as { constructor?: { name?: unknown } }).constructor
      ?.name;
    return {
      path,
      reason: `is a ${typeof name === "string" && name !== "" ? name : "non-plain"} instance`,
    };
  }

  for (const key of Reflect.ownKeys(value)) {
    if (Array.isArray(value) && key === "length") continue;
    if (typeof key === "symbol") {
      return { path, reason: "has a symbol key" };
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable) continue;
    const childPath = Array.isArray(value)
      ? `${path}[${key}]`
      : `${path}.${key}`;
    if (!("value" in descriptor)) {
      return { path: childPath, reason: "is an accessor property" };
    }
    const problem = cloneProblem(descriptor.value, childPath, seen);
    if (problem !== undefined) return problem;
  }
  return undefined;
}
