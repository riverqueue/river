#!/usr/bin/env node

import process from "node:process";

try {
  // The executable owns its runtime setup. Importing @riverqueue/cli as a
  // library continues to use the application's Temporal implementation.
  const temporal: unknown = Reflect.get(globalThis, "Temporal");
  if (temporal === undefined) {
    const { Temporal } = await import("@js-temporal/polyfill");
    Object.defineProperty(globalThis, "Temporal", {
      configurable: true,
      value: Temporal,
      writable: true,
    });
  }

  const { run } = await import("./run.js");
  process.exitCode = await run(process.argv.slice(2));
} catch (error: unknown) {
  const message =
    error instanceof Error ? error.message : "could not initialize CLI";
  process.stderr.write(`riverqueue: ${message}\n`);
  process.exitCode = 1;
}
