import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^@riverqueue\/test$/,
        replacement: path.resolve(import.meta.dirname, "test/src/index.ts"),
      },
      {
        find: "riverqueue/unstable-driver",
        replacement: path.resolve(
          import.meta.dirname,
          "src/unstable-driver.ts"
        ),
      },
      {
        find: /^riverqueue$/,
        replacement: path.resolve(import.meta.dirname, "src/index.ts"),
      },
    ],
  },
  test: {
    // Vitest filters the parent's execArgv. Preload explicitly so River's
    // worker threads inherit the same application-provided Temporal setup.
    execArgv: [
      "--import",
      path.resolve(import.meta.dirname, "scripts/temporal-setup.mjs"),
    ],
    // Every Postgres integration fixture owns the canonical River tables.
    // Keep files sequential so one fixture cannot truncate another's rows.
    fileParallelism: false,
    include: ["**/*.integration.test.ts"],
    setupFiles: ["./scripts/vitest-setup.mjs"],
  },
});
