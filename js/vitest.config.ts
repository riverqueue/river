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
    // Line and branch coverage is supplementary evidence, not a gate; see
    // `pnpm run test:coverage` in docs/development.md.
    coverage: {
      exclude: ["**/*.test.ts", "**/testdata/**", "examples/**"],
      include: [
        "cli/src/**/*.ts",
        "driver/*/src/**/*.ts",
        "migrate/src/**/*.ts",
        "src/**/*.ts",
        "test/src/**/*.ts",
        "worker-threads/src/**/*.ts",
      ],
      provider: "v8",
      reporter: ["text-summary", "html", "json-summary"],
      reportsDirectory: "coverage",
    },
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/*.integration.test.ts",
      // Runs with `node --test` against packed tarballs in package:check.
      "scripts/packed-tests/**",
    ],
    // Vitest filters the parent's execArgv. Preload explicitly so River's
    // worker threads inherit the same application-provided Temporal setup.
    execArgv: [
      "--import",
      path.resolve(import.meta.dirname, "scripts/temporal-setup.mjs"),
    ],
    setupFiles: ["./scripts/vitest-setup.mjs"],
  },
});
