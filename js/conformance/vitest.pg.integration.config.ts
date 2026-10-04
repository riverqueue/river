import path from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  root: import.meta.dirname,
  resolve: {
    alias: [
      {
        find: /^riverqueue$/,
        replacement: path.resolve(import.meta.dirname, "../src/index.ts"),
      },
      {
        find: /^riverqueue\/unstable-driver$/,
        replacement: path.resolve(
          import.meta.dirname,
          "../src/unstable-driver.ts"
        ),
      },
    ],
  },
  test: {
    include: [
      "src/insert-only-adapter.integration.test.ts",
      "src/pg-adapter.integration.test.ts",
    ],
    setupFiles: ["../scripts/vitest-setup.mjs"],
  },
});
