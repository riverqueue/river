import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      riverqueue: path.resolve(import.meta.dirname, "src/index.ts"),
    },
  },
  test: {
    include: ["**/*.integration.test.ts"],
  },
});
