import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import eslintConfigPrettier from "eslint-config-prettier";

export default tseslint.config(
  {
    ignores: ["**/dist/"],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  eslintConfigPrettier,
  {
    languageOptions: {
      parserOptions: {
        // One program over every package and its tests, the same one
        // `typecheck:tests` checks. The project service would instead pick
        // each package's build tsconfig, which excludes test files. Like that
        // typecheck, it reads `riverqueue` through its built declarations, so
        // run `pnpm run build` before linting.
        project: "./tsconfig.tests.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    linterOptions: {
      reportUnusedDisableDirectives: "error",
    },
    rules: {
      // Concise arrow callbacks such as `() => resolve()` are idiomatic.
      "@typescript-eslint/no-confusing-void-expression": [
        "error",
        { ignoreArrowShorthand: true },
      ],
      // Allow `while (true)` retry loops. Validation of values from untyped
      // JavaScript callers and signal checks after an `await` are disabled
      // line by line with a reason, since the compiler's narrowing cannot see
      // either.
      "@typescript-eslint/no-unnecessary-condition": [
        "error",
        { allowConstantLoopConditions: "only-allowed-literals" },
      ],
      // River propagates caller-supplied abort reasons and caught failures
      // unchanged so callers observe the exact value they supplied or that a
      // handler threw; wrapping them in a new Error would change identity.
      "@typescript-eslint/only-throw-error": [
        "error",
        {
          allowRethrowing: true,
          allowThrowingAny: true,
          allowThrowingUnknown: true,
        },
      ],
      "@typescript-eslint/prefer-promise-reject-errors": [
        "error",
        { allowThrowingAny: true, allowThrowingUnknown: true },
      ],
      // `async` deliberately turns synchronous throws into rejections for
      // Promise-returning interfaces, even when nothing inside is awaited.
      "@typescript-eslint/require-await": "off",
      // Numbers and bigints format predictably in messages.
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowNumber: true },
      ],
    },
  },
  {
    files: ["src/**/*.ts", "driver/*/src/**/*.ts"],
    rules: {
      // Node cleans up `AbortSignal.any` dependents in time quadratic in a
      // long-lived parent's dependents, which stalls the event loop for
      // seconds under load.
      "no-restricted-properties": [
        "error",
        {
          message:
            "use LinkedAbortSignal and dispose it when the operation settles",
          object: "AbortSignal",
          property: "any",
        },
      ],
    },
  },
  {
    files: ["**/*.mjs", "examples/**"],
    ...tseslint.configs.disableTypeChecked,
  },
  {
    files: ["**/*.test.ts"],
    rules: {
      "@typescript-eslint/no-non-null-assertion": "off",
      // Tests inspect untyped `pg` rows, `JSON.parse` output, and Vitest's
      // asymmetric matchers (`expect.any`, `expect.objectContaining`), all of
      // which are typed `any`, and assert on detached `vi.fn()` mocks.
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      "@typescript-eslint/unbound-method": "off",
      "no-restricted-properties": "off",
    },
  }
);
