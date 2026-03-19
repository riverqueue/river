# River Coding Guidelines

## Running Tests and Lint

- **Tests**: use `make test` as the default. Only use `go test ...` when you must pass specific flags (e.g. `-run`, `-count`, `-race`, build tags, etc.) or need to debug a specific test in isolation.
- **Lint**: use `make lint` as the default. Only call `golangci-lint ...` directly when you must pass specific flags.
- Always run test and lint prior to considering a task complete, unless told otherwise (or if you did not touch any Go code/tests).
- If your execution environment has a sandbox/permission model, run `make test` and `make lint` unsandboxed (full permissions) so results match local dev and CI.

## Other Build/Test Commands

- **Run all tests with race detector**: `make test/race`
- **Run single test**: `cd $MODULE && go test ./path/to/package -run TestName`
- **Run benchmark**: `make bench`
- **Generate sqlc**: `make generate`. Use this any time a `.sql` file has been modified and we need to then regenerate `.sql.go` files from it.
- **Run tidy when deps change**: `make tidy`

## Code Style Guidelines

- **Imports**: use gci sections - Standard, Default, github.com/riverqueue.
- **Formatting**: use gofmt, gofumpt, goimports.
- **JSON tags**: use snake_case for JSON field tags.
- **Dependencies**: minimize external dependencies beyond standard library and pgx.
- **SQL access (non-test code)**: avoid ad-hoc SQL strings in library/runtime code. Add or extend a sqlc query, regenerate with `make generate`, and expose it through the driver interface. Keep direct SQL for tests and benchmark/admin utilities only (for example, `pg_stat_statements` and `VACUUM`).
- **Driver interface stability**: treat `riverdriver` as an internal adapter seam, not as an official external API. Its package comments explicitly say it should not be implemented or invoked by user code, and changes there are not considered semver-breaking. Do not preserve driver-interface methods or semantics for outside consumers; add, remove, or reshape them as needed to preserve or improve user-facing functionality.
- **Cross-driver driver tests**: treat `riverdriver/riverdrivertest` as the shared conformance suite for driver behavior. When changing `riverdriver` or a concrete driver, update or extend `riverdrivertest` so the intended semantics are exercised across drivers, not only in a single driver-specific test.
- **Error handling**: prefer context-rich errors; review linting rules before disabling them.
- **Testing**: use require variants instead of assert.
- **Helpers**: use `Func` suffix for function variables, not `Fn`.
- **Documentation**: include comments for exported functions and types.
- **Naming**: use idiomatic Go names; see `.golangci.yaml` for allowed short variable names.

## Package Naming and Organization

- Package names are lowercase, short, and representative; avoid `common`, `util`, or overly broad names.
- Use singular package names; avoid plurals like `httputils`.
- Keep import paths clean; avoid `src/`, `pkg/`, or other repo-structure leakage.
- Organize by responsibility instead of `models`/`types` buckets; keep types close to usage.
- Do not export identifiers from `main` packages that only build binaries.
- Add package docs, and use `doc.go` when documentation is long.

## Code Organization

Alphabetization is important when adding new code (do not reorganize existing code unless asked).

- Types should be sorted alphabetically by name.
- Struct field definitions on a type should be sorted alphabetically by name, unless there is a good reason to deviate (examples: ID fields first, grouping mutexed fields after a mutex, etc.).
- When declaring an instance of a struct, fields should be sorted alphabetically by name unless a similar deviation is justified.
- When defining methods on a type, they should be sorted alphabetically by name.
- Constructors should come immediately after the type definition.
- Keep all methods for a type grouped together, immediately after the type definition and any constructor(s), organized alphabetically by name. Do not intersperse methods with other types or functions, except in special cases where a small utility type is needed to support a method and not used elsewhere.
- In unit tests, the outer test blocks should be sorted alphabetically by name. Inner test blocks should also be sorted alphabetically by name within the outer block.

## Go Testing Conventions: Parallel Test Bundle + Setup Helpers

This repo uses a parallel test bundle pattern (inspired by Brandur's write-up: https://brandur.org/fragments/parallel-test-bundle) to keep parallel subtests isolated and setup/fixtures DRY.

- **Always opt into parallel**:
  - **Top-level tests**: the first statement in every `TestXxx` should be `t.Parallel()`.
  - **Subtests**: the first statement in every `t.Run(..., func(t *testing.T) { ... })` should be `t.Parallel()`, unless the subtest is intentionally non-parallel and includes a short comment explaining why.
- **Statement ordering and spacing**:
  - **Top-level tests**: use `t.Parallel()`, then a blank line, then test preamble (`ctx`, `setup`, helpers), then a blank line before subtests/assertions.
  - **Subtests**: use `t.Parallel()`, then a blank line, then subtest preamble.
  - If a subtest calls `setup(...)`, prefer one blank line after the setup assignment before assertions/actions.
  - For tiny/obvious subtests (one short statement after `t.Parallel()` or `setup(...)`), omitting one of these blank lines is acceptable.
- **Context and setup ordering**:
  - If `setup` needs `ctx` (`setup(ctx, t)`), assign/derive `ctx` before calling `setup`.
  - If `setup` does not need `ctx` (`setup(t)`), call `setup` first and derive specialized contexts (`WithCancel`, `WithTimeout`) close to where they are used.
  - Avoid creating/deriving `ctx` far from usage unless shared setup requires it.
- **Prefer local bundles**:
  - Define a `type testBundle struct { ... }` inside the `TestXxx` function containing the system under test and any fixtures frequently used across subtests.
  - Each parallel subtest should call `setup(t)` to get a fresh bundle. Avoid sharing mutable state across parallel subtests.
- **`setup` helper rules**:
  - Define `setup` as a local closure in the test:
    - `setup := func(t *testing.T) *testBundle { ... }`
    - Always call `t.Helper()` at the top of `setup`.
  - **Only accept a context parameter if it is needed**:
    - **Default**: `setup(t)` should not take `ctx`.
    - **If setup must derive/seed a context**: prefer returning it: `setup := func(t *testing.T) (*testBundle, context.Context)`.
    - **If setup must be passed an existing context**: accept `ctx` as the first parameter: `setup := func(ctx context.Context, t *testing.T) *testBundle`.
  - Keep `setup` deterministic and self-contained; it should only use the `*testing.T` (and `ctx` if explicitly required) passed in.
- **Test signal instrumentation**:
  - Prefer `rivershared/testsignal.TestSignal` in a `...TestSignals` struct with an `Init(tb)` helper over ad hoc `chan struct{}` fields in spies/fakes.
  - Keep test-signal structs zero-value by default and call `Init(t)` only in tests that need to observe those signals.
  - Wait for async events with `WaitOrTimeout()`. For negative assertions, use `RequireEmpty()` or `WaitC()` with a timeout select.
  - Avoid custom channel signaling helpers and hand-managed channel capacities unless there is a specific, documented reason.

Template:

```go
func TestThing(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		// Put SUT + common fixtures here.
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{}
	}

	t.Run("CaseName", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// ... use `bundle` in assertions/actions ...
	})

	t.Run("CaseNameWithCtxRequiredBySetup", func(t *testing.T) {
		t.Parallel()

		setupWithCtx := func(ctx context.Context, t *testing.T) *testBundle {
			t.Helper()

			_ = ctx
			return &testBundle{}
		}

		ctx := context.Background()
		bundle := setupWithCtx(ctx, t)

		// ... use `bundle` in assertions/actions ...
	})
}
```
