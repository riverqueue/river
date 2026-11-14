// Package harness runs River's cross-language conformance scenarios: River
// Go, the reference, and another implementation share one database, hand
// jobs, rows, notifications, leadership, and unique keys back and forth, and
// must agree. It proves what no single implementation's tests can, so it
// holds only scenarios with two implementations in them. Behavior one
// implementation exhibits alone belongs in that implementation's own tests,
// and pure functions of their inputs (unique keys, retry delays, cron
// schedules) in the Go-generated fixtures in conformance/testdata.
//
// The harness talks to each implementation through an adapter process (see
// package protocol), and reads and faults the database itself with SQL.
// Every scenario runs on each driver, Postgres and SQLite, unless it
// exercises something only one has, in a database of its own: a schema on
// Postgres, which adapters use through their search path, and a file on
// SQLite. Scenarios therefore run in parallel.
//
// # Running
//
//	RIVER_CONFORMANCE=go go test ./harness           # Go against itself
//	RIVER_CONFORMANCE=rust go test ./harness         # Go against Rust
//	make test/conformance CANDIDATE=js               # the same through make
//
// The environment:
//
//   - RIVER_CONFORMANCE names the implementation under test: go, rust, or
//     js. Unset, every scenario skips. Set, a run that executes no scenario
//     fails, so a mistyped -run pattern can't pass.
//   - RIVER_CONFORMANCE_REFERENCE names the reference implementation, go by
//     default. Setting it pairs two non-Go implementations.
//   - RIVER_CONFORMANCE_DRIVERS limits the drivers, "postgres,sqlite" by
//     default.
//   - RIVER_CONFORMANCE_NIGHTLY=1 adds the nightly tier: process kills,
//     database faults, three-engine fleets, rolling deploys, and
//     performance and soak runs.
//   - TEST_DATABASE_URL is the Postgres database scenarios create their
//     schemas in, postgres://localhost:5432/river_test by default.
//
// The harness builds each adapter once per run: Go's with `go build`,
// Rust's with `cargo build -p riverqueue-conformance`, and JavaScript's with
// `pnpm --filter @riverqueue/conformance... run build` after building
// the riverqueue package itself. Another module can run its own scenarios
// against adapters of its own by passing their implementations to
// UseImplementations from its TestMain before calling Main.
package harness
