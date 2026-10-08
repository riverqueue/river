# Ruby conformance

Ruby reads fixtures generated from this checkout's Go implementation, alongside
Rust and JavaScript. From the repository root:

```sh
make -C ruby install
make test/ruby/conformance
```

This generates `conformance/testdata/` and runs `ruby/spec/conformance_spec.rb`.
No Postgres server, pinned upstream checkout, protocol adapter, or separately
maintained golden files are needed. Missing fixtures fail with instructions to
regenerate them. The regular Ruby suite includes these checks too.

The fixture checks cover:

- All unique-key cases, including typed-only cases, raw JSON tokens, selected
  fields, periods, queues, and state masks.
- Job state names and unique bits, attempt-error encoding/decoding, retry bounds,
  and all six shared metadata keys, including actual output, rescue, periodic,
  unique insertion, and resumable-step persistence.
- All snooze-counter fixtures through real worker attempts and both SQL drivers.
- Cron occurrences, named zones, daylight-saving transitions, interval schedules,
  and invalid expressions, with the Ruby API differences below checked explicitly.
- Insertion, cancellation, queue pause/resume/metadata, and leadership resignation
  notification encoding through both SQL drivers, using in-memory SQLite and
  the bundled canonical migrations.

The Ruby workflow runs on Ruby changes. The shared Conformance workflow also
runs these tests when Go code changes. The ordinary driver suites retain
Postgres notification delivery/commit-ordering tests and shared Postgres /
SQLite insertion, transaction, uniqueness, and worker tests.

This replaces the old `insert-only-v1` adapter and its pinned external harness.
It checks shared data formats, not live mixed-language worker execution. Adapter
handshake and request-schema tests were specific to that removed harness; their
protocol is not part of the Ruby API.

## Coverage gaps and API differences

Ruby's runtime still polls job and queue state rather than consuming Postgres
LISTEN events or the SQLite outbox. The fixture target checks notification
emission, not dispatch. Ruby does not emit or handle `request_resign`; other
clients' requests therefore cannot force a Ruby leader to resign. Completing
that coverage would require a notification listener and runtime dispatch path.
The ordinary driver suites verify Postgres delivery, commit ordering, and
rollback for queue controls and resignations as well as insertion/cancellation.

Shared snooze-counter fixtures cover non-negative integers and absent counters.
Recovery from other JSON values is implementation-specific. Ruby's shared driver
tests cover its lenient conversion rules separately on Postgres and SQLite.

Cron retains Ruby's existing Fugit extensions: six fields with seconds, Sunday
as 7, hour 24, and descending ranges. These four expressions are accepted in
Ruby and rejected in Go; the fixture suite explicitly asserts the difference.
Fugit rejects impossible calendar dates at construction instead of returning
Go's zero time. Ruby's default calendar zone remains UTC; tests explicitly pass
the reference time's fixed offset to model Go's time-location API. `CRON_TZ=` /
`TZ=` prefixes, `?`, and `@every` duration rounding now follow Go for the shared
fixtures. No new dependencies were added.

Selected unique fields use `by_args: [:field, [:nested, :field]]`, rather than Go
struct tags. Dotted string keys are literal. Cross-language unique keys require
the same encoded argument values, including escaping, numeric tokens, and
nested ordering; Ruby does not override the application's JSON serialization.

Transactions are connection-scoped blocks. Workers use exceptions and thread
interruption. List cursors encode the selected field explicitly. Historical
error timestamps accept Ruby's `Time.parse` formats, and metadata must decode
to an object. Live interoperability across these boundaries is not covered by
the fixture target.
