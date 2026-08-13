# Rust API design

River's Rust API is a native Rust interface to River's shared database
protocol. It is not a mechanical translation of the Go API. Compatibility is
defined by persisted data, state transitions, transaction visibility,
notifications, scheduling, and extension behavior—not by giving constants or
methods the same spelling in both languages.

This document records the pre-publication design decisions that are expensive
to reverse once applications depend on the crates. Behavioral evidence and
release gates live in [rust-compatibility-validation.md](rust-compatibility-validation.md).

## Design principles

### Keep the common client concrete

`Client`, `ClientBuilder`, `Worker`, and `WorkContext` are deliberately
non-generic. A database type parameter on `Client` would spread into workers,
plugins, application state, error types, companion crates, and most rustdoc
examples even though ordinary job code does not care which SQL dialect stores
the job.

Instead, `Client::builder` accepts a sealed built-in database source and erases
it behind River operations. PostgreSQL and SQLite are built in. A future MySQL
backend can be added without changing the public client or worker types. The
sealed boundary is intentional: it lets River evolve database operations and
capabilities without turning an internal adapter seam into a permanent
third-party SPI.

This follows the successful pattern of clients such as
[`reqwest::Client`](https://docs.rs/reqwest/latest/reqwest/struct.Client.html):
a cheap, cloneable, concrete handle configured through a builder, with complex
implementation types kept out of application signatures. It differs from
generic worker frameworks such as
[`apalis::WorkerBuilder`](https://docs.rs/apalis/latest/apalis/prelude/struct.WorkerBuilder.html),
whose backend and middleware types are carried through the worker type. That
approach is powerful for freely composable storage implementations, but it is
a poor fit for River's deliberately closed, protocol-compatible drivers and
exact-version extensions.

### Preserve native transactions without accepting pools

Transaction methods accept a sealed `DatabaseTransactionExecutor`. Only SQLx
PostgreSQL and SQLite transactions satisfy it; pools, bare connections, and
pool connections do not. This keeps the explicit `&mut transaction` call shape while
making accidental non-transactional use of a `_tx` method a compile error.

The client checks that the executor and client use the same backend before a
hook, query, or notification can have a side effect. River does not expose
SQLx's open `Executor` trait as its stable database abstraction: SQL syntax,
claiming, notifications, leadership, maintenance, and bulk insertion all have
backend-specific semantics that cannot be made correct by abstracting only
placeholder syntax.

### Keep batch insertion typed and complete

Homogeneous batches use `insert_many(args)` for the common defaults-only case
and clearly named `*_with` variants for per-item options. `InsertBatch` is the
safe heterogeneous equivalent of Go's mixed `InsertMany` input: each appended
`JobArgs` type retains its own defaults and uniqueness fields, the whole batch
is atomic, and type-erased results remain in input order. Backend-optimized
fast insertion follows the same naming, using PostgreSQL COPY where available
and an atomic transactional path on SQLite.

### Optimize periodic jobs for the ordinary case

`PeriodicJob::new(schedule, || Args)` is the zero-options constructor.
Registration options use `with_options`, while conditional occurrences and
per-occurrence insertion options have explicit `conditional*` names. This
keeps the most visible constructor simple without hiding the advanced form.

### Model configuration as valid, composable values

Complex construction uses consuming builders with private fields, following
the [Rust API Guidelines' builder and validation
guidance](https://rust-lang.github.io/api-guidelines/checklist.html). Required
values are constructor arguments; optional values have fluent `with_*`
methods; fallible validation happens before runtime work begins.

Insertion options are partial overrides rather than fully resolved jobs.
Resolution has an explicit precedence:

1. call-site overrides;
2. `JobArgs` defaults;
3. client defaults;
4. River protocol defaults.

This makes `client.insert(args)` the normal operation and prevents sentinel
default values from accidentally overriding job-type configuration. The
resolved representation is separate so exact-version companion crates can
compose metadata and protocol flags without losing typed defaults.

Booleans and loosely related `Option` fields are replaced with enums or
validated value types when combinations have domain meaning. Passive database
records retain readable fields, while authored configuration and wire-protocol
types prevent invalid construction. Newtypes are reserved for distinctions
that repay their ergonomic cost rather than wrapping every integer ID.

### Make lifecycle ownership visible

`Client::start` returns a `#[must_use]` `RunHandle`. Awaited shutdown methods
both request shutdown and observe persistence/task failures. Dropping the
handle requests immediate best-effort cancellation; `detach` explicitly opts
into an unsupervised runtime.

This is intentionally stricter than Tokio's
[`JoinHandle`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html),
which detaches on drop. A detached database worker can keep claiming jobs after
its owner assumes it stopped, so River makes detachment the conspicuous action.
The shutdown protocol follows the division recommended by
[`TaskTracker`](https://docs.rs/tokio-util/latest/tokio_util/task/struct.TaskTracker.html):
signal cancellation separately from waiting for all tracked work to exit.

Tokio aborts asynchronous work only when it yields. River therefore documents
the distinction between cooperative cancellation, task abortion after the
stuck threshold, and blocking or CPU-bound work that cannot be forcibly
stopped safely.

### Use native async worker traits and function adapters

`Worker` uses return-position `impl Future + Send`, so users can implement it
with `async fn` without an attribute macro or a boxed future at the public
boundary. River performs one type-erasing box when a heterogeneous worker is
registered. The common case can use `WorkerRegistry::register_fn` with an async
function or capturing closure; implementing the trait remains available for a
custom timeout or retry policy.

Worker and handler errors preserve their concrete source through erasure,
middleware, hooks, and error handlers. Formatting an error into a string is
not an acceptable substitute for a source chain because it prevents callers
from downcasting or inspecting structured causes.

### Represent valid outcomes and events

Worker outcomes are an enum, not a set of nullable fields or magic errors.
Events are a sum type with separate job and queue payloads, so an impossible
combination cannot be constructed. Public enums that may grow are
`#[non_exhaustive]`; persisted discriminators are decoded explicitly so an
unknown future wire value is not silently interpreted as an existing state.

Subscriptions are bounded local observations, not durable streams. Lag is a
typed receive result with a dropped count. Events are emitted after the
corresponding database transition commits, but unrelated jobs have no global
completion order.

### Expose structured failures

Validation, backend mismatch, runtime availability, task failure, database
failure, and extension failure have distinct error variants. Database and
extension variants retain `Error::source`. Error messages add operational
context but are not used as a type system.

Public operations document their error and panic conditions. Expected misuse
returns an error or is rejected by the type system; internal `expect` calls are
limited to invariants made exhaustive by the sealed backend and valid event
representations.

### Keep database capabilities explicit

PostgreSQL uses schemas, `LISTEN`/`NOTIFY`, advisory locking, concurrent
reindexing, `SKIP LOCKED`, and `COPY`. SQLite uses its canonical main schema,
serialized writer transactions, and a durable notification outbox polled by
clients. Those mechanisms differ; public job and queue semantics do not.

Backend-only configuration lives on the backend source, such as
`PostgresDatabase`, rather than on common maintenance configuration. An
unsupported exact-version extension returns a structured backend error. River
does not publish a capability-boolean collection that callers must interpret,
and it does not silently weaken a requested operation.

New backends must implement semantic River operations per dialect. SQLx
`AnyPool`, raw SQL fragments, and a public user-implemented driver trait are
not substitutes for this work. MySQL support is additive only after its
claiming, transaction, notification/polling, leadership, migration, and
maintenance behavior passes the same applicable conformance scenarios.

### Keep companion crates behind the same boundary

River Pro is a separate, private implementation, but its public Rust API must
not reintroduce PostgreSQL types after core River erases them. Its client
builder accepts the same sealed database sources, and its transaction methods
use the same sealed transaction executor. Pro dispatches through a private
semantic driver owned by the private repository; Pro SQL, migrations,
capabilities, and fixtures never enter the public driver boundary.

Until a Pro backend implements a feature, selecting it returns a structured
unsupported-backend error. That is preferable to a PostgreSQL-only constructor
or `PgConnection` parameter, which would make adding SQLite or MySQL a breaking
API change, and preferable to a panic based on an internal pool invariant.

`ProClient` is a newtype with `AsRef<Client>` and an explicit `river()` access
method, plus curated forwarding for frequent operations. It does not implement
`Deref`: a companion client is not a smart pointer, and implicit method lookup
would become ambiguous as either API grows. Uncommon core configuration is
available through a closure over the core builder so River Pro cannot silently
fall behind new core options.

## Comparative API review

The design was compared against established Rust libraries at the level where
their constraints actually overlap with River. The goal is not to imitate a
project's surface syntax; it is to reuse patterns whose ownership and failure
semantics have held up in real applications.

| Library or convention | Pattern worth carrying forward | River decision |
|---|---|---|
| [`reqwest::Client`](https://docs.rs/reqwest/latest/reqwest/struct.Client.html) and [`ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html) | A cheap concrete cloneable handle, with construction policy isolated in a builder | Keep `Client` non-generic and cloneable; validate the complete configuration in `build`. |
| [`sqlx::Transaction`](https://docs.rs/sqlx/latest/sqlx/struct.Transaction.html) | Caller-owned native transactions compose with application SQL | Accept actual SQLx transactions through a sealed transaction-executor capability; reject pools and bare connections at compile time, and never acquire another connection inside a `_tx` operation. |
| [`tokio::task::JoinHandle`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html) and [`TaskTracker`](https://docs.rs/tokio-util/latest/tokio_util/task/struct.TaskTracker.html) | Task ownership, cancellation, and waiting are distinct concerns | Return a must-use River handle that owns cancellation and observation; make unsupervised detachment explicit. |
| [`serde`](https://docs.rs/serde/latest/serde/) derive conventions | Rust field names are not necessarily serialized field names | Unique-path derivation follows serialization-side rename rules and rejects ambiguous flatten/skip combinations. |
| [`thiserror`](https://docs.rs/thiserror/latest/thiserror/) | Public failures retain typed variants and source chains | Preserve database, worker, hook, encryption, workflow, and handler sources instead of formatting them away. |
| [`apalis`](https://docs.rs/apalis/latest/apalis/) | Typed jobs, async workers, middleware, and backend-aware worker construction are natural in Rust | Adopt typed jobs, native async workers, and function adapters. Do not adopt backend-generic worker/client types because River supports a closed cross-language database protocol, not arbitrary storage implementations. |
| [`tower::Service`](https://docs.rs/tower/latest/tower/trait.Service.html) | A uniform request/response middleware abstraction enables broad ecosystem composition | Defer a Tower façade until a concrete integration needs it; River's insert/work/hooks have different ordering and transactional guarantees that are clearer in lifecycle-specific traits. |

This comparison also identifies negative guidance. A public SQLx `AnyPool`
would erase types without implementing River semantics. A generic `Client<D>`
would make every worker and companion API carry a backend it does not use. A
Tokio-like detach-on-drop handle would make a database worker continue claiming
jobs after its owner could reasonably believe it had stopped. Those are
idiomatic tools in their own domains, but not good fits for River's contract.

## Alternatives considered

| Proposal | Decision | Reason |
|---|---|---|
| `Client<D>` and `Worker<D>` | Reject | Backend types would contaminate ordinary application and extension APIs. |
| SQLx `AnyPool` | Reject | It erases types but does not normalize dialect or runtime semantics. |
| Public `Driver` trait | Reject for now | It would freeze a large semantic SPI before River has enough backend experience. |
| Sealed built-in backends | Accept | It permits additive first-party drivers while preserving freedom to evolve internals. |
| Builders with private fields | Accept selectively | Authored configurations need validation and forward evolution; passive records still benefit from direct reads. |
| Blanket newtypes | Reject | Use them only where they prevent a real category error or encode validation. |
| `Deref` from a companion client | Reject | Rust's API guidelines reserve `Deref` for smart-pointer behavior; explicit access and curated forwarding avoid method ambiguity. |
| Tower as the public extension model | Defer | Hooks and middleware already express River lifecycle semantics; Tower is justified only by a concrete integration use case. |
| `Stream` for local events | Defer | A typed bounded receiver exposes lag and shutdown semantics without another dependency or ambiguous stream errors. |
| Go/Rust public-name manifest | Reject | Name equality rewards transliteration and says nothing about protocol or behavioral compatibility. |

## Proposal decision record

The initial API audit was reviewed independently from core, Pro, compatibility,
and adversarial perspectives. “Modify” means the underlying problem is real,
but the originally suggested shape would either hurt ergonomics or break a
shared River behavior.

| Proposal | Decision | Implemented direction |
|---|---|---|
| Own the runtime through a handle | Modify | Keep `start`; validate the Tokio runtime before changing state; return a must-use handle with awaited stop/wait, cancel-on-drop, and explicit detach. A larger `run`/`spawn` family is deferred until a real use case distinguishes it. |
| Derive every unique JSON path directly from Rust fields | Modify | Follow Serde serialization renames and raw identifiers; reject ambiguous `flatten`/skip combinations; preserve Go's omission of absent optional fields rather than making absence an error. |
| Make resolved insertion defaults public call-site options | Reject | Keep partial `InsertOpts` distinct from resolved `InsertParams`, with documented overlay precedence. |
| Rename common insertion to `insert` | Accept | `insert(args)` is the default path; `insert_with(args, opts)` is the explicit override path; transaction forms remain recognizable `_tx` methods. |
| Privatize every public struct field | Modify | Author-written configuration and wire values become validated/private; passive job, queue, event, and result snapshots remain convenient to read and are made non-exhaustive where appropriate. |
| Add a new error variant for every validation sentence | Reject | Use structured error families with field/operation context and retained sources rather than an unbounded stringly enum. |
| Remove `async_trait` from workers | Accept | Native RPITIT permits ordinary `async fn` implementations and boxes only at heterogeneous registration. Database-bound internal extension traits may still use erasure where it materially simplifies object safety. |
| Accept async functions and closures as workers | Accept | Add a first-class function adapter while retaining the trait for per-kind timeout/retry customization. |
| Make events a valid sum type | Accept | Separate job and queue events; retain `EventKind` only as the subscription discriminator. |
| Implement `Stream` for event receivers | Defer | `recv` makes bounded lag and closure errors explicit without another public dependency. |
| Wrap all job IDs in newtypes | Reject | Add validated domain types where they prevent mistakes; do not make ordinary IDs cumbersome solely for stylistic consistency. |
| Add typed worker-test helpers | Accept | Provide a job builder, one-shot worker execution, output, and metadata assertions in `riverqueue-test`. |
| Expose Tower as the middleware API | Defer | Existing lifecycle-specific hooks and middleware are clearer until a concrete Tower composition requirement appears. |
| Let users implement database drivers | Reject for the initial API | Seal the operation boundary and ship first-party PostgreSQL/SQLite implementations; revisit only with enough independent backend experience to define a durable SPI. |
| Re-export PostgreSQL schema configuration from the common root | Reject | Keep schemas and reindexing with the PostgreSQL source so common configuration remains backend-neutral. |
| `Deref<Target = Client>` from `ProClient` | Reject | Use explicit `river`, `AsRef`, and curated forwarding. |
| Forward every core builder method manually from Pro | Modify | Own the core builder and expose a configuration closure, forwarding only frequent operations. This prevents option drift and preserves Pro hook ordering. |
| Keep separate core/Pro queue worker counts | Reject | One combined validated Pro queue configuration owns the core queue config and Pro additions. |
| Encode Pro selections as `Option<Vec<String>>` and zero limits | Reject | Use named argument-selection states and validated optional positive limits while preserving Go-compatible hash bytes. |
| Keep placeholder empty option structs | Reject | Remove empty positional option values; introduce a real options type only when it conveys behavior. |
| Accept positional `bool` in durable periodic definitions | Reject | Use validated IDs and named options such as run-on-start. |
| Expose workflow persistence structs as the domain API | Reject | Decode exact versioned wire DTOs privately into valid typed workflow, wait, signal, and timer values; reject malformed or future unsupported wire forms. |
| Leave workflow operations as unrelated free functions | Modify | Move toward an ID-bound workflow handle and separate new-workflow builder, while keeping transaction variants explicit. |
| Map unknown worker outcomes to discard in batching | Reject | Convert every current outcome exhaustively and fail closed when the outcome model grows. |
| Return positional batch outcomes | Reject | Use a validated set keyed by job ID so sorting or filtering cannot apply an outcome to the wrong job. Preserve handler error sources and support async closures. |
| Keep mutually exclusive encryption flags | Reject | Use selection/mode enums, structured source-preserving failures, and zeroize secret keys on drop. |
| Mark every enum non-exhaustive | Modify | Apply it where downstream wildcard matching is useful; persisted values still require explicit unknown-value handling because an attribute cannot make decoding safe. |

## Compatibility and evolution

Public API stability is checked with Rust semver tooling and compile-checked
examples, not a language-to-language symbol inventory. Database compatibility
is checked through canonical migrations, language-neutral fixtures, and a
versioned process adapter whose scenarios can be implemented by a future
JavaScript client.

The following changes require shared conformance evidence before release:

- persisted fields, encodings, states, or reserved metadata;
- unique-key inputs or hashing;
- transaction commit/rollback visibility;
- claim, completion, cancellation, retry, rescue, and scheduling behavior;
- notification and polling recovery semantics;
- migration SQL or migration-line history;
- leader-owned maintenance behavior.

Language-local ergonomics—method spelling, builder layout, worker traits, and
error type organization—do not need to match Go. They do need Rust API tests,
rustdoc examples, source-preserving errors, and a semver review.

## Documentation standard

Each published crate includes its README as crate-level rustdoc so the quick
start cannot drift. Public APIs should have useful `Debug` implementations
except secret-bearing types, use standard conversion traits where natural,
and document `# Errors`, `# Panics`, cancellation, transaction, and drop
behavior where relevant. Examples use the public API and are compiled as
doctests.

The website's Go guides remain the conceptual source for River behavior. Rust
documentation restates those concepts with Rust-native code and ownership,
async, error, and database guidance; it does not import or edit the website
sources.
