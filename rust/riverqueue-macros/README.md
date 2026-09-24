# riverqueue-macros

Derive macros for River's Rust client. Applications normally receive these
macros through `riverqueue` and do not need to depend on this crate directly.

`#[derive(JobArgs)]` requires a stable `#[river(kind = "...")]` and can declare
kind aliases, default queue, max attempts, priority, pending state, and tags,
and default uniqueness:

```rust,ignore
#[derive(Deserialize, JobArgs, Serialize)]
#[river(
    kind = "send_invoice",
    queue = "billing",
    tags("billing"),
    unique(by_args, by_period = "24h", by_state(available, pending, running, scheduled)),
    insert_opts = invoice_insert_opts,
)]
struct SendInvoice {
    #[river(unique)]
    invoice_id: i64,
    note: String,
}
```

`unique(...)` accepts `by_args`, `by_args("nested.path", ...)`, `by_period`
(a Go duration such as `"1h30m"`), `by_queue`, `by_state(...)`, and
`exclude_kind`, mirroring River Go's `UniqueOpts`. Fields marked
`#[river(unique)]`, plus any `by_args` paths, are the arguments hashed for
uniqueness; with neither, every argument is hashed. Marking a field without
`unique(by_args)` is a compile error, since it would otherwise have no effect.
Unique paths follow Serde's serialization-side `rename` and `rename_all` rules,
including raw Rust identifiers. Conditionally skipped optional fields are
omitted in the same way as River Go; flattened or always skipped unique fields
are rejected because their wire path is ambiguous.

`insert_opts = path::to_fn` names a `fn() -> InsertOpts` whose options are
overlaid on the attribute defaults, like Go's `JobArgsWithInsertOpts`.
`crate = "path"` sets the path to `riverqueue` when it is renamed or
re-exported.
