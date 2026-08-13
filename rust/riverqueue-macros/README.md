# riverqueue-macros

Derive macros for River's Rust client. Applications normally receive these
macros through `riverqueue` and do not need to depend on this crate directly.

`#[derive(JobArgs)]` requires a stable `#[river(kind = "...")]` and can declare
kind aliases, queue/max-attempt/priority/pending defaults, and argument paths
used for unique jobs. Unique paths follow Serde's serialization-side `rename`
and `rename_all` rules, including raw Rust identifiers. Conditionally skipped
optional fields are omitted in the same way as River Go; flattened or always
skipped unique fields are rejected at compile time because their wire path is
ambiguous.
