# riverqueue

`riverqueue` is the native Rust and Tokio client for
[River](https://riverqueue.com), backed by River's canonical PostgreSQL
schema and distributed protocol.

This crate is a preview matched to the exact Go revision recorded in the
repository's compatibility manifest. See the workspace README and
`docs/rust-compatibility.md` in the River repository for examples, deployment
rules, and the supported capability set.

Installing this crate also provides `riverqueue bench`, a destructive
development-database benchmark analogous to Go's `river bench`. It reports
periodic throughput and final throughput/p95 latency; run `riverqueue bench
--help` before using it.
