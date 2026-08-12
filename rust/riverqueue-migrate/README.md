# riverqueue-migrate

River's canonical PostgreSQL and SQLite migration lines for Rust, including
the `riverqueue-migrate` command-line program. Migration contents are mirrored
from River Go; both backends are verified byte-for-byte and by hashes in the
shared compatibility artifacts.

The checked-in mirror is intentional. Cargo package archives may only contain
files rooted in the crate, while River's independently released Go driver
modules must likewise embed migration files from their own module trees. A
repository-relative Rust include would work locally but produce a crate that
cannot build after download. Keeping each distributable package self-contained
therefore requires the duplicate bytes. Generation and verification leave the
Go driver migration directories as the sources developers edit.

Use `PostgresMigrator` for PostgreSQL and `SqliteMigrator` for SQLite. The
backend-explicit names keep call sites and rustdoc unambiguous and leave room
for additional built-in databases without changing either existing type.
