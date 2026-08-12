# riverqueue-migrate

River's canonical PostgreSQL migration line for Rust, including the
`riverqueue-migrate` command-line program. Migration contents are mirrored
from River Go and verified by hashes in the shared compatibility artifacts.

The checked-in mirror is intentional. Cargo package archives may only contain
files rooted in the crate, while River's independently released Go driver
modules must likewise embed migration files from their own module trees. A
repository-relative Rust include would work locally but produce a crate that
cannot build after download. Keeping each distributable package self-contained
therefore requires the duplicate bytes; generation and verification leave the
Pgx migration directory as the only source developers edit.
