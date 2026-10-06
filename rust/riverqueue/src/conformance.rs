//! Access to the fixtures River's Go implementation generates for the ports'
//! conformance tests.

use std::{io::ErrorKind, path::Path};

/// Reads `name` from `conformance/testdata`, where `make generate/fixtures`
/// writes fixtures produced by River's Go implementation.
///
/// # Panics
///
/// Panics when the fixture can't be read, so a missing fixture fails the test
/// rather than skipping it.
pub(crate) fn read_fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/testdata")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| match error.kind() {
        ErrorKind::NotFound => panic!(
            "missing conformance fixture {}; run `make generate/fixtures` from the repository root",
            path.display()
        ),
        _ => panic!(
            "error reading conformance fixture {}: {error}",
            path.display()
        ),
    })
}
