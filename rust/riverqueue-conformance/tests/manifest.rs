//! Checks the repository's conformance manifest against the Rust crates,
//! which are versioned together. The published crates can't read files
//! outside their packages, so the check lives in this unpublished crate.

use serde_json::Value;

#[test]
fn conformance_manifest_pins_this_version() {
    let manifest: Value =
        serde_json::from_str(include_str!("../../../conformance/manifest.json")).unwrap();
    assert_eq!(
        manifest["implementations"]["rust"]["version"],
        env!("CARGO_PKG_VERSION")
    );
}
