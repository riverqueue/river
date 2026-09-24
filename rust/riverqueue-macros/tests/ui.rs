//! Compile-time diagnostics of `#[derive(JobArgs)]`.

#[test]
fn derive_errors() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
