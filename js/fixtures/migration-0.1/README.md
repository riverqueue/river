# `riverqueue@0.1.0` migration fixture

This fixture preserves the original insertion-only release at tag `v0.1.0` and
commit `7d1ad4d56ccd0eaaa06c5f0ebddbf24fdee82796`. `before.ts.txt`
intentionally uses the 0.1 names and types; `after.ts` is the mechanical
migration and is compiled against the packed current package by the package
consumer check.

`codemod.ts.txt` is the exact output of `riverqueue codemod-0.1 --write` on
`before.ts.txt`. It keeps a `.txt` extension because it intentionally still
fails to compile at the one site the codemod marks for review (a `number`
annotation on the now-`bigint` job ID). The legacy fixture check reruns the
codemod, requires this output, requires every compiler error in it to sit
under a `TODO(riverqueue-0.1)` comment, and compiles it cleanly with both
TypeScript compilers after applying that one manual fix.

The original package used argument classes, lossy numeric IDs, `Date`,
`InsertManyParams`, and boolean unique-skip results. The current API uses job
definitions, `bigint`, `Temporal.Instant`, plain batch items, and discriminated
results. See `docs/migrating-from-0.1.md` for the complete rationale.

`riverqueue-0.1.0.tgz` is the exact npm registry artifact, pinned by SHA-1,
SHA-256, and npm SHA-512 integrity in `original/manifest.json`. The release did
not package a README and npm has no README metadata for it, so `original/`
also retains the documentation and example README files from the exact Git tag.
CI verifies their hashes, inspects the archive, and compiles the before/after
consumers without needing registry or Git history access.
