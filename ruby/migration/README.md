# Canonical River migrations

The SQL files in this directory are unmodified copies from
https://github.com/riverqueue/river, distributed under the upstream Mozilla
Public License 2.0 (see LICENSE). The Ruby implementation remains licensed
under the repository's MPL-2.0 license.

The bundle includes main migrations through version 008.
manifest.json records the SHA-256 of every SQL file in the repository's
riverdriver/ directory.
Verification compares against this checkout, not a pinned upstream revision.
Update or verify these files with scripts/sync_migrations.rb; do not edit SQL
here independently of upstream. Private Pro migrations are not included here.
