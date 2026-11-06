# SQLite CLI URLs

The Go, Java, JavaScript, Ruby, and Rust migration CLIs accept the same SQLite
prefixes. The `//` after `sqlite:` is optional:

| Database | Long form | Short form |
| --- | --- | --- |
| Relative file | `sqlite://river.db` | `sqlite:river.db` |
| Absolute file | `sqlite:///var/lib/app/river.db` | `sqlite:/var/lib/app/river.db` |
| In memory | `sqlite://:memory:` | `sqlite::memory:` |

Relative paths are resolved from the command's working directory. An in-memory
database lasts only for that invocation; use a file to share a database between
migration commands and application processes.

For `migrate-get`, `--database-url` only selects the SQL dialect. Both `sqlite:`
and `sqlite://` work without a path, and export never opens or creates a database.

Ruby also accepts `sqlite3:` and `sqlite3://` with either Active Record or Sequel.
Java also accepts native `jdbc:sqlite:` URLs, passed unchanged to the JDBC driver.
These aliases are specific to those languages. Driver-specific URI options and
query parameters are not portable between implementations; for example, Rust's
`?mode=rwc` creates a missing database file.
