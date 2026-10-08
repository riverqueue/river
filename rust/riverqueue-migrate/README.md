# riverqueue-migrate

River's Postgres and SQLite migrations for Rust, identical to the ones River
for Go applies, so either language can migrate a database the other uses. The
`riverqueue` command from the `riverqueue-cli` crate runs the same migrations
from a shell.

Use `PostgresMigrator` for Postgres and `SqliteMigrator` for SQLite:

```rust,no_run
use riverqueue_migrate::{Direction, MigrateOpts, PostgresMigrator, SqliteMigrator};
use sqlx::{PgPool, SqlitePool};

async fn migrate(postgres: PgPool, sqlite: SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
    // Apply every outstanding migration.
    let migrator = PostgresMigrator::new(postgres);
    let applied = migrator.migrate_up().await?;
    println!("applied versions {applied:?}");

    // Or preview what migrating down one version would run.
    let preview = migrator
        .migrate(Direction::Down, MigrateOpts::new().with_dry_run(true))
        .await?;
    for version in preview.versions {
        println!("would revert {:03} {}", version.version, version.name);
    }

    // Check that every migration is applied before starting clients.
    let validation = SqliteMigrator::new(sqlite).validate(None).await?;
    if !validation.is_valid() {
        return Err(validation.to_string().into());
    }
    Ok(())
}
```
