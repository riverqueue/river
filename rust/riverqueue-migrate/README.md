# riverqueue-migrate

River's Postgres, MySQL, and SQLite migrations for Rust, identical to the ones
other River clients apply, so a client in any language can migrate a database
the others use. The `riverqueue` command from the `riverqueue-cli` crate runs
the same migrations from a shell.

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

Enable the `mysql` feature to use `MySqlMigrator::new(pool)` with a SQLx
`MySqlPool`. The database named in the pool's URL must already exist. MySQL
uses the same eight canonical migrations as River for Go. A named lock
serializes migration runs. Because MySQL commits DDL implicitly, successful
statements remain applied after an error; only a fully successful migration
is added to the migration history.
