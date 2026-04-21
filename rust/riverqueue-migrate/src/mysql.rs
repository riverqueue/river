use std::time::{Duration, Instant};

use sqlx::{MySqlConnection, MySqlPool};

use crate::{
    Direction, Error, MIGRATION_LINE_MAIN, MigrateOpts, MigrateResult, MigrateVersion, Migration,
    ValidateResult, select_migrations, validate_migrations, validate_target,
};

macro_rules! mysql_migration {
    ($version:literal, $name:literal, $file:literal) => {
        Migration {
            down_sql: include_str!(concat!("../migrations/mysql/main/", $file, ".down.sql")),
            name: $name,
            up_sql: include_str!(concat!("../migrations/mysql/main/", $file, ".up.sql")),
            version: $version,
        }
    };
}

/// Canonical MySQL migration bundle, shared with River's other clients.
pub const MYSQL_MIGRATIONS: [Migration; 8] = [
    mysql_migration!(1, "create_river_migration", "001_create_river_migration"),
    mysql_migration!(2, "initial_schema", "002_initial_schema"),
    mysql_migration!(3, "river_job_tags_non_null", "003_river_job_tags_non_null"),
    mysql_migration!(4, "pending_and_more", "004_pending_and_more"),
    mysql_migration!(5, "migration_unique_client", "005_migration_unique_client"),
    mysql_migration!(6, "bulk_unique", "006_bulk_unique"),
    mysql_migration!(
        7,
        "notification_outbox_sqlite_jsonb_and_sql_cleanup",
        "007_notification_outbox_sqlite_jsonb_and_sql_cleanup"
    ),
    mysql_migration!(8, "job_id_autoincrement", "008_job_id_autoincrement"),
];

/// Applies migrations to the MySQL database selected by the pool.
///
/// The database must already exist. MySQL implicitly commits DDL: successful
/// statements remain applied after an error. A named lock serializes migrators,
/// and a version is recorded only after all its statements succeed.
#[derive(Clone, Debug)]
pub struct MySqlMigrator {
    pool: MySqlPool,
}

impl MySqlMigrator {
    /// Creates a migrator for the pool's database.
    #[must_use]
    pub const fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Returns every bundled MySQL migration.
    #[must_use]
    pub fn all_versions() -> &'static [Migration] {
        &MYSQL_MIGRATIONS
    }

    /// Returns applied main-line versions in ascending order.
    ///
    /// # Errors
    /// Returns the database error if the history cannot be read.
    pub async fn existing_versions(&self) -> Result<Vec<i64>, Error> {
        existing_versions(&mut *self.pool.acquire().await?).await
    }

    /// Applies migrations with target, step, and dry-run controls.
    ///
    /// # Errors
    /// Returns invalid-target errors or database errors, including failure to
    /// acquire the migration lock within ten seconds. Other migration lines
    /// prevent reverting version 5.
    ///
    /// # Cancel safety
    /// Migration runs on its own task. Dropping this future lets the selected
    /// migrations finish and releases the named lock before returning the
    /// connection to its pool.
    ///
    /// # Panics
    /// Resumes a panic from the migration task.
    pub async fn migrate(
        &self,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, Error> {
        validate_target(&MYSQL_MIGRATIONS, opts.target_version, true)?;
        let migrator = self.clone();
        match tokio::spawn(async move { migrator.migrate_locked(direction, opts).await }).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => Err(sqlx::Error::Io(std::io::Error::other(format!(
                "migration task failed: {error}"
            )))
            .into()),
        }
    }

    /// Applies all outstanding migrations and returns their versions.
    ///
    /// # Errors
    /// Returns the database error if a migration fails.
    pub async fn migrate_up(&self) -> Result<Vec<i64>, Error> {
        Ok(self
            .migrate(Direction::Up, MigrateOpts::default())
            .await?
            .versions
            .into_iter()
            .map(|version| version.version)
            .collect())
    }

    /// Checks that every migration through the optional target is applied.
    ///
    /// # Errors
    /// Returns invalid-target errors or errors reading migration history.
    pub async fn validate(&self, target_version: Option<i64>) -> Result<ValidateResult, Error> {
        validate_target(&MYSQL_MIGRATIONS, target_version, false)?;
        let applied = self.existing_versions().await?;
        Ok(validate_migrations(
            &MYSQL_MIGRATIONS,
            target_version,
            &applied,
        ))
    }

    async fn migrate_locked(
        &self,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, Error> {
        let mut connection = self.pool.acquire().await?;
        let acquired: Option<i64> =
            sqlx::query_scalar("SELECT GET_LOCK(CONCAT('river_migrate:', DATABASE()), 10)")
                .fetch_one(&mut *connection)
                .await?;
        if acquired != Some(1) {
            return Err(sqlx::Error::Protocol(
                "could not acquire MySQL migration lock; select a database in the connection URL"
                    .into(),
            )
            .into());
        }
        // A task abort or panic closes the connection, releasing its named lock.
        connection.close_on_drop();
        let result = migrate_on(&mut connection, direction, opts).await;
        let released = sqlx::query("SELECT RELEASE_LOCK(CONCAT('river_migrate:', DATABASE()))")
            .execute(&mut *connection)
            .await;
        match result {
            Ok(result) => {
                released?;
                Ok(result)
            }
            Err(error) => Err(error),
        }
    }
}

async fn existing_versions(connection: &mut MySqlConnection) -> Result<Vec<i64>, Error> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'river_migration')").fetch_one(&mut *connection).await?;
    if !exists {
        return Ok(Vec::new());
    }
    let has_line: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'river_migration' AND column_name = 'line')").fetch_one(&mut *connection).await?;
    Ok(if has_line {
        sqlx::query_scalar(
            "SELECT CAST(version AS SIGNED) FROM river_migration WHERE line = ? ORDER BY version",
        )
        .bind(MIGRATION_LINE_MAIN)
        .fetch_all(connection)
        .await?
    } else {
        sqlx::query_scalar("SELECT CAST(version AS SIGNED) FROM river_migration ORDER BY version")
            .fetch_all(connection)
            .await?
    })
}

async fn migrate_on(
    connection: &mut MySqlConnection,
    direction: Direction,
    opts: MigrateOpts,
) -> Result<MigrateResult, Error> {
    let applied = existing_versions(connection).await?;
    let selected = select_migrations(&MYSQL_MIGRATIONS, direction, opts, &applied)?;
    let mut versions = Vec::with_capacity(selected.len());
    for migration in selected {
        let sql = match direction {
            Direction::Down => migration.down_sql,
            Direction::Up => migration.up_sql,
        }
        .replace("/* TEMPLATE: schema */", "");
        let mut duration = Duration::ZERO;
        if !opts.dry_run {
            let start = Instant::now();
            apply(connection, direction, migration, &sql).await?;
            duration = start.elapsed();
        }
        versions.push(MigrateVersion {
            duration,
            name: migration.name,
            sql,
            version: migration.version,
        });
    }
    Ok(MigrateResult {
        direction,
        versions,
    })
}

async fn apply(
    connection: &mut MySqlConnection,
    direction: Direction,
    migration: Migration,
    sql: &str,
) -> Result<(), Error> {
    if direction == Direction::Down && migration.version == 5 {
        let other_lines: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM river_migration WHERE line <> ?)")
                .bind(MIGRATION_LINE_MAIN)
                .fetch_one(&mut *connection)
                .await?;
        if other_lines {
            return Err(Error::OtherMigrationLines {
                version: migration.version,
            });
        }
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut *connection)
        .await?;
    match direction {
        Direction::Down if migration.version == 1 => {}
        Direction::Down if migration.version <= 5 => {
            sqlx::query("DELETE FROM river_migration WHERE version = ?")
                .bind(migration.version)
                .execute(&mut *connection)
                .await?;
        }
        Direction::Down => {
            sqlx::query("DELETE FROM river_migration WHERE line = ? AND version = ?")
                .bind(MIGRATION_LINE_MAIN)
                .bind(migration.version)
                .execute(&mut *connection)
                .await?;
        }
        Direction::Up if migration.version >= 5 => {
            sqlx::query("INSERT INTO river_migration (line, version) VALUES (?, ?)")
                .bind(MIGRATION_LINE_MAIN)
                .bind(migration.version)
                .execute(&mut *connection)
                .await?;
        }
        Direction::Up => {
            sqlx::query("INSERT INTO river_migration (version) VALUES (?)")
                .bind(migration.version)
                .execute(&mut *connection)
                .await?;
        }
    }
    Ok(())
}
