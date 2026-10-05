use std::time::{Duration, Instant};

use sqlx::{Row, SqlitePool};

use crate::{
    Direction, Error, MIGRATION_LINE_MAIN, MigrateOpts, MigrateResult, MigrateVersion, Migration,
    ValidateResult, run_to_completion, select_migrations, validate_migrations, validate_target,
};

macro_rules! sqlite_migration {
    ($version:literal, $name:literal, $file:literal) => {
        Migration {
            down_sql: include_str!(concat!("../migrations/sqlite/main/", $file, ".down.sql")),
            name: $name,
            up_sql: include_str!(concat!("../migrations/sqlite/main/", $file, ".up.sql")),
            version: $version,
        }
    };
}

/// Canonical SQLite migration bundle.
pub const SQLITE_MIGRATIONS: [Migration; 8] = [
    sqlite_migration!(1, "create_river_migration", "001_create_river_migration"),
    sqlite_migration!(2, "initial_schema", "002_initial_schema"),
    sqlite_migration!(3, "river_job_tags_non_null", "003_river_job_tags_non_null"),
    sqlite_migration!(4, "pending_and_more", "004_pending_and_more"),
    sqlite_migration!(5, "migration_unique_client", "005_migration_unique_client"),
    sqlite_migration!(6, "bulk_unique", "006_bulk_unique"),
    sqlite_migration!(
        7,
        "notification_outbox_sqlite_jsonb_and_sql_cleanup",
        "007_notification_outbox_sqlite_jsonb_and_sql_cleanup"
    ),
    sqlite_migration!(8, "job_id_autoincrement", "008_job_id_autoincrement"),
];

/// Applies and validates River's SQLite migration history.
#[derive(Clone, Debug)]
pub struct SqliteMigrator {
    pool: SqlitePool,
}

impl SqliteMigrator {
    /// Creates a migrator for a SQLite pool.
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Returns every SQLite migration bundled with this crate.
    #[must_use]
    pub fn all_versions() -> &'static [Migration] {
        &SQLITE_MIGRATIONS
    }

    /// Returns applied main-line versions in ascending order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Database`] when the query fails.
    pub async fn existing_versions(&self) -> Result<Vec<i64>, Error> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'river_migration')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(Error::Database)?;
        if !exists {
            return Ok(Vec::new());
        }

        let has_line: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pragma_table_info('river_migration') WHERE name = 'line')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(Error::Database)?;
        let rows = if has_line {
            sqlx::query("SELECT version FROM river_migration WHERE line = ?1 ORDER BY version")
                .bind(MIGRATION_LINE_MAIN)
                .fetch_all(&self.pool)
                .await
                .map_err(Error::Database)?
        } else {
            sqlx::query("SELECT version FROM river_migration ORDER BY version")
                .fetch_all(&self.pool)
                .await
                .map_err(Error::Database)?
        };
        Ok(rows.iter().map(|row| row.get("version")).collect())
    }

    /// Applies up or down migrations with target, step, and dry-run controls.
    ///
    /// Each migration runs in its own transaction, so a failure leaves the
    /// migrations before it applied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnknownVersion`] when the target version doesn't exist,
    /// [`Error::TargetNotSelected`] when a down target isn't applied or is
    /// beyond the step limit, [`Error::OtherMigrationLines`] when reverting
    /// version 5 would lose other migration lines' records, and [`Error::Database`] when a migration fails.
    ///
    /// # Cancel safety
    ///
    /// Each migration and its record in `river_migration` commit together in
    /// their own transaction, on a task of their own. Dropping the future
    /// stops migrating once the migration in progress finishes: it and
    /// every migration before it stay applied, and migrating again
    /// continues from there.
    pub async fn migrate(
        &self,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, Error> {
        validate_target(&SQLITE_MIGRATIONS, opts.target_version, true)?;
        let applied = self.existing_versions().await?;
        let selected = select_migrations(&SQLITE_MIGRATIONS, direction, opts, &applied)?;

        let mut versions = Vec::with_capacity(selected.len());
        for migration in selected {
            let sql = migration_sql(direction, migration).to_owned();
            let mut duration = Duration::ZERO;
            if !opts.dry_run {
                let started_at = Instant::now();
                // Each migration runs to completion on its own task, so
                // dropping this future never abandons one partway.
                let migrator = self.clone();
                let task_sql = sql.clone();
                run_to_completion(
                    async move { migrator.apply(direction, migration, &task_sql).await },
                )
                .await?;
                duration = started_at.elapsed();
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

    /// Applies all outstanding up migrations and returns their versions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Database`] when a migration fails.
    ///
    /// # Cancel safety
    ///
    /// Each migration and its record in `river_migration` commit together in
    /// their own transaction, on a task of their own. Dropping the future
    /// stops migrating once the migration in progress finishes: it and
    /// every migration before it stay applied, and migrating again
    /// continues from there.
    pub async fn migrate_up(&self) -> Result<Vec<i64>, Error> {
        Ok(self
            .migrate(Direction::Up, MigrateOpts::default())
            .await?
            .versions
            .into_iter()
            .map(|version| version.version)
            .collect())
    }

    /// Checks that every migration through an optional target is applied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnknownVersion`] when the target version doesn't exist and
    /// [`Error::Database`] when reading the applied versions fails.
    pub async fn validate(&self, target_version: Option<i64>) -> Result<ValidateResult, Error> {
        validate_target(&SQLITE_MIGRATIONS, target_version, false)?;
        let applied = self.existing_versions().await?;
        Ok(validate_migrations(
            &SQLITE_MIGRATIONS,
            target_version,
            &applied,
        ))
    }

    async fn apply(
        &self,
        direction: Direction,
        migration: Migration,
        sql: &str,
    ) -> Result<(), Error> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(Error::Database)?;
        if direction == Direction::Down && migration.version == 5 {
            let has_other_lines: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM river_migration WHERE line <> ?1)",
            )
            .bind(MIGRATION_LINE_MAIN)
            .fetch_one(&mut *transaction)
            .await
            .map_err(Error::Database)?;
            if has_other_lines {
                return Err(Error::OtherMigrationLines {
                    version: migration.version,
                });
            }
        }

        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await
            .map_err(Error::Database)?;
        match direction {
            Direction::Down if migration.version == 1 => {}
            Direction::Down if migration.version <= 5 => {
                sqlx::query("DELETE FROM river_migration WHERE version = ?1")
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Database)?;
            }
            Direction::Down => {
                sqlx::query("DELETE FROM river_migration WHERE line = ?1 AND version = ?2")
                    .bind(MIGRATION_LINE_MAIN)
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Database)?;
            }
            Direction::Up if migration.version >= 5 => {
                sqlx::query("INSERT INTO river_migration (line, version) VALUES (?1, ?2)")
                    .bind(MIGRATION_LINE_MAIN)
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Database)?;
            }
            Direction::Up => {
                sqlx::query("INSERT INTO river_migration (version) VALUES (?1)")
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Database)?;
            }
        }
        transaction.commit().await.map_err(Error::Database)?;
        Ok(())
    }
}

const fn migration_sql(direction: Direction, migration: Migration) -> &'static str {
    match direction {
        Direction::Down => migration.down_sql,
        Direction::Up => migration.up_sql,
    }
}
