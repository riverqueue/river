use std::time::{Duration, Instant};

use sqlx::{Row, SqlitePool};

use crate::{
    Direction, Error, MIGRATION_LINE_MAIN, MigrateOpts, MigrateResult, MigrateVersion, Migration,
    ValidateResult, select_migrations, validate_migrations, validate_target,
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
pub const SQLITE_MIGRATIONS: [Migration; 7] = [
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
    pub fn all_versions(&self) -> &'static [Migration] {
        &SQLITE_MIGRATIONS
    }

    /// Returns applied main-line versions in ascending order.
    pub async fn existing_versions(&self) -> Result<Vec<i64>, Error> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'river_migration')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(Error::Sqlite)?;
        if !exists {
            return Ok(Vec::new());
        }

        let has_line: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pragma_table_info('river_migration') WHERE name = 'line')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(Error::Sqlite)?;
        let rows = if has_line {
            sqlx::query("SELECT version FROM river_migration WHERE line = ?1 ORDER BY version")
                .bind(MIGRATION_LINE_MAIN)
                .fetch_all(&self.pool)
                .await
                .map_err(Error::Sqlite)?
        } else {
            sqlx::query("SELECT version FROM river_migration ORDER BY version")
                .fetch_all(&self.pool)
                .await
                .map_err(Error::Sqlite)?
        };
        Ok(rows.iter().map(|row| row.get("version")).collect())
    }

    /// Applies up or down migrations with target, step, and dry-run controls.
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
                self.apply(direction, migration, &sql).await?;
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

    /// Applies all outstanding up migrations.
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
            .map_err(Error::Sqlite)?;
        if direction == Direction::Down && migration.version == 5 {
            let has_other_lines: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM river_migration WHERE line <> ?1)",
            )
            .bind(MIGRATION_LINE_MAIN)
            .fetch_one(&mut *transaction)
            .await
            .map_err(Error::Sqlite)?;
            if has_other_lines {
                return Err(Error::Invalid(
                    "found non-main migration lines; version 005 is irreversible without losing migration information"
                        .to_owned(),
                ));
            }
        }

        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await
            .map_err(Error::Sqlite)?;
        match direction {
            Direction::Down if migration.version == 1 => {}
            Direction::Down if migration.version <= 5 => {
                sqlx::query("DELETE FROM river_migration WHERE version = ?1")
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Sqlite)?;
            }
            Direction::Down => {
                sqlx::query("DELETE FROM river_migration WHERE line = ?1 AND version = ?2")
                    .bind(MIGRATION_LINE_MAIN)
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Sqlite)?;
            }
            Direction::Up if migration.version >= 5 => {
                sqlx::query("INSERT INTO river_migration (line, version) VALUES (?1, ?2)")
                    .bind(MIGRATION_LINE_MAIN)
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Sqlite)?;
            }
            Direction::Up => {
                sqlx::query("INSERT INTO river_migration (version) VALUES (?1)")
                    .bind(migration.version)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Error::Sqlite)?;
            }
        }
        transaction.commit().await.map_err(Error::Sqlite)?;
        Ok(())
    }
}

const fn migration_sql(direction: Direction, migration: Migration) -> &'static str {
    match direction {
        Direction::Down => migration.down_sql,
        Direction::Up => migration.up_sql,
    }
}
