//! Applies River's canonical PostgreSQL migration history.

#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use riverqueue_internal::SchemaName;
use sqlx::{PgPool, Row};
use thiserror::Error;

const TEMPLATE_SCHEMA: &str = "/* TEMPLATE: schema */";

/// River's main migration line.
pub const MIGRATION_LINE_MAIN: &str = "main";

/// Latest migration version bundled with this release.
pub const MIGRATION_VERSION_LATEST: i64 = 7;

macro_rules! migration {
    ($version:literal, $name:literal, $file:literal) => {
        Migration {
            down_sql: include_str!(concat!("../migrations/main/", $file, ".down.sql")),
            name: $name,
            up_sql: include_str!(concat!("../migrations/main/", $file, ".up.sql")),
            version: $version,
        }
    };
}

/// One canonical River migration.
#[derive(Clone, Copy, Debug)]
pub struct Migration {
    /// Down migration SQL.
    pub down_sql: &'static str,
    /// Human-readable migration name.
    pub name: &'static str,
    /// Up migration SQL.
    pub up_sql: &'static str,
    /// Monotonically increasing version.
    pub version: i64,
}

/// Migration direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    /// Revert applied migrations.
    Down,
    /// Apply outstanding migrations.
    Up,
}

/// Controls a migration operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct MigrateOpts {
    /// Report SQL without applying it.
    pub dry_run: bool,
    /// Maximum number of steps. Down migrations default to one step.
    pub max_steps: Option<usize>,
    /// Target schema version. Down excludes the target; `-1` removes River.
    pub target_version: Option<i64>,
}

/// One migration selected or applied by an operation.
#[derive(Clone, Debug)]
pub struct MigrateVersion {
    /// Database execution time, or zero for a dry run.
    pub duration: Duration,
    /// Human-readable migration name.
    pub name: &'static str,
    /// Rendered SQL.
    pub sql: String,
    /// Migration version.
    pub version: i64,
}

/// Result of a migration operation.
#[derive(Clone, Debug)]
pub struct MigrateResult {
    /// Direction requested.
    pub direction: Direction,
    /// Versions applied or selected.
    pub versions: Vec<MigrateVersion>,
}

/// Result of checking whether required migrations are applied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidateResult {
    /// Human-readable validation failures.
    pub messages: Vec<String>,
    /// Whether all required migrations are applied.
    pub ok: bool,
}

/// Canonical PostgreSQL migration bundle.
pub const MIGRATIONS: [Migration; 7] = [
    migration!(1, "create_river_migration", "001_create_river_migration"),
    migration!(2, "initial_schema", "002_initial_schema"),
    migration!(3, "river_job_tags_non_null", "003_river_job_tags_non_null"),
    migration!(4, "pending_and_more", "004_pending_and_more"),
    migration!(5, "migration_unique_client", "005_migration_unique_client"),
    migration!(6, "bulk_unique", "006_bulk_unique"),
    migration!(
        7,
        "notification_outbox_sqlite_jsonb_and_sql_cleanup",
        "007_notification_outbox_sqlite_jsonb_and_sql_cleanup"
    ),
];

/// River migration failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Migration options are inconsistent or refer to an unknown version.
    #[error("invalid migration request: {0}")]
    Invalid(String),

    /// Database operation failed.
    #[error("PostgreSQL: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Applies and validates River's migration history.
#[derive(Clone, Debug)]
pub struct Migrator {
    pool: PgPool,
    schema: SchemaName,
}

impl Migrator {
    /// Returns every migration bundled with this crate.
    #[must_use]
    pub fn all_versions(&self) -> &'static [Migration] {
        &MIGRATIONS
    }

    /// Creates a migrator for PostgreSQL's current schema.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: SchemaName::current(),
        }
    }

    /// Uses an explicitly validated schema.
    #[must_use]
    pub fn with_schema(mut self, schema: SchemaName) -> Self {
        self.schema = schema;
        self
    }

    /// Returns applied main-line versions in ascending order.
    pub async fn existing_versions(&self) -> Result<Vec<i64>, Error> {
        let table = self.schema.qualify("river_migration");
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table.replace('"', ""))
            .fetch_one(&self.pool)
            .await?;
        if !exists {
            return Ok(Vec::new());
        }

        let has_line: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = coalesce($1, current_schema()) AND table_name = 'river_migration' AND column_name = 'line')",
        )
        .bind(self.schema.as_deref())
        .fetch_one(&self.pool)
        .await?;
        let sql = if has_line {
            format!("SELECT version FROM {table} WHERE line = $1 ORDER BY version")
        } else {
            format!("SELECT version FROM {table} ORDER BY version")
        };
        let rows = if has_line {
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(MIGRATION_LINE_MAIN)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .fetch_all(&self.pool)
                .await?
        };
        Ok(rows.iter().map(|row| row.get("version")).collect())
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

    /// Applies up or down migrations with target, step, and dry-run controls.
    pub async fn migrate(
        &self,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, Error> {
        if let Some(target) = opts.target_version
            && target != -1
            && !MIGRATIONS
                .iter()
                .any(|migration| migration.version == target)
        {
            return Err(Error::Invalid(format!(
                "version {target} is not a River migration"
            )));
        }

        let applied = self.existing_versions().await?;
        for version in &applied {
            if !MIGRATIONS
                .iter()
                .any(|migration| migration.version == *version)
            {
                return Err(Error::Invalid(format!(
                    "database contains unknown River migration {version}"
                )));
            }
        }

        let mut selected = match direction {
            Direction::Up => MIGRATIONS
                .iter()
                .filter(|migration| !applied.contains(&migration.version))
                .filter(|migration| {
                    opts.target_version
                        .is_none_or(|target| target == -1 || migration.version <= target)
                })
                .copied()
                .collect::<Vec<_>>(),
            Direction::Down => MIGRATIONS
                .iter()
                .rev()
                .filter(|migration| applied.contains(&migration.version))
                .filter(|migration| {
                    opts.target_version
                        .is_none_or(|target| target == -1 || migration.version > target)
                })
                .copied()
                .collect::<Vec<_>>(),
        };
        let maximum = opts.max_steps.or_else(|| {
            (direction == Direction::Down && opts.target_version.is_none()).then_some(1)
        });
        if let Some(maximum) = maximum {
            selected.truncate(maximum);
        }

        let mut versions = Vec::with_capacity(selected.len());
        for migration in selected {
            let sql = self.render(match direction {
                Direction::Down => migration.down_sql,
                Direction::Up => migration.up_sql,
            });
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

    /// Checks that every migration through an optional target is applied.
    pub async fn validate(&self, target_version: Option<i64>) -> Result<ValidateResult, Error> {
        if let Some(target) = target_version
            && !MIGRATIONS
                .iter()
                .any(|migration| migration.version == target)
        {
            return Err(Error::Invalid(format!(
                "version {target} is not a River migration"
            )));
        }
        let applied = self.existing_versions().await?;
        let missing = MIGRATIONS
            .iter()
            .filter(|migration| target_version.is_none_or(|target| migration.version <= target))
            .filter(|migration| !applied.contains(&migration.version))
            .map(|migration| migration.version)
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(ValidateResult {
                messages: Vec::new(),
                ok: true,
            })
        } else {
            Ok(ValidateResult {
                messages: vec![format!("unapplied migrations: {missing:?}")],
                ok: false,
            })
        }
    }

    async fn apply(
        &self,
        direction: Direction,
        migration: Migration,
        sql: &str,
    ) -> Result<(), Error> {
        let mut transaction = self.pool.begin().await?;
        // The only dynamic fragment is a validated and quoted schema name.
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await?;
        let table = self.schema.qualify("river_migration");
        match direction {
            Direction::Down if migration.version == 1 => {}
            Direction::Down if migration.version <= 5 => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE version = $1"
                )))
                .bind(migration.version)
                .execute(&mut *transaction)
                .await?;
            }
            Direction::Down => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE line = $1 AND version = $2"
                )))
                .bind(MIGRATION_LINE_MAIN)
                .bind(migration.version)
                .execute(&mut *transaction)
                .await?;
            }
            Direction::Up if migration.version >= 5 => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} (line, version) VALUES ($1, $2)"
                )))
                .bind(MIGRATION_LINE_MAIN)
                .bind(migration.version)
                .execute(&mut *transaction)
                .await?;
            }
            Direction::Up => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} (version) VALUES ($1)"
                )))
                .bind(migration.version)
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    fn render(&self, sql: &str) -> String {
        sql.replace(TEMPLATE_SCHEMA, &self.schema.migration_prefix())
    }
}
