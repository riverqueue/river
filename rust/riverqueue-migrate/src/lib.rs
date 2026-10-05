// The README's example uses both backends, so it's the crate documentation
// (and its example runs as a doctest) when both are enabled, as on docs.rs.
#![cfg_attr(
    all(feature = "postgres", feature = "sqlite"),
    doc = include_str!("../README.md")
)]
#![cfg_attr(
    not(all(feature = "postgres", feature = "sqlite")),
    doc = "River's PostgreSQL and SQLite migrations for Rust. The full crate documentation, \
           from the README, builds with both the `postgres` and `sqlite` features, as on \
           [docs.rs](https://docs.rs/riverqueue-migrate)."
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!("riverqueue-migrate requires at least one database feature: `postgres` or `sqlite`");

use std::time::Duration;
#[cfg(feature = "postgres")]
use std::time::Instant;

#[cfg(feature = "postgres")]
use sqlx::{PgPool, Row};
use thiserror::Error;

mod schema;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use schema::{SCHEMA_MAX_LEN, SchemaName, SchemaNameError};

#[cfg(feature = "sqlite")]
pub use sqlite::{SQLITE_MIGRATIONS, SqliteMigrator};

#[cfg(feature = "postgres")]
const TEMPLATE_SCHEMA: &str = "/* TEMPLATE: schema */";

/// River's main migration line.
pub const MIGRATION_LINE_MAIN: &str = "main";

/// Latest migration version bundled with this release.
pub const MIGRATION_VERSION_LATEST: i64 = 8;

#[cfg(feature = "postgres")]
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
#[non_exhaustive]
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
    dry_run: bool,
    /// Maximum number of steps. Down migrations default to one step.
    max_steps: Option<usize>,
    /// Target schema version. Down excludes the target; `-1` removes River.
    target_version: Option<i64>,
}

impl MigrateOpts {
    /// Creates migration options with no target, step limit, or dry run.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            dry_run: false,
            max_steps: None,
            target_version: None,
        }
    }

    /// Returns whether SQL is reported without being applied.
    #[must_use]
    pub const fn dry_run(&self) -> bool {
        self.dry_run
    }

    /// Returns the maximum number of migration steps.
    #[must_use]
    pub const fn max_steps(&self) -> Option<usize> {
        self.max_steps
    }

    /// Returns the requested target version.
    #[must_use]
    pub const fn target_version(&self) -> Option<i64> {
        self.target_version
    }

    /// Reports selected SQL without applying it.
    #[must_use]
    pub const fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Limits the number of migration steps.
    #[must_use]
    pub const fn with_max_steps(mut self, maximum: usize) -> Self {
        self.max_steps = Some(maximum);
        self
    }

    /// Migrates toward a target schema version. `-1` removes River. An up
    /// migration whose target is already applied does nothing, even if later
    /// versions are pending.
    #[must_use]
    pub const fn with_target_version(mut self, version: i64) -> Self {
        self.target_version = Some(version);
        self
    }
}

/// One migration selected or applied by an operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
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
#[non_exhaustive]
pub struct MigrateResult {
    /// Direction requested.
    pub direction: Direction,
    /// Versions applied or selected.
    pub versions: Vec<MigrateVersion>,
}

/// Result of checking whether required migrations are applied.
///
/// Its `Display` output describes the unapplied versions, for reporting a
/// failed validation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ValidateResult {
    /// Required versions that aren't applied, in ascending order.
    pub unapplied: Vec<i64>,
}

impl ValidateResult {
    /// Returns whether every required migration is applied.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.unapplied.is_empty()
    }
}

impl std::fmt::Display for ValidateResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_valid() {
            formatter.write_str("all required migrations are applied")
        } else {
            write!(formatter, "unapplied migrations: {:?}", self.unapplied)
        }
    }
}

/// Canonical PostgreSQL migration bundle.
#[cfg(feature = "postgres")]
pub const POSTGRES_MIGRATIONS: [Migration; 8] = [
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
    migration!(8, "job_id_autoincrement", "008_job_id_autoincrement"),
];

/// River migration failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A database operation failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Migrating down past `version` would delete the migration records of
    /// other migration lines, which the down migration can't restore.
    #[error(
        "found non-main migration lines; version {version:03} is irreversible without losing migration information"
    )]
    OtherMigrationLines {
        /// The migration that would be reverted.
        version: i64,
    },

    /// A down migration's target isn't among the applied versions it can
    /// revert to within its step limit.
    #[error("version {version} is not in target list of valid migrations to apply")]
    TargetNotSelected {
        /// The requested target version.
        version: i64,
    },

    /// A target version isn't one of River's migrations.
    #[error("version {version} is not a River migration")]
    UnknownVersion {
        /// The requested target version.
        version: i64,
    },
}

/// Applies and validates River's PostgreSQL migration history.
#[cfg(feature = "postgres")]
#[derive(Clone, Debug)]
pub struct PostgresMigrator {
    pool: PgPool,
    schema: SchemaName,
}

#[cfg(feature = "postgres")]
impl PostgresMigrator {
    /// Returns every migration bundled with this crate.
    #[must_use]
    pub fn all_versions() -> &'static [Migration] {
        &POSTGRES_MIGRATIONS
    }

    /// Creates a migrator for PostgreSQL's current schema.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: SchemaName::current(),
        }
    }

    /// Returns the migrator with `schema` migrated instead of the
    /// connection's current schema.
    #[must_use]
    pub fn with_schema(mut self, schema: SchemaName) -> Self {
        self.schema = schema;
        self
    }

    /// Returns the schema this migrator migrates.
    #[must_use]
    pub const fn schema(&self) -> &SchemaName {
        &self.schema
    }

    /// Returns applied main-line versions in ascending order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Database`] when the query fails.
    pub async fn existing_versions(&self) -> Result<Vec<i64>, Error> {
        let table = self.schema.qualify("river_migration");
        // Pass the quoted, qualified name through unchanged like Go's
        // `TableExists`, so a mixed-case schema is not folded to lowercase.
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&table)
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

    /// Applies up or down migrations with target, step, and dry-run controls.
    ///
    /// Each migration runs in its own transaction, so a failure leaves the
    /// migrations before it applied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnknownVersion`] when the target version doesn't exist,
    /// [`Error::TargetNotSelected`] when a down target isn't applied or is
    /// beyond the step limit, and [`Error::Database`] when a migration fails.
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
        validate_target(&POSTGRES_MIGRATIONS, opts.target_version, true)?;
        let applied = self.existing_versions().await?;
        let selected = select_migrations(&POSTGRES_MIGRATIONS, direction, opts, &applied)?;

        let mut versions = Vec::with_capacity(selected.len());
        for migration in selected {
            let sql = self.render(match direction {
                Direction::Down => migration.down_sql,
                Direction::Up => migration.up_sql,
            });
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

    /// Checks that every migration through an optional target is applied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnknownVersion`] when the target version doesn't exist and
    /// [`Error::Database`] when reading the applied versions fails.
    pub async fn validate(&self, target_version: Option<i64>) -> Result<ValidateResult, Error> {
        validate_target(&POSTGRES_MIGRATIONS, target_version, false)?;
        let applied = self.existing_versions().await?;
        Ok(validate_migrations(
            &POSTGRES_MIGRATIONS,
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

/// Runs a migration step on its own task and waits for it, so the step
/// finishes even when the caller stops waiting.
async fn run_to_completion(
    step: impl std::future::Future<Output = Result<(), Error>> + Send + 'static,
) -> Result<(), Error> {
    match tokio::spawn(step).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The runtime is shutting down and cancelled the task.
        Err(error) => Err(Error::Database(sqlx::Error::Io(std::io::Error::other(
            format!("migration task failed: {error}"),
        )))),
    }
}

fn select_migrations(
    migrations: &'static [Migration],
    direction: Direction,
    opts: MigrateOpts,
    applied: &[i64],
) -> Result<Vec<Migration>, Error> {
    // Versions recorded by a newer River release are ignored like Go: up
    // migrations apply only unapplied known versions, and down migrations
    // revert only applied known versions.
    let mut selected = match direction {
        Direction::Up => migrations
            .iter()
            .filter(|migration| !applied.contains(&migration.version))
            .copied()
            .collect::<Vec<_>>(),
        Direction::Down => migrations
            .iter()
            .rev()
            .filter(|migration| applied.contains(&migration.version))
            .copied()
            .collect::<Vec<_>>(),
    };

    // An up migration whose target is already applied does nothing, even
    // when later versions are pending. Check before the step limit trims
    // the list, which can also drop a pending target.
    let target_was_pending = opts
        .target_version
        .is_some_and(|target| selected.iter().any(|migration| migration.version == target));

    // Go limits steps before locating the target, so a target outside the
    // step window is not reached.
    let maximum = opts
        .max_steps
        .or_else(|| (direction == Direction::Down && opts.target_version.is_none()).then_some(1));
    if let Some(maximum) = maximum {
        selected.truncate(maximum);
    }

    if let Some(target) = opts.target_version.filter(|target| *target != -1) {
        match selected
            .iter()
            .position(|migration| migration.version == target)
        {
            Some(index) => {
                selected.truncate(index + 1);
                // A down target is the version that remains applied.
                if direction == Direction::Down {
                    selected.pop();
                }
            }
            None if direction == Direction::Down => {
                return Err(Error::TargetNotSelected { version: target });
            }
            // A pending up target beyond the step limit keeps the trimmed
            // list; an applied one is a no-op.
            None if target_was_pending => {}
            None => selected.clear(),
        }
    }
    Ok(selected)
}

fn validate_migrations(
    migrations: &[Migration],
    target_version: Option<i64>,
    applied: &[i64],
) -> ValidateResult {
    let missing = migrations
        .iter()
        .filter(|migration| target_version.is_none_or(|target| migration.version <= target))
        .filter(|migration| !applied.contains(&migration.version))
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    ValidateResult { unapplied: missing }
}

fn validate_target(
    migrations: &[Migration],
    target_version: Option<i64>,
    allow_empty: bool,
) -> Result<(), Error> {
    if let Some(target) = target_version
        && !(allow_empty && target == -1)
        && !migrations
            .iter()
            .any(|migration| migration.version == target)
    {
        return Err(Error::UnknownVersion { version: target });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Direction, Error, MigrateOpts, Migration, select_migrations};

    const MIGRATIONS: [Migration; 7] = {
        const fn migration(version: i64) -> Migration {
            Migration {
                down_sql: "",
                name: "test",
                up_sql: "",
                version,
            }
        }
        [
            migration(1),
            migration(2),
            migration(3),
            migration(4),
            migration(5),
            migration(6),
            migration(7),
        ]
    };

    fn versions(
        direction: Direction,
        opts: MigrateOpts,
        applied: &[i64],
    ) -> Result<Vec<i64>, Error> {
        select_migrations(&MIGRATIONS, direction, opts, applied).map(|selected| {
            selected
                .into_iter()
                .map(|migration| migration.version)
                .collect()
        })
    }

    #[test]
    fn applied_up_targets_are_no_ops() {
        // Like Go, even with a gap below the target or later versions
        // pending, with or without a step limit.
        assert_eq!(
            versions(
                Direction::Up,
                MigrateOpts::new().with_target_version(3),
                &[1, 3]
            )
            .unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            versions(
                Direction::Up,
                MigrateOpts::new().with_target_version(3).with_max_steps(1),
                &[1, 2, 3]
            )
            .unwrap(),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn selection_matches_go_target_and_step_semantics() {
        let all = [1, 2, 3, 4, 5, 6, 7];

        // Down defaults to one step, and a target is the version left applied.
        assert_eq!(
            versions(Direction::Down, MigrateOpts::new(), &all).unwrap(),
            [7]
        );
        assert_eq!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(4),
                &all
            )
            .unwrap(),
            [7, 6, 5]
        );
        assert_eq!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(4),
                &[1, 2, 3, 4]
            )
            .unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(-1),
                &all
            )
            .unwrap(),
            [7, 6, 5, 4, 3, 2, 1]
        );

        // Steps limit the list before the target is located, like Go.
        assert_eq!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(5).with_max_steps(3),
                &all
            )
            .unwrap(),
            [7, 6]
        );
        assert!(matches!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(5).with_max_steps(2),
                &all
            ),
            Err(Error::TargetNotSelected { version: 5 })
        ));

        // A down target that is not applied is an error rather than a no-op.
        assert!(matches!(
            versions(
                Direction::Down,
                MigrateOpts::new().with_target_version(5),
                &[1, 2, 3]
            ),
            Err(Error::TargetNotSelected { version: 5 })
        ));

        // Up targets stop at the target and are no-ops once applied.
        assert_eq!(
            versions(
                Direction::Up,
                MigrateOpts::new().with_target_version(5),
                &[1, 2]
            )
            .unwrap(),
            [3, 4, 5]
        );
        assert_eq!(
            versions(
                Direction::Up,
                MigrateOpts::new().with_target_version(2),
                &[1, 2, 3]
            )
            .unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            versions(
                Direction::Up,
                MigrateOpts::new().with_target_version(6).with_max_steps(2),
                &[1]
            )
            .unwrap(),
            [2, 3]
        );

        // Versions recorded by a newer release are ignored.
        assert_eq!(
            versions(Direction::Up, MigrateOpts::new(), &[1, 2, 3, 99]).unwrap(),
            [4, 5, 6, 7]
        );
        assert_eq!(
            versions(
                Direction::Down,
                MigrateOpts::new(),
                &[1, 2, 3, 4, 5, 6, 7, 8]
            )
            .unwrap(),
            [7]
        );
    }
}
