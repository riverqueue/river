//! Shared fixtures for maintenance, leadership, and storage parity tests.
//!
//! PostgreSQL tests run in a freshly migrated schema with a unique name so that
//! concurrent test binaries sharing one disposable database never clobber each
//! other. They fail rather than skip when `RIVER_RUST_DATABASE_URL` is unset.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "postgres")]
use riverqueue::internal::SchemaName;
#[cfg(feature = "postgres")]
use riverqueue_migrate::PostgresMigrator;
#[cfg(feature = "sqlite")]
use riverqueue_migrate::SqliteMigrator;
#[cfg(feature = "postgres")]
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
#[cfg(feature = "sqlite")]
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

static NONCE: AtomicUsize = AtomicUsize::new(0);

/// Returns a process-unique suffix for schema and file names.
pub fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .subsec_nanos();
    format!(
        "{:x}_{:x}_{:x}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed),
        nanos
    )
}

/// A migrated PostgreSQL schema owned by one test.
#[cfg(feature = "postgres")]
pub struct PostgresSchema {
    pub pool: PgPool,
    pub schema: SchemaName,
    name: String,
}

#[cfg(feature = "postgres")]
impl PostgresSchema {
    /// Creates and migrates a uniquely named schema.
    ///
    /// # Panics
    ///
    /// Panics when `RIVER_RUST_DATABASE_URL` is unset so an explicitly
    /// selected database test can never pass vacuously.
    pub async fn new(prefix: &str) -> Self {
        let url = std::env::var("RIVER_RUST_DATABASE_URL")
            .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&url)
            .await
            .expect("connect to RIVER_RUST_DATABASE_URL");
        let mut name = format!("{prefix}_{}", unique_suffix());
        name.truncate(riverqueue::internal::SCHEMA_MAX_LEN);
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .expect("create test schema");
        let schema = SchemaName::new(name.clone()).expect("valid test schema name");
        PostgresMigrator::new(pool.clone())
            .with_schema(schema.clone())
            .migrate_up()
            .await
            .expect("migrate test schema");
        Self { pool, schema, name }
    }

    /// Qualifies a River table in this schema.
    pub fn table(&self, table: &str) -> String {
        self.schema.qualify(table)
    }

    /// Drops the schema and closes the pool.
    pub async fn cleanup(self) {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .expect("drop test schema");
        self.pool.close().await;
    }
}

/// Opens a migrated SQLite database in a unique temporary file.
#[cfg(feature = "sqlite")]
pub async fn sqlite_file_pool(max_connections: u32) -> (SqlitePool, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("river-maint-{}.sqlite", unique_suffix()));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .expect("open SQLite test database");
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .expect("migrate SQLite test database");
    (pool, path)
}

/// Closes a SQLite pool and removes its database files.
#[cfg(feature = "sqlite")]
pub async fn sqlite_cleanup(pool: SqlitePool, path: std::path::PathBuf) {
    pool.close().await;
    let _ = std::fs::remove_file(&path);
    for suffix in ["-shm", "-wal"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let _ = std::fs::remove_file(sidecar);
    }
}
