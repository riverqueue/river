#[cfg(feature = "postgres")]
use riverqueue::database::PostgresDatabase;
#[cfg(feature = "sqlite")]
use riverqueue::database::SqliteDatabase;
use riverqueue::database::{Database, DatabaseKind, DatabaseTransactionExecutor, IntoDatabase};
#[cfg(feature = "postgres")]
use riverqueue_internal::SchemaName;
use sqlx::Transaction;
#[cfg(feature = "postgres")]
use sqlx::{
    PgPool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};
#[cfg(feature = "sqlite")]
use sqlx::{
    Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

#[cfg(feature = "postgres")]
fn postgres_pool() -> PgPool {
    PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new())
}

#[cfg(feature = "sqlite")]
fn sqlite_pool() -> SqlitePool {
    SqlitePoolOptions::new().connect_lazy_with(SqliteConnectOptions::new())
}

fn require_executor<'executor, E: DatabaseTransactionExecutor<'executor>>(
    database: &Database,
    executor: E,
) -> DatabaseKind {
    database.executor(executor).unwrap().kind()
}

fn require_source<D: IntoDatabase>(database: D) -> Database {
    Database::from_source(database)
}

#[test]
fn transactions_implement_the_executor_contract() {
    #[cfg(feature = "postgres")]
    fn postgres_transaction(database: &Database, transaction: &mut Transaction<'_, Postgres>) {
        assert_eq!(
            require_executor(database, transaction),
            DatabaseKind::Postgres
        );
    }

    #[cfg(feature = "sqlite")]
    fn sqlite_transaction(database: &Database, transaction: &mut Transaction<'_, Sqlite>) {
        assert_eq!(
            require_executor(database, transaction),
            DatabaseKind::Sqlite
        );
    }

    // These function-pointer assignments compile only while the sealed public
    // contract accepts actual SQLx transactions. No live server is needed.
    #[cfg(feature = "postgres")]
    let _: for<'executor, 'transaction> fn(
        &Database,
        &'executor mut Transaction<'transaction, Postgres>,
    ) = postgres_transaction;
    #[cfg(feature = "sqlite")]
    let _: for<'executor, 'transaction> fn(
        &Database,
        &'executor mut Transaction<'transaction, Sqlite>,
    ) = sqlite_transaction;
}

#[tokio::test]
async fn pool_sources_preserve_backend_options() {
    #[cfg(feature = "postgres")]
    let postgres_pool = postgres_pool();
    #[cfg(feature = "sqlite")]
    let sqlite_pool = sqlite_pool();
    #[cfg(feature = "postgres")]
    let schema = SchemaName::new("river_other").unwrap();

    #[cfg(feature = "postgres")]
    let postgres = require_source(PostgresDatabase::new(postgres_pool.clone()).schema(schema));
    #[cfg(feature = "postgres")]
    assert_eq!(postgres.kind(), DatabaseKind::Postgres);
    #[cfg(feature = "postgres")]
    assert_eq!(
        postgres.postgres_schema().and_then(SchemaName::as_deref),
        Some("river_other")
    );
    #[cfg(feature = "sqlite")]
    let sqlite = require_source(SqliteDatabase::new(sqlite_pool.clone()));
    #[cfg(feature = "sqlite")]
    assert_eq!(sqlite.kind(), DatabaseKind::Sqlite);
    #[cfg(feature = "sqlite")]
    assert_eq!(sqlite.postgres_schema(), None);
    #[cfg(feature = "postgres")]
    assert_eq!(require_source(postgres_pool).kind(), DatabaseKind::Postgres);
    #[cfg(feature = "sqlite")]
    assert_eq!(require_source(sqlite_pool).kind(), DatabaseKind::Sqlite);
}

#[tokio::test]
#[cfg(all(feature = "postgres", feature = "sqlite"))]
async fn rejects_an_executor_from_another_backend() {
    let postgres = require_source(postgres_pool());
    let sqlite_pool = sqlite_pool();

    let Err(error) = postgres.executor(&sqlite_pool) else {
        panic!("SQLite executor should not be accepted by a PostgreSQL client");
    };
    assert_eq!(error.expected(), DatabaseKind::Postgres);
    assert_eq!(error.actual(), DatabaseKind::Sqlite);
    assert_eq!(
        error.to_string(),
        "database executor mismatch: expected PostgreSQL, received SQLite"
    );
}
