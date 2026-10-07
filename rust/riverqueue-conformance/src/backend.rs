//! What differs between the databases: connecting, transactions, the schema
//! a client uses, and migrations. Everything else goes through River's
//! database-independent `Client`.

use std::{str::FromStr, time::Duration};

use riverqueue::{
    BoxError, Client, ClientBuilder,
    database::{
        DatabaseTransactionExecutor, PostgresDatabase, SchemaName, SqliteDatabase, begin_postgres,
        begin_sqlite_write,
    },
    migrate::{Direction, MigrateOpts, MigrateResult, PostgresMigrator, SqliteMigrator},
    sqlx::{
        self, PgPool, SqlitePool, Transaction,
        postgres::{PgConnectOptions, PgPoolOptions},
        sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    },
};

use crate::protocol::RpcError;

/// A database River supports, which the server is generic over.
pub trait Backend: Sized {
    type Db: sqlx::Database;

    /// The contract's name for the driver.
    const DRIVER: &'static str;

    /// Opens a pool on `url`, whose connections use `application_name` where
    /// the database has one.
    fn connect(url: &str, application_name: &str) -> Result<Self, BoxError>;

    /// Starts building a client on `schema`, or on the default for an empty
    /// one.
    fn builder(&self, schema: &str) -> Result<ClientBuilder, RpcError>;

    /// Begins a transaction for River's `.tx` requests.
    async fn begin(&self) -> Result<Transaction<'static, Self::Db>, sqlx::Error>;

    /// Passes a transaction to River's `.tx` requests.
    fn executor<'t>(
        transaction: &'t mut Transaction<'static, Self::Db>,
    ) -> impl DatabaseTransactionExecutor<'t>;

    /// Migrates `schema`.
    async fn migrate(
        &self,
        schema: &str,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, RpcError>;
}

#[derive(Debug)]
pub struct Postgres(PgPool);

impl Postgres {
    fn schema(schema: &str) -> Result<Option<SchemaName>, RpcError> {
        (!schema.is_empty())
            .then(|| SchemaName::new(schema).map_err(RpcError::rejected))
            .transpose()
    }
}

impl Backend for Postgres {
    type Db = sqlx::Postgres;

    const DRIVER: &'static str = "postgres";

    fn connect(url: &str, application_name: &str) -> Result<Self, BoxError> {
        // The URL's `options` set the search path, which River uses when no
        // schema is given.
        let options = PgConnectOptions::from_str(url)?.application_name(application_name);
        Ok(Self(
            PgPoolOptions::new()
                .max_connections(10)
                .connect_lazy_with(options),
        ))
    }

    fn builder(&self, schema: &str) -> Result<ClientBuilder, RpcError> {
        let mut database = PostgresDatabase::new(self.0.clone());
        if let Some(schema) = Self::schema(schema)? {
            database = database.with_schema(schema);
        }
        Ok(Client::builder(database))
    }

    async fn begin(&self) -> Result<Transaction<'static, Self::Db>, sqlx::Error> {
        begin_postgres(&self.0).await
    }

    fn executor<'t>(
        transaction: &'t mut Transaction<'static, Self::Db>,
    ) -> impl DatabaseTransactionExecutor<'t> {
        transaction
    }

    async fn migrate(
        &self,
        schema: &str,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, RpcError> {
        let mut migrator = PostgresMigrator::new(self.0.clone());
        if let Some(schema) = Self::schema(schema)? {
            migrator = migrator.with_schema(schema);
        }
        migrator
            .migrate(direction, opts)
            .await
            .map_err(RpcError::rejected)
    }
}

#[derive(Debug)]
pub struct Sqlite(SqlitePool);

/// SQLite has no schemas.
fn no_schema(schema: &str) -> Result<(), RpcError> {
    if schema.is_empty() {
        Ok(())
    } else {
        Err(RpcError::rejected("SQLite has no schemas"))
    }
}

impl Backend for Sqlite {
    type Db = sqlx::Sqlite;

    const DRIVER: &'static str = "sqlite";

    fn connect(url: &str, _application_name: &str) -> Result<Self, BoxError> {
        // Several processes share the file, so the busy timeout must cover
        // another's writes, including its switch of a new database to WAL.
        let options = SqliteConnectOptions::new()
            .filename(url)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        Ok(Self(
            SqlitePoolOptions::new()
                .max_connections(4)
                .connect_lazy_with(options),
        ))
    }

    fn builder(&self, schema: &str) -> Result<ClientBuilder, RpcError> {
        no_schema(schema)?;
        Ok(Client::builder(SqliteDatabase::new(self.0.clone())))
    }

    async fn begin(&self) -> Result<Transaction<'static, Self::Db>, sqlx::Error> {
        begin_sqlite_write(&self.0).await
    }

    fn executor<'t>(
        transaction: &'t mut Transaction<'static, Self::Db>,
    ) -> impl DatabaseTransactionExecutor<'t> {
        transaction
    }

    async fn migrate(
        &self,
        schema: &str,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, RpcError> {
        no_schema(schema)?;
        SqliteMigrator::new(self.0.clone())
            .migrate(direction, opts)
            .await
            .map_err(RpcError::rejected)
    }
}
