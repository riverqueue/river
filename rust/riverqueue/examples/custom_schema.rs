//! Keeps River's tables in their own PostgreSQL schema.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example custom_schema
//! ```

use std::error::Error;

use riverqueue::{
    Client,
    database::{PostgresDatabase, SchemaName},
};
use riverqueue_migrate::PostgresMigrator;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let schema = SchemaName::new("river_jobs")?;
    // Migrations create River's tables but not the schema itself.
    sqlx::query("CREATE SCHEMA IF NOT EXISTS river_jobs")
        .execute(&pool)
        .await?;
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await?;

    let client = Client::builder(PostgresDatabase::new(pool).schema(schema)).build()?;
    println!(
        "River schema: {}",
        client
            .postgres_schema()
            .expect("client is configured for PostgreSQL")
    );
    Ok(())
}
