use std::error::Error;

use riverqueue::{Client, SchemaName};
use riverqueue_migrate::Migrator;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let schema = SchemaName::new("river_jobs")?;
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await?;

    let client = Client::builder(pool).schema(schema).build()?;
    println!("River schema: {}", client.schema());
    Ok(())
}
