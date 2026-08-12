use std::error::Error;

use riverqueue::{Client, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "transactional_email")]
struct TransactionalEmail {
    address: String,
}

async fn complete_with_business_write(
    client: &Client,
    connection: &mut PgConnection,
    job_id: i64,
) -> Result<(), Box<dyn Error>> {
    sqlx::query("INSERT INTO email_audit (job_id) VALUES ($1)")
        .bind(job_id)
        .execute(&mut *connection)
        .await?;
    client.job_complete_tx(connection, job_id).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let client = Client::builder(pool.clone()).build()?;
    let mut transaction = pool.begin().await?;
    let inserted = client
        .insert_tx(
            &mut transaction,
            TransactionalEmail {
                address: "person@example.com".to_owned(),
            },
            InsertOpts::default(),
        )
        .await?;
    transaction.commit().await?;

    // A worker that owns a business transaction can call
    // `complete_with_business_write` after it has locked/claimed the job.
    let _ = (complete_with_business_write, inserted.job.row.id);
    Ok(())
}
