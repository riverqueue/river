//! Persisted metadata stays readable when JSON numbers exceed `f64`.

mod support;

use riverqueue::{Client, InsertOpts, JobArgs, JobMetadata, JobRow, JobUpdateParams};
use serde::{Deserialize, Serialize};

const METADATA: &str = r#"{"zeta":"first","big_integer":123456789012345678901234567890,"beyond_float":1e400,"long_decimal":0.1000000000000000055511151231257827}"#;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "metadata_exact_insert")]
struct InsertArgs {}

/// Inserts one job with `insert` and one with `insert_many`, both carrying
/// [`METADATA`] through [`InsertOpts`], and returns the stored rows.
async fn insert_with_exact_metadata(client: &Client) -> Vec<JobRow> {
    let metadata: JobMetadata = METADATA.parse().unwrap();
    let opts = InsertOpts::default().with_metadata(metadata);
    assert_eq!(opts.metadata().unwrap().as_raw().get(), METADATA);

    let single = client
        .insert(InsertArgs {})
        .opts(opts.clone())
        .await
        .unwrap();
    let many = client.insert_many([(InsertArgs {}, opts)]).await.unwrap();
    let mut rows = Vec::new();
    for id in std::iter::once(single.id()).chain(many.iter().map(riverqueue::InsertResult::id)) {
        rows.push(client.jobs().get(id).await.unwrap());
    }
    rows
}

#[cfg(feature = "postgres")]
fn raw_field<'a>(metadata: &'a JobMetadata, key: &str) -> &'a str {
    metadata.get_raw(key).unwrap().get()
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_reads_metadata_with_large_numbers() {
    use riverqueue::__private::{ExtensionClient, ExtensionInsertParams};
    use riverqueue::database::PostgresDatabase;
    use sqlx::AssertSqlSafe;

    let schema = support::PostgresSchema::new("meta_exact").await;
    let table = schema.table("river_job");
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {table} (args, kind, max_attempts, metadata) \
         VALUES ('{{}}', 'metadata_exact', 25, $1::jsonb) RETURNING id"
    )))
    .bind(METADATA)
    .fetch_one(&schema.pool)
    .await
    .unwrap();
    let client =
        Client::builder(PostgresDatabase::new(schema.pool.clone()).schema(schema.schema.clone()))
            .build()
            .unwrap();

    let original = client.jobs().get(id).await.unwrap();
    let big = raw_field(&original.metadata, "big_integer").to_owned();
    let beyond = raw_field(&original.metadata, "beyond_float").to_owned();
    assert_eq!(big, "123456789012345678901234567890");
    assert!(beyond.len() > 400); // PostgreSQL expands `1e400` in jsonb.

    let updated = client
        .jobs()
        .update(id, JobUpdateParams::default().with_output("done".into()))
        .await
        .unwrap();
    assert_eq!(raw_field(&updated.metadata, "big_integer"), big);
    assert_eq!(raw_field(&updated.metadata, "beyond_float"), beyond);
    assert_eq!(
        updated.decode_output::<String>().unwrap().as_deref(),
        Some("done")
    );
    let reread = client.jobs().get(id).await.unwrap();
    assert_eq!(raw_field(&reread.metadata, "beyond_float"), beyond);

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {table} SET state = 'running' WHERE id = $1"
    )))
    .bind(id)
    .execute(&schema.pool)
    .await
    .unwrap();
    let mut complete_tx = schema.pool.begin().await.unwrap();
    client
        .jobs()
        .complete(id)
        .tx(&mut complete_tx)
        .await
        .unwrap();
    complete_tx.commit().await.unwrap();
    let completed = client.jobs().get(id).await.unwrap();
    assert_eq!(raw_field(&completed.metadata, "beyond_float"), beyond);

    let mut transaction = schema.pool.begin().await.unwrap();
    let reinserted = ExtensionClient::new(&client)
        .insert_tx(
            &mut transaction,
            ExtensionInsertParams {
                created_at: reread.created_at,
                encoded_args: reread.encoded_args.clone(),
                kind: reread.kind.clone(),
                max_attempts: reread.max_attempts,
                metadata: reread.metadata.clone(),
                priority: reread.priority,
                queue: reread.queue.clone(),
                scheduled_at: reread.scheduled_at,
                tags: reread.tags.clone(),
                unique_key: reread.unique_key.clone(),
                unique_states: reread.unique_states.clone(),
            },
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(raw_field(&reinserted.job.metadata, "beyond_float"), beyond);

    schema.cleanup().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_insert_opts_keep_metadata_number_tokens() {
    use riverqueue::database::PostgresDatabase;

    let schema = support::PostgresSchema::new("meta_exact_insert").await;
    let client =
        Client::builder(PostgresDatabase::new(schema.pool.clone()).schema(schema.schema.clone()))
            .build()
            .unwrap();

    for row in insert_with_exact_metadata(&client).await {
        assert_eq!(
            raw_field(&row.metadata, "big_integer"),
            "123456789012345678901234567890"
        );
        assert_eq!(
            raw_field(&row.metadata, "long_decimal"),
            "0.1000000000000000055511151231257827"
        );
        // PostgreSQL expands `1e400` in jsonb rather than rejecting it.
        let beyond = raw_field(&row.metadata, "beyond_float");
        assert!(beyond.starts_with('1') && beyond.len() > 400, "{beyond}");
    }

    schema.cleanup().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_insert_opts_keep_metadata_number_tokens() {
    let (pool, path) = support::sqlite_file_pool(4).await;
    let client = Client::builder(pool.clone()).build().unwrap();

    for row in insert_with_exact_metadata(&client).await {
        for (key, token) in [
            ("big_integer", "123456789012345678901234567890"),
            ("beyond_float", "1e400"),
            ("long_decimal", "0.1000000000000000055511151231257827"),
        ] {
            assert_eq!(row.metadata.get_raw(key).unwrap().get(), token);
        }
    }

    support::sqlite_cleanup(pool, path).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_reads_metadata_with_large_numbers() {
    let (pool, path) = support::sqlite_file_pool(4).await;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, kind, max_attempts, metadata) \
         VALUES (jsonb('{}'), 'metadata_exact', 25, jsonb(?)) RETURNING id",
    )
    .bind(METADATA)
    .fetch_one(&pool)
    .await
    .unwrap();
    let client = Client::builder(pool.clone()).build().unwrap();

    let original = client.jobs().get(id).await.unwrap();
    assert_eq!(
        original.metadata.get_raw("beyond_float").unwrap().get(),
        "1e400"
    );
    assert_eq!(
        original.metadata.get_raw("long_decimal").unwrap().get(),
        "0.1000000000000000055511151231257827"
    );

    let updated = client
        .jobs()
        .update(id, JobUpdateParams::default().with_output("done".into()))
        .await
        .unwrap();
    assert_eq!(
        updated.metadata.get_raw("beyond_float").unwrap().get(),
        "1e400"
    );
    assert_eq!(
        updated.decode_output::<String>().unwrap().as_deref(),
        Some("done")
    );
    let reread = client.jobs().get(id).await.unwrap();
    assert_eq!(
        reread.metadata.get_raw("beyond_float").unwrap().get(),
        "1e400"
    );

    sqlx::query("UPDATE river_job SET state = 'running' WHERE id = ?")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    let mut complete_tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    client
        .jobs()
        .complete(id)
        .tx(&mut complete_tx)
        .await
        .unwrap();
    complete_tx.commit().await.unwrap();
    let completed = client.jobs().get(id).await.unwrap();
    assert_eq!(
        completed.metadata.get_raw("beyond_float").unwrap().get(),
        "1e400"
    );

    support::sqlite_cleanup(pool, path).await;
}
