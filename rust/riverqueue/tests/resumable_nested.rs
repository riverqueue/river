//! Rejected nested steps cannot replay rolled-back transactional checkpoints.
#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod support;

use riverqueue::{__private, BoxError, Client, JobArgs, JobRow, JobUpdateParams, WorkContext};
#[cfg(feature = "sqlite")]
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(feature = "sqlite")]
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "nested_resumable")]
struct Args {}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cursor_only_sqlite_step_remains_recoverable() {
    let (_pool, client, _, _) = setup().await;
    assert_cursor_only_recovery(&client).await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
#[tokio::test]
async fn cursor_only_postgres_step_remains_recoverable() {
    let database = support::PostgresSchema::new("cursor_only").await;
    let client = Client::builder(
        riverqueue::database::PostgresDatabase::new(database.pool.clone())
            .with_schema(database.schema.clone()),
    )
    .build()
    .unwrap();
    assert_cursor_only_recovery(&client).await;
    database.cleanup().await;
}

async fn assert_cursor_only_recovery(client: &Client) {
    let inserted = client.insert(Args {}).await.unwrap();
    let metadata = json!({"river:resumable_cursor": {"loop": 0}});
    client
        .jobs()
        .update(
            inserted.job.row.id,
            JobUpdateParams::default().metadata(metadata.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    let (row, context) = __private::claim_job_for_test(client, inserted.job.row.id)
        .await
        .unwrap();
    context
        .resumable_step("later", || async { Ok::<(), BoxError>(()) })
        .await
        .unwrap();
    assert!(
        __private::work_context_resumable_finish(&context, false)
            .unwrap()
            .to_string()
            .contains("resumable cursor step \"loop\" not found")
    );
    let saved = client
        .jobs()
        .update(
            row.id,
            JobUpdateParams::default().metadata(__private::work_context_metadata_updates(&context)),
        )
        .await
        .unwrap();
    let saved_metadata = serde_json::to_value(&saved.metadata).unwrap();
    assert_eq!(
        saved_metadata["river:resumable_cursor"],
        metadata["river:resumable_cursor"]
    );
    assert!(saved_metadata.get("river:resumable_step").is_none());
    let retry = __private::work_context_for_job(&saved);
    retry
        .resumable_step_with_cursor("loop", |cursor: Value| async move {
            assert_eq!(cursor, json!(0));
            Ok::<(), BoxError>(())
        })
        .await
        .unwrap();
    assert!(__private::work_context_resumable_finish(&retry, false).is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn committed_parent_cursor_is_preserved() {
    assert_parent_checkpoint(true).await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
#[tokio::test]
async fn postgres_parent_checkpoints_survive_rejected_nesting() {
    for commit in [false, true] {
        let database = support::PostgresSchema::new("nested_checkpoint").await;
        let pool = database.pool.clone();
        let client = Client::builder(
            riverqueue::database::PostgresDatabase::new(pool.clone())
                .with_schema(database.schema.clone()),
        )
        .build()
        .unwrap();
        let inserted = client.insert(Args {}).await.unwrap();
        let (row, context) = __private::claim_job_for_test(&client, inserted.job.row.id)
            .await
            .unwrap();
        let result = context
            .resumable_step_with_cursor("parent", |_: i64| async {
                let mut tx = pool.begin().await?;
                context.resumable_set_step_cursor_tx(&mut tx, &1).await?;
                let child_result = context
                    .resumable_step_with_cursor("child", |_: i64| async {
                        Err::<(), BoxError>("nested child must not run".into())
                    })
                    .await;
                if commit {
                    tx.commit().await?;
                } else {
                    tx.rollback().await?;
                }
                child_result?;
                Ok::<(), BoxError>(())
            })
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("resumable steps cannot be nested")
        );
        assert_parent_metadata(&client, &row, &context, commit).await;
        database.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn rolled_back_parent_cursor_is_not_replayed() {
    assert_parent_checkpoint(false).await;
}

#[cfg(feature = "sqlite")]
async fn assert_parent_checkpoint(commit: bool) {
    let (pool, client, row, context) = setup().await;
    let result = context
        .resumable_step_with_cursor("parent", |_: i64| async {
            let mut tx = riverqueue::database::begin_sqlite_write(&pool).await?;
            context.resumable_set_step_cursor_tx(&mut tx, &1).await?;
            let child_result = context
                .resumable_step_with_cursor("child", |_: i64| async {
                    Err::<(), BoxError>("nested child must not run".into())
                })
                .await;
            if commit {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            child_result?;
            Ok::<(), BoxError>(())
        })
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("resumable steps cannot be nested")
    );
    assert_parent_metadata(&client, &row, &context, commit).await;
}

async fn assert_parent_metadata(
    client: &Client,
    row: &JobRow,
    context: &WorkContext,
    commit: bool,
) {
    __private::work_context_resumable_finish(context, true);
    if !commit {
        let before_result = client.jobs().get(row.id).await.unwrap();
        assert!(
            !before_result
                .metadata
                .contains_key("river:resumable_cursor")
        );
    }
    // Rejecting the child preserves committed parent progress without
    // replaying a parent checkpoint that rolled back.
    let result = client
        .jobs()
        .update(
            row.id,
            JobUpdateParams::default().metadata(__private::work_context_metadata_updates(context)),
        )
        .await
        .unwrap();
    let metadata: Value = serde_json::to_value(&result.metadata).unwrap();
    let actual = Value::Object(
        metadata
            .as_object()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with("river:resumable_"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    assert_eq!(
        actual,
        if commit {
            json!({"river:resumable_step": "parent", "river:resumable_cursor": {"parent": 1}})
        } else {
            json!({})
        }
    );
}

#[cfg(feature = "sqlite")]
async fn setup() -> (SqlitePool, Client, JobRow, WorkContext) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(pool.clone()).build().unwrap();
    let inserted = client.insert(Args {}).await.unwrap();
    let (row, context) = __private::claim_job_for_test(&client, inserted.job.row.id)
        .await
        .unwrap();
    (pool, client, row, context)
}
