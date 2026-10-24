//! Draft regressions for nested steps. These assert supported nesting behavior
//! and intentionally fail until its checkpoint and resume semantics are resolved.
#![cfg(feature = "sqlite")]

use std::sync::atomic::{AtomicUsize, Ordering};

use riverqueue::{__private, BoxError, Client, JobArgs, JobRow, JobUpdateParams, WorkContext};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "nested_resumable")]
struct Args {}

#[tokio::test]
async fn committed_parent_cursor_is_preserved() {
    assert_parent_checkpoint(true).await;
}

#[tokio::test]
async fn completed_cursor_child_remains_reachable() {
    assert_child_resume(true).await;
}

#[tokio::test]
async fn completed_plain_child_remains_reachable() {
    assert_child_resume(false).await;
}

#[tokio::test]
async fn rolled_back_parent_cursor_is_not_replayed() {
    assert_parent_checkpoint(false).await;
}

async fn assert_child_resume(cursor: bool) {
    let (_pool, _client, row, context) = setup().await;
    let parent_runs = AtomicUsize::new(0);
    let child_runs = AtomicUsize::new(0);
    assert!(
        run_parent(&context, cursor, true, &parent_runs, &child_runs)
            .await
            .is_err()
    );
    __private::work_context_resumable_finish(&context, true);
    let mut retry_row = row;
    retry_row.metadata = __private::work_context_metadata_updates(&context).into();
    let retry = __private::work_context_for_job(&retry_row);
    run_parent(&retry, cursor, false, &parent_runs, &child_runs)
        .await
        .unwrap();
    let failure = __private::work_context_resumable_finish(&retry, false);
    assert!(
        failure.is_none(),
        "retry cannot reach the saved child: {failure:?}"
    );
    assert_eq!(parent_runs.load(Ordering::SeqCst), 2);
    assert_eq!(child_runs.load(Ordering::SeqCst), 1);
}

async fn assert_parent_checkpoint(commit: bool) {
    let (pool, client, row, context) = setup().await;
    let result = context
        .resumable_step_with_cursor("parent", |_: i64| async {
            let mut tx = riverqueue::database::begin_sqlite_write(&pool).await?;
            context.resumable_set_step_cursor_tx(&mut tx, &1).await?;
            let child_result = context
                .resumable_step_with_cursor("child", |_: i64| async {
                    context.resumable_set_cursor(&7)?;
                    Err::<(), BoxError>("child interrupted".into())
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
    assert!(result.is_err());
    __private::work_context_resumable_finish(&context, true);
    if !commit {
        let before_result = client.jobs().get(row.id).await.unwrap();
        assert!(
            !before_result
                .metadata
                .contains_key("river:resumable_cursor")
        );
    }
    // Exercise the database merge, including the control where the parent's
    // checkpoint committed and must survive the child's deferred cursor write.
    let result = client
        .jobs()
        .update(
            row.id,
            JobUpdateParams::default().metadata(__private::work_context_metadata_updates(&context)),
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
            json!({"river:resumable_step": "parent", "river:resumable_cursor": {"parent": 1, "child": 7}})
        } else {
            json!({"river:resumable_cursor": {"child": 7}})
        }
    );
}

async fn run_parent(
    context: &WorkContext,
    cursor: bool,
    fail: bool,
    parent_runs: &AtomicUsize,
    child_runs: &AtomicUsize,
) -> Result<(), riverqueue::Error> {
    context
        .resumable_step("parent", || async {
            parent_runs.fetch_add(1, Ordering::SeqCst);
            if cursor {
                context
                    .resumable_step_with_cursor("child", |_: i64| async {
                        child_runs.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .await?;
            } else {
                context
                    .resumable_step("child", || async {
                        child_runs.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .await?;
            }
            if fail {
                return Err::<(), BoxError>("parent interrupted after child completed".into());
            }
            Ok(())
        })
        .await
}

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
