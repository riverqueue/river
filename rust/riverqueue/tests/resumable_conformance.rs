//! Shared resumable semantics, exercised through a real worker and caller tx.
#![cfg(feature = "sqlite")]

use std::{collections::BTreeMap, path::Path, time::Duration};

use riverqueue::{
    BoxError, Client, EventKind, InsertOpts, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome,
    Workers,
};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "resumable_conformance")]
struct Args {
    scenario: String,
}

#[tokio::test]
async fn resumable_conformance() {
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/testdata/resumable.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF metadata ON river_job WHEN json_extract(NEW.args, '$.scenario') = 'checkpoint_write_failure' AND json_extract(NEW.metadata, '$.\"river:resumable_step\"') = 'loop' BEGIN SELECT RAISE(ABORT, 'checkpoint write failed'); END").execute(&pool).await.unwrap();
    let worker_pool = pool.clone();
    let mut workers = Workers::new();
    workers
        .add_fn(move |context: WorkContext, job: Job<Args>| {
            run_scenario(context, job, worker_pool.clone())
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .workers(workers)
        .without_notifications()
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[EventKind::JobFailed, EventKind::JobSnoozed])
        .unwrap();
    let mut expected = BTreeMap::new();
    for case in fixture["cases"].as_array().unwrap() {
        let inserted = client
            .insert(Args {
                scenario: case["name"].as_str().unwrap().to_owned(),
            })
            .opts(InsertOpts::default().with_max_attempts(1))
            .await
            .unwrap();
        expected.insert(inserted.job.row.id, case["expected_metadata"].clone());
    }
    let mut run = client.start().unwrap();
    for _ in 0..expected.len() {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap()
            .unwrap();
        let row = &event.as_job().unwrap().job;
        let metadata: Value = serde_json::to_value(&row.metadata).unwrap();
        let actual: serde_json::Map<String, Value> = metadata
            .as_object()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with("river:resumable_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            Value::Object(actual),
            expected.remove(&row.id).unwrap(),
            "job {}",
            row.id
        );
    }
    run.stop().await.unwrap();
}

async fn run_scenario(
    context: WorkContext,
    job: Job<Args>,
    pool: sqlx::SqlitePool,
) -> Result<WorkOutcome, BoxError> {
    let name = job.args.scenario;
    if name != "first_cursor_error" {
        context
            .resumable_step("first", || async { Ok::<_, BoxError>(()) })
            .await?;
    }
    if name == "step_snooze" {
        return Ok::<_, BoxError>(WorkOutcome::Snooze(Duration::from_secs(60)));
    }
    context
        .resumable_step_with_cursor("loop", |_: i64| async {
            if name == "first_cursor_error" {
                context.resumable_set_cursor(&7)?;
            } else {
                let mut tx = riverqueue::database::begin_sqlite_write(&pool).await?;
                context.resumable_set_step_cursor_tx(&mut tx, &1).await?;
                if name == "checkpoint_commit_error" || name == "checkpoint_then_cursor_error" {
                    tx.commit().await?;
                } else {
                    tx.rollback().await?;
                }
            }
            if name == "checkpoint_then_cursor_error" {
                context.resumable_set_cursor(&2)?;
            }
            Err::<(), BoxError>("retry".into())
        })
        .await?;
    Ok(WorkOutcome::Complete)
}
