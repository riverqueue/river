//! Metadata updates replace top-level keys on every database, as Postgres's
//! `||` does: nested objects aren't merged and a JSON null is stored as null.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use riverqueue::{Client, InsertOpts, JobArgs, JobMetadata, JobRow, JobUpdateParams};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "metadata_merge")]
struct MergeArgs {}

/// Updates a job's stored metadata through `client` and checks the merged
/// result, which must be the same on every database.
async fn assert_update_replaces_top_level_keys(client: &Client) {
    let stored = json!({
        "gone": 5,
        "keep": {"x": 1},
        "quoted \"key\".with.dots": {"old": true},
        "river:resumable_cursor": {"c": 1},
        "river:resumable_step": "a",
    });
    let metadata: JobMetadata = stored.to_string().parse().unwrap();
    let inserted = client
        .insert(MergeArgs {})
        .opts(InsertOpts::default().with_metadata(metadata))
        .await
        .unwrap();

    let updates = json!({
        "gone": null,
        "new \"key\".with.dots": 1,
        "quoted \"key\".with.dots": {"new": true},
        "river:resumable_cursor": {"d": 7, "n": null},
        "river:resumable_step": "c",
    });
    let Value::Object(updates) = updates else {
        unreachable!()
    };
    let row = client
        .jobs()
        .update(
            inserted.id(),
            JobUpdateParams::default().metadata(Map::from_iter(updates)),
        )
        .await
        .unwrap();

    let expected = json!({
        "gone": null,
        "keep": {"x": 1},
        "new \"key\".with.dots": 1,
        "quoted \"key\".with.dots": {"new": true},
        "river:resumable_cursor": {"d": 7, "n": null},
        "river:resumable_step": "c",
    });
    assert_eq!(stored_metadata(&row), expected);
    let reread = client.jobs().get(inserted.id()).await.unwrap();
    assert_eq!(stored_metadata(&reread), expected);
}

/// A row's metadata without the nonce a SQLite insert marks its row with.
fn stored_metadata(row: &JobRow) -> Value {
    let mut metadata: Value = serde_json::from_str(row.metadata.as_raw().get()).unwrap();
    metadata
        .as_object_mut()
        .unwrap()
        .remove("river:unique_nonce");
    metadata
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
#[tokio::test]
async fn postgres_update_replaces_top_level_keys() {
    use riverqueue::database::PostgresDatabase;

    let schema = support::PostgresSchema::new("meta_merge").await;
    let client = Client::builder(
        PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
    )
    .build()
    .unwrap();

    assert_update_replaces_top_level_keys(&client).await;

    schema.cleanup().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_update_replaces_top_level_keys() {
    let (pool, path) = support::sqlite_file_pool(4).await;
    let client = Client::builder(pool.clone()).build().unwrap();

    assert_update_replaces_top_level_keys(&client).await;

    support::sqlite_cleanup(pool, path).await;
}

#[cfg(feature = "sqlite")]
mod sqlite_resumable {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use riverqueue::{
        BoxError, Client, Job, JobArgs, JobState, QueueConfig, WorkContext, WorkError, WorkOutcome,
        Worker, Workers,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::support;

    #[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
    #[river(kind = "metadata_merge_resumable")]
    struct ResumableArgs {}

    /// One run of a step, as `(attempt, step, cursor)`.
    type StepRun = (i32, &'static str, i64);

    /// Runs step `a`, then cursor steps `c` and `d`, recording each step run.
    /// `c` fails on attempt 1 and `d` on attempt 2, each after setting a
    /// cursor.
    struct ResumableWorker {
        runs: Arc<Mutex<Vec<StepRun>>>,
    }

    impl Worker<ResumableArgs> for ResumableWorker {
        type Error = BoxError;

        fn next_retry(
            &self,
            _job: &Job<ResumableArgs>,
            _error: &WorkError,
            _now: chrono::DateTime<chrono::Utc>,
        ) -> Option<Duration> {
            Some(Duration::from_millis(10))
        }

        async fn work(
            &self,
            context: WorkContext,
            job: Job<ResumableArgs>,
        ) -> Result<WorkOutcome, Self::Error> {
            let attempt = job.row.attempt;
            context
                .resumable_step("a", || async {
                    self.runs.lock().unwrap().push((attempt, "a", 0));
                    Ok::<_, BoxError>(())
                })
                .await?;
            for (step, failing_attempt, next_cursor) in [("c", 1, 1_i64), ("d", 2, 7)] {
                let step_context = context.clone();
                context
                    .resumable_step_with_cursor(step, |cursor: i64| async move {
                        self.runs.lock().unwrap().push((attempt, step, cursor));
                        if attempt == failing_attempt {
                            step_context.resumable_set_cursor(&next_cursor)?;
                            return Err("intentional resumable cursor failure".into());
                        }
                        Ok::<_, BoxError>(())
                    })
                    .await?;
            }
            Ok(WorkOutcome::Complete)
        }
    }

    #[tokio::test]
    async fn completed_cursor_step_is_not_rerun_after_a_later_step_fails() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        let runs = Arc::new(Mutex::new(Vec::new()));
        let mut workers = Workers::new();
        workers
            .add(ResumableWorker {
                runs: Arc::clone(&runs),
            })
            .unwrap();
        let client = Client::builder(pool.clone())
            .workers(workers)
            .queue(
                "default",
                QueueConfig::new(1)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(10)),
            )
            .build()
            .unwrap();
        let inserted = client.insert(ResumableArgs {}).await.unwrap();
        let mut run = client.start().unwrap();
        run.wait_ready().await.unwrap();

        let row = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row = client.jobs().get(inserted.id()).await.unwrap();
                if row.state == JobState::Completed {
                    break row;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        run.stop().await.unwrap();

        assert_eq!(row.attempt, 3);
        // Attempt 3 skips `c`, which completed on attempt 2, and resumes `d`
        // from the cursor it set.
        assert_eq!(
            *runs.lock().unwrap(),
            [
                (1, "a", 0),
                (1, "c", 0),
                (2, "c", 1),
                (2, "d", 0),
                (3, "d", 7),
            ]
        );
        assert_eq!(
            row.metadata
                .get::<serde_json::Value>("river:resumable_cursor")
                .unwrap(),
            Some(json!({"d": 7}))
        );

        support::sqlite_cleanup(pool, path).await;
    }
}
