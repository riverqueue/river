//! Behavior of the client's scoped operation handles on every backend.
//!
//! Each scenario runs against PostgreSQL (in a unique schema, failing rather
//! than skipping when `RIVER_RUST_DATABASE_URL` is unset) and SQLite (in a
//! temporary file). PostgreSQL scenarios build only with `--cfg river_postgres_tests`.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{convert::Infallible, time::Duration};

use riverqueue::{
    Client, Error, EventKind, InsertBatch, InsertContext, InsertMiddleware, InsertNext, InsertOpts,
    InsertedJob, Job, JobArgs, JobDeleteManyParams, JobListParams, JobState, JobUpdateParams,
    QueueConfig, QueueListParams, QueueSelector, QueueUpdateParams, UniqueOpts, WorkContext,
    WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};

/// Blocks until its attempt is cancelled.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "client_handles_blocking")]
struct BlockingArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "client_handles")]
struct HandleArgs {
    name: String,
}

/// Encodes like `HandleArgs` under another kind.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "client_handles_other_kind")]
struct OtherKindArgs {
    name: String,
}

fn args(name: &str) -> HandleArgs {
    HandleArgs {
        name: name.to_owned(),
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "client_handles_float")]
struct FloatArgs {
    value: f64,
}

/// Fails every insertion after River has written its jobs.
struct FailAfterWrite;

impl InsertMiddleware for FailAfterWrite {
    async fn insert_many(
        &self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> Result<Vec<InsertedJob>, Error> {
        let inserted = next.run(jobs).await?;
        assert!(!inserted.is_empty());
        Err(Error::extension(
            riverqueue::ExtensionPhase::InsertMiddleware,
            std::io::Error::other("failed after the write"),
        ))
    }
}

/// Defines each scenario for one backend's `Fixture`.
macro_rules! scenarios {
    () => {
        // Like Go, a claim appends the client to at most the 100 most
        // recent `attempted_by` entries.
        #[tokio::test(flavor = "multi_thread")]
        async fn attempted_by_keeps_the_most_recent_hundred_clients() {
            let fixture = Fixture::new().await;
            let id = fixture
                .client
                .insert(args("attempted_by"))
                .await
                .unwrap()
                .id();
            let previous = (1..=100)
                .map(|index| format!("client-{index}"))
                .collect::<Vec<_>>();
            fixture.set_attempted_by(id, &previous).await;
            let client = fixture
                .builder()
                .id("attempted-by-worker")
                .workers(workers())
                .queue("default", fast_queue())
                .build()
                .unwrap();
            let mut run = client.start().unwrap();
            wait_for_completion(&client, id).await;
            run.shutdown().await.unwrap();

            let attempted_by = client.jobs().get(id).await.unwrap().attempted_by;
            let mut expected = previous[1..].to_vec();
            expected.push("attempted-by-worker".to_owned());
            assert_eq!(attempted_by, expected);
            fixture.cleanup().await;
        }

        // A worker that returns successfully after its job is cancelled
        // completes the job, as in Go.
        #[tokio::test(flavor = "multi_thread")]
        async fn cancelled_job_that_succeeds_is_completed() {
            let fixture = Fixture::new().await;
            let (started_sender, mut started) = tokio::sync::mpsc::unbounded_channel();
            let mut workers = WorkerRegistry::new();
            workers
                .register_fn(move |context: WorkContext, job: Job<BlockingArgs>| {
                    let started_sender = started_sender.clone();
                    async move {
                        let _ = started_sender.send(job.id());
                        context.cancellation_token().cancelled().await;
                        Ok::<_, Infallible>(WorkOutcome::Complete)
                    }
                })
                .unwrap();
            let client = fixture
                .builder()
                .without_notifications()
                .workers(workers)
                .queue("default", fast_queue())
                .build()
                .unwrap();
            let mut run = client.start().unwrap();
            let id = client.insert(BlockingArgs {}).await.unwrap().id();
            tokio::time::timeout(Duration::from_secs(10), started.recv())
                .await
                .expect("job starts")
                .unwrap();
            client.jobs().cancel(id).await.unwrap();
            wait_for_completion(&client, id).await;
            run.shutdown().await.unwrap();
            fixture.cleanup().await;
        }

        // Like Go's `Insert` and `InsertMany`, an insertion without a
        // caller transaction runs middleware, hooks, and the write in one
        // transaction, so middleware failing after the write rolls it back.
        #[tokio::test(flavor = "multi_thread")]
        async fn insert_middleware_error_after_write_rolls_back() {
            let fixture = Fixture::new().await;
            let client = fixture
                .builder()
                .insert_middleware(FailAfterWrite)
                .build()
                .unwrap();

            let error = client.insert(args("single")).await.unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");
            let error = client
                .insert_many([args("many_1"), args("many_2")])
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");
            let mut batch = InsertBatch::new();
            batch.push(args("batch_1")).push(BlockingArgs {});
            let error = client.insert_batch(batch).await.unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");

            assert_eq!(fixture.job_count().await, 0);
            fixture.cleanup().await;
        }

        // Go's `encoding/json` can't encode NaN or infinities, so River Go
        // refuses such arguments. River Rust refuses them too rather than
        // storing `null`, which a Go worker would decode as a different
        // value.
        #[tokio::test(flavor = "multi_thread")]
        async fn insert_rejects_non_finite_float_args() {
            let fixture = Fixture::new().await;
            let client = fixture.builder().build().unwrap();

            for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                let error = client.insert(FloatArgs { value }).await.unwrap_err();
                assert!(matches!(error, Error::Json(_)), "{error}");
                let error = client
                    .insert_many([FloatArgs { value: 1.0 }, FloatArgs { value }])
                    .await
                    .unwrap_err();
                assert!(matches!(error, Error::Json(_)), "{error}");
                let mut batch = InsertBatch::new();
                batch.push(args("batch")).push(FloatArgs { value });
                let error = client.insert_batch(batch).await.unwrap_err();
                assert!(matches!(error, Error::Json(_)), "{error}");
            }

            assert_eq!(fixture.job_count().await, 0);
            fixture.cleanup().await;
        }

        // Like Go, a custom unique state set must include the states a job
        // passes through while it's being worked; an empty set means the
        // default states.
        #[tokio::test(flavor = "multi_thread")]
        async fn insert_requires_unique_states_to_include_required_states() {
            let fixture = Fixture::new().await;
            let client = fixture.builder().build().unwrap();

            let missing = InsertOpts::default().with_unique(
                UniqueOpts::new().with_by_state([JobState::Available, JobState::Completed]),
            );
            let error = client
                .insert(args("missing_states"))
                .opts(missing)
                .await
                .unwrap_err();
            assert!(matches!(error, Error::InvalidJob(_)), "{error}");
            assert!(
                error.to_string().contains("pending, running, scheduled"),
                "{error}"
            );
            assert_eq!(fixture.job_count().await, 0);

            let empty = InsertOpts::default().with_unique(UniqueOpts::new().with_by_state([]));
            let first = client
                .insert(args("empty_states"))
                .opts(empty.clone())
                .await
                .unwrap();
            let duplicate = client
                .insert(args("empty_states"))
                .opts(empty)
                .await
                .unwrap();
            assert!(duplicate.unique_skipped_as_duplicate);
            assert_eq!(duplicate.id(), first.id());
            fixture.cleanup().await;
        }

        // Like Go, with `exclude_kind` jobs of different kinds share a unique
        // key, and an insertion skipped as a duplicate of a job of another
        // kind returns that job as it is rather than rewriting its kind.
        #[tokio::test(flavor = "multi_thread")]
        async fn insert_unique_skip_keeps_the_existing_jobs_kind() {
            let fixture = Fixture::new().await;
            let unique = InsertOpts::default().with_unique(
                UniqueOpts::new()
                    .with_by_args(true)
                    .with_exclude_kind(true),
            );
            let first = fixture
                .client
                .insert(args("exclude_kind"))
                .opts(unique.clone())
                .await
                .unwrap();
            assert!(!first.unique_skipped_as_duplicate);

            let other = OtherKindArgs {
                name: "exclude_kind".to_owned(),
            };
            let duplicate = fixture
                .client
                .insert(other.clone())
                .opts(unique.clone())
                .await
                .unwrap();
            assert!(duplicate.unique_skipped_as_duplicate);
            assert_eq!(duplicate.id(), first.id());
            assert_eq!(duplicate.job.row.kind, HandleArgs::KIND);

            let batch = fixture
                .client
                .insert_many([(other, unique)])
                .await
                .unwrap();
            assert!(batch[0].unique_skipped_as_duplicate);
            assert_eq!(batch[0].id(), first.id());
            assert_eq!(batch[0].job.row.kind, HandleArgs::KIND);

            let stored = fixture.client.jobs().get(first.id()).await.unwrap();
            assert_eq!(stored.kind, HandleArgs::KIND);
            assert_eq!(fixture.job_count().await, 1);
            fixture.cleanup().await;
        }

        // Each delete-many filter deletes exactly the jobs it matches.
        #[tokio::test(flavor = "multi_thread")]
        async fn delete_many_filters_by_kind_queue_priority_and_state() {
            let fixture = Fixture::new().await;
            let client = fixture.builder().build().unwrap();
            let jobs = client.jobs();

            let plain = client.insert(args("plain")).await.unwrap().id();
            let other_kind = client.insert(FloatArgs { value: 1.0 }).await.unwrap().id();
            let other_queue = client
                .insert(args("other_queue"))
                .opts(InsertOpts::default().with_queue("delete_many_other"))
                .await
                .unwrap()
                .id();
            let urgent = client
                .insert(args("urgent"))
                .opts(InsertOpts::default().with_priority(2))
                .await
                .unwrap()
                .id();
            let cancelled = client.insert(args("cancelled")).await.unwrap().id();
            jobs.cancel(cancelled).await.unwrap();

            let deleted_ids = |rows: Vec<riverqueue::JobRow>| {
                let mut ids = rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
                ids.sort_unstable();
                ids
            };
            let delete =
                |params: JobListParams| jobs.delete_many(JobDeleteManyParams::matching(params));
            assert_eq!(
                deleted_ids(
                    delete(JobListParams::default().kinds([FloatArgs::KIND]))
                        .await
                        .unwrap()
                ),
                [other_kind]
            );
            assert_eq!(
                deleted_ids(
                    delete(JobListParams::default().queues(["delete_many_other"]))
                        .await
                        .unwrap()
                ),
                [other_queue]
            );
            assert_eq!(
                deleted_ids(
                    delete(JobListParams::default().priorities([2]))
                        .await
                        .unwrap()
                ),
                [urgent]
            );
            assert_eq!(
                deleted_ids(
                    delete(JobListParams::default().states([JobState::Cancelled]))
                        .await
                        .unwrap()
                ),
                [cancelled]
            );
            // Combined filters must all match.
            assert!(
                delete(
                    JobListParams::default()
                        .kinds([HandleArgs::KIND])
                        .states([JobState::Cancelled])
                )
                .await
                .unwrap()
                .is_empty()
            );
            let remaining = jobs.list(JobListParams::default()).await.unwrap().jobs;
            assert_eq!(
                remaining.iter().map(|row| row.id).collect::<Vec<_>>(),
                [plain]
            );
            fixture.cleanup().await;
        }

        // Jobs are fetched by priority, then scheduled time, then ID, as in
        // Go.
        #[tokio::test(flavor = "multi_thread")]
        async fn fetches_by_priority_then_schedule_then_id() {
            let fixture = Fixture::new().await;
            let base = chrono::Utc::now() - chrono::Duration::minutes(10);
            let mut ids = Vec::new();
            for (name, priority, minutes) in [
                ("p2_early", 2, 0),
                ("p1_late", 1, 5),
                ("p1_early_first", 1, 1),
                ("p1_early_second", 1, 1),
                ("p4_earliest", 4, -5),
            ] {
                let id = fixture
                    .client
                    .insert(args(name))
                    .opts(
                        InsertOpts::default()
                            .with_priority(priority)
                            .with_scheduled_at(base + chrono::Duration::minutes(minutes)),
                    )
                    .await
                    .unwrap()
                    .id();
                ids.push(id);
            }
            let worked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut workers = WorkerRegistry::new();
            let recorder = std::sync::Arc::clone(&worked);
            workers
                .register_fn(move |_context: WorkContext, job: Job<HandleArgs>| {
                    let recorder = std::sync::Arc::clone(&recorder);
                    async move {
                        recorder.lock().unwrap().push(job.id());
                        Ok::<_, Infallible>(WorkOutcome::Complete)
                    }
                })
                .unwrap();
            let client = fixture
                .builder()
                .workers(workers)
                .queue("default", fast_queue())
                .build()
                .unwrap();
            let mut run = client.start().unwrap();
            wait_for_completion(&client, ids[4]).await;
            run.shutdown().await.unwrap();

            assert_eq!(
                *worked.lock().unwrap(),
                [ids[2], ids[3], ids[1], ids[0], ids[4]]
            );
            fixture.cleanup().await;
        }

        // Port of Go's `CancelRunningJobPollOnly`: with no listener, the
        // cancelling client must wake its own running attempt.
        #[tokio::test(flavor = "multi_thread")]
        async fn cancel_reaches_a_running_job_on_a_poll_only_client() {
            let fixture = Fixture::new().await;
            let (started_sender, mut started) = tokio::sync::mpsc::unbounded_channel();
            let mut workers = WorkerRegistry::new();
            workers
                .register_fn(move |context: WorkContext, job: Job<BlockingArgs>| {
                    let started_sender = started_sender.clone();
                    async move {
                        let _ = started_sender.send(job.id());
                        context.cancellation_token().cancelled().await;
                        Err::<WorkOutcome, _>(std::io::Error::other("cancelled"))
                    }
                })
                .unwrap();
            let client = fixture
                .builder()
                .without_notifications()
                .workers(workers)
                .queue("default", fast_queue())
                .build()
                .unwrap();
            let mut events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
            let mut run = client.start().unwrap();
            run.wait_ready().await.unwrap();

            let id = client.insert(BlockingArgs {}).await.unwrap().id();
            let started_id = tokio::time::timeout(Duration::from_secs(10), started.recv())
                .await
                .expect("job starts")
                .unwrap();
            assert_eq!(started_id, id);

            let row = client.jobs().cancel(id).await.unwrap();
            assert_eq!(row.state, JobState::Running);

            let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
                .await
                .expect("cancellation reaches the running attempt")
                .unwrap();
            let job = &event.as_job().unwrap().job;
            assert_eq!(job.id, id);
            assert_eq!(job.state, JobState::Cancelled);
            let finalized_at = job.finalized_at.unwrap();
            assert!((chrono::Utc::now() - finalized_at).num_seconds().abs() < 2);

            run.shutdown().await.unwrap();
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn job_requests_take_effect_only_when_the_transaction_commits() {
            let fixture = Fixture::new().await;
            let client = &fixture.client;
            let jobs = client.jobs();
            let cancelled = client.insert(args("cancel")).await.unwrap().id();
            let deleted = client.insert(args("delete")).await.unwrap().id();
            let updated = client.insert(args("update")).await.unwrap().id();

            // Every write is rolled back with the caller's transaction.
            let mut tx = fixture.begin().await;
            let row = jobs.cancel(cancelled).tx(&mut tx).await.unwrap();
            assert_eq!(row.state, JobState::Cancelled);
            let row = jobs.delete(deleted).tx(&mut tx).await.unwrap();
            assert_eq!(row.id, deleted);
            let row = jobs
                .update(
                    updated,
                    JobUpdateParams::default().output(serde_json::json!("rolled back")),
                )
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(
                row.metadata.get::<String>("output").unwrap().as_deref(),
                Some("rolled back")
            );
            // Reads in the transaction see its uncommitted writes.
            assert!(matches!(
                jobs.get(deleted).tx(&mut tx).await,
                Err(Error::NotFound(_))
            ));
            let inserted = client
                .insert(args("uncommitted"))
                .tx(&mut tx)
                .await
                .unwrap()
                .id();
            let listed = jobs
                .list(JobListParams::default().ids([inserted]))
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(listed.jobs.len(), 1);
            tx.rollback().await.unwrap();

            assert_eq!(
                jobs.get(cancelled).await.unwrap().state,
                JobState::Available
            );
            assert!(jobs.get(deleted).await.is_ok());
            assert!(
                !jobs
                    .get(updated)
                    .await
                    .unwrap()
                    .metadata
                    .contains_key("output")
            );
            let listed = jobs
                .list(JobListParams::default().ids([inserted]))
                .await
                .unwrap();
            assert!(listed.jobs.is_empty());
            assert!(listed.last_cursor.is_none());

            // The same requests persist once the transaction commits.
            let mut tx = fixture.begin().await;
            jobs.cancel(cancelled).tx(&mut tx).await.unwrap();
            jobs.delete_many(JobDeleteManyParams::matching(
                JobListParams::default().ids([deleted]),
            ))
            .tx(&mut tx)
            .await
            .unwrap();
            let retried = jobs.retry(cancelled).tx(&mut tx).await.unwrap();
            assert_eq!(retried.state, JobState::Available);
            tx.commit().await.unwrap();

            assert_eq!(
                jobs.get(cancelled).await.unwrap().state,
                JobState::Available
            );
            assert!(matches!(jobs.get(deleted).await, Err(Error::NotFound(_))));

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn job_requests_run_on_the_pool_without_a_transaction() {
            let fixture = Fixture::new().await;
            let jobs = fixture.client.jobs();
            let first = fixture.client.insert(args("first")).await.unwrap().id();
            let second = fixture.client.insert(args("second")).await.unwrap().id();

            let page = jobs
                .list(JobListParams::default().ids([first, second]).limit(1))
                .await
                .unwrap();
            assert_eq!(
                page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
                [first]
            );
            let cursor = page.last_cursor.expect("a nonempty page has a cursor");
            let page = jobs
                .list(
                    JobListParams::default()
                        .ids([first, second])
                        .limit(1)
                        .after(cursor),
                )
                .await
                .unwrap();
            assert_eq!(
                page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
                [second]
            );

            assert_eq!(jobs.cancel(first).await.unwrap().state, JobState::Cancelled);
            assert_eq!(jobs.retry(first).await.unwrap().state, JobState::Available);
            assert_eq!(jobs.delete(second).await.unwrap().id, second);
            assert!(matches!(jobs.delete(second).await, Err(Error::NotFound(_))));
            assert!(matches!(
                jobs.cancel(i64::MAX).await,
                Err(Error::NotFound(_))
            ));
            assert!(matches!(
                jobs.retry(i64::MAX).await,
                Err(Error::NotFound(_))
            ));

            fixture.cleanup().await;
        }

        // SQLite must match PostgreSQL's `@>` containment exactly.
        #[tokio::test(flavor = "multi_thread")]
        async fn metadata_filters_match_postgres_containment() {
            let fixture = Fixture::new().await;
            let client = &fixture.client;
            let mut ids = std::collections::HashMap::new();
            for (name, metadata) in [
                ("null", r#"{"a":null}"#),
                ("missing", "{}"),
                ("integer", r#"{"a":1}"#),
                ("float", r#"{"a":1.0}"#),
                ("string", r#"{"a":"1"}"#),
                ("array", r#"{"a":[1,2,{"b":"x"}],"s":"a<b"}"#),
                ("nested", r#"{"a":{"b":{"c":true},"d":2}}"#),
                ("scalar_array", r#"{"a":["x"]}"#),
            ] {
                let metadata: riverqueue::JobMetadata = metadata.parse().unwrap();
                let id = client
                    .insert(args(name))
                    .opts(InsertOpts::default().with_metadata(metadata))
                    .await
                    .unwrap()
                    .id();
                ids.insert(name, id);
            }

            for (fragment, want) in [
                (
                    "{}",
                    &[
                        "null",
                        "missing",
                        "integer",
                        "float",
                        "string",
                        "array",
                        "nested",
                        "scalar_array",
                    ][..],
                ),
                (r#"{"a":null}"#, &["null"][..]),
                (r#"{"a":1}"#, &["integer", "float"][..]),
                (r#"{"a":1.0}"#, &["integer", "float"][..]),
                (r#"{"a":"1"}"#, &["string"][..]),
                (r#"{"a":[]}"#, &["array", "scalar_array"][..]),
                (r#"{"a":[2]}"#, &["array"][..]),
                (r#"{"a":[2,1,2]}"#, &["array"][..]),
                (r#"{"a":[{}]}"#, &["array"][..]),
                (r#"{"a":[{"b":"x"}]}"#, &["array"][..]),
                (r#"{"a":[[1]]}"#, &[][..]),
                (r#"{"a":"x"}"#, &[][..]),
                (r#"{"a":{}}"#, &["nested"][..]),
                (r#"{"a":{"b":{"c":true}}}"#, &["nested"][..]),
                (r#"{"a":{"b":{"c":false}}}"#, &[][..]),
                (r#"{"s":"a<b"}"#, &["array"][..]),
                (r#"{"a":1,"z":null}"#, &[][..]),
            ] {
                let fragment: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(fragment).unwrap();
                let mut want = want.iter().map(|name| ids[name]).collect::<Vec<_>>();
                want.sort_unstable();
                let listed = client
                    .jobs()
                    .list(JobListParams::default().metadata(fragment.clone()))
                    .await
                    .unwrap();
                let got = listed.jobs.iter().map(|job| job.id).collect::<Vec<_>>();
                assert_eq!(got, want, "{fragment:?}");
            }

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn queue_requests_take_effect_only_when_the_transaction_commits() {
            let fixture = Fixture::new().await;
            let queues = fixture.client.queues();
            fixture.insert_queue("alpha").await;
            fixture.insert_queue("beta").await;
            let owner = serde_json::Map::from_iter([("owner".to_owned(), "rust".into())]);

            let mut tx = fixture.begin().await;
            queues.pause(QueueSelector::All).tx(&mut tx).await.unwrap();
            assert!(
                queues
                    .get("alpha")
                    .tx(&mut tx)
                    .await
                    .unwrap()
                    .paused_at
                    .is_some()
            );
            let beta = queues
                .update("beta", QueueUpdateParams::new().metadata(owner.clone()))
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(beta.metadata, owner);
            tx.rollback().await.unwrap();
            assert!(paused(&fixture.client).await.is_empty());
            assert!(queues.get("beta").await.unwrap().metadata.is_empty());

            let mut tx = fixture.begin().await;
            queues.pause(QueueSelector::All).tx(&mut tx).await.unwrap();
            queues
                .update("beta", QueueUpdateParams::new().metadata(owner.clone()))
                .tx(&mut tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert_eq!(paused(&fixture.client).await, ["alpha", "beta"]);
            assert_eq!(queues.get("beta").await.unwrap().metadata, owner);

            queues.resume("alpha").await.unwrap();
            assert_eq!(paused(&fixture.client).await, ["beta"]);
            queues.resume(QueueSelector::All).await.unwrap();
            assert!(paused(&fixture.client).await.is_empty());

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn queue_selectors_match_names_literally() {
            let fixture = Fixture::new().await;
            let queues = fixture.client.queues();

            // Selecting every queue succeeds when there are none, like Go.
            queues.pause(QueueSelector::All).await.unwrap();
            queues.resume(QueueSelector::All).await.unwrap();

            fixture.insert_queue("alpha").await;
            assert_eq!(
                QueueSelector::from("alpha"),
                QueueSelector::Named("alpha".to_owned())
            );
            assert_eq!(
                QueueSelector::from("*".to_owned()),
                QueueSelector::Named("*".to_owned())
            );
            // `*` is only a name, and no queue can have it.
            assert!(matches!(queues.pause("*").await, Err(Error::NotFound(_))));
            assert!(matches!(queues.resume("*").await, Err(Error::NotFound(_))));
            assert!(paused(&fixture.client).await.is_empty());
            assert!(matches!(
                queues.pause("missing").await,
                Err(Error::NotFound(_))
            ));
            assert!(matches!(
                queues.get("missing").await,
                Err(Error::NotFound(_))
            ));
            assert!(matches!(
                queues.update("missing", QueueUpdateParams::new()).await,
                Err(Error::NotFound(_))
            ));

            // Updating without metadata keeps it while refreshing the record.
            let owner = serde_json::Map::from_iter([("owner".to_owned(), "rust".into())]);
            let before = queues
                .update("alpha", QueueUpdateParams::new().metadata(owner.clone()))
                .await
                .unwrap();
            let after = queues
                .update("alpha", QueueUpdateParams::new())
                .await
                .unwrap();
            assert_eq!(after.metadata, owner);
            assert!(after.updated_at >= before.updated_at);

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn local_queues_change_configuration_without_lock_errors() {
            let fixture = Fixture::new().await;
            let client = fixture
                .builder()
                .workers(workers())
                .queue("default", QueueConfig::new(1))
                .build()
                .unwrap();
            let local = client.local_queues();
            assert_eq!(
                local.configs(),
                [("default".to_owned(), QueueConfig::new(1))].into()
            );

            local.add("second", QueueConfig::new(2)).unwrap();
            // Like Go, adding a queue twice is an error; update reconfigures.
            assert!(matches!(
                local.add("default", QueueConfig::new(3)),
                Err(Error::QueueAlreadyAdded { name }) if name == "default"
            ));
            local.update("default", QueueConfig::new(3)).unwrap();
            assert!(matches!(
                local.update("missing", QueueConfig::new(3)),
                Err(Error::QueueNotAdded { name }) if name == "missing"
            ));
            assert_eq!(
                local.configs(),
                [
                    ("default".to_owned(), QueueConfig::new(3)),
                    ("second".to_owned(), QueueConfig::new(2)),
                ]
                .into()
            );
            assert!(matches!(
                local.add("not a queue name", QueueConfig::new(1)),
                Err(Error::InvalidJob(_))
            ));
            assert!(matches!(
                local.add("third", QueueConfig::new(0)),
                Err(Error::Configuration(_))
            ));
            // A client that isn't running has no producer to wait for.
            assert_eq!(local.remove("second").await.unwrap(), QueueConfig::new(2));
            assert!(matches!(
                local.remove("second").await,
                Err(Error::QueueNotAdded { .. })
            ));
            assert_eq!(local.configs().len(), 1);

            // A client without workers can't run any queue.
            assert!(matches!(
                fixture
                    .client
                    .local_queues()
                    .add("default", QueueConfig::new(1)),
                Err(Error::Configuration(_))
            ));

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn local_queues_start_producers_while_running() {
            let fixture = Fixture::new().await;
            let client = fixture
                .builder()
                .workers(workers())
                .queue("default", fast_queue())
                .build()
                .unwrap();
            let mut run = client.start().unwrap();

            client.local_queues().add("dynamic", fast_queue()).unwrap();
            let job = client
                .insert(args("dynamic"))
                .opts(InsertOpts::default().with_queue("dynamic"))
                .await
                .unwrap();
            wait_for_completion(&client, job.id()).await;
            assert_eq!(
                client.local_queues().remove("dynamic").await.unwrap(),
                fast_queue()
            );
            assert!(!client.local_queues().configs().contains_key("dynamic"));

            run.shutdown().await.unwrap();
            fixture.cleanup().await;
        }
    };
}

fn workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<HandleArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

fn fast_queue() -> QueueConfig {
    QueueConfig::new(1)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(10))
}

/// Waits for a job to complete, failing after ten seconds.
async fn wait_for_completion(client: &Client, id: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client.jobs().get(id).await.unwrap().state != JobState::Completed {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {id} did not complete"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Returns the names of paused queues.
async fn paused(client: &Client) -> Vec<String> {
    client
        .queues()
        .list(QueueListParams::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|queue| queue.paused_at.is_some())
        .map(|queue| queue.name)
        .collect()
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use sqlx::{PgPool, Postgres, Transaction};

    use super::*;
    use crate::support::PostgresSchema;

    struct Fixture {
        client: Client,
        pool: PgPool,
        schema: PostgresSchema,
    }

    impl Fixture {
        async fn new() -> Self {
            let schema = PostgresSchema::new("river_handles").await;
            let client = builder(&schema).build().unwrap();
            Self {
                client,
                pool: schema.pool.clone(),
                schema,
            }
        }

        fn builder(&self) -> riverqueue::ClientBuilder {
            builder(&self.schema)
        }

        async fn begin(&self) -> Transaction<'static, Postgres> {
            self.pool.begin().await.unwrap()
        }

        async fn job_count(&self) -> i64 {
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {}",
                self.schema.table("river_job")
            )))
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn insert_queue(&self, name: &str) {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {} (name, created_at, metadata, updated_at) \
                 VALUES ($1, now(), '{{}}', now())",
                self.schema.table("river_queue")
            )))
            .bind(name)
            .execute(&self.pool)
            .await
            .unwrap();
        }

        async fn set_attempted_by(&self, id: i64, attempted_by: &[String]) {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {} SET attempted_by = $2 WHERE id = $1",
                self.schema.table("river_job")
            )))
            .bind(id)
            .bind(attempted_by)
            .execute(&self.pool)
            .await
            .unwrap();
        }

        async fn cleanup(self) {
            self.schema.cleanup().await;
        }
    }

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(
            riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                .with_schema(schema.schema.clone()),
        )
    }

    scenarios!();
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use sqlx::{Sqlite, SqlitePool, Transaction};

    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    struct Fixture {
        client: Client,
        path: std::path::PathBuf,
        pool: SqlitePool,
    }

    impl Fixture {
        async fn new() -> Self {
            let (pool, path) = sqlite_file_pool(4).await;
            let client = Client::builder(pool.clone()).build().unwrap();
            Self { client, path, pool }
        }

        fn builder(&self) -> riverqueue::ClientBuilder {
            Client::builder(self.pool.clone())
        }

        async fn begin(&self) -> Transaction<'static, Sqlite> {
            self.pool.begin_with("BEGIN IMMEDIATE").await.unwrap()
        }

        async fn job_count(&self) -> i64 {
            sqlx::query_scalar("SELECT count(*) FROM river_job")
                .fetch_one(&self.pool)
                .await
                .unwrap()
        }

        async fn insert_queue(&self, name: &str) {
            sqlx::query("INSERT INTO river_queue (name, metadata) VALUES (?, jsonb('{}'))")
                .bind(name)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        async fn set_attempted_by(&self, id: i64, attempted_by: &[String]) {
            sqlx::query("UPDATE river_job SET attempted_by = jsonb(?) WHERE id = ?")
                .bind(serde_json::to_string(attempted_by).unwrap())
                .bind(id)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        async fn cleanup(self) {
            sqlite_cleanup(self.pool, self.path).await;
        }
    }

    scenarios!();

    /// SQLite delivers notifications through an outbox table, which shows
    /// that a resignation request is sent only when its transaction commits.
    #[tokio::test(flavor = "multi_thread")]
    async fn resign_requests_are_sent_when_the_transaction_commits() {
        let fixture = Fixture::new().await;
        let requests = async || -> i64 {
            sqlx::query_scalar("SELECT count(*) FROM river_notification WHERE topic = ?")
                .bind(riverqueue::protocol::NOTIFICATION_TOPIC_LEADERSHIP)
                .fetch_one(&fixture.pool)
                .await
                .unwrap()
        };

        let mut tx = fixture.begin().await;
        fixture.client.request_resign().tx(&mut tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(requests().await, 0);

        let mut tx = fixture.begin().await;
        fixture.client.request_resign().tx(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(requests().await, 1);

        fixture.client.request_resign().await.unwrap();
        assert_eq!(requests().await, 2);

        fixture.cleanup().await;
    }
}
