# riverqueue-test

Test helpers for applications using River's Rust client: assertions about
inserted jobs, and ways to run a worker once, with or without a database.

## Asserting on inserted jobs

`require_inserted`, `require_many_inserted`, and `require_not_inserted` check
the jobs a test's code inserted, like River Go's `rivertest` helpers. Each
lists jobs of the expected kinds in insertion order and panics with a
descriptive message when the expectation isn't met, failing the test.
The `_with` variants take `RequireInsertedOpts`, which adds expected
properties such as the queue, priority, state, or tags. The `_tx` variants
read through an open transaction, to test code that enqueues jobs
transactionally before it commits.

```rust,no_run
use riverqueue::{Client, JobArgs, JobState};
use riverqueue_test::{RequireInsertedOpts, require_inserted_with};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_welcome_email")]
struct SendWelcomeEmail {
    user_id: i64,
}

async fn sign_up(client: &Client, user_id: i64) -> Result<(), riverqueue::Error> {
    client.insert(SendWelcomeEmail { user_id }).await?;
    Ok(())
}

async fn test_sign_up(client: &Client) {
    sign_up(client, 42).await.unwrap();

    let job = require_inserted_with::<SendWelcomeEmail>(
        client,
        &RequireInsertedOpts::new().with_state(JobState::Available),
    )
    .await;
    assert_eq!(job.args.user_id, 42);
}
```

## Running a worker once

`TestJobBuilder` constructs a realistic `Job<A>` from the argument type's
insertion defaults and lets a test override the persisted ID, attempt, state,
and metadata. `work_once` invokes a typed worker with a detached
`WorkContext`, preserving its concrete error and capturing an immutable
snapshot of recorded output and metadata updates.

```rust,no_run
use riverqueue::{Job, JobArgs, WorkContext, WorkOutcome, Worker};
use riverqueue_test::{TestJobBuilder, work_once};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "thumbnail")]
struct Thumbnail {
    image_id: i64,
}

struct ThumbnailWorker;

impl Worker<Thumbnail> for ThumbnailWorker {
    type Error = serde_json::Error;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<Thumbnail>,
    ) -> Result<WorkOutcome, Self::Error> {
        context.record_output(serde_json::json!({"image_id": job.args.image_id}))?;
        Ok(WorkOutcome::Complete)
    }
}

#[tokio::test]
async fn thumbnail_records_its_image() {
    let job = TestJobBuilder::new(Thumbnail { image_id: 42 })
        .id(100)
        .build()
        .unwrap();
    let worked = work_once(&ThumbnailWorker, job).await;

    assert_eq!(worked.result.as_ref().unwrap(), &WorkOutcome::Complete);
    assert_eq!(worked.output(), Some(&serde_json::json!({"image_id": 42})));
}
```

`work_once` restores and finalizes resumable state, including failures that the
worker catches. Its result distinguishes `TestWorkError::Worker` from
`TestWorkError::Resumable` while preserving the original error source. Pass
`metadata_updates` into the next job's metadata to test a resumed attempt.

The helper does not run client hooks, middleware, database transactions,
retries, or completion persistence.

## Running a worker with a client

`work_with_client` is the database-backed counterpart, like Go's
`rivertest.Worker`. It inserts the job with a client, claims it the way a
fetch does, and runs the worker with that client in its `WorkContext`, so a
worker that inserts follow-up jobs through `context.client()` or completes
its job in its own transaction with `context.job_complete_tx` runs as it
would in production. The client doesn't need to be started. River doesn't
record the worker's result, so the job stays running unless the worker
completed it itself, and it also stays running if the test drops the
future partway, for example on a timeout.
