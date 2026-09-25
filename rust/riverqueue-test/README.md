# riverqueue-test

Test helpers for applications using River's Rust client: assertions about
inserted jobs, and a database-free way to run a worker once.

## Asserting on inserted jobs

`require_inserted`, `require_many_inserted`, and `require_not_inserted` check
the jobs a test's code inserted, like River Go's `rivertest` helpers. Each
lists jobs of the expected kinds in insertion order and panics with a
descriptive message when the expectation isn't met, failing the test.
`RequireInsertedOpts` adds expected properties such as the queue, priority,
state, or tags. The `_tx` variants read through an open transaction, to test
code that enqueues jobs transactionally before it commits.

```rust,no_run
use riverqueue::{Client, JobArgs, JobState};
use riverqueue_test::{RequireInsertedOpts, require_inserted, require_not_inserted};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_welcome_email")]
struct SendWelcomeEmail {
    user_id: i64,
}

# async fn sign_up(_client: &Client, _user_id: i64) {}
# async fn example(client: Client) {
sign_up(&client, 42).await;

let job = require_inserted::<SendWelcomeEmail>(
    &client,
    Some(&RequireInsertedOpts::new().state(JobState::Available)),
)
.await;
assert_eq!(job.args.user_id, 42);
# }
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

# async fn example() {
let job = TestJobBuilder::new(Thumbnail { image_id: 42 })
    .id(100)
    .build()
    .unwrap();
let worked = work_once(&ThumbnailWorker, job).await;

assert_eq!(worked.result.as_ref().unwrap(), &WorkOutcome::Complete);
assert_eq!(worked.output(), Some(&serde_json::json!({"image_id": 42})));
# }
```

`work_once` restores and finalizes resumable state, including failures that the
worker catches. Its result distinguishes `TestWorkError::Worker` from
`TestWorkError::Resumable` while preserving the original error source. Pass
`metadata_updates` into the next job's metadata to test a resumed attempt.

The helper does not run client hooks, middleware, database transactions,
retries, or completion persistence. Use River's integration and shared
conformance suites when those boundaries are under test.
