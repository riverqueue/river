# riverqueue-test

Typed, database-free test helpers for applications using River's Rust client.

`TestJobBuilder` constructs a realistic `Job<A>` from the argument type's
insertion defaults and lets a test override the persisted ID, attempt, state,
and metadata. `work_once` invokes a typed worker with a detached
`WorkContext`, preserving its concrete error and capturing an immutable
snapshot of recorded output and metadata updates.

```rust,no_run
use std::convert::Infallible;

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
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<Thumbnail>,
    ) -> Result<WorkOutcome, Self::Error> {
        context
            .record_output(&serde_json::json!({"image_id": job.args.image_id}))
            .await
            .expect("static JSON is serializable");
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
