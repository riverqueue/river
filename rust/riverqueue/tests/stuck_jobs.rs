//! The stuck job log line reports the timeout that applied to the job, the
//! worker's own when it sets one, like River Go's executor.

#![cfg(feature = "sqlite")]

mod support;

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use riverqueue::{
    Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, Worker, WorkerRegistry,
    WorkerTimeout,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
};

use crate::support::{sqlite_cleanup, sqlite_file_pool};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "stuck_job")]
struct StuckArgs {}

/// Ignores cancellation, with a timeout of its own.
struct StuckWorker {
    started: Arc<Notify>,
}

impl Worker<StuckArgs> for StuckWorker {
    type Error = Infallible;

    fn timeout(&self, _job: &Job<StuckArgs>) -> WorkerTimeout {
        WorkerTimeout::After(Duration::from_millis(5))
    }

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<StuckArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.started.notify_one();
        std::future::pending().await
    }
}

/// The `timeout` field of each stuck job log line.
#[derive(Clone, Default)]
struct StuckLines {
    changed: Arc<Notify>,
    timeouts: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> Layer<S> for StuckLines {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        #[derive(Default)]
        struct Fields {
            message: String,
            timeout: Option<String>,
        }

        impl Visit for Fields {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "message" => self.message = format!("{value:?}"),
                    "timeout" => self.timeout = Some(format!("{value:?}")),
                    _ => {}
                }
            }
        }

        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.message.contains("treating it as stuck") {
            self.timeouts
                .lock()
                .unwrap()
                .push(fields.timeout.unwrap_or_default());
            self.changed.notify_waiters();
        }
    }
}

// Current-thread runtime: the subscriber set for this thread sees every
// task the client spawns.
#[tokio::test]
async fn stuck_log_line_reports_the_worker_timeout() {
    let lines = StuckLines::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(lines.clone()));
    let (pool, path) = sqlite_file_pool(4).await;
    let started = Arc::new(Notify::new());
    let mut workers = WorkerRegistry::new();
    workers
        .register(StuckWorker {
            started: Arc::clone(&started),
        })
        .unwrap();
    // A client timeout long enough that the job can only have been
    // cancelled by the worker's timeout.
    let client = Client::builder(pool.clone())
        .job_timeout(Duration::from_mins(1))
        .job_stuck_threshold(Duration::from_millis(10))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .workers(workers)
        .build()
        .unwrap();
    client.insert(StuckArgs {}).await.unwrap();
    let mut run = client.start().unwrap();
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("worker starts");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let changed = lines.changed.notified();
            if !lines.timeouts.lock().unwrap().is_empty() {
                return;
            }
            changed.await;
        }
    })
    .await
    .expect("stuck job logged");
    run.shutdown().await.unwrap();

    assert_eq!(*lines.timeouts.lock().unwrap(), ["Some(5ms)"]);
    sqlite_cleanup(pool, path).await;
}
