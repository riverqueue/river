//! River is a fast and reliable PostgreSQL-backed job system.
//!
//! This crate is wire- and database-compatible with the matched Go River
//! release while exposing an API designed for Rust and Tokio.

#![forbid(unsafe_code)]

extern crate self as riverqueue;

mod client;
mod error;
mod event;
mod extension;
mod job;
mod maintenance;
mod periodic;
mod query;
mod queue;
mod storage;
mod unique;
mod worker;

pub use client::{
    Client, ClientBuilder, MaintenanceConfig, QueueConfig, ReindexerSchedule, RunHandle, WeakClient,
};
pub use error::Error;
pub use event::{Event, EventKind, EventReceiver, EventRecvError, JobStatistics, SubscribeConfig};
pub use extension::{
    DefaultRetryPolicy, ErrorHandler, ErrorHandlerResult, Hook, InsertContext, InsertMiddleware,
    Metric, MetricName, Plugin, RetryPolicy, WorkMiddleware, WorkResult,
};
pub use job::{
    AttemptError, InsertOpts, InsertResult, Job, JobArgs, JobRow, JobState, RawInsertResult,
    UniqueOpts,
};
pub use periodic::{
    CronSchedule, IntervalSchedule, NeverSchedule, PeriodicJob, PeriodicJobHandle, PeriodicJobOpts,
    PeriodicJobs, PeriodicSchedule,
};
pub use query::{
    JobDeleteManyParams, JobListCursor, JobListOrderBy, JobListParams, JobUpdateParams,
    SortDirection,
};
pub use queue::{Queue, QueueListParams};
#[doc(hidden)]
pub use riverqueue_internal as internal;
pub use riverqueue_internal::SchemaName;
pub use riverqueue_macros::JobArgs;
pub use unique::{UniqueKeyInput, build_unique_key};
pub use worker::{WorkContext, WorkOutcome, Worker, WorkerRegistry, WorkerTimeout};

/// Default maximum number of attempts for a job.
pub const MAX_ATTEMPTS_DEFAULT: i16 = 25;

/// Default minimum delay between queue fetches.
pub const FETCH_COOLDOWN_DEFAULT: std::time::Duration = std::time::Duration::from_millis(100);

/// Minimum supported queue fetch cooldown.
pub const FETCH_COOLDOWN_MIN: std::time::Duration = std::time::Duration::from_millis(1);

/// Default polling interval used as notification-loss recovery.
pub const FETCH_POLL_INTERVAL_DEFAULT: std::time::Duration = std::time::Duration::from_secs(1);

/// Minimum supported queue polling interval.
pub const FETCH_POLL_INTERVAL_MIN: std::time::Duration = std::time::Duration::from_millis(1);

/// Default delay before a cancelled worker is considered stuck.
pub const JOB_STUCK_THRESHOLD_DEFAULT: std::time::Duration = std::time::Duration::from_secs(10);

/// Default per-job execution timeout.
pub const JOB_TIMEOUT_DEFAULT: std::time::Duration = std::time::Duration::from_mins(1);

/// Maximum worker concurrency allowed for one queue.
pub const QUEUE_NUM_WORKERS_MAX: usize = 10_000;

/// Default job priority, where one is highest and four is lowest.
pub const PRIORITY_DEFAULT: i16 = 1;

/// Default queue name.
pub const QUEUE_DEFAULT: &str = "default";

/// Notification topic for queue and job control messages.
pub const NOTIFICATION_TOPIC_CONTROL: &str = "river_control";

/// Notification topic for newly available jobs.
pub const NOTIFICATION_TOPIC_INSERT: &str = "river_insert";

/// Notification topic for leadership changes.
pub const NOTIFICATION_TOPIC_LEADERSHIP: &str = "river_leadership";

/// Reserved metadata key containing recorded job output.
pub const METADATA_KEY_OUTPUT: &str = "output";

/// Reserved metadata key containing a periodic job identifier.
pub const METADATA_KEY_PERIODIC_JOB_ID: &str = "river:periodic_job_id";

/// Reserved metadata key containing resumable cursors.
pub const METADATA_KEY_RESUMABLE_CURSOR: &str = "river:resumable_cursor";

/// Reserved metadata key containing the completed resumable step.
pub const METADATA_KEY_RESUMABLE_STEP: &str = "river:resumable_step";

/// Reserved metadata key counting rescues.
pub const METADATA_KEY_RESCUE_COUNT: &str = "river:rescue_count";

/// Reserved metadata key used to distinguish unique upserts.
pub const METADATA_KEY_UNIQUE_NONCE: &str = "river:unique_nonce";
