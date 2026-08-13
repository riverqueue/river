#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!("riverqueue requires at least one database feature: `postgres` or `sqlite`");

extern crate self as riverqueue;

mod client;
pub mod database;
pub mod error;
pub mod event;
pub mod extension;
pub mod job;
mod maintenance;
pub mod periodic;
pub mod query;
pub mod queue;
mod storage;
pub mod unique;
pub mod worker;

pub use client::{Client, ClientBuilder, MaintenanceConfig, QueueConfig, RunHandle, WeakClient};
pub use error::{BoxError, ConfigurationError, Error, JobValidationError, RuntimeError};
pub use event::{
    Event, EventKind, EventKindMismatch, EventReceiver, EventRecvError, JobEvent, JobEventKind,
    JobStatistics, QueueEvent, QueueEventKind, SubscribeConfig,
};
pub use extension::{
    DefaultRetryPolicy, ErrorHandler, ErrorHandlerDecision, Hook, InsertContext, InsertMiddleware,
    Metric, MetricName, Plugin, RetryPolicy, WorkError, WorkMiddleware, WorkResult,
};
pub use job::{
    AttemptError, ExtensionClaimParams, ExtensionInsertParams, InsertBatch, InsertBatchResult,
    InsertOpts, InsertParams, InsertResult, Job, JobArgs, JobRow, JobRowParts, JobState,
    JobStateParseError, RawInsertResult, UniqueOpts,
};
pub use periodic::{
    CronSchedule, CronScheduleParseError, IntervalSchedule, NeverSchedule, PeriodicJob,
    PeriodicJobHandle, PeriodicJobOpts, PeriodicJobs, PeriodicSchedule,
};
pub use query::{
    JobDeleteManyParams, JobListCursor, JobListCursorError, JobListOrderBy, JobListParams,
    JobUpdateParams, SortDirection,
};
pub use queue::{Queue, QueueListParams};
#[doc(hidden)]
pub use riverqueue_internal as internal;
pub(crate) use riverqueue_internal::SchemaName;
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
