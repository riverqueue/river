// The README's examples use both backends, so it's the crate documentation
// (and its examples run as doctests) when both are enabled, as on docs.rs.
#![cfg_attr(
    all(feature = "postgres", feature = "sqlite"),
    doc = include_str!("../README.md")
)]
#![cfg_attr(
    not(all(feature = "postgres", feature = "sqlite")),
    doc = "The Rust and Tokio client for [River](https://riverqueue.com). The full crate \
           documentation, from the README, builds with both the `postgres` and `sqlite` \
           features, as on [docs.rs](https://docs.rs/riverqueue)."
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!("riverqueue requires at least one database feature: `postgres` or `sqlite`");

extern crate self as riverqueue;

#[doc(hidden)]
pub mod __private;
mod client;
#[cfg(test)]
mod conformance;
pub mod database;
pub mod encoding;
pub mod error;
pub mod event;
pub mod extension;
pub mod guide;
pub mod job;
mod maintenance;
pub mod periodic;
mod pilot;
pub mod protocol;
pub mod query;
pub mod queue;
mod storage;
mod unique;
pub mod worker;

/// The Chrono version of the timestamps in River's API, such as
/// [`JobRow::scheduled_at`] and [`InsertOpts::with_scheduled_at`].
pub use chrono;
pub use client::{
    Client, ClientBuilder, InsertBatchRequest, InsertManyItem, InsertManyRequest, InsertRequest,
    JobCancelRequest, JobCompleteRequest, JobCompleteTxRequest, JobDeleteManyRequest,
    JobDeleteRequest, JobGetRequest, JobListRequest, JobRetryRequest, JobUpdateRequest, Jobs,
    LocalQueues, MaintenanceConfig, QueueConfig, QueueGetRequest, QueueListRequest,
    QueuePauseRequest, QueueResumeRequest, QueueUpdateRequest, Queues, ResignRequest, Retention,
    RunHandle, Stopper,
};
#[cfg(feature = "postgres")]
pub(crate) use database::SchemaName;
pub use error::{
    BoxError, ConfigurationError, Error, ExtensionPhase, JobValidationError, Record, RuntimeError,
};
pub use event::{
    Event, EventKind, EventReceiver, EventRecvError, JobEvent, JobEventKind, JobStatistics,
    QueueEvent, QueueEventKind, SubscribeConfig,
};
pub use extension::{
    DefaultRetryPolicy, ErrorHandler, ErrorHandlerDecision, Extensions, Hook, InsertContext,
    InsertMiddleware, InsertNext, InsertedJob, JobCancelError, Metric, MetricName, PanicError,
    Plugin, RetryPolicy, WorkCancelled, WorkError, WorkMiddleware, WorkNext, WorkResult,
};
pub use job::{
    AttemptError, InsertBatch, InsertBatchResult, InsertOpts, InsertParams, InsertResult, Job,
    JobArgs, JobMetadata, JobRow, JobState, JobStateParseError, ScheduleOverride, UniqueOpts,
};
pub use periodic::{
    CronSchedule, CronScheduleParseError, CronTimeZone, IntervalSchedule, NeverSchedule,
    PeriodicJob, PeriodicJobHandle, PeriodicJobOpts, PeriodicJobs, PeriodicSchedule,
};
#[allow(unused_imports, reason = "backend-specific modules use a subset")]
pub(crate) use protocol::{
    NOTIFICATION_TOPIC_CONTROL, NOTIFICATION_TOPIC_INSERT, NOTIFICATION_TOPIC_LEADERSHIP,
};
pub use query::{
    JobDeleteManyParams, JobListCursor, JobListCursorError, JobListOrderBy, JobListParams,
    JobListResult, JobUpdateParams, SortDirection,
};
pub use queue::{Queue, QueueListParams, QueueSelector, QueueUpdateParams};
pub use riverqueue_macros::JobArgs;
/// Database migrations, re-exported from `riverqueue-migrate`.
///
/// Apply migrations before starting clients, for example with
/// [`migrate::PostgresMigrator`].
pub use riverqueue_migrate as migrate;
/// The `serde_json` version of the JSON values in River's API, such as job
/// metadata and outputs.
pub use serde_json;
/// The SQLx version River's pools and transactions come from.
///
/// River accepts SQLx pools and transactions directly, so applications must
/// use the same SQLx major version. Depend on SQLx through this re-export, or
/// pin the same version, to avoid mismatched `PgPool`/`SqlitePool` types. River
/// doesn't choose a TLS implementation; enable one of SQLx's TLS features, such
/// as `tls-rustls` or `tls-native-tls`, in your own dependency on SQLx if your
/// database connections use TLS.
pub use sqlx;
/// The `tokio-util` version of [`WorkContext::cancellation_token`]'s
/// [`CancellationToken`](tokio_util::sync::CancellationToken).
pub use tokio_util;
pub use worker::{WorkContext, WorkOutcome, Worker, WorkerTimeout, Workers};

/// Default maximum number of attempts for a job.
pub const MAX_ATTEMPTS_DEFAULT: i32 = 25;

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
