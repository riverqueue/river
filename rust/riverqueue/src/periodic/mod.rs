//! Leader-owned periodic job scheduling.

mod cron;

pub use self::cron::{CronSchedule, CronScheduleParseError, CronTimeZone};

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::{Client, Error, InsertOpts, JobArgs};

/// A schedule that calculates the next periodic run after a UTC instant.
pub trait PeriodicSchedule: Send + Sync + 'static {
    /// Returns the next run time, or `None` to disable future runs.
    fn next(&self, current: DateTime<Utc>) -> Option<DateTime<Utc>>;
}

/// A fixed-duration periodic schedule.
#[derive(Clone, Copy, Debug)]
pub struct IntervalSchedule(Duration);

impl IntervalSchedule {
    /// Creates a fixed schedule. Intervals shorter than one second are rejected.
    pub fn new(interval: Duration) -> Result<Self, Error> {
        if interval < Duration::from_secs(1) {
            return Err(Error::invalid_job_context(
                "periodic job",
                "periodic interval must be at least one second".to_owned(),
            ));
        }
        Ok(Self(interval))
    }

    /// Returns the fixed interval between occurrences.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.0
    }
}

impl PeriodicSchedule for IntervalSchedule {
    fn next(&self, current: DateTime<Utc>) -> Option<DateTime<Utc>> {
        chrono::Duration::from_std(self.0)
            .ok()
            .and_then(|interval| current.checked_add_signed(interval))
    }
}

/// A schedule that never runs.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeverSchedule;

impl PeriodicSchedule for NeverSchedule {
    fn next(&self, _current: DateTime<Utc>) -> Option<DateTime<Utc>> {
        None
    }
}

/// Options for a periodic job.
#[derive(Clone, Debug, Default)]
pub struct PeriodicJobOpts {
    /// Optional identifier, unique within one client.
    pub(crate) id: Option<String>,
    /// Inserts once whenever this client becomes leader.
    pub(crate) run_on_start: bool,
}

impl PeriodicJobOpts {
    /// Creates periodic-job options with no ID and no initial run.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            id: None,
            run_on_start: false,
        }
    }

    /// Returns the optional identifier.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Returns whether an occurrence is inserted whenever leadership begins.
    #[must_use]
    pub const fn runs_on_start(&self) -> bool {
        self.run_on_start
    }

    /// Inserts once whenever this client becomes leader.
    #[must_use]
    pub const fn run_on_start(mut self) -> Self {
        self.run_on_start = true;
        self
    }

    /// Sets an identifier unique within one client.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}

#[derive(Clone)]
pub(crate) struct PeriodicInsert {
    pub(crate) defaults: InsertOpts,
    pub(crate) encoded_args: Box<serde_json::value::RawValue>,
    pub(crate) kind: &'static str,
    pub(crate) opts: InsertOpts,
    pub(crate) unique_fields: &'static [&'static [&'static str]],
}

/// Type-erased periodic job definition.
#[derive(Clone)]
pub struct PeriodicJob {
    pub(crate) constructor: Arc<dyn Fn() -> Result<Option<PeriodicInsert>, Error> + Send + Sync>,
    pub(crate) opts: PeriodicJobOpts,
    pub(crate) schedule: Arc<dyn PeriodicSchedule>,
}

impl PeriodicJob {
    /// Creates a periodic job that inserts one job using its type defaults on
    /// every scheduled occurrence.
    pub fn new<A, S, F>(schedule: S, constructor: F) -> Self
    where
        A: JobArgs,
        F: Fn() -> A + Send + Sync + 'static,
        S: PeriodicSchedule,
    {
        Self::with_options(schedule, constructor, PeriodicJobOpts::new())
    }

    /// Creates a periodic job with registration options.
    pub fn with_options<A, S, F>(schedule: S, constructor: F, opts: PeriodicJobOpts) -> Self
    where
        A: JobArgs,
        F: Fn() -> A + Send + Sync + 'static,
        S: PeriodicSchedule,
    {
        Self::conditional_with_options(
            schedule,
            move || Some((constructor(), InsertOpts::default())),
            opts,
        )
    }

    /// Creates a periodic job whose constructor can skip an occurrence or set
    /// per-occurrence insertion options.
    pub fn conditional<A, S, F>(schedule: S, constructor: F) -> Self
    where
        A: JobArgs,
        F: Fn() -> Option<(A, InsertOpts)> + Send + Sync + 'static,
        S: PeriodicSchedule,
    {
        Self::conditional_with_options(schedule, constructor, PeriodicJobOpts::new())
    }

    /// Creates a conditional periodic job with registration options.
    pub fn conditional_with_options<A, S, F>(
        schedule: S,
        constructor: F,
        opts: PeriodicJobOpts,
    ) -> Self
    where
        A: JobArgs,
        F: Fn() -> Option<(A, InsertOpts)> + Send + Sync + 'static,
        S: PeriodicSchedule,
    {
        Self {
            constructor: Arc::new(move || {
                let Some((args, opts)) = constructor() else {
                    return Ok(None);
                };
                Ok(Some(PeriodicInsert {
                    defaults: A::default_insert_opts(),
                    encoded_args: crate::encoding::encode_args(&args)?,
                    kind: A::KIND,
                    opts,
                    unique_fields: A::unique_fields(),
                }))
            }),
            opts,
            schedule: Arc::new(schedule),
        }
    }
}

impl fmt::Debug for PeriodicJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeriodicJob")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

/// Opaque handle used to remove a dynamically configured periodic job.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PeriodicJobHandle(u64);

pub(crate) struct PeriodicEntry {
    pub(crate) job: PeriodicJob,
    pub(crate) next_run: Option<DateTime<Utc>>,
    pub(crate) needs_initialization: bool,
}

#[derive(Default)]
pub(crate) struct PeriodicRegistry {
    pub(crate) entries: HashMap<PeriodicJobHandle, PeriodicEntry>,
    next_handle: u64,
}

/// Dynamically configurable periodic jobs for a client.
#[derive(Clone)]
pub struct PeriodicJobs {
    changed: Arc<Notify>,
    leader_election_disabled: bool,
    pub(crate) registry: Arc<Mutex<PeriodicRegistry>>,
}

/// Result of one scheduling pass.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RunDueOutcome {
    /// At least one due occurrence failed to insert and stays due.
    pub(crate) insert_failed: bool,
}

/// Jobs due within this margin are inserted in the current pass, like Go's
/// enqueuer, which also keeps each occurrence's original scheduled time.
const DUE_MARGIN: chrono::Duration = chrono::Duration::milliseconds(100);

impl fmt::Debug for PeriodicJobs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeriodicJobs")
            .field("len", &self.lock().entries.len())
            .finish_non_exhaustive()
    }
}

impl PeriodicJobs {
    pub(crate) fn from_jobs(
        jobs: Vec<PeriodicJob>,
        leader_election_disabled: bool,
    ) -> Result<Self, Error> {
        validate_jobs(&jobs, &HashSet::new())?;
        let mut registry = PeriodicRegistry::default();
        for job in jobs {
            registry.insert(job);
        }
        Ok(Self {
            changed: Arc::new(Notify::new()),
            leader_election_disabled,
            registry: Arc::new(Mutex::new(registry)),
        })
    }

    /// Locks the registry. It's never held across an await, and every update
    /// leaves it consistent, so a poisoned lock is still usable.
    fn lock(&self) -> MutexGuard<'_, PeriodicRegistry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Completes after the next registration change.
    pub(crate) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    fn notify_changed(&self) {
        self.changed.notify_waiters();
    }

    /// Rejects additions to a client that never leads, which would never
    /// enqueue them.
    fn ensure_electable(&self) -> Result<(), Error> {
        if self.leader_election_disabled {
            return Err(Error::configuration(
                "periodic jobs can't be added when leader election is disabled".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns the earliest scheduled occurrence, or now when a job still
    /// needs its first schedule computed.
    pub(crate) fn next_run_at(&self) -> Option<DateTime<Utc>> {
        let registry = self.lock();
        let mut next: Option<DateTime<Utc>> = None;
        for entry in registry.entries.values() {
            let candidate = if entry.needs_initialization {
                Some(Utc::now())
            } else {
                entry.next_run
            };
            if let Some(candidate) = candidate {
                next = Some(next.map_or(candidate, |next| next.min(candidate)));
            }
        }
        next
    }

    /// Adds one periodic job and returns its removal handle.
    ///
    /// Adding or removing periodic jobs affects only this client, which
    /// enqueues them only while it's the elected leader. To make sure a
    /// periodic job is fully enabled or disabled, change it on every client
    /// eligible for leader election across all processes.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the job's identifier is invalid or
    /// already configured, or when the client was built with
    /// [`ClientBuilder::without_leader_election`](crate::ClientBuilder::without_leader_election).
    pub fn add(&self, job: PeriodicJob) -> Result<PeriodicJobHandle, Error> {
        self.ensure_electable()?;
        let mut registry = self.lock();
        let ids = registry
            .entries
            .values()
            .filter_map(|entry| entry.job.opts.id.clone())
            .collect();
        validate_jobs(std::slice::from_ref(&job), &ids)?;
        let handle = registry.insert(job);
        drop(registry);
        self.notify_changed();
        Ok(handle)
    }

    /// Adds many jobs atomically after validating their identifiers.
    ///
    /// Like [`PeriodicJobs::add`], this affects only this client.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`PeriodicJobs::add`]; no job is added
    /// when any is rejected.
    pub fn add_many(&self, jobs: Vec<PeriodicJob>) -> Result<Vec<PeriodicJobHandle>, Error> {
        self.ensure_electable()?;
        let mut registry = self.lock();
        let ids = registry
            .entries
            .values()
            .filter_map(|entry| entry.job.opts.id.clone())
            .collect();
        validate_jobs(&jobs, &ids)?;
        let handles = jobs.into_iter().map(|job| registry.insert(job)).collect();
        drop(registry);
        self.notify_changed();
        Ok(handles)
    }

    /// Removes all configured periodic jobs.
    pub fn clear(&self) {
        self.lock().entries.clear();
        self.notify_changed();
    }

    /// Removes a job by handle.
    pub fn remove(&self, handle: PeriodicJobHandle) -> bool {
        let removed = self.lock().entries.remove(&handle).is_some();
        self.notify_changed();
        removed
    }

    /// Removes a job by identifier.
    pub fn remove_by_id(&self, id: &str) -> bool {
        let mut registry = self.lock();
        let handle = registry.entries.iter().find_map(|(handle, entry)| {
            (entry.job.opts.id.as_deref() == Some(id)).then_some(*handle)
        });
        let removed = handle.is_some_and(|handle| registry.entries.remove(&handle).is_some());
        drop(registry);
        self.notify_changed();
        removed
    }

    pub(crate) fn reset_for_leadership(&self) {
        for entry in self.lock().entries.values_mut() {
            entry.needs_initialization = true;
            entry.next_run = None;
        }
    }

    /// Inserts every occurrence due by `now` plus a small margin. Newly added
    /// jobs are scheduled from `now` and, when configured, inserted once.
    ///
    /// An occurrence whose insert fails keeps its scheduled time and is
    /// retried by the caller instead of being skipped as Go does.
    pub(crate) async fn run_due(&self, client: &Client, now: DateTime<Utc>) -> RunDueOutcome {
        struct DueJob {
            advance_handle: Option<PeriodicJobHandle>,
            job: PeriodicJob,
            target: DateTime<Utc>,
        }

        let due = {
            let mut registry = self.lock();
            let mut due = Vec::new();
            for (handle, entry) in &mut registry.entries {
                if entry.needs_initialization {
                    entry.needs_initialization = false;
                    entry.next_run = entry.job.schedule.next(now);
                    if entry.job.opts.run_on_start {
                        due.push(DueJob {
                            advance_handle: None,
                            job: entry.job.clone(),
                            target: now,
                        });
                    }
                    continue;
                }
                if let Some(target) = entry.next_run
                    && target < now + DUE_MARGIN
                {
                    due.push(DueJob {
                        advance_handle: Some(*handle),
                        job: entry.job.clone(),
                        target,
                    });
                }
            }
            due
        };

        let mut outcome = RunDueOutcome::default();
        for due_job in due {
            let result = (due_job.job.constructor)();
            let advance = match result {
                Ok(Some(insert)) => {
                    let mut opts = InsertOpts::resolve(
                        client.default_max_attempts(),
                        insert.defaults.clone(),
                        insert.opts.clone(),
                    );
                    opts.metadata
                        .insert("periodic", true)
                        .expect("boolean metadata serializes");
                    if let Some(id) = &due_job.job.opts.id {
                        opts.metadata
                            .insert(crate::METADATA_KEY_PERIODIC_JOB_ID, id)
                            .expect("string metadata serializes");
                    }
                    if let Err(error) = client.insert_periodic(insert, opts, due_job.target).await {
                        tracing::error!(error = %error, "River periodic job insertion failed");
                        outcome.insert_failed = true;
                        false
                    } else {
                        true
                    }
                }
                Ok(None) => true,
                Err(error) => {
                    tracing::error!(error = %error, "River periodic job constructor failed");
                    true
                }
            };

            if advance && let Some(handle) = due_job.advance_handle {
                let mut registry = self.lock();
                if let Some(entry) = registry.entries.get_mut(&handle)
                    && !entry.needs_initialization
                    && entry.next_run == Some(due_job.target)
                {
                    entry.next_run = entry.job.schedule.next(due_job.target);
                }
            }
        }
        outcome
    }
}

impl PeriodicRegistry {
    fn insert(&mut self, job: PeriodicJob) -> PeriodicJobHandle {
        self.next_handle = self.next_handle.wrapping_add(1);
        let handle = PeriodicJobHandle(self.next_handle);
        self.entries.insert(
            handle,
            PeriodicEntry {
                job,
                needs_initialization: true,
                next_run: None,
            },
        );
        handle
    }
}

fn validate_jobs(jobs: &[PeriodicJob], existing_ids: &HashSet<String>) -> Result<(), Error> {
    let mut ids = existing_ids.clone();
    for job in jobs {
        if let Some(id) = &job.opts.id {
            if id.is_empty() {
                return Err(Error::invalid_job_context(
                    "periodic job",
                    "periodic job ID cannot be empty".to_owned(),
                ));
            }
            if !ids.insert(id.clone()) {
                return Err(Error::invalid_job_context(
                    "periodic job",
                    format!("periodic job with ID already registered: {id}"),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "postgres")]
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use serde::{Deserialize, Serialize};
    #[cfg(feature = "postgres")]
    use sqlx::postgres::PgPoolOptions;

    #[derive(Clone, Deserialize, Serialize)]
    struct TestArgs;

    impl JobArgs for TestArgs {
        const KIND: &'static str = "periodic_test";
    }

    fn job(id: &str) -> PeriodicJob {
        PeriodicJob::with_options(
            NeverSchedule,
            || TestArgs,
            PeriodicJobOpts::new().with_id(id),
        )
    }

    #[test]
    fn dynamic_registration_is_atomic_and_removable() {
        let jobs = PeriodicJobs::from_jobs(Vec::new(), false).unwrap();
        let first = jobs.add(job("first")).unwrap();
        let added = jobs.add_many(vec![job("second"), job("third")]).unwrap();
        assert_eq!(added.len(), 2);
        assert_eq!(jobs.lock().entries.len(), 3);

        assert!(jobs.add_many(vec![job("fourth"), job("second")]).is_err());
        assert_eq!(jobs.lock().entries.len(), 3);

        assert!(jobs.remove(first));
        assert!(!jobs.remove(first));
        assert!(jobs.remove_by_id("second"));
        assert!(!jobs.remove_by_id("missing"));

        jobs.clear();
        assert!(jobs.lock().entries.is_empty());
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn insert_failure_does_not_advance_next_run() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let constructed = Arc::clone(&attempts);
        let jobs = PeriodicJobs::from_jobs(
            vec![PeriodicJob::new(
                IntervalSchedule::new(Duration::from_secs(1)).unwrap(),
                move || {
                    constructed.fetch_add(1, Ordering::SeqCst);
                    TestArgs
                },
            )],
            false,
        )
        .unwrap();
        // Nothing listens on this port, so every insertion fails.
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://127.0.0.1:1/river_periodic_test")
            .unwrap();
        let client = Client::builder(pool).build().unwrap();
        let now = Utc::now();
        let target = now + chrono::Duration::seconds(1);

        jobs.run_due(&client, now).await;
        jobs.run_due(&client, target).await;
        jobs.run_due(&client, target).await;

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let registry = jobs.lock();
        assert_eq!(
            registry.entries.values().next().unwrap().next_run,
            Some(target)
        );
    }

    #[test]
    fn static_registration_rejects_invalid_identifiers() {
        assert!(PeriodicJobs::from_jobs(vec![job("duplicate"), job("duplicate")], false).is_err());
        assert!(PeriodicJobs::from_jobs(vec![job("")], false).is_err());
    }
}
