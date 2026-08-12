//! Leader-owned periodic job scheduling.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_json::Value;
use tokio::sync::Mutex;

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
            return Err(Error::InvalidJob(
                "periodic interval must be at least one second".to_owned(),
            ));
        }
        Ok(Self(interval))
    }
}

impl PeriodicSchedule for IntervalSchedule {
    fn next(&self, current: DateTime<Utc>) -> Option<DateTime<Utc>> {
        chrono::Duration::from_std(self.0)
            .ok()
            .and_then(|interval| current.checked_add_signed(interval))
    }
}

/// A cron-expression periodic schedule interpreted in UTC.
#[derive(Clone, Debug)]
pub struct CronSchedule(Schedule);

impl CronSchedule {
    /// Parses a cron expression using the `cron` crate's seconds-aware syntax.
    pub fn parse(expression: &str) -> Result<Self, Error> {
        Schedule::from_str(expression).map(Self).map_err(|error| {
            Error::InvalidJob(format!("invalid periodic cron expression: {error}"))
        })
    }
}

impl PeriodicSchedule for CronSchedule {
    fn next(&self, current: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.0.after(&current).next()
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
    pub id: Option<String>,
    /// Inserts once whenever this client becomes leader.
    pub run_on_start: bool,
}

#[derive(Clone)]
pub(crate) struct PeriodicInsert {
    pub(crate) encoded_args: Value,
    pub(crate) kind: &'static str,
    pub(crate) opts: InsertOpts,
    pub(crate) unique_fields: &'static [&'static str],
}

/// Type-erased periodic job definition.
#[derive(Clone)]
pub struct PeriodicJob {
    pub(crate) constructor: Arc<dyn Fn() -> Result<Option<PeriodicInsert>, Error> + Send + Sync>,
    pub(crate) opts: PeriodicJobOpts,
    pub(crate) schedule: Arc<dyn PeriodicSchedule>,
}

impl PeriodicJob {
    /// Creates a periodic job whose constructor may skip an occurrence by
    /// returning `None`.
    pub fn new<A, S, F>(schedule: S, constructor: F, opts: PeriodicJobOpts) -> Self
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
                    encoded_args: serde_json::to_value(args)?,
                    kind: A::KIND,
                    opts,
                    unique_fields: A::unique_fields(),
                }))
            }),
            opts,
            schedule: Arc::new(schedule),
        }
    }

    /// Creates a periodic job that always inserts using type defaults.
    pub fn with_defaults<A, S, F>(schedule: S, constructor: F, opts: PeriodicJobOpts) -> Self
    where
        A: JobArgs,
        F: Fn() -> A + Send + Sync + 'static,
        S: PeriodicSchedule,
    {
        Self::new(
            schedule,
            move || Some((constructor(), A::default_insert_opts())),
            opts,
        )
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
#[derive(Clone, Default)]
pub struct PeriodicJobs {
    pub(crate) registry: Arc<Mutex<PeriodicRegistry>>,
}

impl PeriodicJobs {
    pub(crate) fn from_jobs(jobs: Vec<PeriodicJob>) -> Result<Self, Error> {
        validate_jobs(&jobs, &HashSet::new())?;
        let mut registry = PeriodicRegistry::default();
        for job in jobs {
            registry.insert(job);
        }
        Ok(Self {
            registry: Arc::new(Mutex::new(registry)),
        })
    }

    /// Adds one periodic job and returns its removal handle.
    pub async fn add(&self, job: PeriodicJob) -> Result<PeriodicJobHandle, Error> {
        let mut registry = self.registry.lock().await;
        let ids = registry
            .entries
            .values()
            .filter_map(|entry| entry.job.opts.id.clone())
            .collect();
        validate_jobs(std::slice::from_ref(&job), &ids)?;
        Ok(registry.insert(job))
    }

    /// Adds many jobs atomically after validating their identifiers.
    pub async fn add_many(&self, jobs: Vec<PeriodicJob>) -> Result<Vec<PeriodicJobHandle>, Error> {
        let mut registry = self.registry.lock().await;
        let ids = registry
            .entries
            .values()
            .filter_map(|entry| entry.job.opts.id.clone())
            .collect();
        validate_jobs(&jobs, &ids)?;
        Ok(jobs.into_iter().map(|job| registry.insert(job)).collect())
    }

    /// Removes all configured periodic jobs.
    pub async fn clear(&self) {
        self.registry.lock().await.entries.clear();
    }

    /// Removes a job by handle.
    pub async fn remove(&self, handle: PeriodicJobHandle) -> bool {
        self.registry.lock().await.entries.remove(&handle).is_some()
    }

    /// Removes a job by identifier.
    pub async fn remove_by_id(&self, id: &str) -> bool {
        let mut registry = self.registry.lock().await;
        let handle = registry.entries.iter().find_map(|(handle, entry)| {
            (entry.job.opts.id.as_deref() == Some(id)).then_some(*handle)
        });
        handle.is_some_and(|handle| registry.entries.remove(&handle).is_some())
    }

    pub(crate) async fn reset_for_leadership(&self) {
        for entry in self.registry.lock().await.entries.values_mut() {
            entry.needs_initialization = true;
            entry.next_run = None;
        }
    }

    pub(crate) async fn run_due(&self, client: &Client, now: DateTime<Utc>) {
        let due = {
            let mut registry = self.registry.lock().await;
            let mut due = Vec::new();
            for entry in registry.entries.values_mut() {
                if entry.needs_initialization {
                    entry.needs_initialization = false;
                    entry.next_run = entry.job.schedule.next(now);
                    if entry.job.opts.run_on_start {
                        due.push((entry.job.clone(), now));
                    }
                    continue;
                }
                if let Some(target) = entry.next_run
                    && target <= now
                {
                    due.push((entry.job.clone(), target));
                    entry.next_run = entry.job.schedule.next(target);
                }
            }
            due
        };

        for (job, target) in due {
            let result = (job.constructor)();
            match result {
                Ok(Some(mut insert)) => {
                    insert.opts.scheduled_at.get_or_insert(target);
                    insert
                        .opts
                        .metadata
                        .insert("periodic".to_owned(), true.into());
                    if let Some(id) = &job.opts.id {
                        insert.opts.metadata.insert(
                            crate::METADATA_KEY_PERIODIC_JOB_ID.to_owned(),
                            id.clone().into(),
                        );
                    }
                    if let Err(error) = client.insert_periodic(insert).await {
                        tracing::error!(error = %error, "River periodic job insertion failed");
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(error = %error, "River periodic job constructor failed");
                }
            }
        }
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
                return Err(Error::InvalidJob(
                    "periodic job ID cannot be empty".to_owned(),
                ));
            }
            if !ids.insert(id.clone()) {
                return Err(Error::InvalidJob(format!(
                    "periodic job with ID already registered: {id}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Deserialize, Serialize)]
    struct TestArgs;

    impl JobArgs for TestArgs {
        const KIND: &'static str = "periodic_test";
    }

    fn job(id: &str) -> PeriodicJob {
        PeriodicJob::with_defaults(
            NeverSchedule,
            || TestArgs,
            PeriodicJobOpts {
                id: Some(id.to_owned()),
                run_on_start: false,
            },
        )
    }

    #[tokio::test]
    async fn dynamic_registration_is_atomic_and_removable() {
        let jobs = PeriodicJobs::default();
        let first = jobs.add(job("first")).await.unwrap();
        let added = jobs
            .add_many(vec![job("second"), job("third")])
            .await
            .unwrap();
        assert_eq!(added.len(), 2);
        assert_eq!(jobs.registry.lock().await.entries.len(), 3);

        assert!(
            jobs.add_many(vec![job("fourth"), job("second")])
                .await
                .is_err()
        );
        assert_eq!(jobs.registry.lock().await.entries.len(), 3);

        assert!(jobs.remove(first).await);
        assert!(!jobs.remove(first).await);
        assert!(jobs.remove_by_id("second").await);
        assert!(!jobs.remove_by_id("missing").await);

        jobs.clear().await;
        assert!(jobs.registry.lock().await.entries.is_empty());
    }

    #[test]
    fn static_registration_rejects_invalid_identifiers() {
        assert!(PeriodicJobs::from_jobs(vec![job("duplicate"), job("duplicate")]).is_err());
        assert!(PeriodicJobs::from_jobs(vec![job("")]).is_err());
    }
}
