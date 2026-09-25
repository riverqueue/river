//! Assertions about inserted jobs, like River Go's `rivertest.RequireInserted`
//! family.
//!
//! Each assertion lists the jobs of the expected kinds, in every state, in
//! insertion (ID) order, and panics with a descriptive message when the
//! expectation isn't met, so it fails the calling test. The `_tx` variants
//! look inside a caller's open transaction, which is how to test code that
//! enqueues jobs transactionally before it commits.

use std::fmt::Write as _;

use chrono::{DateTime, DurationRound as _, TimeDelta, Utc};
use riverqueue::{
    Client, Job, JobArgs, JobListOrderBy, JobListParams, JobRow, JobState,
    database::DatabaseTransactionExecutor,
};

/// The most jobs an assertion reads, which is River's list limit.
const LIST_LIMIT: u32 = 10_000;

/// Expected properties of an inserted job, like River Go's
/// `rivertest.RequireInsertedOpts`.
///
/// Every property that's set must match. [`require_not_inserted`] fails only
/// when a job matches all of them.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct RequireInsertedOpts {
    max_attempts: Option<i16>,
    priority: Option<i16>,
    queue: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    state: Option<JobState>,
    tags: Option<Vec<String>>,
}

impl RequireInsertedOpts {
    /// Creates expectations that match any job.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Expects this maximum number of attempts.
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: i16) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Expects this priority.
    #[must_use]
    pub const fn priority(mut self, priority: i16) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Expects this queue.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Expects this scheduled time, compared at microsecond precision like
    /// the database stores it.
    #[must_use]
    pub const fn scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }

    /// Expects this state.
    #[must_use]
    pub const fn state(mut self, state: JobState) -> Self {
        self.state = Some(state);
        self
    }

    /// Expects exactly these tags, in order.
    #[must_use]
    pub fn tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags = Some(tags.into_iter().map(Into::into).collect());
        self
    }

    /// Compares each set property with `job`, returning a description of the
    /// differences (for [`require_inserted`]) or of the matches (for
    /// [`require_not_inserted`]), or `None` when the assertion holds.
    fn compare(&self, job: &JobRow, excluding: bool) -> Option<Vec<String>> {
        let mut failures = Vec::new();
        let mut check = |matches: bool, matched: String, differs: String| -> bool {
            match (matches, excluding) {
                // One differing property is enough for a job not to match.
                (false, true) => return false,
                (true, true) => failures.push(matched),
                (false, false) => failures.push(differs),
                (true, false) => {}
            }
            true
        };
        if let Some(expected) = self.max_attempts
            && !check(
                job.max_attempts == expected,
                format!("max attempts equal to excluded {expected}"),
                format!(
                    "max attempts {} not equal to expected {expected}",
                    job.max_attempts
                ),
            )
        {
            return None;
        }
        if let Some(expected) = self.priority
            && !check(
                job.priority == expected,
                format!("priority equal to excluded {expected}"),
                format!("priority {} not equal to expected {expected}", job.priority),
            )
        {
            return None;
        }
        if let Some(expected) = &self.queue
            && !check(
                &job.queue == expected,
                format!("queue equal to excluded '{expected}'"),
                format!("queue '{}' not equal to expected '{expected}'", job.queue),
            )
        {
            return None;
        }
        if let Some(expected) = self.scheduled_at {
            let micros = TimeDelta::microseconds(1);
            let expected = expected.duration_trunc(micros).unwrap_or(expected);
            let actual = job
                .scheduled_at
                .duration_trunc(micros)
                .unwrap_or(job.scheduled_at);
            if !check(
                actual == expected,
                format!("scheduled at equal to excluded {}", micro_time(expected)),
                format!(
                    "scheduled at {} not equal to expected {}",
                    micro_time(actual),
                    micro_time(expected)
                ),
            ) {
                return None;
            }
        }
        if let Some(expected) = self.state
            && !check(
                job.state == expected,
                format!("state equal to excluded '{}'", expected.as_str()),
                format!(
                    "state '{}' not equal to expected '{}'",
                    job.state.as_str(),
                    expected.as_str()
                ),
            )
        {
            return None;
        }
        if let Some(expected) = &self.tags
            && !check(
                &job.tags == expected,
                format!("tags equal to excluded {expected:?}"),
                format!("tags {:?} not equal to expected {expected:?}", job.tags),
            )
        {
            return None;
        }
        (!failures.is_empty()).then_some(failures)
    }
}

fn micro_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// One job expected by [`require_many_inserted`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ExpectedJob {
    kind: &'static str,
    opts: Option<RequireInsertedOpts>,
}

impl ExpectedJob {
    /// Expects a job of `A`'s kind.
    #[must_use]
    pub const fn of<A: JobArgs>() -> Self {
        Self {
            kind: A::KIND,
            opts: None,
        }
    }

    /// Adds expected properties for this job.
    #[must_use]
    pub fn opts(mut self, opts: RequireInsertedOpts) -> Self {
        self.opts = Some(opts);
        self
    }
}

fn params(kinds: impl IntoIterator<Item = &'static str>) -> JobListParams {
    JobListParams::default()
        .kinds(kinds)
        .states(JobState::ALL)
        .order_by(JobListOrderBy::Id)
        .limit(LIST_LIMIT)
}

async fn list(client: &Client, params: JobListParams) -> Vec<JobRow> {
    client
        .jobs()
        .list(params)
        .await
        .unwrap_or_else(|error| panic!("Internal failure: listing jobs failed: {error}"))
        .jobs
}

async fn list_tx<'t, E>(client: &'t Client, executor: E, params: JobListParams) -> Vec<JobRow>
where
    E: DatabaseTransactionExecutor<'t>,
{
    client
        .jobs()
        .list(params)
        .tx(executor)
        .await
        .unwrap_or_else(|error| panic!("Internal failure: listing jobs failed: {error}"))
        .jobs
}

fn check_inserted<A: JobArgs>(jobs: Vec<JobRow>, opts: Option<&RequireInsertedOpts>) -> Job<A> {
    let mut jobs = jobs.into_iter();
    let Some(job) = jobs.next() else {
        panic!("No jobs found with kind: {}", A::KIND);
    };
    assert!(
        jobs.next().is_none(),
        "More than one job found with kind: {} (you might want require_many_inserted instead)",
        A::KIND
    );
    if let Some(failures) = opts.and_then(|opts| opts.compare(&job, false)) {
        panic!("Job with kind '{}' {}", job.kind, failures.join(", "));
    }
    let args = job
        .decode_args::<A>()
        .unwrap_or_else(|error| panic!("Internal failure: decoding job args failed: {error}"));
    Job::new(args, job)
}

fn check_not_inserted(kind: &str, jobs: &[JobRow], opts: Option<&RequireInsertedOpts>) {
    let Some(opts) = opts else {
        assert!(
            jobs.is_empty(),
            "{} jobs found with kind, but expected to find none: {kind}",
            jobs.len()
        );
        return;
    };
    for job in jobs {
        if let Some(failures) = opts.compare(job, true) {
            panic!("Job with kind '{}' {}", job.kind, failures.join(", "));
        }
    }
}

fn check_many_inserted(expected: &[ExpectedJob], jobs: Vec<JobRow>) -> Vec<JobRow> {
    let expected_kinds = expected.iter().map(|job| job.kind).collect::<Vec<_>>();
    let actual_kinds = jobs.iter().map(|job| job.kind.as_str()).collect::<Vec<_>>();
    assert!(
        expected_kinds == actual_kinds,
        "Inserted jobs didn't match expectation; expected: {expected_kinds:?}, actual: {actual_kinds:?}"
    );
    for (index, (expected, job)) in expected.iter().zip(&jobs).enumerate() {
        if let Some(failures) = expected
            .opts
            .as_ref()
            .and_then(|opts| opts.compare(job, false))
        {
            let mut message = format!("Job with kind '{}'", job.kind);
            let _ = write!(message, " (expected job slice index {index})");
            panic!("{message} {}", failures.join(", "));
        }
    }
    jobs
}

/// Asserts that exactly one job of `A`'s kind was inserted, in any state,
/// and returns it with decoded arguments.
///
/// # Panics
///
/// Panics, failing the calling test, when there is no such job, when there
/// is more than one, when a property in `opts` doesn't match, or when the
/// jobs can't be listed or decoded.
pub async fn require_inserted<A: JobArgs>(
    client: &Client,
    opts: Option<&RequireInsertedOpts>,
) -> Job<A> {
    check_inserted(list(client, params([A::KIND])).await, opts)
}

/// Like [`require_inserted`], but reads through `executor`'s open
/// transaction.
///
/// # Panics
///
/// Panics under the same conditions as [`require_inserted`].
pub async fn require_inserted_tx<'t, A, E>(
    client: &'t Client,
    executor: E,
    opts: Option<&RequireInsertedOpts>,
) -> Job<A>
where
    A: JobArgs,
    E: DatabaseTransactionExecutor<'t>,
{
    check_inserted(list_tx(client, executor, params([A::KIND])).await, opts)
}

/// Asserts that jobs of exactly the expected kinds were inserted, in this
/// order and number, and returns them.
///
/// Only jobs of the expected kinds are considered, so a job of any other
/// kind doesn't affect the assertion. Expect a kind once for every job of it.
///
/// # Panics
///
/// Panics, failing the calling test, when the inserted kinds differ from the
/// expectation, when a property of an expected job doesn't match, or when
/// the jobs can't be listed.
pub async fn require_many_inserted(client: &Client, expected: &[ExpectedJob]) -> Vec<JobRow> {
    let jobs = list(client, params(expected.iter().map(|job| job.kind))).await;
    check_many_inserted(expected, jobs)
}

/// Like [`require_many_inserted`], but reads through `executor`'s open
/// transaction.
///
/// # Panics
///
/// Panics under the same conditions as [`require_many_inserted`].
pub async fn require_many_inserted_tx<'t, E>(
    client: &'t Client,
    executor: E,
    expected: &[ExpectedJob],
) -> Vec<JobRow>
where
    E: DatabaseTransactionExecutor<'t>,
{
    let jobs = list_tx(
        client,
        executor,
        params(expected.iter().map(|job| job.kind)),
    )
    .await;
    check_many_inserted(expected, jobs)
}

/// Asserts that no job of `A`'s kind was inserted or, with `opts`, that no
/// job of the kind matches every property set in them.
///
/// # Panics
///
/// Panics, failing the calling test, when a matching job exists or the jobs
/// can't be listed.
pub async fn require_not_inserted<A: JobArgs>(client: &Client, opts: Option<&RequireInsertedOpts>) {
    let jobs = list(client, params([A::KIND])).await;
    check_not_inserted(A::KIND, &jobs, opts);
}

/// Like [`require_not_inserted`], but reads through `executor`'s open
/// transaction.
///
/// # Panics
///
/// Panics under the same conditions as [`require_not_inserted`].
pub async fn require_not_inserted_tx<'t, A, E>(
    client: &'t Client,
    executor: E,
    opts: Option<&RequireInsertedOpts>,
) where
    A: JobArgs,
    E: DatabaseTransactionExecutor<'t>,
{
    let jobs = list_tx(client, executor, params([A::KIND])).await;
    check_not_inserted(A::KIND, &jobs, opts);
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use riverqueue::{
        InsertOpts, JobArgs,
        migrate::SqliteMigrator,
        sqlx::{
            SqlitePool,
            sqlite::{SqliteConnectOptions, SqlitePoolOptions},
        },
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone, Debug, Deserialize, JobArgs, PartialEq, Serialize)]
    #[river(kind = "require_first")]
    struct FirstArgs {
        value: i64,
    }

    #[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
    #[river(kind = "require_second")]
    struct SecondArgs {}

    struct TestBundle {
        client: Client,
        pool: SqlitePool,
    }

    async fn setup() -> TestBundle {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(":memory:"))
            .await
            .unwrap();
        SqliteMigrator::new(pool.clone())
            .migrate_up()
            .await
            .unwrap();
        let client = Client::builder(pool.clone()).build().unwrap();
        TestBundle { client, pool }
    }

    #[tokio::test]
    async fn require_inserted_returns_the_decoded_job() {
        let bundle = setup().await;
        bundle
            .client
            .insert(FirstArgs { value: 7 })
            .opts(InsertOpts::default().with_queue("custom").with_priority(2))
            .await
            .unwrap();

        let job = require_inserted::<FirstArgs>(
            &bundle.client,
            Some(
                &RequireInsertedOpts::new()
                    .queue("custom")
                    .priority(2)
                    .state(JobState::Available),
            ),
        )
        .await;
        assert_eq!(job.args, FirstArgs { value: 7 });
    }

    #[tokio::test]
    #[should_panic(expected = "No jobs found with kind: require_first")]
    async fn require_inserted_fails_without_a_job() {
        let bundle = setup().await;
        require_inserted::<FirstArgs>(&bundle.client, None).await;
    }

    #[tokio::test]
    #[should_panic(expected = "More than one job found with kind: require_first")]
    async fn require_inserted_fails_with_two_jobs() {
        let bundle = setup().await;
        for value in [1, 2] {
            bundle.client.insert(FirstArgs { value }).await.unwrap();
        }
        require_inserted::<FirstArgs>(&bundle.client, None).await;
    }

    #[tokio::test]
    #[should_panic(
        expected = "Job with kind 'require_first' priority 1 not equal to expected 3, queue 'default' not equal to expected 'other'"
    )]
    async fn require_inserted_reports_every_mismatch() {
        let bundle = setup().await;
        bundle.client.insert(FirstArgs { value: 1 }).await.unwrap();
        require_inserted::<FirstArgs>(
            &bundle.client,
            Some(&RequireInsertedOpts::new().queue("other").priority(3)),
        )
        .await;
    }

    #[tokio::test]
    async fn require_inserted_tx_sees_uncommitted_jobs() {
        let bundle = setup().await;
        let mut transaction = bundle.pool.begin().await.unwrap();
        bundle
            .client
            .insert(FirstArgs { value: 1 })
            .tx(&mut transaction)
            .await
            .unwrap();

        require_inserted_tx::<FirstArgs, _>(&bundle.client, &mut transaction, None).await;
        require_not_inserted_tx::<SecondArgs, _>(&bundle.client, &mut transaction, None).await;
        transaction.rollback().await.unwrap();
        require_not_inserted::<FirstArgs>(&bundle.client, None).await;
    }

    #[tokio::test]
    async fn require_many_inserted_matches_kinds_in_order() {
        let bundle = setup().await;
        bundle.client.insert(FirstArgs { value: 1 }).await.unwrap();
        bundle.client.insert(SecondArgs {}).await.unwrap();
        bundle.client.insert(FirstArgs { value: 2 }).await.unwrap();

        let jobs = require_many_inserted(
            &bundle.client,
            &[
                ExpectedJob::of::<FirstArgs>(),
                ExpectedJob::of::<SecondArgs>().opts(RequireInsertedOpts::new().queue("default")),
                ExpectedJob::of::<FirstArgs>(),
            ],
        )
        .await;
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    #[should_panic(
        expected = "Inserted jobs didn't match expectation; expected: [\"require_second\", \"require_first\"], actual: [\"require_first\", \"require_second\"]"
    )]
    async fn require_many_inserted_fails_on_a_different_order() {
        let bundle = setup().await;
        bundle.client.insert(FirstArgs { value: 1 }).await.unwrap();
        bundle.client.insert(SecondArgs {}).await.unwrap();

        require_many_inserted(
            &bundle.client,
            &[
                ExpectedJob::of::<SecondArgs>(),
                ExpectedJob::of::<FirstArgs>(),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn require_not_inserted_passes_when_any_property_differs() {
        let bundle = setup().await;
        bundle.client.insert(FirstArgs { value: 1 }).await.unwrap();

        require_not_inserted::<SecondArgs>(&bundle.client, None).await;
        require_not_inserted::<FirstArgs>(
            &bundle.client,
            Some(&RequireInsertedOpts::new().queue("default").priority(4)),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "Job with kind 'require_first' queue equal to excluded 'default'")]
    async fn require_not_inserted_fails_when_every_property_matches() {
        let bundle = setup().await;
        bundle.client.insert(FirstArgs { value: 1 }).await.unwrap();

        require_not_inserted::<FirstArgs>(
            &bundle.client,
            Some(&RequireInsertedOpts::new().queue("default")),
        )
        .await;
    }
}
