//! Job list ordering and keyset pagination on every backend.
//!
//! Each scenario runs against PostgreSQL (in a unique schema, failing rather
//! than skipping when `RIVER_RUST_DATABASE_URL` is unset) and SQLite (in a
//! temporary file). PostgreSQL scenarios build only with `postgres-tests`.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

use chrono::{DateTime, Duration, TimeZone, Utc};
use riverqueue::{Client, JobListCursor, JobListOrderBy, JobListParams, JobState, SortDirection};

/// A job to seed, with the time fields job lists order by.
#[derive(Clone, Copy)]
struct Seed {
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    state: JobState,
}

impl Seed {
    fn new(state: JobState) -> Self {
        Self {
            attempted_at: None,
            finalized_at: None,
            scheduled_at: now(),
            state,
        }
    }

    const fn attempted(mut self, at: DateTime<Utc>) -> Self {
        self.attempted_at = Some(at);
        self
    }

    const fn finalized(mut self, at: DateTime<Utc>) -> Self {
        self.finalized_at = Some(at);
        self
    }

    const fn scheduled(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = at;
        self
    }
}

/// A fixed time with millisecond precision, which both backends store
/// exactly.
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 9, 12, 0, 0).unwrap() + Duration::milliseconds(123)
}

fn at(offset: Duration) -> DateTime<Utc> {
    now() + offset
}

/// Lists jobs with `order_by` and `states` in both directions, and checks
/// the full listing and a one-job-at-a-time pagination against `want_order`
/// (indexes into `seeds`, ascending). Pages alternate serialized cursors and
/// cursors built from the page's job.
async fn assert_pagination(
    client: &Client,
    ids: &[i64],
    states: &[JobState],
    order_by: JobListOrderBy,
    want_order: &[usize],
) {
    for direction in [SortDirection::Ascending, SortDirection::Descending] {
        let mut want = want_order
            .iter()
            .map(|&index| ids[index])
            .collect::<Vec<_>>();
        if direction == SortDirection::Descending {
            want.reverse();
        }
        let params = JobListParams::default()
            .states(states.iter().copied())
            .order_by(order_by)
            .direction(direction);

        let listed = client.jobs().list(params.clone()).await.unwrap();
        assert_eq!(
            listed.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
            want,
            "{direction:?} listing"
        );

        let mut got = Vec::new();
        let mut page_params = params.clone().limit(1);
        for page in 0.. {
            assert!(
                page <= want.len(),
                "{direction:?}: too many pages; got IDs so far: {got:?}"
            );
            let result = client.jobs().list(page_params).await.unwrap();
            let Some(job) = result.jobs.first() else {
                break;
            };
            got.push(job.id);
            let cursor = if page % 2 == 0 {
                let encoded = result.last_cursor.expect("nonempty page").encode();
                JobListCursor::decode(&encoded).unwrap()
            } else {
                JobListCursor::from_job(job, &params).unwrap()
            };
            page_params = params.clone().limit(1).after(cursor);
        }
        assert_eq!(got, want, "{direction:?} pagination");
    }
}

/// Defines each scenario for one backend's `Fixture`.
macro_rules! scenarios {
    () => {
        /// Time ordering over running and available jobs orders every job
        /// by `attempted_at`, which is null for jobs never run.
        #[tokio::test(flavor = "multi_thread")]
        async fn mixed_states_page_by_attempted_at() {
            let fixture = Fixture::new().await;
            let ids = fixture
                .insert(&[
                    Seed::new(JobState::Running).attempted(at(Duration::seconds(2))),
                    Seed::new(JobState::Available),
                    Seed::new(JobState::Running).attempted(at(Duration::seconds(1))),
                    Seed::new(JobState::Available)
                        .attempted(at(-Duration::hours(1)))
                        .scheduled(at(Duration::hours(1))),
                    Seed::new(JobState::Running).attempted(at(Duration::seconds(1))),
                    Seed::new(JobState::Available),
                ])
                .await;

            assert_pagination(
                &fixture.client,
                &ids,
                &[JobState::Running, JobState::Available],
                JobListOrderBy::Time,
                &[3, 2, 4, 0, 1, 5],
            )
            .await;

            fixture.cleanup().await;
        }

        /// Time ordering over completed and available jobs orders every job
        /// by `finalized_at`, which is null for the available ones.
        #[tokio::test(flavor = "multi_thread")]
        async fn mixed_states_page_by_finalized_at() {
            let fixture = Fixture::new().await;
            let ids = fixture
                .insert(&[
                    Seed::new(JobState::Available).scheduled(at(-Duration::hours(2))),
                    Seed::new(JobState::Completed)
                        .finalized(at(Duration::seconds(1)))
                        .scheduled(at(-Duration::hours(3))),
                    Seed::new(JobState::Completed)
                        .finalized(now())
                        .scheduled(at(-Duration::hours(1))),
                    Seed::new(JobState::Available).scheduled(at(-Duration::hours(4))),
                    Seed::new(JobState::Completed)
                        .finalized(at(Duration::seconds(1)))
                        .scheduled(at(-Duration::hours(5))),
                ])
                .await;

            assert_pagination(
                &fixture.client,
                &ids,
                &[JobState::Completed, JobState::Available],
                JobListOrderBy::Time,
                &[2, 1, 4, 0, 3],
            )
            .await;

            fixture.cleanup().await;
        }

        /// Time ordering over available and cancelled jobs orders every job
        /// by `scheduled_at`, even though a cancelled job's own time field
        /// is `finalized_at`.
        #[tokio::test(flavor = "multi_thread")]
        async fn mixed_states_page_by_scheduled_at() {
            let fixture = Fixture::new().await;
            let ids = fixture
                .insert(&[
                    Seed::new(JobState::Available).scheduled(at(Duration::seconds(1))),
                    Seed::new(JobState::Cancelled)
                        .finalized(at(-Duration::hours(1)))
                        .scheduled(at(Duration::seconds(2))),
                    Seed::new(JobState::Available).scheduled(at(Duration::seconds(3))),
                    Seed::new(JobState::Cancelled)
                        .finalized(at(Duration::hours(1)))
                        .scheduled(at(Duration::seconds(2))),
                    Seed::new(JobState::Available).scheduled(at(Duration::seconds(4))),
                ])
                .await;

            assert_pagination(
                &fixture.client,
                &ids,
                &[JobState::Available, JobState::Cancelled],
                JobListOrderBy::Time,
                &[0, 1, 3, 2, 4],
            )
            .await;

            fixture.cleanup().await;
        }

        /// Time ordering without a state filter lists every state by
        /// `scheduled_at`, the field for available jobs.
        #[tokio::test(flavor = "multi_thread")]
        async fn time_order_without_states_pages_by_scheduled_at() {
            let fixture = Fixture::new().await;
            let ids = fixture
                .insert(&[
                    Seed::new(JobState::Available).scheduled(at(Duration::seconds(3))),
                    Seed::new(JobState::Running)
                        .attempted(at(-Duration::hours(1)))
                        .scheduled(at(Duration::seconds(1))),
                    Seed::new(JobState::Completed)
                        .finalized(at(-Duration::hours(1)))
                        .scheduled(at(Duration::seconds(2))),
                    Seed::new(JobState::Scheduled).scheduled(at(Duration::seconds(1))),
                ])
                .await;

            assert_pagination(
                &fixture.client,
                &ids,
                &[],
                JobListOrderBy::Time,
                &[1, 3, 2, 0],
            )
            .await;

            fixture.cleanup().await;
        }
    };
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use super::*;
    use crate::support::PostgresSchema;

    struct Fixture {
        client: Client,
        schema: PostgresSchema,
    }

    impl Fixture {
        async fn new() -> Self {
            let schema = PostgresSchema::new("river_job_list").await;
            let client = Client::builder(
                riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                    .schema(schema.schema.clone()),
            )
            .build()
            .unwrap();
            Self { client, schema }
        }

        async fn insert(&self, seeds: &[Seed]) -> Vec<i64> {
            let sql = format!(
                "INSERT INTO {} (args, attempted_at, finalized_at, kind, max_attempts, \
                 scheduled_at, state) \
                 VALUES ('{{}}', $1, $2, 'job_list', 25, $3, $4::text::{}) RETURNING id",
                self.schema.table("river_job"),
                self.schema.table("river_job_state"),
            );
            let mut ids = Vec::with_capacity(seeds.len());
            for seed in seeds {
                let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                    .bind(seed.attempted_at)
                    .bind(seed.finalized_at)
                    .bind(seed.scheduled_at)
                    .bind(seed.state.as_str())
                    .fetch_one(&self.schema.pool)
                    .await
                    .unwrap();
                ids.push(id);
            }
            ids
        }

        async fn cleanup(self) {
            self.schema.cleanup().await;
        }
    }

    scenarios!();
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use sqlx::SqlitePool;

    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    struct Fixture {
        client: Client,
        path: std::path::PathBuf,
        pool: SqlitePool,
    }

    impl Fixture {
        async fn new() -> Self {
            let (pool, path) = sqlite_file_pool(4).await;
            let client = Client::builder(pool.clone()).build().unwrap();
            Self { client, path, pool }
        }

        async fn insert(&self, seeds: &[Seed]) -> Vec<i64> {
            let time = |time: DateTime<Utc>| time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            let mut ids = Vec::with_capacity(seeds.len());
            for seed in seeds {
                let id: i64 = sqlx::query_scalar(
                    "INSERT INTO river_job (args, attempt, attempted_at, attempted_by, \
                     created_at, errors, finalized_at, kind, max_attempts, metadata, priority, \
                     queue, scheduled_at, state, tags) \
                     VALUES (jsonb('{}'), 0, ?, jsonb('[]'), ?, jsonb('[]'), ?, 'job_list', 25, \
                     jsonb('{}'), 1, 'default', ?, ?, jsonb('[]')) RETURNING id",
                )
                .bind(seed.attempted_at.map(time))
                .bind(time(now()))
                .bind(seed.finalized_at.map(time))
                .bind(time(seed.scheduled_at))
                .bind(seed.state.as_str())
                .fetch_one(&self.pool)
                .await
                .unwrap();
                ids.push(id);
            }
            ids
        }

        async fn cleanup(self) {
            sqlite_cleanup(self.pool, self.path).await;
        }
    }

    scenarios!();
}
