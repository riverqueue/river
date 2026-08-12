//! River's Rust command-line utilities.

use std::{
    convert::Infallible,
    env,
    error::Error as StdError,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use riverqueue::{
    Client, InsertOpts, Job, JobArgs, QueueConfig, SchemaName, WorkContext, WorkOutcome, Worker,
    WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio_util::sync::CancellationToken;

const DEFAULT_BACKLOG: u64 = 75_000;
const DEFAULT_BATCH_SIZE: usize = 5_000;
const DEFAULT_MAX_CONNECTIONS: u32 = 50;
const DEFAULT_MAX_WORKERS: usize = 2_000;
const ITERATION_PERIOD: Duration = Duration::from_secs(2);

const HELP: &str = r"River for Rust command-line utilities

Usage:
  riverqueue bench [options]

The benchmark truncates the selected River job table, optionally vacuums it,
then inserts and works no-op jobs while reporting rough throughput and p95
end-to-end latency. Use only a disposable development or benchmark database.

Options:
      --database-url URL       PostgreSQL URL (or set DATABASE_URL)
      --schema NAME            River schema (default: current schema)
      --duration DURATION      Stop after a Go-style duration such as 30s or 5m
  -n, --num-total-jobs COUNT   Insert COUNT jobs, then work them all
      --backlog COUNT          Target continuous-mode backlog (default: 75000)
      --batch-size COUNT       COPY insertion batch size (default: 5000)
      --max-connections COUNT  SQLx pool size (default: 50)
      --max-workers COUNT      Concurrent workers (default: 2000)
      --skip-vacuum            Truncate without VACUUM FULL
  -h, --help                   Print help
  -V, --version                Print version

With neither --duration nor --num-total-jobs, the benchmark runs until Ctrl-C.
The two stopping options are mutually exclusive.
";

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "benchmark")]
struct BenchmarkArgs {
    number: u64,
}

struct BenchmarkWorker;

#[async_trait]
impl Worker<BenchmarkArgs> for BenchmarkWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<BenchmarkArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BenchOptions {
    backlog: u64,
    batch_size: usize,
    database_url: String,
    duration: Option<Duration>,
    max_connections: u32,
    max_workers: usize,
    num_total_jobs: Option<u64>,
    schema: SchemaName,
    vacuum: bool,
}

enum Command {
    Bench(BenchOptions),
    Help,
    Version,
}

#[derive(Clone, Copy, Debug)]
struct DatabaseCounts {
    failed: i64,
    worked: u64,
}

#[derive(Clone, Copy, Debug)]
struct DatabaseStatistics {
    failed: i64,
    p95_seconds: Option<f64>,
    worked: u64,
}

struct Producer {
    backlog: u64,
    batch_size: usize,
    cancellation: CancellationToken,
    client: Client,
    inserted: Arc<AtomicU64>,
    next_number: u64,
    pool: PgPool,
    schema: SchemaName,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError + Send + Sync>> {
    match parse_command(env::args().skip(1), env::var("DATABASE_URL").ok())? {
        Command::Bench(options) => run_benchmark(options).await?,
        Command::Help => print!("{HELP}"),
        Command::Version => println!("riverqueue {}", env!("CARGO_PKG_VERSION")),
    }
    Ok(())
}

fn parse_command(
    arguments: impl IntoIterator<Item = String>,
    database_url_env: Option<String>,
) -> Result<Command, io::Error> {
    let mut arguments = arguments.into_iter();
    let Some(command) = arguments.next() else {
        return Ok(Command::Help);
    };
    if matches!(command.as_str(), "-h" | "--help") {
        return Ok(Command::Help);
    }
    if matches!(command.as_str(), "-V" | "--version") {
        return Ok(Command::Version);
    }
    if command != "bench" {
        return Err(invalid_input(format!(
            "unknown command {command:?}\n\n{HELP}"
        )));
    }

    let mut backlog = DEFAULT_BACKLOG;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut database_url = None;
    let mut duration = None;
    let mut max_connections = DEFAULT_MAX_CONNECTIONS;
    let mut max_workers = DEFAULT_MAX_WORKERS;
    let mut num_total_jobs = None;
    let mut schema = SchemaName::current();
    let mut vacuum = true;

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--backlog" => {
                backlog = parse_positive(&take_value(&mut arguments, "--backlog")?, "backlog")?;
            }
            "--batch-size" => {
                batch_size =
                    parse_positive(&take_value(&mut arguments, "--batch-size")?, "batch size")?;
            }
            "--database-url" => {
                database_url = Some(take_value(&mut arguments, "--database-url")?);
            }
            "--duration" => {
                duration = Some(parse_duration(&take_value(&mut arguments, "--duration")?)?);
            }
            "--max-connections" => {
                max_connections = parse_positive(
                    &take_value(&mut arguments, "--max-connections")?,
                    "maximum connections",
                )?;
            }
            "--max-workers" => {
                max_workers = parse_positive(
                    &take_value(&mut arguments, "--max-workers")?,
                    "maximum workers",
                )?;
            }
            "-n" | "--num-total-jobs" => {
                num_total_jobs = Some(parse_positive(
                    &take_value(&mut arguments, "--num-total-jobs")?,
                    "total jobs",
                )?);
            }
            "--schema" => {
                schema = SchemaName::new(take_value(&mut arguments, "--schema")?)
                    .map_err(|error| invalid_input(error.to_string()))?;
            }
            "--skip-vacuum" => vacuum = false,
            _ => return Err(invalid_input(format!("unknown bench option {argument:?}"))),
        }
    }

    if duration.is_some() && num_total_jobs.is_some() {
        return Err(invalid_input(
            "--duration and --num-total-jobs are mutually exclusive",
        ));
    }
    if max_workers > riverqueue::QUEUE_NUM_WORKERS_MAX {
        return Err(invalid_input(format!(
            "maximum workers cannot exceed {}",
            riverqueue::QUEUE_NUM_WORKERS_MAX
        )));
    }
    let database_url = database_url.or(database_url_env).ok_or_else(|| {
        invalid_input("--database-url or the DATABASE_URL environment variable is required")
    })?;

    Ok(Command::Bench(BenchOptions {
        backlog,
        batch_size,
        database_url,
        duration,
        max_connections,
        max_workers,
        num_total_jobs,
        schema,
        vacuum,
    }))
}

fn take_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, io::Error> {
    arguments
        .next()
        .ok_or_else(|| invalid_input(format!("{option} requires a value")))
}

fn parse_positive<T>(value: &str, name: &str) -> Result<T, io::Error>
where
    T: TryFrom<u64>,
{
    let value = value
        .parse::<u64>()
        .map_err(|error| invalid_input(format!("invalid {name}: {error}")))?;
    if value == 0 {
        return Err(invalid_input(format!("{name} must be positive")));
    }
    T::try_from(value).map_err(|_| invalid_input(format!("{name} is too large")))
}

fn parse_duration(value: &str) -> Result<Duration, io::Error> {
    if value.is_empty() {
        return Err(invalid_input("duration cannot be empty"));
    }

    let bytes = value.as_bytes();
    let mut index = 0;
    let mut total_nanos = 0_u128;
    while index < bytes.len() {
        let number_start = index;
        let mut decimal_seen = false;
        while index < bytes.len()
            && (bytes[index].is_ascii_digit() || (!decimal_seen && bytes[index] == b'.'))
        {
            decimal_seen |= bytes[index] == b'.';
            index += 1;
        }
        if number_start == index {
            return Err(invalid_input(format!("invalid duration {value:?}")));
        }
        let number = value[number_start..index]
            .parse::<f64>()
            .map_err(|error| invalid_input(format!("invalid duration {value:?}: {error}")))?;
        if !number.is_finite() || number < 0.0 {
            return Err(invalid_input(format!("invalid duration {value:?}")));
        }

        let units = [
            ("ns", 1_u128),
            ("us", 1_000),
            ("µs", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60 * 1_000_000_000),
            ("h", 60 * 60 * 1_000_000_000),
        ];
        let (unit, multiplier) = units
            .into_iter()
            .find(|(unit, _)| value[index..].starts_with(unit))
            .ok_or_else(|| invalid_input(format!("invalid duration unit in {value:?}")))?;
        index += unit.len();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            clippy::cast_sign_loss
        )]
        let segment_nanos = (number * multiplier as f64).round() as u128;
        total_nanos = total_nanos
            .checked_add(segment_nanos)
            .ok_or_else(|| invalid_input("duration is too large"))?;
    }
    if total_nanos == 0 {
        return Err(invalid_input("duration must be positive"));
    }
    let seconds = u64::try_from(total_nanos / 1_000_000_000)
        .map_err(|_| invalid_input("duration is too large"))?;
    let nanos =
        u32::try_from(total_nanos % 1_000_000_000).expect("nanosecond remainder always fits u32");
    Ok(Duration::new(seconds, nanos))
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

async fn run_benchmark(options: BenchOptions) -> Result<(), Box<dyn StdError + Send + Sync>> {
    eprintln!(
        "bench: WARNING: truncating {} in the selected database",
        options.schema.qualify("river_job")
    );
    let pool = PgPoolOptions::new()
        .max_connections(options.max_connections)
        .connect_with(postgres_connect_options(&options.database_url)?)
        .await?;
    reset_jobs(&pool, &options.schema, options.vacuum).await?;

    let mut workers = WorkerRegistry::new();
    workers.register::<BenchmarkArgs, _>(BenchmarkWorker)?;
    let client = Client::builder(pool.clone())
        .id("riverqueue-benchmark")
        .workers(workers)
        .queue(
            riverqueue::QUEUE_DEFAULT,
            QueueConfig {
                fetch_cooldown: Duration::from_millis(2),
                fetch_poll_interval: Duration::from_millis(20),
                max_workers: options.max_workers,
            },
        )
        .build()?;
    let inserted = Arc::new(AtomicU64::new(0));
    let mut next_number = 0_u64;
    let initial_jobs = options.num_total_jobs.unwrap_or(options.backlog);
    insert_jobs(
        &client,
        &inserted,
        &mut next_number,
        initial_jobs,
        options.batch_size,
    )
    .await?;

    let mut run = client.start()?;
    run.wait_ready().await?;
    let started_at = Instant::now();
    let stop_producer = CancellationToken::new();
    let mut producer = options.num_total_jobs.is_none().then(|| {
        tokio::spawn(run_producer(Producer {
            backlog: options.backlog,
            batch_size: options.batch_size,
            cancellation: stop_producer.child_token(),
            client: client.clone(),
            inserted: Arc::clone(&inserted),
            next_number,
            pool: pool.clone(),
            schema: options.schema.clone(),
        }))
    });

    let run_result =
        monitor_benchmark(&pool, &options, &inserted, started_at, producer.as_mut()).await;
    stop_producer.cancel();
    if let Some(producer) = producer {
        producer.await.map_err(|error| join_error(&error))??;
    }
    run.shutdown().await?;
    run_result?;

    let final_stats = database_statistics(&pool, &options.schema).await?;
    if final_stats.failed > 0 {
        return Err(format!("{} benchmark jobs failed", final_stats.failed).into());
    }
    let elapsed = started_at.elapsed();
    println!(
        "bench: total jobs worked [ {:10} ], total jobs inserted [ {:10} ], overall job/sec [ {:10.1} ], p95 [ {:>10} ], running {}",
        final_stats.worked,
        inserted.load(Ordering::Relaxed),
        throughput(final_stats.worked, elapsed),
        display_p95(final_stats.p95_seconds),
        display_duration(elapsed),
    );
    Ok(())
}

fn postgres_connect_options(database_url: &str) -> Result<PgConnectOptions, sqlx::Error> {
    use std::str::FromStr;

    let mut options = PgConnectOptions::from_str(database_url)?;
    if !database_url_has_userinfo(database_url)
        && let Some(username) = ["PGUSER", "USER", "LOGNAME"]
            .into_iter()
            .find_map(|name| env::var(name).ok().filter(|value| !value.is_empty()))
    {
        options = options.username(&username);
    }
    Ok(options)
}

fn database_url_has_userinfo(database_url: &str) -> bool {
    database_url
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split('/').next())
        .is_some_and(|authority| authority.contains('@'))
}

async fn reset_jobs(pool: &PgPool, schema: &SchemaName, vacuum: bool) -> Result<(), sqlx::Error> {
    let table = schema.qualify("river_job");
    sqlx::query(AssertSqlSafe(format!("TRUNCATE TABLE {table}")))
        .execute(pool)
        .await?;
    if vacuum {
        sqlx::query(AssertSqlSafe(format!("VACUUM FULL {table}")))
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn insert_jobs(
    client: &Client,
    inserted: &AtomicU64,
    next_number: &mut u64,
    count: u64,
    batch_size: usize,
) -> Result<(), riverqueue::Error> {
    let mut remaining = count;
    while remaining > 0 {
        let batch_size = u64::try_from(batch_size).unwrap_or(u64::MAX);
        let current_batch =
            usize::try_from(remaining.min(batch_size)).expect("batch size bounds the conversion");
        let jobs = (0..current_batch)
            .map(|_| {
                *next_number = next_number.wrapping_add(1);
                (
                    BenchmarkArgs {
                        number: *next_number,
                    },
                    InsertOpts::default(),
                )
            })
            .collect::<Vec<_>>();
        let count = client.insert_many_fast(jobs).await?;
        inserted.fetch_add(count, Ordering::Relaxed);
        remaining -= count;
    }
    Ok(())
}

async fn run_producer(mut producer: Producer) -> Result<(), Box<dyn StdError + Send + Sync>> {
    loop {
        let statistics = database_counts(&producer.pool, &producer.schema).await?;
        let jobs_left = producer
            .inserted
            .load(Ordering::Relaxed)
            .saturating_sub(statistics.worked);
        if jobs_left < producer.backlog {
            insert_jobs(
                &producer.client,
                &producer.inserted,
                &mut producer.next_number,
                producer.backlog - jobs_left,
                producer.batch_size,
            )
            .await?;
        }
        tokio::select! {
            () = producer.cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn monitor_benchmark(
    pool: &PgPool,
    options: &BenchOptions,
    inserted: &AtomicU64,
    started_at: Instant,
    mut producer: Option<&mut tokio::task::JoinHandle<Result<(), Box<dyn StdError + Send + Sync>>>>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let mut interval = tokio::time::interval(ITERATION_PERIOD);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    let deadline = options.duration.map(|duration| started_at + duration);
    let mut last_inserted = 0_u64;
    let mut last_worked = 0_u64;

    loop {
        let statistics = database_counts(pool, &options.schema).await?;
        if statistics.failed > 0 {
            return Err(format!("{} benchmark jobs failed", statistics.failed).into());
        }
        if options
            .num_total_jobs
            .is_some_and(|total| statistics.worked >= total)
        {
            return Ok(());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(());
        }

        tokio::select! {
            _ = interval.tick() => {
                let now = Instant::now();
                let worked = database_counts(pool, &options.schema).await?;
                let inserted_now = inserted.load(Ordering::Relaxed);
                let jobs_inserted = inserted_now.saturating_sub(last_inserted);
                let jobs_worked = worked.worked.saturating_sub(last_worked);
                println!(
                    "bench: jobs worked [ {jobs_worked:10} ], inserted [ {jobs_inserted:10} ], job/sec [ {:10.1} ] [{}]",
                    throughput(jobs_worked, ITERATION_PERIOD),
                    display_duration(now.duration_since(started_at)),
                );
                last_inserted = inserted_now;
                last_worked = worked.worked;
            }
            result = async {
                match producer.as_mut() {
                    Some(producer) => Some(producer.await),
                    None => std::future::pending().await,
                }
            } => {
                let result = result.expect("producer result is present");
                return match result {
                    Ok(Ok(())) => Err("benchmark producer stopped unexpectedly".into()),
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(join_error(&error)),
                };
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(());
            }
            () = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            } => return Ok(()),
        }
    }
}

async fn database_counts(
    pool: &PgPool,
    schema: &SchemaName,
) -> Result<DatabaseCounts, sqlx::Error> {
    let table = schema.qualify("river_job");
    let sql = format!(
        "SELECT \
            count(*) FILTER (WHERE state IN ('cancelled', 'discarded'))::bigint, \
            count(*) FILTER (WHERE state = 'completed')::bigint \
         FROM {table}"
    );
    let (failed, worked) = sqlx::query_as::<_, (i64, i64)>(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await?;
    Ok(DatabaseCounts {
        failed,
        worked: u64::try_from(worked).unwrap_or_default(),
    })
}

async fn database_statistics(
    pool: &PgPool,
    schema: &SchemaName,
) -> Result<DatabaseStatistics, sqlx::Error> {
    let table = schema.qualify("river_job");
    let sql = format!(
        "SELECT \
            count(*) FILTER (WHERE state IN ('cancelled', 'discarded'))::bigint, \
            percentile_cont(0.95) WITHIN GROUP (ORDER BY \
                extract(epoch FROM (finalized_at - created_at))::double precision) \
                FILTER (WHERE state = 'completed'), \
            count(*) FILTER (WHERE state = 'completed')::bigint \
         FROM {table}"
    );
    let (failed, p95_seconds, worked) =
        sqlx::query_as::<_, (i64, Option<f64>, i64)>(AssertSqlSafe(sql))
            .fetch_one(pool)
            .await?;
    Ok(DatabaseStatistics {
        failed,
        p95_seconds,
        worked: u64::try_from(worked).unwrap_or_default(),
    })
}

#[allow(clippy::cast_precision_loss)]
fn throughput(jobs: u64, duration: Duration) -> f64 {
    if duration.is_zero() {
        return 0.0;
    }
    jobs as f64 / duration.as_secs_f64()
}

fn display_p95(seconds: Option<f64>) -> String {
    seconds.map_or_else(|| "n/a".to_owned(), |seconds| format!("{seconds:.3}s"))
}

fn display_duration(duration: Duration) -> String {
    format!("{:.1}s", duration.as_secs_f64())
}

fn join_error(error: &tokio::task::JoinError) -> Box<dyn StdError + Send + Sync> {
    Box::new(io::Error::other(format!(
        "benchmark producer task failed: {error}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_explicit_postgres_userinfo() {
        assert!(!database_url_has_userinfo(
            "postgres://localhost/river_bench"
        ));
        assert!(database_url_has_userinfo(
            "postgres://river@localhost/river_bench"
        ));
        assert!(database_url_has_userinfo(
            "postgres://river:secret@localhost/river_bench"
        ));
    }

    #[test]
    fn parses_bench_options_and_composite_duration() {
        let command = parse_command(
            [
                "bench",
                "--database-url",
                "postgres://localhost/river_bench",
                "--duration",
                "1m30.5s",
                "--max-workers",
                "32",
                "--skip-vacuum",
            ]
            .into_iter()
            .map(str::to_owned),
            None,
        )
        .unwrap();
        let Command::Bench(options) = command else {
            panic!("expected bench command");
        };
        assert_eq!(options.duration, Some(Duration::from_millis(90_500)));
        assert_eq!(options.max_workers, 32);
        assert!(!options.vacuum);
    }

    #[test]
    fn rejects_conflicting_stopping_options() {
        let error = parse_command(
            ["bench", "--duration", "1s", "--num-total-jobs", "10"]
                .into_iter()
                .map(str::to_owned),
            Some("postgres://localhost/river_bench".to_owned()),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("mutually exclusive"));
    }
}
