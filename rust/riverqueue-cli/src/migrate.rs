//! Migration commands for River's canonical migration line.

use std::{error::Error, time::Duration};

#[cfg(feature = "postgres")]
use riverqueue::database::SchemaName;
#[cfg(feature = "postgres")]
use riverqueue_migrate::PostgresMigrator;
#[cfg(feature = "sqlite")]
use riverqueue_migrate::SqliteMigrator;
use riverqueue_migrate::{Direction, MigrateOpts, MigrateResult, Migration, ValidateResult};
#[cfg(feature = "postgres")]
use sqlx::PgPool;
#[cfg(feature = "sqlite")]
use sqlx::SqlitePool;

#[derive(Default)]
struct Args {
    command: String,
    database_url: String,
    dry_run: bool,
    max_steps: Option<usize>,
    schema: Option<String>,
    show_sql: bool,
    target_version: Option<i64>,
}

enum CommandMigrator {
    #[cfg(feature = "postgres")]
    Postgres(PostgresMigrator),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteMigrator),
}

impl CommandMigrator {
    fn all_versions(&self) -> &'static [Migration] {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => PostgresMigrator::all_versions(),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => SqliteMigrator::all_versions(),
        }
    }

    async fn existing_versions(&self) -> Result<Vec<i64>, riverqueue_migrate::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(migrator) => migrator.existing_versions().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(migrator) => migrator.existing_versions().await,
        }
    }

    async fn migrate(
        &self,
        direction: Direction,
        opts: MigrateOpts,
    ) -> Result<MigrateResult, riverqueue_migrate::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(migrator) => migrator.migrate(direction, opts).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(migrator) => migrator.migrate(direction, opts).await,
        }
    }

    async fn validate(
        &self,
        target_version: Option<i64>,
    ) -> Result<ValidateResult, riverqueue_migrate::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(migrator) => migrator.validate(target_version).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(migrator) => migrator.validate(target_version).await,
        }
    }
}

/// Runs a migration command with the arguments that follow it.
pub(crate) async fn run(
    command: String,
    arguments: impl IntoIterator<Item = String>,
    database_url_env: Option<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(args) = parse_args(command, arguments, database_url_env)? else {
        println!("{}", usage());
        return Ok(());
    };
    let migrator = if args.database_url.starts_with("sqlite:") {
        #[cfg(not(feature = "sqlite"))]
        return Err("SQLite support requires the `sqlite` feature".into());
        #[cfg(feature = "sqlite")]
        {
            if args.schema.is_some() {
                return Err("--schema is only supported for Postgres".into());
            }
            CommandMigrator::Sqlite(SqliteMigrator::new(
                SqlitePool::connect(&args.database_url).await?,
            ))
        }
    } else {
        #[cfg(not(feature = "postgres"))]
        return Err("Postgres support requires the `postgres` feature".into());
        #[cfg(feature = "postgres")]
        {
            let pool = PgPool::connect(&args.database_url).await?;
            let mut migrator = PostgresMigrator::new(pool);
            if let Some(schema) = &args.schema {
                migrator = migrator.with_schema(SchemaName::new(schema.clone())?);
            }
            CommandMigrator::Postgres(migrator)
        }
    };

    match args.command.as_str() {
        "migrate-down" => {
            print_migrations(
                &migrator
                    .migrate(Direction::Down, migrate_opts(&args))
                    .await?,
                &args,
            );
        }
        "migrate-list" => {
            let current = migrator.existing_versions().await?.last().copied();
            for migration in migrator.all_versions() {
                let prefix = match current {
                    Some(version) if version == migration.version => "* ",
                    Some(_) => "  ",
                    None => "",
                };
                println!("{prefix}{:03} {}", migration.version, migration.name);
            }
        }
        "migrate-up" => {
            print_migrations(
                &migrator.migrate(Direction::Up, migrate_opts(&args)).await?,
                &args,
            );
        }
        "validate" => {
            let result = migrator.validate(args.target_version).await?;
            if !result.is_valid() {
                eprintln!("{result}");
                std::process::exit(2);
            }
        }
        command => return Err(format!("unknown command {command:?}\n{}", usage()).into()),
    }
    Ok(())
}

fn format_duration(duration: Duration) -> String {
    // Match the Go CLI's truncation and duration units, including minutes and hours.
    let precision = if duration > Duration::from_secs(1) {
        10_000_000
    } else if duration < Duration::from_millis(1) {
        10
    } else {
        10_000
    };
    let nanos = duration.as_nanos() / precision * precision;
    if nanos == 0 {
        return "0s".to_owned();
    }
    let mut prefix = String::new();
    let (value, scale, digits, unit) = if nanos < 1_000 {
        (nanos, 1, 0, "ns")
    } else if nanos < 1_000_000 {
        (nanos, 1_000, 3, "µs")
    } else if nanos < 1_000_000_000 {
        (nanos, 1_000_000, 6, "ms")
    } else {
        let seconds = nanos / 1_000_000_000;
        if seconds >= 3_600 {
            prefix = format!("{}h{}m", seconds / 3_600, seconds / 60 % 60);
        } else if seconds >= 60 {
            prefix = format!("{}m", seconds / 60);
        }
        (nanos % 60_000_000_000, 1_000_000_000, 9, "s")
    };
    let fraction = value % scale;
    if fraction == 0 {
        format!("{prefix}{}{unit}", value / scale)
    } else {
        let fraction = format!("{fraction:0digits$}");
        format!(
            "{prefix}{}.{}{unit}",
            value / scale,
            fraction.trim_end_matches('0')
        )
    }
}

fn migrate_opts(args: &Args) -> MigrateOpts {
    let mut opts = MigrateOpts::new().with_dry_run(args.dry_run);
    if let Some(max_steps) = args.max_steps {
        opts = opts.with_max_steps(max_steps);
    }
    if let Some(target_version) = args.target_version {
        opts = opts.with_target_version(target_version);
    }
    opts
}

fn parse_args(
    command: String,
    arguments: impl IntoIterator<Item = String>,
    database_url_env: Option<String>,
) -> Result<Option<Args>, Box<dyn Error + Send + Sync>> {
    let mut raw = arguments.into_iter();
    let mut args = Args {
        command,
        ..Args::default()
    };
    while let Some(argument) = raw.next() {
        match argument.as_str() {
            "--database-url" => {
                args.database_url = raw.next().ok_or("--database-url requires a value")?;
            }
            "--dry-run" => args.dry_run = true,
            "--max-steps" => {
                args.max_steps = Some(raw.next().ok_or("--max-steps requires a value")?.parse()?);
            }
            "--schema" => args.schema = Some(raw.next().ok_or("--schema requires a value")?),
            "--show-sql" => args.show_sql = true,
            "--target-version" => {
                args.target_version = Some(
                    raw.next()
                        .ok_or("--target-version requires a value")?
                        .parse()?,
                );
            }
            "-h" | "--help" => return Ok(None),
            _ => return Err(format!("unknown argument {argument:?}\n{}", usage()).into()),
        }
    }
    if args.database_url.is_empty() {
        args.database_url = database_url_env.ok_or("--database-url or DATABASE_URL is required")?;
    }
    Ok(Some(args))
}

fn print_migrations(result: &MigrateResult, args: &Args) {
    if result.versions.is_empty() {
        println!("no migrations to apply");
        return;
    }
    let direction = match result.direction {
        Direction::Down => "down",
        Direction::Up => "up",
    };
    let name_width = result
        .versions
        .iter()
        .map(|version| version.name.len())
        .max()
        .unwrap_or(0);
    for version in &result.versions {
        if args.dry_run {
            println!("migration {:03} [{direction}] [DRY RUN]", version.version);
        } else {
            println!(
                "applied migration {:03} [{direction}] {:name_width$} [{}]",
                version.version,
                version.name,
                format_duration(version.duration)
            );
        }
        if args.show_sql {
            println!("{}", "-".repeat(80));
            println!(
                "-- River main migration {:03} [{direction}]",
                version.version
            );
            println!("{}\n", version.sql.trim());
        }
    }
    if args
        .max_steps
        .is_some_and(|steps| result.versions.len() < steps)
    {
        println!("no more migrations to apply");
    }
}

fn usage() -> &'static str {
    "usage: riverqueue <migrate-up|migrate-down|migrate-list|validate> [--database-url URL] [--schema NAME] [--target-version N] [--max-steps N] [--dry-run] [--show-sql]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_format_matches_go() {
        for (nanos, expected) in [
            (0, "0s"),
            (9, "0s"),
            (999, "990ns"),
            (1_000, "1µs"),
            (1_332, "1.33µs"),
            (765_625, "765.62µs"),
            (1_000_000, "1ms"),
            (4_422_125, "4.42ms"),
            (13_280_834, "13.28ms"),
            (234_910_750, "234.91ms"),
            (1_000_000_000, "1s"),
            (3_937_042_000, "3.93s"),
            (34_042_234_000, "34.04s"),
            (60_000_000_000, "1m0s"),
            (154_042_234_000, "2m34.04s"),
            (3_600_000_000_000, "1h0m0s"),
            (3_754_042_234_000, "1h2m34.04s"),
        ] {
            assert_eq!(format_duration(Duration::from_nanos(nanos)), expected);
        }
    }
}
