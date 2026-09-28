//! Migration commands for River's canonical migration line.

use std::error::Error;

#[cfg(feature = "postgres")]
use riverqueue::database::SchemaName;
#[cfg(feature = "postgres")]
use riverqueue_migrate::PostgresMigrator;
#[cfg(feature = "sqlite")]
use riverqueue_migrate::SqliteMigrator;
use riverqueue_migrate::{Direction, MigrateOpts, MigrateResult, ValidateResult};
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
    target_version: Option<i64>,
}

enum CommandMigrator {
    #[cfg(feature = "postgres")]
    Postgres(PostgresMigrator),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteMigrator),
}

impl CommandMigrator {
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
                return Err("--schema is only supported for PostgreSQL".into());
            }
            CommandMigrator::Sqlite(SqliteMigrator::new(
                SqlitePool::connect(&args.database_url).await?,
            ))
        }
    } else {
        #[cfg(not(feature = "postgres"))]
        return Err("PostgreSQL support requires the `postgres` feature".into());
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
                migrator
                    .migrate(Direction::Down, migrate_opts(&args))
                    .await?,
            );
        }
        "migrate-list" => {
            for version in migrator.existing_versions().await? {
                println!("{version:03}");
            }
        }
        "migrate-up" => {
            print_migrations(migrator.migrate(Direction::Up, migrate_opts(&args)).await?);
        }
        "validate" => {
            let result = migrator.validate(args.target_version).await?;
            if !result.ok {
                for message in result.messages {
                    eprintln!("{message}");
                }
                std::process::exit(2);
            }
            println!("River migrations valid");
        }
        command => return Err(format!("unknown command {command:?}\n{}", usage()).into()),
    }
    Ok(())
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

fn print_migrations(result: MigrateResult) {
    for version in result.versions {
        println!(
            "{:03} {:?} {} ({:?})",
            version.version, result.direction, version.name, version.duration
        );
        if !version.sql.is_empty() && version.duration.is_zero() {
            println!("{}", version.sql);
        }
    }
}

fn usage() -> &'static str {
    "usage: riverqueue <migrate-up|migrate-down|migrate-list|validate> [--database-url URL] [--schema NAME] [--target-version N] [--max-steps N] [--dry-run]"
}
