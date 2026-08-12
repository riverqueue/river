//! Command-line interface for River's canonical migration line.

#![forbid(unsafe_code)]

use std::{env, error::Error};

use riverqueue_internal::SchemaName;
use riverqueue_migrate::{Direction, MigrateOpts, Migrator};
use sqlx::PgPool;

#[derive(Default)]
struct Args {
    command: String,
    database_url: String,
    dry_run: bool,
    max_steps: Option<usize>,
    schema: Option<String>,
    target_version: Option<i64>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("riverqueue-migrate: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let pool = PgPool::connect(&args.database_url).await?;
    let mut migrator = Migrator::new(pool);
    if let Some(schema) = args.schema {
        migrator = migrator.with_schema(SchemaName::new(schema)?);
    }

    match args.command.as_str() {
        "down" | "migrate-down" => {
            print_migrations(
                migrator
                    .migrate(
                        Direction::Down,
                        MigrateOpts {
                            dry_run: args.dry_run,
                            max_steps: args.max_steps,
                            target_version: args.target_version,
                        },
                    )
                    .await?,
            );
        }
        "list" | "migrate-list" => {
            for version in migrator.existing_versions().await? {
                println!("{version:03}");
            }
        }
        "up" | "migrate-up" => {
            print_migrations(
                migrator
                    .migrate(
                        Direction::Up,
                        MigrateOpts {
                            dry_run: args.dry_run,
                            max_steps: args.max_steps,
                            target_version: args.target_version,
                        },
                    )
                    .await?,
            );
        }
        "validate" | "migrate-validate" => {
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

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut raw = env::args().skip(1);
    let Some(command) = raw.next() else {
        return Err(usage().into());
    };
    if matches!(command.as_str(), "-h" | "--help") {
        println!("{}", usage());
        std::process::exit(0);
    }
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
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument {argument:?}\n{}", usage()).into()),
        }
    }
    if args.database_url.is_empty() {
        args.database_url =
            env::var("DATABASE_URL").map_err(|_| "--database-url or DATABASE_URL is required")?;
    }
    Ok(args)
}

fn print_migrations(result: riverqueue_migrate::MigrateResult) {
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
    "usage: riverqueue-migrate <up|down|list|validate> [--database-url URL] [--schema NAME] [--target-version N] [--max-steps N] [--dry-run]"
}
