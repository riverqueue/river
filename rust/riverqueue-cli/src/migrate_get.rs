//! Offline export of River's canonical migration SQL, matching Go's CLI.

use std::{
    error::Error,
    io::{self, Write},
};

#[cfg(feature = "mysql")]
use riverqueue_migrate::MySqlMigrator;
#[cfg(feature = "postgres")]
use riverqueue_migrate::PostgresMigrator;
#[cfg(feature = "sqlite")]
use riverqueue_migrate::SqliteMigrator;
use riverqueue_migrate::{MIGRATION_LINE_MAIN, Migration};

const HELP: &str = "Retrieve River migration SQL for use with another migration framework

Usage:
  riverqueue migrate-get [options]

Options:
      --all                   Print all migrations; down uses descending order
      --database-url URL      Database dialect hint (default: postgres://)
      --down                  Print down migrations
      --exclude-version N     Exclude versions (comma-separated or repeated)
      --line LINE             Migration line (default: main)
      --schema NAME           Schema prefix for migration SQL
      --up                    Print up migrations
      --version N             Versions to print (comma-separated or repeated)
  -h, --help                  Print help

Pass exactly one of --all or --version, and exactly one of --up or --down.
Explicit versions retain their supplied order, including for down migrations.
No database connection is made. DATABASE_URL is ignored; use --database-url
to select MySQL (mysql://), Postgres (postgres:// or postgresql://), or SQLite (sqlite://).
Only the main migration line is bundled.

Examples:
  riverqueue migrate-get --version 3 --up > river_3.up.sql
  riverqueue migrate-get --version 3,2,1 --down > river.down.sql
  riverqueue migrate-get --all --exclude-version 1 --up > river_all.up.sql
  riverqueue migrate-get --all --down --database-url sqlite:// > river_sqlite.down.sql
";

#[derive(Default)]
struct Args {
    all: bool,
    database_url: String,
    down: bool,
    exclude_version: Vec<i64>,
    line: String,
    schema: String,
    up: bool,
    version: Vec<i64>,
}

/// Exports SQL without opening a database pool or consulting connection settings.
pub(crate) fn run(
    arguments: impl IntoIterator<Item = String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(args) = parse_args(arguments)? else {
        print!("{HELP}");
        return Ok(());
    };
    let migrations = all_versions(&args.database_url)?;
    // Resolve every requested version before writing anything, even versions
    // that will be excluded, just as Go does.
    let mut selected = if args.all {
        migrations.iter().collect::<Vec<_>>()
    } else {
        args.version
            .iter()
            .map(|version| {
                migrations
                    .iter()
                    .find(|migration| migration.version == *version)
                    .ok_or_else(|| {
                        let available: Vec<_> = migrations.iter().map(|m| m.version).collect();
                        format!("migration {version} not found (available versions: {available:?})")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if args.all && args.down {
        selected.reverse();
    }
    let direction = if args.down { "down" } else { "up" };
    // Go's migrate-get substitutes the supplied schema verbatim. In contrast
    // to executing migrations, callers can supply SQL quoting themselves.
    let schema = if args.schema.is_empty() {
        String::new()
    } else {
        format!("{}.", args.schema)
    };
    let mut out = io::BufWriter::new(io::stdout().lock());
    for (index, migration) in selected
        .into_iter()
        .filter(|migration| !args.exclude_version.contains(&migration.version))
        .enumerate()
    {
        if index > 0 {
            writeln!(out)?;
        }
        let sql = if args.down {
            migration.down_sql
        } else {
            migration.up_sql
        };
        writeln!(
            out,
            "-- River {MIGRATION_LINE_MAIN} migration {:03} [{direction}]",
            migration.version
        )?;
        writeln!(
            out,
            "{}",
            sql.replace("/* TEMPLATE: schema */", &schema).trim()
        )?;
    }
    out.flush()?;
    Ok(())
}

fn all_versions(database_url: &str) -> Result<&'static [Migration], Box<dyn Error + Send + Sync>> {
    match database_url.split_once("://").map(|(scheme, _)| scheme) {
        Some("postgres" | "postgresql") => {
            #[cfg(feature = "postgres")]
            return Ok(PostgresMigrator::all_versions());
            #[cfg(not(feature = "postgres"))]
            return Err("Postgres support requires the `postgres` feature".into());
        }
        Some("mysql") => {
            #[cfg(feature = "mysql")]
            return Ok(MySqlMigrator::all_versions());
            #[cfg(not(feature = "mysql"))]
            return Err("MySQL support requires the `mysql` feature".into());
        }
        Some("sqlite") => {
            #[cfg(feature = "sqlite")]
            return Ok(SqliteMigrator::all_versions());
            #[cfg(not(feature = "sqlite"))]
            return Err("SQLite support requires the `sqlite` feature".into());
        }
        _ => Err(
            "--database-url must use a mysql://, postgres://, postgresql://, or sqlite:// prefix"
                .into(),
        ),
    }
}

fn parse_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Option<Args>, Box<dyn Error + Send + Sync>> {
    let mut raw = arguments.into_iter();
    let mut args = Args {
        database_url: "postgres://".to_owned(),
        ..Args::default()
    };
    while let Some(argument) = raw.next() {
        let (option, inline_value) = argument
            .split_once('=')
            .map_or((argument.as_str(), None), |(option, value)| {
                (option, Some(value))
            });
        match option {
            "--all" | "--down" | "--up" => {
                let value = inline_value.map_or(Ok(true), str::parse::<bool>)?;
                match option {
                    "--all" => args.all = value,
                    "--down" => args.down = value,
                    _ => args.up = value,
                }
            }
            "--database-url" | "--exclude-version" | "--line" | "--schema" | "--version" => {
                let value = inline_value
                    .map(str::to_owned)
                    .or_else(|| raw.next())
                    .ok_or_else(|| format!("{option} requires a value"))?;
                match option {
                    "--database-url" => args.database_url = value,
                    "--exclude-version" => {
                        args.exclude_version.extend(parse_versions(&value, option)?);
                    }
                    "--line" => args.line = value,
                    "--schema" => args.schema = value,
                    _ => args.version.extend(parse_versions(&value, option)?),
                }
            }
            "-h" | "--help" => return Ok(None),
            _ => return Err(format!("unknown argument {argument:?}\n\n{HELP}").into()),
        }
    }
    if args.all != args.version.is_empty() {
        return Err("pass exactly one of --all or --version".into());
    }
    if args.down == args.up {
        return Err("pass exactly one of --up or --down".into());
    }
    if !args.line.is_empty() && args.line != MIGRATION_LINE_MAIN {
        return Err(format!("migration line does not exist: {}", args.line).into());
    }
    Ok(Some(args))
}

fn parse_versions(value: &str, option: &str) -> Result<Vec<i64>, Box<dyn Error + Send + Sync>> {
    value
        .split(',')
        .map(|version| {
            version.parse().map_err(|error| {
                format!("invalid version {version:?} for {option}: {error}").into()
            })
        })
        .collect()
}
