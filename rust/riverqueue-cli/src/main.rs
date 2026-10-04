//! River's Rust command-line interface.
//!
//! Runs River migrations against PostgreSQL or SQLite and benchmarks the
//! worker runtime:
//!
//! ```text
//! riverqueue migrate-up --database-url postgres://localhost/app
//! riverqueue migrate-list --database-url sqlite://app.sqlite3
//! riverqueue bench --database-url postgres://localhost/river_bench --duration 30s
//! ```

#![forbid(unsafe_code)]

use std::{env, error::Error, process::ExitCode};

#[cfg(feature = "postgres")]
mod bench;
mod migrate;

const HELP: &str = "River for Rust

Usage:
  riverqueue <command> [options]

Commands:
  migrate-down  Run down migrations
  migrate-list  List applied migration versions
  migrate-up    Run up migrations
  validate      Check that all migrations are applied
  bench         Benchmark job throughput against a disposable database

Run `riverqueue <command> --help` for a command's options. Commands read the
database URL from --database-url or the DATABASE_URL environment variable.
";

#[tokio::main]
async fn main() -> ExitCode {
    match run(env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("riverqueue: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(mut arguments: Vec<String>) -> Result<(), Box<dyn Error + Send + Sync>> {
    if arguments.is_empty() {
        print!("{HELP}");
        return Ok(());
    }
    let command = arguments.remove(0);
    let database_url_env = env::var("DATABASE_URL").ok();
    match command.as_str() {
        "-h" | "--help" | "help" => print!("{HELP}"),
        "-V" | "--version" | "version" => println!("riverqueue {}", env!("CARGO_PKG_VERSION")),
        #[cfg(feature = "postgres")]
        "bench" => bench::run(arguments, database_url_env).await?,
        "migrate-down" | "migrate-list" | "migrate-up" | "validate" => {
            migrate::run(command, arguments, database_url_env).await?;
        }
        _ => return Err(format!("unknown command {command:?}\n\n{HELP}").into()),
    }
    Ok(())
}
