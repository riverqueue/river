#![cfg(any(feature = "sqlite", all(feature = "postgres", river_postgres_tests)))]

use std::process::{Command, Output};

#[cfg(all(feature = "postgres", river_postgres_tests))]
use sqlx::{AssertSqlSafe, PgPool};

#[cfg(all(feature = "postgres", river_postgres_tests))]
#[tokio::test]
async fn postgres_migration_output() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("set RIVER_RUST_DATABASE_URL to run Postgres CLI tests");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = format!("RiverCli_{}_{}", std::process::id(), nonce());
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .unwrap();
    let command = |arguments: &[&str]| run(&database_url, Some(&schema), arguments);

    assert_listing(&command, None);
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("\"{schema}\".river_migration"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(table.is_none(), "listing must not create migration tables");
    exercise_migrations(&command);

    sqlx::raw_sql(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_migration_output() {
    let path =
        std::env::temp_dir().join(format!("river-cli-{}-{}.db", std::process::id(), nonce()));
    let database_url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = sqlx::SqlitePool::connect(&database_url).await.unwrap();
    let command = |arguments: &[&str]| run(&database_url, None, arguments);

    assert_listing(&command, None);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name = 'river_migration'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0, "listing must not create migration tables");
    exercise_migrations(&command);

    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

fn assert_listing(command: &impl Fn(&[&str]) -> Output, current: Option<i64>) {
    let names = [
        "create_river_migration",
        "initial_schema",
        "river_job_tags_non_null",
        "pending_and_more",
        "migration_unique_client",
        "bulk_unique",
        "notification_outbox_sqlite_jsonb_and_sql_cleanup",
        "job_id_autoincrement",
    ];
    let expected: Vec<_> = (1..)
        .zip(names)
        .map(|(version, name)| {
            let prefix = if current == Some(version) {
                "* "
            } else if current.is_some() {
                "  "
            } else {
                ""
            };
            format!("{prefix}{version:03} {name}")
        })
        .collect();
    let stdout = success(command(&["migrate-list"]));
    assert_eq!(stdout.lines().collect::<Vec<_>>(), expected);
    assert!(stdout.ends_with('\n'));
}

fn assert_progress(output: Output, expected: &[&str]) {
    let stdout = success(output);
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), expected.len(), "{stdout}");
    let duration_column = lines[0].rfind(" [").unwrap();
    for (line, expected) in lines.into_iter().zip(expected) {
        assert_eq!(line.rfind(" ["), Some(duration_column), "{line}");
        let (prefix, duration) = line.rsplit_once(" [").expect("duration suffix");
        assert_eq!(prefix.trim_end(), *expected);
        let duration = duration.strip_suffix(']').unwrap();
        assert!(
            duration.chars().next().unwrap().is_ascii_digit(),
            "{duration}"
        );
        assert!(duration.ends_with('s'), "{duration}");
    }
}

fn exercise_completion(command: &impl Fn(&[&str]) -> Output) {
    let stdout = success(command(&["migrate-up", "--max-steps", "100"]));
    assert!(
        stdout.ends_with("no more migrations to apply\n"),
        "{stdout}"
    );
    assert_listing(command, Some(8));
    assert_eq!(success(command(&["validate"])), "");
    assert_eq!(
        success(command(&["migrate-up"])),
        "no migrations to apply\n"
    );

    let preview = success(command(&["migrate-down", "--dry-run"]));
    assert_eq!(preview, "migration 008 [down] [DRY RUN]\n");
    assert_listing(command, Some(8));

    let stdout = success(command(&[
        "migrate-down",
        "--target-version",
        "-1",
        "--max-steps",
        "100",
    ]));
    assert!(stdout.starts_with("applied migration 008 [down] job_id_autoincrement"));
    assert!(stdout.ends_with("no more migrations to apply\n"));
    assert_listing(command, None);
    assert_eq!(
        success(command(&["migrate-down"])),
        "no migrations to apply\n"
    );
}

fn exercise_migrations(command: &impl Fn(&[&str]) -> Output) {
    let validation = command(&["validate"]);
    assert_eq!(validation.status.code(), Some(2));
    assert_eq!(String::from_utf8(validation.stdout).unwrap(), "");
    assert_eq!(
        String::from_utf8(validation.stderr).unwrap(),
        "unapplied migrations: [1, 2, 3, 4, 5, 6, 7, 8]\n"
    );

    assert_eq!(
        success(command(&["migrate-up", "--dry-run", "--max-steps", "2"])),
        "migration 001 [up] [DRY RUN]\nmigration 002 [up] [DRY RUN]\n"
    );
    let sql = success(command(&[
        "migrate-up",
        "--dry-run",
        "--show-sql",
        "--max-steps",
        "1",
    ]));
    assert!(
        sql.starts_with(&format!(
            "migration 001 [up] [DRY RUN]\n{}\n-- River main migration 001 [up]\n",
            "-".repeat(80)
        )),
        "{sql}"
    );
    assert!(sql.contains("CREATE TABLE"), "{sql}");
    assert!(sql.ends_with("\n\n"));
    assert_listing(command, None);

    assert_progress(
        command(&["migrate-up", "--max-steps", "2"]),
        &[
            "applied migration 001 [up] create_river_migration",
            "applied migration 002 [up] initial_schema",
        ],
    );
    assert_listing(command, Some(2));
    assert_eq!(success(command(&["validate", "--target-version", "2"])), "");
    assert_eq!(command(&["validate"]).status.code(), Some(2));
    assert_eq!(
        success(command(&["migrate-up", "--target-version", "2"])),
        "no migrations to apply\n"
    );

    let sql = success(command(&[
        "migrate-up",
        "--show-sql",
        "--target-version",
        "3",
    ]));
    assert!(sql.starts_with("applied migration 003 [up] river_job_tags_non_null ["));
    assert!(sql.contains("-- River main migration 003 [up]\n"));
    assert!(sql.ends_with("\n\n"));
    assert_listing(command, Some(3));

    assert_progress(
        command(&["migrate-down"]),
        &["applied migration 003 [down] river_job_tags_non_null"],
    );
    assert_listing(command, Some(2));
    exercise_completion(command);
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn run(database_url: &str, schema: Option<&str>, arguments: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_riverqueue"));
    command.env_remove("DATABASE_URL");
    command
        .args(arguments)
        .args(["--database-url", database_url]);
    if let Some(schema) = schema {
        command.args(["--schema", schema]);
    }
    command.output().unwrap()
}

fn success(output: Output) -> String {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
