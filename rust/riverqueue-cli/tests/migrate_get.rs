//! Exercise offline SQL export through the installed CLI entry point.

use std::process::{Command, Output};

use riverqueue_migrate::Migration;

#[test]
fn help_documents_export() {
    let root = success(command(&["--help"]).output().unwrap());
    assert!(root.contains("migrate-get"));
    for flag in ["-h", "--help"] {
        let help = success(command(&["migrate-get", flag]).output().unwrap());
        for option in [
            "--all",
            "--database-url",
            "--down",
            "--exclude-version",
            "--line",
            "--schema",
            "--up",
            "--version",
        ] {
            assert!(help.contains(option), "{help}");
        }
        assert!(help.contains("No database connection is made"));
    }
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_export() {
    exercise_export(
        "postgres://",
        riverqueue_migrate::PostgresMigrator::all_versions(),
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_is_default_and_ignores_connection_settings() {
    let arguments = ["migrate-get", "--version", "1", "--up"];
    let expected = success(command(&arguments).output().unwrap());
    assert!(expected.contains("id bigserial PRIMARY KEY"));
    for database_url in ["sqlite://", "not a database URL"] {
        let output = command(&arguments)
            .env("DATABASE_URL", database_url)
            .env("PGHOST", "invalid.invalid")
            .env("PGPORT", "not a port")
            .output()
            .unwrap();
        assert_eq!(success(output), expected);
    }
    for database_url in ["postgres://127.0.0.1:1/missing", "postgresql://"] {
        let output = command(&arguments)
            .args(["--database-url", database_url])
            .output()
            .unwrap();
        assert_eq!(success(output), expected);
    }
}

#[cfg(not(feature = "postgres"))]
#[test]
fn postgres_requires_feature() {
    failure(
        command(&["migrate-get", "--all", "--up"]).output().unwrap(),
        "Postgres support requires the `postgres` feature",
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_does_not_create_database() {
    let path = std::env::temp_dir().join(format!(
        "river-cli-export-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    for prefix in ["sqlite:", "sqlite://"] {
        let database_url = format!("{prefix}{}?mode=rwc", path.display());
        let output = success(run(&database_url, &["--version", "1", "--up"]));
        assert!(output.contains("id integer PRIMARY KEY"));
        assert!(!path.exists(), "export must not create a database file");
    }
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_export() {
    for url in [
        "sqlite:",
        "sqlite://",
        "sqlite::memory:",
        "sqlite://:memory:",
    ] {
        exercise_export(url, riverqueue_migrate::SqliteMigrator::all_versions());
    }
}

#[cfg(not(feature = "sqlite"))]
#[test]
fn sqlite_requires_feature() {
    for url in [
        "sqlite:",
        "sqlite://",
        "sqlite::memory:",
        "sqlite://:memory:",
    ] {
        failure(
            run(url, &["--all", "--up"]),
            "SQLite support requires the `sqlite` feature",
        );
    }
}

fn command(arguments: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_riverqueue"));
    command.env_remove("DATABASE_URL").args(arguments);
    command
}

fn exercise_export(database_url: &str, migrations: &[Migration]) {
    for direction in ["down", "up"] {
        let flag = format!("--{direction}");
        assert_eq!(
            success(run(database_url, &["--version", "1", &flag])),
            expected_sql(migrations, &[1], direction, "")
        );
        assert_eq!(
            success(run(
                database_url,
                &["--version=3,1", "--version", "2,3", &flag]
            )),
            expected_sql(migrations, &[3, 1, 2, 3], direction, "")
        );

        let mut all: Vec<_> = migrations
            .iter()
            .map(|migration| migration.version)
            .collect();
        if direction == "down" {
            all.reverse();
        }
        assert_eq!(
            success(run(database_url, &["--all", &flag])),
            expected_sql(migrations, &all, direction, "")
        );
        all.retain(|version| ![1, 2, 3].contains(version));
        assert_eq!(
            success(run(
                database_url,
                &[
                    "--all",
                    "--exclude-version=1,3",
                    "--exclude-version",
                    "2,999",
                    &flag,
                ]
            )),
            expected_sql(migrations, &all, direction, "")
        );
        assert_eq!(
            success(run(
                database_url,
                &["--version", "3,1,2", "--exclude-version", "1,3", &flag]
            )),
            expected_sql(migrations, &[2], direction, "")
        );
        assert_eq!(
            success(run(
                database_url,
                &["--version", "1,2", "--exclude-version", "1,2", &flag]
            )),
            ""
        );

        for schema in ["", "custom_schema", "\"MyRiver\""] {
            let output = success(run(
                database_url,
                &[
                    "--version",
                    "1",
                    &flag,
                    "--schema",
                    schema,
                    "--line",
                    "main",
                ],
            ));
            assert_eq!(output, expected_sql(migrations, &[1], direction, schema));
            if !schema.is_empty() {
                assert!(output.contains(&format!("{schema}.river_migration")));
            }
            assert!(!output.contains("/* TEMPLATE:"));
        }
    }
    exercise_invalid_arguments(database_url);
}

fn exercise_invalid_arguments(database_url: &str) {
    for (arguments, error) in [
        (vec!["--up"], "pass exactly one of --all or --version"),
        (
            vec!["--all", "--version", "1", "--up"],
            "pass exactly one of --all or --version",
        ),
        (vec!["--all"], "pass exactly one of --up or --down"),
        (
            vec!["--all", "--up", "--down"],
            "pass exactly one of --up or --down",
        ),
        (vec!["--version", "0", "--up"], "migration 0 not found"),
        (vec!["--version", "-1", "--down"], "migration -1 not found"),
        (
            vec!["--version", "1,999", "--up"],
            "migration 999 not found",
        ),
        (
            vec!["--version", "1,999", "--exclude-version", "999", "--up"],
            "migration 999 not found",
        ),
        (vec!["--version", "1,x", "--up"], "invalid version"),
        (vec!["--version", "1,", "--up"], "invalid version"),
        (vec!["--version", "", "--up"], "invalid version"),
        (
            vec!["--all", "--exclude-version", "x", "--up"],
            "invalid version",
        ),
        (
            vec!["--all", "--up", "--line", "missing"],
            "migration line does not exist: missing",
        ),
        (
            vec!["--all", "--up", "--database-url", "mysql://"],
            "--database-url must use a postgres://, postgresql://, or sqlite:// prefix",
        ),
    ] {
        failure(run(database_url, &arguments), error);
    }
    for option in [
        "--database-url",
        "--exclude-version",
        "--line",
        "--schema",
        "--version",
    ] {
        failure(
            run(database_url, &["--all", "--up", option]),
            &format!("{option} requires a value"),
        );
    }
    for argument in [
        "--dry-run",
        "--max-steps",
        "--show-sql",
        "--target-version",
        "unexpected",
    ] {
        failure(
            run(database_url, &["--all", "--up", argument]),
            "unknown argument",
        );
    }
}

fn expected_sql(
    migrations: &[Migration],
    versions: &[i64],
    direction: &str,
    schema: &str,
) -> String {
    versions
        .iter()
        .map(|version| {
            let migration = migrations.iter().find(|m| m.version == *version).unwrap();
            let sql = if direction == "down" {
                migration.down_sql
            } else {
                migration.up_sql
            };
            let prefix = if schema.is_empty() {
                String::new()
            } else {
                format!("{schema}.")
            };
            format!(
                "-- River main migration {version:03} [{direction}]\n{}\n",
                sql.replace("/* TEMPLATE: schema */", &prefix).trim()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn failure(output: Output, message: &str) {
    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stdout.is_empty(),
        "errors must not export partial SQL"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(message), "{stderr}");
}

fn run(database_url: &str, arguments: &[&str]) -> Output {
    command(&["migrate-get", "--database-url", database_url])
        .args(arguments)
        .output()
        .unwrap()
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
