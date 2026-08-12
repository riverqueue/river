use std::time::Duration;

use riverqueue_migrate::{
    Direction, Error, MIGRATION_LINE_MAIN, MIGRATION_VERSION_LATEST, MigrateOpts,
    SQLITE_MIGRATIONS, SqliteMigrator,
};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

#[tokio::test]
async fn all_versions_options_and_validation() {
    let pool = sqlite_pool().await;
    let migrator = SqliteMigrator::new(pool.clone());

    assert_eq!(
        migrator
            .all_versions()
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );
    assert_eq!(migrator.all_versions().len(), SQLITE_MIGRATIONS.len());
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        Vec::<i64>::new()
    );

    let dry_run = migrator
        .migrate(
            Direction::Up,
            MigrateOpts::new().with_dry_run(true).with_target_version(3),
        )
        .await
        .unwrap();
    assert_eq!(
        dry_run
            .versions
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        dry_run
            .versions
            .iter()
            .all(|migration| migration.duration == Duration::ZERO && !migration.sql.is_empty())
    );
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        Vec::<i64>::new()
    );

    let validation = migrator.validate(Some(3)).await.unwrap();
    assert!(!validation.ok);
    assert_eq!(validation.messages, vec!["unapplied migrations: [1, 2, 3]"]);

    migrator
        .migrate(Direction::Up, MigrateOpts::new().with_max_steps(2))
        .await
        .unwrap();
    assert_eq!(migrator.existing_versions().await.unwrap(), vec![1, 2]);
    assert!(migrator.validate(Some(2)).await.unwrap().ok);

    let error = migrator
        .migrate(
            Direction::Up,
            MigrateOpts::new().with_target_version(MIGRATION_VERSION_LATEST + 1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Invalid(_)));

    pool.close().await;
}

#[tokio::test]
async fn downgrade_preserves_non_main_migration_lines() {
    let pool = sqlite_pool().await;
    let migrator = SqliteMigrator::new(pool.clone());
    migrate_to(&migrator, 5).await;
    sqlx::query("INSERT INTO river_migration (line, version) VALUES ('extension', 1)")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=5).collect::<Vec<_>>()
    );
    let error = migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(4))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Invalid(_)));
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=5).collect::<Vec<_>>()
    );
    let extension_version: i64 =
        sqlx::query_scalar("SELECT version FROM river_migration WHERE line = 'extension'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(extension_version, 1);

    sqlx::query("DELETE FROM river_migration WHERE line = 'extension'")
        .execute(&pool)
        .await
        .unwrap();
    migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(4))
        .await
        .unwrap();
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=4).collect::<Vec<_>>()
    );

    pool.close().await;
}

#[tokio::test]
async fn latest_schema_and_json_survive_version_seven_round_trip() {
    let pool = sqlite_pool().await;
    let migrator = SqliteMigrator::new(pool.clone());
    migrate_to(&migrator, 6).await;

    let job_id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, kind, max_attempts, metadata, tags) \
         VALUES (json(?1), 'sqlite_migration_test', 9, json(?2), json(?3)) RETURNING id",
    )
    .bind(r#"{"message":"hello"}"#)
    .bind(r#"{"source":"test"}"#)
    .bind(r#"["one","two"]"#)
    .fetch_one(&pool)
    .await
    .unwrap();

    migrator.migrate_up().await.unwrap();
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=7).collect::<Vec<_>>()
    );
    assert_eq!(
        column_default(&pool, "river_job", "max_attempts")
            .await
            .as_deref(),
        Some("25")
    );
    assert_eq!(
        column_default(&pool, "river_queue", "updated_at")
            .await
            .as_deref(),
        Some("CURRENT_TIMESTAMP")
    );
    let row = sqlx::query(
        "SELECT typeof(args) AS args_type, json(args) AS args, json(metadata) AS metadata, \
         json(tags) AS tags FROM river_job WHERE id = ?1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("args_type"), "blob");
    assert_eq!(row.get::<String, _>("args"), r#"{"message":"hello"}"#);
    assert_eq!(row.get::<String, _>("metadata"), r#"{"source":"test"}"#);
    assert_eq!(row.get::<String, _>("tags"), r#"["one","two"]"#);

    migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(6))
        .await
        .unwrap();
    assert_eq!(
        column_default(&pool, "river_job", "max_attempts").await,
        None
    );
    let row = sqlx::query(
        "SELECT typeof(args) AS args_type, json(args) AS args, json(metadata) AS metadata, \
         json(tags) AS tags FROM river_job WHERE id = ?1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("args_type"), "text");
    assert_eq!(row.get::<String, _>("args"), r#"{"message":"hello"}"#);
    assert_eq!(row.get::<String, _>("metadata"), r#"{"source":"test"}"#);
    assert_eq!(row.get::<String, _>("tags"), r#"["one","two"]"#);

    migrator.migrate_up().await.unwrap();
    let args_type: String = sqlx::query_scalar("SELECT typeof(args) FROM river_job WHERE id = ?1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(args_type, "blob");

    pool.close().await;
}

#[tokio::test]
async fn migrates_up_from_every_historical_version() {
    let expected = schema_at(MIGRATION_VERSION_LATEST).await;

    for version in 1..=MIGRATION_VERSION_LATEST {
        let pool = sqlite_pool().await;
        let migrator = SqliteMigrator::new(pool.clone());
        migrate_to(&migrator, version).await;
        assert_eq!(
            migrator.existing_versions().await.unwrap(),
            (1..=version).collect::<Vec<_>>()
        );

        migrator.migrate_up().await.unwrap();
        assert_eq!(schema_snapshot(&pool).await, expected, "version {version}");
        assert!(migrator.validate(None).await.unwrap().ok);
        pool.close().await;
    }
}

#[tokio::test]
async fn migrates_down_to_every_historical_version_and_empty() {
    for target in 1..MIGRATION_VERSION_LATEST {
        let expected = schema_at(target).await;
        let pool = sqlite_pool().await;
        let migrator = SqliteMigrator::new(pool.clone());
        migrator.migrate_up().await.unwrap();
        migrator
            .migrate(
                Direction::Down,
                MigrateOpts::new().with_target_version(target),
            )
            .await
            .unwrap();
        assert_eq!(
            migrator.existing_versions().await.unwrap(),
            (1..=target).collect::<Vec<_>>()
        );
        assert_eq!(schema_snapshot(&pool).await, expected, "version {target}");
        pool.close().await;
    }

    let pool = sqlite_pool().await;
    let migrator = SqliteMigrator::new(pool.clone());
    migrator.migrate_up().await.unwrap();
    let result = migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(-1))
        .await
        .unwrap();
    assert_eq!(
        result
            .versions
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        (1..=MIGRATION_VERSION_LATEST).rev().collect::<Vec<_>>()
    );
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        Vec::<i64>::new()
    );
    assert!(schema_snapshot(&pool).await.is_empty());
    pool.close().await;
}

async fn column_default(pool: &SqlitePool, table: &str, column: &str) -> Option<String> {
    sqlx::query("SELECT dflt_value FROM pragma_table_info(?1) WHERE name = ?2")
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .unwrap()
        .get("dflt_value")
}

async fn migrate_to(migrator: &SqliteMigrator, version: i64) {
    migrator
        .migrate(
            Direction::Up,
            MigrateOpts::new().with_target_version(version),
        )
        .await
        .unwrap();
}

async fn schema_at(version: i64) -> Vec<(String, String, String, String)> {
    let pool = sqlite_pool().await;
    let migrator = SqliteMigrator::new(pool.clone());
    migrate_to(&migrator, version).await;
    let snapshot = schema_snapshot(&pool).await;
    pool.close().await;
    snapshot
}

async fn schema_snapshot(pool: &SqlitePool) -> Vec<(String, String, String, String)> {
    sqlx::query(
        "SELECT type, name, tbl_name, coalesce(sql, '') AS sql \
         FROM sqlite_schema WHERE name LIKE 'river_%' ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get("type"),
            row.get("name"),
            row.get("tbl_name"),
            row.get("sql"),
        )
    })
    .collect()
}

async fn sqlite_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

#[test]
fn sqlite_migrations_match_main_line_metadata() {
    assert_eq!(
        SQLITE_MIGRATIONS.len(),
        usize::try_from(MIGRATION_VERSION_LATEST).unwrap()
    );
    for (index, migration) in SQLITE_MIGRATIONS.iter().enumerate() {
        assert_eq!(migration.version, i64::try_from(index).unwrap() + 1);
        assert!(!migration.name.is_empty());
        assert!(!migration.up_sql.is_empty());
        assert!(!migration.down_sql.is_empty());
        assert!(!migration.up_sql.contains("CREATE TYPE"));
        assert!(!migration.up_sql.contains("LANGUAGE plpgsql"));
    }
    assert_eq!(MIGRATION_LINE_MAIN, "main");
}
