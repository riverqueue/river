#![cfg(feature = "postgres-tests")]

use riverqueue_migrate::SchemaName;
use riverqueue_migrate::{Direction, MIGRATION_VERSION_LATEST, MigrateOpts, PostgresMigrator};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool};

#[tokio::test]
async fn upgrades_from_every_historical_version() {
    let pool = test_pool().await;

    let reference_schema = "rust_migrate_reference";
    recreate_schema(&pool, reference_schema).await;
    let reference =
        PostgresMigrator::new(pool.clone()).with_schema(SchemaName::new(reference_schema).unwrap());
    reference.migrate_up().await.unwrap();
    let expected = schema_snapshot(&pool, reference_schema).await;

    for version in 1..=MIGRATION_VERSION_LATEST {
        let schema = format!("rust_migrate_from_{version}");
        recreate_schema(&pool, &schema).await;
        let migrator = PostgresMigrator::new(pool.clone())
            .with_schema(SchemaName::new(schema.clone()).unwrap());
        migrator
            .migrate(
                Direction::Up,
                MigrateOpts::new().with_target_version(version),
            )
            .await
            .unwrap();
        assert_eq!(
            migrator.existing_versions().await.unwrap(),
            (1..=version).collect::<Vec<_>>()
        );
        migrator.migrate_up().await.unwrap();
        assert_eq!(schema_snapshot(&pool, &schema).await, expected);

        if version < MIGRATION_VERSION_LATEST {
            migrator
                .migrate(
                    Direction::Down,
                    MigrateOpts::new().with_target_version(version),
                )
                .await
                .unwrap();
            assert_eq!(
                migrator.existing_versions().await.unwrap(),
                (1..=version).collect::<Vec<_>>()
            );
            migrator.migrate_up().await.unwrap();
            assert_eq!(schema_snapshot(&pool, &schema).await, expected);
        }
    }

    for version in 1..=MIGRATION_VERSION_LATEST {
        let schema = format!("rust_migrate_from_{version}");
        drop_schema(&pool, &schema).await;
    }
    drop_schema(&pool, reference_schema).await;
}

#[tokio::test]
async fn mixed_case_schema_is_detected_as_migrated() {
    let pool = test_pool().await;
    let schema = unique_schema("RiverMixedCase");
    recreate_schema(&pool, &schema).await;
    let migrator =
        PostgresMigrator::new(pool.clone()).with_schema(SchemaName::new(schema.clone()).unwrap());

    migrator.migrate_up().await.unwrap();
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );
    // A second run must see the applied versions instead of re-running 001.
    assert!(migrator.migrate_up().await.unwrap().is_empty());
    assert!(migrator.validate(None).await.unwrap().ok);

    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn unknown_versions_are_ignored_and_unapplied_down_targets_fail() {
    let pool = test_pool().await;
    let schema = unique_schema("rust_migrate_semantics");
    recreate_schema(&pool, &schema).await;
    let schema_name = SchemaName::new(schema.clone()).unwrap();
    let migrator = PostgresMigrator::new(pool.clone()).with_schema(schema_name.clone());
    migrator.migrate_up().await.unwrap();

    // A newer River release recorded a version this crate does not bundle.
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (line, version) VALUES ('main', $1)",
        schema_name.qualify("river_migration")
    )))
    .bind(MIGRATION_VERSION_LATEST + 1)
    .execute(&pool)
    .await
    .unwrap();
    assert!(migrator.migrate_up().await.unwrap().is_empty());
    assert!(migrator.validate(None).await.unwrap().ok);
    let reverted = migrator
        .migrate(Direction::Down, MigrateOpts::new())
        .await
        .unwrap();
    assert_eq!(
        reverted
            .versions
            .iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        vec![MIGRATION_VERSION_LATEST]
    );

    // Migrating down to a version that is not applied errors like Go.
    migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(3))
        .await
        .unwrap();
    let error = migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(5))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 5 is not in target list of valid migrations to apply"),
        "{error}"
    );

    drop_schema(&pool, &schema).await;
}

async fn test_pool() -> PgPool {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    PgPool::connect(&database_url).await.unwrap()
}

fn unique_schema(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{prefix}_{:x}_{nanos:x}", std::process::id())
}

async fn drop_schema(pool: &PgPool, schema: &str) {
    let sql = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

async fn recreate_schema(pool: &PgPool, schema: &str) {
    drop_schema(pool, schema).await;
    let sql = format!("CREATE SCHEMA \"{schema}\"");
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

async fn schema_snapshot(pool: &PgPool, schema: &str) -> Value {
    let mut snapshot = sqlx::query_scalar::<_, Value>(
        r"
        WITH objects AS (
            SELECT 'column' AS object_kind,
                   table_name || '.' || column_name || ':' || data_type || ':' || udt_name || ':' || is_nullable || ':' || coalesce(column_default, '') AS definition
            FROM information_schema.columns
            WHERE table_schema = $1
            UNION ALL
            SELECT 'constraint', c.relname || ':' || pg_get_constraintdef(con.oid, true)
            FROM pg_constraint AS con
            JOIN pg_class AS c ON c.oid = con.conrelid
            WHERE con.connamespace = $1::regnamespace
            UNION ALL
            SELECT 'function', proname || ':' || pg_get_functiondef(oid)
            FROM pg_proc
            WHERE pronamespace = $1::regnamespace
            UNION ALL
            SELECT 'index', tablename || ':' || regexp_replace(indexdef, ' ON [^ ]+\\.', ' ON <schema>.')
            FROM pg_indexes
            WHERE schemaname = $1
            UNION ALL
            SELECT 'trigger', event_object_table || ':' || trigger_name || ':' || action_timing || ':' || event_manipulation || ':' || action_statement
            FROM information_schema.triggers
            WHERE trigger_schema = $1
            UNION ALL
            SELECT 'type', t.typname || ':' || string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
            FROM pg_type AS t
            JOIN pg_enum AS e ON e.enumtypid = t.oid
            WHERE t.typnamespace = $1::regnamespace
            GROUP BY t.typname
        )
        SELECT coalesce(jsonb_agg(jsonb_build_array(object_kind, definition) ORDER BY object_kind, definition), '[]'::jsonb)
        FROM objects
        ",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .unwrap();
    normalize_schema_name(&mut snapshot, schema);
    snapshot
}

fn normalize_schema_name(value: &mut Value, schema: &str) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_schema_name(value, schema);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                normalize_schema_name(value, schema);
            }
        }
        Value::String(string) => {
            *string = string
                .replace(&format!("\"{schema}\"."), "<schema>.")
                .replace(&format!("{schema}."), "<schema>.");
        }
        Value::Bool(_) | Value::Null | Value::Number(_) => {}
    }
}
