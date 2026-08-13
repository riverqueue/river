#![cfg(feature = "postgres-tests")]

use riverqueue_internal::SchemaName;
use riverqueue_migrate::{Direction, MIGRATION_VERSION_LATEST, MigrateOpts, PostgresMigrator};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool};

#[tokio::test]
async fn upgrades_from_every_historical_version() {
    let Ok(database_url) = std::env::var("RIVER_RUST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL migration test without RIVER_RUST_DATABASE_URL");
        return;
    };
    let pool = PgPool::connect(&database_url).await.unwrap();

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
