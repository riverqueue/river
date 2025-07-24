-- name: ColumnExists :one
SELECT EXISTS (
    SELECT column_name
    FROM information_schema.columns 
    WHERE table_name = @table_name::text
        AND table_schema = /* TEMPLATE_BEGIN: schema */ CURRENT_SCHEMA /* TEMPLATE_END */
        AND column_name = @column_name::text
);

-- name: IndexExists :one
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class
        JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE pg_class.relname = @index::text
        AND pg_namespace.nspname = coalesce(sqlc.narg('schema')::text, current_schema())
        AND pg_class.relkind = 'i'
);

-- name: IndexesExist :many
WITH index_names AS (
    SELECT unnest(@index_names::text[]) as index_name
)
SELECT index_names.index_name::text AS index_name,
       EXISTS (
         SELECT 1
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = coalesce(sqlc.narg('schema')::text, current_schema())
         AND c.relname = index_names.index_name
         AND c.relkind = 'i'
       ) AS exists
FROM index_names;

-- name: SchemaGetExpired :many
SELECT schema_name::text
FROM information_schema.schemata
WHERE schema_name LIKE @prefix
    AND schema_name < @before_name
ORDER BY schema_name;

-- name: TableExists :one
SELECT CASE WHEN to_regclass(@schema_and_table) IS NULL THEN false
            ELSE true END;
