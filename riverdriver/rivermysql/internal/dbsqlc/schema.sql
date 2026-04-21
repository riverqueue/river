-- Dummy table definitions for INFORMATION_SCHEMA system tables so sqlc can
-- resolve column types.  At runtime the template prefix replaces the empty
-- default with "INFORMATION_SCHEMA.", making queries target the real system
-- tables.
CREATE TABLE COLUMNS (
    COLUMN_NAME VARCHAR(128) NOT NULL,
    TABLE_NAME VARCHAR(128) NOT NULL,
    TABLE_SCHEMA VARCHAR(128) NOT NULL
);

CREATE TABLE SCHEMATA (
    SCHEMA_NAME VARCHAR(128) NOT NULL
);

CREATE TABLE STATISTICS (
    INDEX_NAME VARCHAR(128) NOT NULL,
    TABLE_NAME VARCHAR(128) NOT NULL,
    TABLE_SCHEMA VARCHAR(128) NOT NULL
);

CREATE TABLE TABLES (
    TABLE_NAME VARCHAR(128) NOT NULL,
    TABLE_SCHEMA VARCHAR(128) NOT NULL
);

-- name: ColumnExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: information_schema */COLUMNS
    WHERE COLUMN_NAME = sqlc.arg('column_name')
        AND TABLE_NAME = sqlc.arg('table_name')
        AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
);

-- name: IndexExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: information_schema */STATISTICS
    WHERE INDEX_NAME = sqlc.arg('index_name')
        AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
);

-- name: IndexGetTableName :one
SELECT TABLE_NAME
FROM /* TEMPLATE: information_schema */STATISTICS
WHERE INDEX_NAME = sqlc.arg('index_name')
    AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
LIMIT 1;

-- name: IndexReindexArtifacts :many
SELECT DISTINCT INDEX_NAME AS index_name
FROM /* TEMPLATE: information_schema */STATISTICS
WHERE TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
    AND REGEXP_LIKE(INDEX_NAME, sqlc.arg('artifact_pattern'), 'c')
ORDER BY INDEX_NAME;

-- name: IndexesExist :many
SELECT DISTINCT INDEX_NAME AS index_name
FROM /* TEMPLATE: information_schema */STATISTICS
WHERE INDEX_NAME IN (sqlc.slice('index_names'))
    AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE());

-- name: SchemaGetExpired :many
SELECT SCHEMA_NAME
FROM /* TEMPLATE: information_schema */SCHEMATA
WHERE SCHEMA_NAME LIKE CONCAT(sqlc.arg('prefix'), '%')
    AND SCHEMA_NAME < sqlc.arg('before_name')
ORDER BY SCHEMA_NAME;

-- name: TableExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: information_schema */TABLES
    WHERE TABLE_NAME = sqlc.arg('table_name')
        AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
);
