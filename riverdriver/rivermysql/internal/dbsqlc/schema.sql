-- Dummy table definitions for INFORMATION_SCHEMA system tables so sqlc can
-- resolve column types.  At runtime the template prefix replaces the empty
-- default with "INFORMATION_SCHEMA.", making queries target the real system
-- tables.
CREATE TABLE STATISTICS (
    INDEX_NAME VARCHAR(128) NOT NULL,
    TABLE_NAME VARCHAR(128) NOT NULL,
    TABLE_SCHEMA VARCHAR(128) NOT NULL
);

CREATE TABLE TABLES (
    TABLE_NAME VARCHAR(128) NOT NULL,
    TABLE_SCHEMA VARCHAR(128) NOT NULL
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

-- name: IndexesExist :many
SELECT DISTINCT INDEX_NAME AS index_name
FROM /* TEMPLATE: information_schema */STATISTICS
WHERE INDEX_NAME IN (sqlc.slice('index_names'))
    AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE());

-- name: TableExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: information_schema */TABLES
    WHERE TABLE_NAME = sqlc.arg('table_name')
        AND TABLE_SCHEMA = COALESCE(sqlc.narg('schema'), DATABASE())
);
