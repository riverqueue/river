CREATE TABLE river_migration (
    line text NOT NULL,
    version integer NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT line_length CHECK (length(line) > 0 AND length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
);

-- name: RiverMigrationDeleteAssumingMainMany :many
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE version IN (sqlc.slice('version'))
RETURNING
    created_at,
    version;

-- name: RiverMigrationDeleteByLineAndVersionMany :many
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE line = @line
    AND version IN (sqlc.slice('version'))
RETURNING *;

-- This is a compatibility query for getting existing migrations before the
-- `line` column was added to the table in version 005. We need to make sure to
-- only select non-line properties so the query doesn't error on older schemas.
-- (Even if we use `SELECT *` below, sqlc materializes it to a list of column
-- names in the generated query.)
--
-- name: RiverMigrationGetAllAssumingMain :many
SELECT
    created_at,
    version
FROM /* TEMPLATE: schema */river_migration
ORDER BY version;

-- name: RiverMigrationGetByLine :many
SELECT *
FROM /* TEMPLATE: schema */river_migration
WHERE line = @line
ORDER BY version;

-- Insert a migration.
--
-- This is supposed to be a batch insert, but various limitations of the
-- combined SQLite + sqlc has left me unable to find a way of injecting many
-- arguments en masse (like how we slightly abuse arrays to pull it off for the
-- Postgres drivers), so we loop over many insert operations instead, with the
-- expectation that this may be fixable in the future. Because SQLite targets
-- will often be local and therefore with a very minimal round trip compared to
-- a network, looping over operations is probably okay performance-wise.
-- name: RiverMigrationInsert :one
INSERT INTO /* TEMPLATE: schema */river_migration (
    line,
    version
) VALUES (
    @line,
    @version
) RETURNING *;

-- name: RiverMigrationInsertAssumingMain :one
INSERT INTO /* TEMPLATE: schema */river_migration (
    version
) VALUES (
    @version
)
RETURNING
    created_at,
    version;

-- Built-in table that sqlc doesn't know about.
CREATE TABLE sqlite_master (
    type text,
    name text,
    tbl_name text,
    rootpage integer,
    sql text
);

-- name: TableExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: schema */sqlite_master WHERE type = 'table' AND name = cast(@table AS text)
);