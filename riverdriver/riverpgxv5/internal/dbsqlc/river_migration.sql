CREATE TABLE river_migration(
    line text NOT NULL,
    version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
);

-- name: RiverMigrationDeleteAssumingMainMany :many
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE version = any(@version::bigint[])
RETURNING
    created_at,
    version;

-- name: RiverMigrationDeleteByLineAndVersionMany :many
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE line = @line
    AND version = any(@version::bigint[])
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

-- name: RiverMigrationInsert :one
INSERT INTO /* TEMPLATE: schema */river_migration (
    line,
    version
) VALUES (
    @line,
    @version
) RETURNING *;

-- name: RiverMigrationInsertMany :many
INSERT INTO /* TEMPLATE: schema */river_migration (
    line,
    version
)
SELECT
    @line,
    unnest(@version::bigint[])
RETURNING *;

-- name: RiverMigrationInsertManyAssumingMain :many
INSERT INTO /* TEMPLATE: schema */river_migration (
    version
)
SELECT
    unnest(@version::bigint[])
RETURNING
    created_at,
    version;