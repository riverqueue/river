CREATE TABLE river_migration(
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    line TEXT NOT NULL,
    version bigint NOT NULL,
    CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128),
    CONSTRAINT version CHECK (version >= 1)
);

-- name: RiverMigrationDeleteAssumingMainMany :many
DELETE FROM river_migration
WHERE version = any(@version::bigint[])
RETURNING
    id,
    created_at,
    version;

-- name: RiverMigrationDeleteByLineAndVersionMany :many
DELETE FROM river_migration
WHERE line = @line
    AND version = any(@version::bigint[])
RETURNING *;

-- This is a compatibility query for getting existing migrations before the
-- `line` column was added to the table in version 005. We need to make sure to
-- only select non-line properties so the query doesn't error on older schemas.
-- (Even if we use `SELECT *` below, sqlc materializes it to a list of column
-- names in the generated query.)
-- name: RiverMigrationGetAllAssumingMain :many
SELECT
    id,
    created_at,
    version
FROM river_migration
ORDER BY version;

-- name: RiverMigrationGetByLine :many
SELECT *
FROM river_migration
WHERE line = @line
ORDER BY version;

-- name: RiverMigrationInsert :one
INSERT INTO river_migration (
    line,
    version
) VALUES (
    @line,
    @version
) RETURNING *;

-- name: RiverMigrationInsertMany :many
INSERT INTO river_migration (
    line,
    version
)
SELECT
    @line,
    unnest(@version::bigint[])
RETURNING *;

-- name: RiverMigrationInsertManyAssumingMain :many
INSERT INTO river_migration (
    version
)
SELECT
    unnest(@version::bigint[])
RETURNING
    id,
    created_at,
    version;

-- name: ColumnExists :one
SELECT EXISTS (
    SELECT column_name
    FROM information_schema.columns 
    WHERE table_name = @table_name and column_name = @column_name
);

-- name: TableExists :one
SELECT CASE WHEN to_regclass(@table_name) IS NULL THEN false
            ELSE true END;