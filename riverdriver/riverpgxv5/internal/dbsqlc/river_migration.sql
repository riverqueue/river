CREATE TABLE river_migration(
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  version bigint NOT NULL,
  CONSTRAINT version CHECK (version >= 1)
);

-- name: RiverMigrationDeleteByVersionMany :many
DELETE FROM river_migration
WHERE version = any(@version::bigint[])
RETURNING *;

-- name: RiverMigrationGetAll :many
SELECT *
FROM river_migration
ORDER BY version;

-- name: RiverMigrationInsert :one
INSERT INTO river_migration (
  version
) VALUES (
  @version
) RETURNING *;

-- name: RiverMigrationInsertMany :many
INSERT INTO river_migration (
  version
)
SELECT
  unnest(@version::bigint[])
RETURNING *;

-- name: TableExists :one
SELECT CASE WHEN to_regclass(@table_name) IS NULL THEN false
            ELSE true END;