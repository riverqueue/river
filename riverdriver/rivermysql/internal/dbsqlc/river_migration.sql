CREATE TABLE river_migration (
    line VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    PRIMARY KEY (line, version)
);

-- name: RiverMigrationDeleteAssumingMainMany :many
SELECT created_at, version
FROM /* TEMPLATE: schema */river_migration
WHERE version IN (sqlc.slice('version'));

-- name: RiverMigrationDeleteAssumingMainManyExec :exec
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE version IN (sqlc.slice('version'));

-- name: RiverMigrationDeleteByLineAndVersionMany :many
SELECT line, version, created_at
FROM /* TEMPLATE: schema */river_migration
WHERE line = sqlc.arg('line')
    AND version IN (sqlc.slice('version'));

-- name: RiverMigrationDeleteByLineAndVersionManyExec :exec
DELETE FROM /* TEMPLATE: schema */river_migration
WHERE line = sqlc.arg('line')
    AND version IN (sqlc.slice('version'));

-- name: RiverMigrationGetAllAssumingMain :many
SELECT
    created_at,
    version
FROM /* TEMPLATE: schema */river_migration
ORDER BY version;

-- name: RiverMigrationGetByLine :many
SELECT line, version, created_at
FROM /* TEMPLATE: schema */river_migration
WHERE line = sqlc.arg('line')
ORDER BY version;

-- name: RiverMigrationInsertExec :exec
INSERT INTO /* TEMPLATE: schema */river_migration (
    line,
    version
) VALUES (
    sqlc.arg('line'),
    sqlc.arg('version')
);

-- name: RiverMigrationGetByLineAndVersion :one
SELECT line, version, created_at
FROM /* TEMPLATE: schema */river_migration
WHERE line = sqlc.arg('line') AND version = sqlc.arg('version');

-- name: RiverMigrationGetByLineAndVersionMany :many
SELECT line, version, created_at
FROM /* TEMPLATE: schema */river_migration
WHERE line = sqlc.arg('line') AND version IN (sqlc.slice('version'))
ORDER BY version;

-- name: RiverMigrationInsertAssumingMainExec :exec
INSERT INTO /* TEMPLATE: schema */river_migration (
    version
) VALUES (
    sqlc.arg('version')
);

-- name: RiverMigrationGetByVersion :one
SELECT created_at, version
FROM /* TEMPLATE: schema */river_migration
WHERE version = sqlc.arg('version');

-- name: RiverMigrationGetByVersionMany :many
SELECT created_at, version
FROM /* TEMPLATE: schema */river_migration
WHERE version IN (sqlc.slice('version'))
ORDER BY version;
