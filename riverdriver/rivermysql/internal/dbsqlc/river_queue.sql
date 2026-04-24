CREATE TABLE river_queue (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL
);

-- name: QueueCreateOrSetUpdatedAtExec :exec
INSERT INTO /* TEMPLATE: schema */river_queue (
    created_at,
    metadata,
    name,
    paused_at,
    updated_at
) VALUES (
    COALESCE(sqlc.narg('now'), NOW(6)),
    CAST(sqlc.arg('metadata') AS JSON),
    sqlc.arg('name'),
    sqlc.narg('paused_at'),
    COALESCE(sqlc.narg('updated_at'), sqlc.narg('now'), NOW(6))
) ON DUPLICATE KEY UPDATE
    updated_at = VALUES(updated_at);

-- name: QueueGet :one
SELECT name, created_at, metadata, paused_at, updated_at
FROM /* TEMPLATE: schema */river_queue
WHERE name = sqlc.arg('name');

-- name: QueueDeleteExpiredSelect :many
SELECT name
FROM /* TEMPLATE: schema */river_queue
WHERE updated_at < sqlc.arg('updated_at_horizon')
ORDER BY name ASC
LIMIT ?;

-- name: QueueDeleteExpiredExec :exec
DELETE FROM /* TEMPLATE: schema */river_queue
WHERE name IN (sqlc.slice('names'));

-- name: QueueList :many
SELECT name, created_at, metadata, paused_at, updated_at
FROM /* TEMPLATE: schema */river_queue
ORDER BY name ASC
LIMIT ?;

-- name: QueueNameList :many
SELECT name
FROM /* TEMPLATE: schema */river_queue
WHERE
    name COLLATE utf8mb4_general_ci > sqlc.arg('after')
    AND (sqlc.arg('match') = '' OR LOWER(name) COLLATE utf8mb4_general_ci LIKE CONCAT('%', LOWER(sqlc.arg('match')), '%'))
    AND name NOT IN (sqlc.slice('exclude'))
ORDER BY name ASC
LIMIT ?;

-- MySQL evaluates SET clauses left-to-right using already-updated values, so
-- updated_at must be set BEFORE paused_at to see the original paused_at value.

-- name: QueuePauseAll :execresult
UPDATE /* TEMPLATE: schema */river_queue
SET
    updated_at = CASE WHEN paused_at IS NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE updated_at END,
    paused_at = CASE WHEN paused_at IS NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE paused_at END;

-- name: QueuePauseByName :execresult
UPDATE /* TEMPLATE: schema */river_queue
SET
    updated_at = CASE WHEN paused_at IS NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE updated_at END,
    paused_at = CASE WHEN paused_at IS NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE paused_at END
WHERE name = sqlc.arg('name');

-- name: QueueResumeAll :execresult
UPDATE /* TEMPLATE: schema */river_queue
SET
    updated_at = CASE WHEN paused_at IS NOT NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE updated_at END,
    paused_at = NULL;

-- name: QueueResumeByName :execresult
UPDATE /* TEMPLATE: schema */river_queue
SET
    updated_at = CASE WHEN paused_at IS NOT NULL THEN COALESCE(sqlc.narg('now'), NOW(6)) ELSE updated_at END,
    paused_at = NULL
WHERE name = sqlc.arg('name');

-- name: QueueUpdateExec :exec
UPDATE /* TEMPLATE: schema */river_queue
SET
    metadata = CASE WHEN CAST(sqlc.arg('metadata_do_update') AS SIGNED) THEN CAST(sqlc.arg('metadata') AS JSON) ELSE metadata END,
    updated_at = NOW(6)
WHERE name = sqlc.arg('name');
