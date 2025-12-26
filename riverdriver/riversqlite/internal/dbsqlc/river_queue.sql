CREATE TABLE river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- name: QueueCreateOrSetUpdatedAt :one
INSERT INTO /* TEMPLATE: schema */river_queue (
    created_at,
    metadata,
    name,
    paused_at,
    updated_at
) VALUES (
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    json(cast(@metadata AS blob)),
    @name,
    cast(sqlc.narg('paused_at') AS text),
    coalesce(cast(sqlc.narg('updated_at') AS text), cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
) ON CONFLICT (name) DO UPDATE
SET
    updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: QueueDeleteExpired :many
DELETE FROM /* TEMPLATE: schema */river_queue
WHERE name IN (
    SELECT name
    FROM /* TEMPLATE: schema */river_queue
    WHERE river_queue.updated_at < @updated_at_horizon
    ORDER BY name ASC
    LIMIT @max
)
RETURNING *;

-- name: QueueGet :one
SELECT *
FROM /* TEMPLATE: schema */river_queue
WHERE name = @name;

-- name: QueueList :many
SELECT *
FROM /* TEMPLATE: schema */river_queue
ORDER BY name ASC
LIMIT @max;

-- name: QueueNameList :many
SELECT name
FROM /* TEMPLATE: schema */river_queue
WHERE
    name > cast(@after AS text)
    AND (cast(@match AS text) = '' OR LOWER(name) LIKE '%' || LOWER(cast(@match AS text)) || '%')
    AND name NOT IN (sqlc.slice('exclude'))
ORDER BY name ASC
LIMIT @max;

-- name: QueuePause :execrows
UPDATE /* TEMPLATE: schema */river_queue
SET
    paused_at = CASE WHEN paused_at IS NULL THEN coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')) ELSE paused_at END,
    updated_at = CASE WHEN paused_at IS NULL THEN coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')) ELSE updated_at END
WHERE CASE WHEN cast(@name AS text) = '*' THEN true ELSE name = @name END;

-- name: QueueResume :execrows
UPDATE /* TEMPLATE: schema */river_queue
SET
    paused_at = NULL,
    updated_at = CASE WHEN paused_at IS NOT NULL THEN coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')) ELSE updated_at END
WHERE CASE WHEN cast(@name AS text) = '*' THEN true ELSE name = @name END;

-- name: QueueUpdate :one
UPDATE /* TEMPLATE: schema */river_queue
SET
    metadata = CASE WHEN cast(@metadata_do_update AS boolean) THEN json(cast(@metadata AS blob)) ELSE metadata END,
    updated_at = datetime('now', 'subsec')
WHERE name = @name
RETURNING *;
