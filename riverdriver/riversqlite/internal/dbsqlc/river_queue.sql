CREATE TABLE river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata jsonb NOT NULL DEFAULT (jsonb('{}')),
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
    jsonb(@metadata),
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
        AND CASE
            WHEN json_type(river_queue.metadata, '$') != 'object'
                OR NOT EXISTS (
                    SELECT 1 FROM json_each(river_queue.metadata) WHERE key = 'river:rate_limit_rollup'
                ) THEN true
            WHEN json_type(river_queue.metadata, '$."river:rate_limit_rollup".expires_at_unix') = 'integer'
                AND cast(json_extract(river_queue.metadata, '$."river:rate_limit_rollup".expires_at_unix') AS integer) >= 0
                THEN cast(json_extract(river_queue.metadata, '$."river:rate_limit_rollup".expires_at_unix') AS integer)
                    <= unixepoch(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')))
            ELSE false
        END
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
    metadata = CASE WHEN cast(@metadata_do_update AS boolean) THEN
        jsonb_patch(
            CASE WHEN json_type(jsonb(@metadata), '$') = 'object' THEN
                jsonb(@metadata)
            ELSE
                jsonb_object('river:user_metadata', jsonb(@metadata))
            END,
            CASE WHEN json_type(river_queue.metadata, '$') = 'object' THEN
                coalesce(
                    (
                        SELECT jsonb_group_object(key, jsonb(value))
                        FROM json_each(river_queue.metadata)
                        WHERE key GLOB 'river:*' AND key != 'river:user_metadata'
                    ),
                    jsonb('{}')
                )
            ELSE jsonb('{}') END
        )
    ELSE metadata END,
    updated_at = datetime('now', 'subsec')
WHERE name = @name
RETURNING *;
