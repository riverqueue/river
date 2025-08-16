CREATE TABLE river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
    paused_at timestamptz,
    updated_at timestamptz NOT NULL
);

-- name: QueueCreateOrSetUpdatedAt :one
INSERT INTO /* TEMPLATE: schema */river_queue (
    created_at,
    metadata,
    name,
    paused_at,
    updated_at
) VALUES (
    coalesce(sqlc.narg('now')::timestamptz, now()),
    coalesce(@metadata::jsonb, '{}'::jsonb),
    @name::text,
    coalesce(sqlc.narg('paused_at')::timestamptz, NULL),
    coalesce(sqlc.narg('updated_at')::timestamptz, sqlc.narg('now')::timestamptz, now())
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
    LIMIT @max::bigint
)
RETURNING *;

-- name: QueueGet :one
SELECT *
FROM /* TEMPLATE: schema */river_queue
WHERE name = @name::text;

-- name: QueueList :many
SELECT *
FROM /* TEMPLATE: schema */river_queue
ORDER BY name ASC
LIMIT @max;

-- name: QueueNameList :many
SELECT name
FROM /* TEMPLATE: schema */river_queue
WHERE name > @after::text
    AND (@match::text = '' OR name ILIKE '%' || @match::text || '%')
    AND (@exclude::text[] IS NULL OR name != ALL(@exclude))
ORDER BY name
LIMIT @max::int;

-- name: QueuePause :execrows
UPDATE /* TEMPLATE: schema */river_queue
SET
    paused_at = CASE WHEN paused_at IS NULL THEN coalesce(sqlc.narg('now')::timestamptz, now()) ELSE paused_at END,
    updated_at = CASE WHEN paused_at IS NULL THEN coalesce(sqlc.narg('now')::timestamptz, now()) ELSE updated_at END
WHERE CASE WHEN @name::text = '*' THEN true ELSE name = @name END;

-- name: QueueResume :execrows
UPDATE /* TEMPLATE: schema */river_queue
SET
    paused_at = NULL,
    updated_at = CASE WHEN paused_at IS NOT NULL THEN coalesce(sqlc.narg('now')::timestamptz, now()) ELSE updated_at END
WHERE CASE WHEN @name::text = '*' THEN true ELSE name = @name END;

-- name: QueueUpdate :one
UPDATE /* TEMPLATE: schema */river_queue
SET
    metadata = CASE WHEN @metadata_do_update::boolean THEN @metadata::jsonb ELSE metadata END,
    updated_at = now()
WHERE name = @name
RETURNING *;
