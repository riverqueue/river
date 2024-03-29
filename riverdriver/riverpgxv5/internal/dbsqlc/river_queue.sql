CREATE TABLE river_queue(
  name text PRIMARY KEY NOT NULL,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
  paused_at timestamptz,
  updated_at timestamptz NOT NULL
);

-- name: QueueCreateOrSetUpdatedAt :one
INSERT INTO river_queue(
    created_at,
    metadata,
    name,
    paused_at,
    updated_at
) VALUES (
    now(),
    coalesce(@metadata::jsonb, '{}'::jsonb),
    @name::text,
    coalesce(sqlc.narg('paused_at')::timestamptz, NULL),
    coalesce(sqlc.narg('updated_at')::timestamptz, now())
) ON CONFLICT (name) DO UPDATE
SET
    updated_at = coalesce(sqlc.narg('updated_at')::timestamptz, now())
RETURNING *;

-- name: QueueDeleteExpired :many
DELETE FROM river_queue
WHERE name IN (
    SELECT name
    FROM river_queue
    WHERE updated_at < @updated_at_horizon::timestamptz
    ORDER BY name ASC
    LIMIT @max::bigint
)
RETURNING *;

-- name: QueueGet :one
SELECT *
FROM river_queue
WHERE name = @name::text;

-- name: QueueList :many
SELECT *
FROM river_queue
ORDER BY name ASC
LIMIT @limit_count::integer;

-- name: QueuePause :execresult
WITH queue_to_update AS (
    SELECT name
    FROM river_queue
    WHERE CASE WHEN @name::text = '*' THEN true ELSE river_queue.name = @name::text END
      AND paused_at IS NULL
    FOR UPDATE
)

UPDATE river_queue
SET
    paused_at = now(),
    updated_at = now()
FROM queue_to_update
WHERE river_queue.name = queue_to_update.name;

-- name: QueueResume :execresult
WITH queue_to_update AS (
    SELECT name
    FROM river_queue
    WHERE CASE WHEN @name::text = '*' THEN true ELSE river_queue.name = @name::text END
      AND paused_at IS NOT NULL
    FOR UPDATE
)

UPDATE river_queue
SET
    paused_at = NULL,
    updated_at = now()
FROM queue_to_update
WHERE river_queue.name = queue_to_update.name;
