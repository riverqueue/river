CREATE TABLE river_queue(
  name text PRIMARY KEY NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
  paused_at timestamptz,
  updated_at timestamptz NOT NULL
);

-- name: QueueCreateOrSetUpdatedAt :one
INSERT INTO /* TEMPLATE: schema */river_queue(
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
    updated_at = coalesce(sqlc.narg('updated_at')::timestamptz, sqlc.narg('now')::timestamptz, now())
RETURNING *;

-- name: QueueDeleteExpired :many
DELETE FROM /* TEMPLATE: schema */river_queue
WHERE name IN (
    SELECT name
    FROM /* TEMPLATE: schema */river_queue
    WHERE updated_at < @updated_at_horizon::timestamptz
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
LIMIT @limit_count::integer;

-- name: QueuePause :execresult
WITH queue_to_update AS (
    SELECT name, paused_at
    FROM /* TEMPLATE: schema */river_queue
    WHERE CASE WHEN @name::text = '*' THEN true ELSE name = @name END
    FOR UPDATE
),
updated_queue AS (
    UPDATE /* TEMPLATE: schema */river_queue
    SET
        paused_at = now(),
        updated_at = now()
    FROM queue_to_update
    WHERE river_queue.name = queue_to_update.name
        AND river_queue.paused_at IS NULL
    RETURNING river_queue.*
)
SELECT *
FROM /* TEMPLATE: schema */river_queue
WHERE name = @name
    AND name NOT IN (SELECT name FROM updated_queue)
UNION
SELECT *
FROM updated_queue;

-- name: QueueResume :execresult
WITH queue_to_update AS (
    SELECT name
    FROM /* TEMPLATE: schema */river_queue
    WHERE CASE WHEN @name::text = '*' THEN true ELSE river_queue.name = @name::text END
    FOR UPDATE
),
updated_queue AS (
    UPDATE /* TEMPLATE: schema */river_queue
    SET
        paused_at = NULL,
        updated_at = now()
    FROM queue_to_update
    WHERE river_queue.name = queue_to_update.name
    RETURNING river_queue.*
)
SELECT *
FROM /* TEMPLATE: schema */river_queue
WHERE name = @name
    AND name NOT IN (SELECT name FROM updated_queue)
UNION
SELECT *
FROM updated_queue;

-- name: QueueUpdate :one
UPDATE /* TEMPLATE: schema */river_queue
SET
    metadata = CASE WHEN @metadata_do_update::boolean THEN @metadata::jsonb ELSE metadata END,
    updated_at = now()
WHERE name = @name::text
RETURNING river_queue.*;
