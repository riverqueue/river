CREATE UNLOGGED TABLE river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
    paused_at timestamptz,
    updated_at timestamptz NOT NULL,
    CONSTRAINT name_length CHECK (char_length(id) > 0 AND char_length(id) < 128)
);

-- name: ClientCreateOrSetUpdatedAt :one
INSERT INTO river_client (
    id,
    metadata,
    paused_at,
    updated_at
) VALUES (
    @id,
    coalesce(@metadata::jsonb, '{}'::jsonb),
    coalesce(sqlc.narg('paused_at')::timestamptz, NULL),
    coalesce(sqlc.narg('updated_at')::timestamptz, now())
) ON CONFLICT (name) DO UPDATE
SET
    updated_at = coalesce(sqlc.narg('updated_at')::timestamptz, now())
RETURNING *;