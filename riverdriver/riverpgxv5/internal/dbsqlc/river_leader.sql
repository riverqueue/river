CREATE UNLOGGED TABLE river_leader(
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY DEFAULT 'default' CHECK (name = 'default'),
    CONSTRAINT name_length CHECK (name = 'default'),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

-- name: LeaderAttemptElect :execrows
INSERT INTO /* TEMPLATE: schema */river_leader (
    leader_id,
    elected_at,
    expires_at
) VALUES (
    @leader_id,
    coalesce(sqlc.narg('now')::timestamptz, now()),
    -- @ttl is inserted as as seconds rather than a duration because `lib/pq` doesn't support the latter
    coalesce(sqlc.narg('now')::timestamptz, now()) + make_interval(secs => @ttl)
)
ON CONFLICT (name)
    DO NOTHING;

-- name: LeaderAttemptReelect :execrows
INSERT INTO /* TEMPLATE: schema */river_leader (
    leader_id,
    elected_at,
    expires_at
) VALUES (
    @leader_id,
    coalesce(sqlc.narg('now')::timestamptz, now()),
    coalesce(sqlc.narg('now')::timestamptz, now()) + make_interval(secs => @ttl)
)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = EXCLUDED.expires_at
    WHERE
        river_leader.leader_id = @leader_id;

-- name: LeaderDeleteExpired :execrows
DELETE FROM /* TEMPLATE: schema */river_leader
WHERE expires_at < coalesce(sqlc.narg('now')::timestamptz, now());

-- name: LeaderGetElectedLeader :one
SELECT *
FROM /* TEMPLATE: schema */river_leader;

-- name: LeaderInsert :one
INSERT INTO /* TEMPLATE: schema */river_leader(
    elected_at,
    expires_at,
    leader_id
) VALUES (
    coalesce(sqlc.narg('elected_at')::timestamptz, coalesce(sqlc.narg('now')::timestamptz, now())),
    coalesce(sqlc.narg('expires_at')::timestamptz, coalesce(sqlc.narg('now')::timestamptz, now()) + make_interval(secs => @ttl)),
    @leader_id
) RETURNING *;

-- name: LeaderResign :execrows
WITH currently_held_leaders AS (
    SELECT *
    FROM /* TEMPLATE: schema */river_leader
    WHERE leader_id = @leader_id::text
    FOR UPDATE
),
notified_resignations AS (
    SELECT pg_notify(
        concat(coalesce(sqlc.narg('schema')::text, current_schema()), '.', @leadership_topic::text),
        json_build_object('leader_id', leader_id, 'action', 'resigned')::text
    )
    FROM currently_held_leaders
)
DELETE FROM /* TEMPLATE: schema */river_leader USING notified_resignations;