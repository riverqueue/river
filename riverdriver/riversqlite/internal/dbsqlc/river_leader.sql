CREATE TABLE river_leader (
    elected_at timestamp NOT NULL,
    expires_at timestamp NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY NOT NULL DEFAULT 'default' CHECK (name = 'default'),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT leader_id_length CHECK (length(leader_id) > 0 AND length(leader_id) < 128)
);

-- name: LeaderAttemptElect :execrows
INSERT INTO /* TEMPLATE: schema */river_leader (
    leader_id,
    elected_at,
    expires_at
) VALUES (
    @leader_id,
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    datetime(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')), 'subsec', cast(@ttl as text))
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
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    datetime(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')), 'subsec', cast(@ttl as text))
)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = EXCLUDED.expires_at
    WHERE
        leader_id = EXCLUDED.leader_id;

-- name: LeaderDeleteExpired :execrows
DELETE FROM /* TEMPLATE: schema */river_leader
WHERE expires_at < coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'));

-- name: LeaderGetElectedLeader :one
SELECT *
FROM /* TEMPLATE: schema */river_leader;

-- name: LeaderInsert :one
INSERT INTO /* TEMPLATE: schema */river_leader(
    elected_at,
    expires_at,
    leader_id
) VALUES (
    coalesce(cast(sqlc.narg('elected_at') AS text), cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    coalesce(cast(sqlc.narg('expires_at') AS text), datetime(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')), 'subsec', cast(@ttl as text))),
    @leader_id
) RETURNING *;

-- name: LeaderResign :execrows
DELETE
FROM /* TEMPLATE: schema */river_leader
WHERE leader_id = @leader_id;