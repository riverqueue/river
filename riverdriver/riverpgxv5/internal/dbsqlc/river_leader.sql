CREATE UNLOGGED TABLE river_leader(
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY DEFAULT 'default',
    CONSTRAINT name_length CHECK (name = 'default'),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

-- name: LeaderAttemptElect :execrows
INSERT INTO river_leader(leader_id, elected_at, expires_at)
    VALUES (@leader_id, now(), now() + @ttl::interval)
ON CONFLICT (name)
    DO NOTHING;

-- name: LeaderAttemptReelect :execrows
INSERT INTO river_leader(leader_id, elected_at, expires_at)
    VALUES (@leader_id, now(), now() + @ttl::interval)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = now() + @ttl
    WHERE
        river_leader.leader_id = @leader_id;

-- name: LeaderDeleteExpired :execrows
DELETE FROM river_leader
WHERE expires_at < now();

-- name: LeaderGetElectedLeader :one
SELECT *
FROM river_leader;

-- name: LeaderInsert :one
INSERT INTO river_leader(
    elected_at,
    expires_at,
    leader_id
) VALUES (
    coalesce(sqlc.narg('elected_at')::timestamptz, now()),
    coalesce(sqlc.narg('expires_at')::timestamptz, now() + @ttl::interval),
    @leader_id
) RETURNING *;

-- name: LeaderResign :execrows
WITH currently_held_leaders AS (
  SELECT *
  FROM river_leader
  WHERE leader_id = @leader_id::text
  FOR UPDATE
),
notified_resignations AS (
    SELECT pg_notify(
        concat(current_schema(), '.', @leadership_topic::text),
        json_build_object('leader_id', leader_id, 'action', 'resigned')::text
    )
    FROM currently_held_leaders
)
DELETE FROM river_leader USING notified_resignations;