CREATE UNLOGGED TABLE river_leader(
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY,
    CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

-- name: LeaderAttemptElect :execrows
INSERT INTO river_leader(name, leader_id, elected_at, expires_at)
    VALUES (@name::text, @leader_id::text, now(), now() + @ttl::interval)
ON CONFLICT (name)
    DO NOTHING;

-- name: LeaderAttemptReelect :execrows
INSERT INTO river_leader(name, leader_id, elected_at, expires_at)
    VALUES (@name::text, @leader_id::text, now(), now() + @ttl::interval)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = now() + @ttl::interval
    WHERE
        river_leader.leader_id = @leader_id::text;

-- name: LeaderDeleteExpired :execrows
DELETE FROM river_leader
WHERE name = @name::text
    AND expires_at < now();

-- name: LeaderGetElectedLeader :one
SELECT *
FROM river_leader
WHERE name = @name;

-- name: LeaderInsert :one
INSERT INTO river_leader(
    elected_at,
    expires_at,
    leader_id,
    name
) VALUES (
    coalesce(sqlc.narg('elected_at')::timestamptz, now()),
    coalesce(sqlc.narg('expires_at')::timestamptz, now() + @ttl::interval),
    @leader_id,
    @name
) RETURNING *;

-- name: LeaderResign :execrows
WITH currently_held_leaders AS (
  SELECT *
  FROM river_leader
  WHERE
      name = @name::text
      AND leader_id = @leader_id::text
  FOR UPDATE
),
notified_resignations AS (
  SELECT
      pg_notify(@leadership_topic, json_build_object('name', name, 'leader_id', leader_id, 'action', 'resigned')::text),
      currently_held_leaders.name
  FROM currently_held_leaders
)
DELETE FROM river_leader USING notified_resignations
WHERE river_leader.name = notified_resignations.name;