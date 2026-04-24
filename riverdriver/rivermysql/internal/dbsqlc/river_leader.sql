CREATE TABLE river_leader (
    elected_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    leader_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL DEFAULT 'default' PRIMARY KEY
);

-- name: LeaderAttemptElectExec :execresult
INSERT IGNORE INTO /* TEMPLATE: schema */river_leader (
    leader_id,
    elected_at,
    expires_at
) VALUES (
    sqlc.arg('leader_id'),
    COALESCE(sqlc.narg('now'), NOW(6)),
    TIMESTAMPADD(MICROSECOND, sqlc.arg('ttl'), COALESCE(sqlc.narg('now'), NOW(6)))
);

-- name: LeaderAttemptReelectExec :execresult
UPDATE /* TEMPLATE: schema */river_leader
SET expires_at = TIMESTAMPADD(MICROSECOND, sqlc.arg('ttl'), COALESCE(sqlc.narg('now'), NOW(6)))
WHERE
    elected_at = sqlc.arg('elected_at')
    AND expires_at >= COALESCE(sqlc.narg('now'), NOW(6))
    AND leader_id = sqlc.arg('leader_id');

-- name: LeaderDeleteExpired :execrows
DELETE FROM /* TEMPLATE: schema */river_leader
WHERE expires_at < COALESCE(sqlc.narg('now'), NOW(6));

-- name: LeaderGetElectedLeader :one
SELECT elected_at, expires_at, leader_id, name
FROM /* TEMPLATE: schema */river_leader;

-- name: LeaderInsertExec :exec
INSERT INTO /* TEMPLATE: schema */river_leader (
    elected_at,
    expires_at,
    leader_id
) VALUES (
    COALESCE(sqlc.narg('elected_at'), sqlc.narg('now'), NOW(6)),
    COALESCE(sqlc.narg('expires_at'), TIMESTAMPADD(MICROSECOND, sqlc.arg('ttl'), COALESCE(sqlc.narg('now'), NOW(6)))),
    sqlc.arg('leader_id')
);

-- name: LeaderResign :execrows
DELETE FROM /* TEMPLATE: schema */river_leader
WHERE
    elected_at = sqlc.arg('elected_at')
    AND leader_id = sqlc.arg('leader_id');
