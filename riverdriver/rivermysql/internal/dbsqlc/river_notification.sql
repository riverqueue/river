CREATE TABLE river_notification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- sqlc's MySQL parser doesn't accept UTC_TIMESTAMP(6) in a DEFAULT
    -- expression. Runtime migrations use UTC_TIMESTAMP(6); this codegen-only
    -- schema declaration uses NOW(6).
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    payload TEXT NOT NULL,
    topic VARCHAR(127) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    CONSTRAINT topic_length CHECK (CHAR_LENGTH(topic) > 0 AND CHAR_LENGTH(topic) < 128)
);

-- name: NotificationDeleteBefore :execrows
DELETE FROM /* TEMPLATE: schema */river_notification
WHERE created_at < sqlc.arg('created_at_horizon');

-- name: NotificationGetAfterForUpdate :one
-- InnoDB allocates auto-increment IDs before commit, so IDs may commit out of
-- order. The ascending locking read waits for an earlier uncommitted row rather
-- than returning a later committed row and causing the listener to skip it.
SELECT *
FROM /* TEMPLATE: schema */river_notification
WHERE id > sqlc.arg('after')
ORDER BY id ASC
LIMIT 1
FOR UPDATE;

-- name: NotificationGetIDsForUpdate :many
-- Used to establish a listener's initial high-water mark. This intentionally
-- scans in ascending order instead of using MAX(id) or a descending LIMIT so
-- that it encounters and waits for any lower uncommitted auto-increment IDs.
SELECT id
FROM /* TEMPLATE: schema */river_notification
ORDER BY id
FOR UPDATE;

-- name: NotificationInsert :exec
INSERT INTO /* TEMPLATE: schema */river_notification (
    payload,
    topic
) VALUES (
    sqlc.arg('payload'),
    sqlc.arg('topic')
);
