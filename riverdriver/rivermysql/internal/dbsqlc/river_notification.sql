CREATE TABLE river_notification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    payload TEXT NOT NULL,
    topic VARCHAR(127) NOT NULL,
    CONSTRAINT topic_length CHECK (CHAR_LENGTH(topic) > 0 AND CHAR_LENGTH(topic) < 128)
);

-- name: NotificationDeleteBefore :execrows
DELETE FROM /* TEMPLATE: schema */river_notification
WHERE created_at < sqlc.arg('created_at_horizon');

-- name: NotificationGetAfterForUpdate :one
SELECT *
FROM /* TEMPLATE: schema */river_notification
WHERE id > sqlc.arg('after')
ORDER BY id ASC
LIMIT 1
FOR UPDATE;

-- name: NotificationGetLastID :one
SELECT CAST(COALESCE(MAX(id), 0) AS SIGNED)
FROM /* TEMPLATE: schema */river_notification;

-- name: NotificationInsert :exec
INSERT INTO /* TEMPLATE: schema */river_notification (
    payload,
    topic
) VALUES (
    sqlc.arg('payload'),
    sqlc.arg('topic')
);
