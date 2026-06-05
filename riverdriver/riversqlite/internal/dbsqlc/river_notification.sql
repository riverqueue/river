CREATE TABLE river_notification (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    payload text NOT NULL,
    topic text NOT NULL,
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

-- name: NotificationDeleteBefore :execrows
DELETE FROM /* TEMPLATE: schema */river_notification
WHERE created_at < cast(@created_at_horizon AS text);

-- name: NotificationGetAfter :one
SELECT *
FROM /* TEMPLATE: schema */river_notification
WHERE id > @after
ORDER BY id ASC
LIMIT 1;

-- name: NotificationGetLastID :one
SELECT cast(coalesce(max(id), 0) AS integer)
FROM /* TEMPLATE: schema */river_notification;

-- name: NotificationInsertMany :exec
INSERT INTO /* TEMPLATE: schema */river_notification (
    payload,
    topic
)
SELECT
    json_extract(value, '$.payload'),
    json_extract(value, '$.topic')
FROM json_each(cast(@notifications AS blob));
