-- This table isn't used under Postgres currently, but we have it in place
-- because its useful for simulating under Postgres as if we were running
-- SQLite, and it may be useful as a good listen/notify alternative for Postgres
-- down the line instead of poll-only mode in cases like where a bouncer makes
-- listen/notify difficult to use.
CREATE TABLE river_notification (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload text NOT NULL,
    topic text NOT NULL,
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

-- name: NotificationDeleteBefore :execrows
DELETE FROM /* TEMPLATE: schema */river_notification
WHERE created_at < @created_at_horizon::timestamptz;
