CREATE TABLE /* TEMPLATE: schema */river_notification (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    payload text NOT NULL,
    topic text NOT NULL,
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

CREATE INDEX /* TEMPLATE: schema */river_notification_created_at_idx ON river_notification (created_at);
CREATE INDEX /* TEMPLATE: schema */river_notification_topic_id_idx ON river_notification (topic, id);
