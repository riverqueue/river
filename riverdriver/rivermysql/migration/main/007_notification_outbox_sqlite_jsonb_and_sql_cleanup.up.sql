--
-- Notification outbox.
--

CREATE TABLE /* TEMPLATE: schema */river_notification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    payload TEXT NOT NULL,
    topic VARCHAR(127) NOT NULL,
    CONSTRAINT topic_length CHECK (CHAR_LENGTH(topic) > 0 AND CHAR_LENGTH(topic) < 128)
) ENGINE=InnoDB;

CREATE INDEX river_notification_created_at_idx ON /* TEMPLATE: schema */river_notification (created_at);
CREATE INDEX river_notification_topic_id_idx ON /* TEMPLATE: schema */river_notification (topic, id);

--
-- SQLite JSONB conversion.
--
-- No-op. MySQL stores River JSON columns in JSON columns.

--
-- SQL cleanup.
--

--
-- Drop unused tables `river_client` and `river_client_queue`.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

--
-- Adds `DEFAULT 25` to `river_job.max_attempts`.
--

ALTER TABLE /* TEMPLATE: schema */river_job
    ALTER COLUMN max_attempts SET DEFAULT 25;

--
-- Changes `river_queue.updated_at` to have a default of `CURRENT_TIMESTAMP`.
--

ALTER TABLE /* TEMPLATE: schema */river_queue
    MODIFY COLUMN updated_at DATETIME(6) NOT NULL DEFAULT (NOW(6));
