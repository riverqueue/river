--
-- Notification outbox.
--

CREATE TABLE /* TEMPLATE: schema */river_notification (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload text NOT NULL,
    topic text NOT NULL,
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

CREATE INDEX river_notification_created_at_idx ON /* TEMPLATE: schema */river_notification (created_at);
CREATE INDEX river_notification_topic_id_idx ON /* TEMPLATE: schema */river_notification (topic, id);

--
-- SQLite JSONB conversion.
--
-- No-op. PostgreSQL already stores River JSON columns as jsonb.

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
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;
