--
-- SQL cleanup rollback.
--

--
-- Add back unused tables `river_client` and `river_client_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL,
    CONSTRAINT client_name_length CHECK (CHAR_LENGTH(id) > 0 AND CHAR_LENGTH(id) < 128)
) ENGINE=InnoDB;

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    max_workers INT NOT NULL DEFAULT 0,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    num_jobs_completed BIGINT NOT NULL DEFAULT 0,
    num_jobs_running BIGINT NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT fk_river_client FOREIGN KEY (river_client_id) REFERENCES river_client (id) ON DELETE CASCADE,
    CONSTRAINT cq_name_length CHECK (CHAR_LENGTH(name) > 0 AND CHAR_LENGTH(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
) ENGINE=InnoDB;

--
-- Revert addition of `DEFAULT 25` to `river_job.max_attempts`.
--

ALTER TABLE /* TEMPLATE: schema */river_job
    MODIFY COLUMN max_attempts INT NOT NULL;

--
-- Changes `river_queue.updated_at` to revert the default of `CURRENT_TIMESTAMP`.
--

ALTER TABLE /* TEMPLATE: schema */river_queue
    MODIFY COLUMN updated_at DATETIME(6) NOT NULL;

--
-- SQLite JSONB conversion rollback.
--
-- No-op. MySQL stores River JSON columns in JSON columns.

--
-- Notification outbox rollback.
--

DROP TABLE /* TEMPLATE: schema */river_notification;
