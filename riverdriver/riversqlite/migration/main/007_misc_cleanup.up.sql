--
-- Drop unused tables `river_client` and `river_client_queue`.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

--
-- Adds `DEFAULT 25` to `river_job.max_attempts`.
--

-- This may look odd in that we're adding a brand new column, but it's because
-- SQLite doesn't support anything beyond the most trivial DDL.

ALTER TABLE /* TEMPLATE: schema */river_job
    RENAME COLUMN max_attempts TO max_attempts_old;

ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN max_attempts integer NOT NULL DEFAULT 25;

UPDATE /* TEMPLATE: schema */river_job
SET max_attempts = max_attempts_old;

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN max_attempts_old;

-- 
-- Changes `river_queue.updated_at` to have a default of `CURRENT_TIMESTAMP`.
--

ALTER TABLE /* TEMPLATE: schema */river_queue
    RENAME COLUMN updated_at TO updated_at_old;

ALTER TABLE /* TEMPLATE: schema */river_queue
    ADD COLUMN updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

UPDATE /* TEMPLATE: schema */river_queue
SET updated_at = updated_at_old;

ALTER TABLE /* TEMPLATE: schema */river_queue
    DROP COLUMN updated_at_old;