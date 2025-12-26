--
-- Add back unused tables `river_client` and `river_client_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT name_length CHECK (length(id) > 0 AND length(id) < 128)
);

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id text NOT NULL REFERENCES river_client (id) ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    max_workers integer NOT NULL DEFAULT 0,
    metadata blob NOT NULL DEFAULT (json('{}')),
    num_jobs_completed integer NOT NULL DEFAULT 0,
    num_jobs_running integer NOT NULL DEFAULT 0,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);

--
-- Revert addition of `DEFAULT 25` to `river_job.max_attempts`.
--

ALTER TABLE /* TEMPLATE: schema */river_job
    RENAME COLUMN max_attempts TO max_attempts_old;

ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN max_attempts integer NOT NULL;

UPDATE /* TEMPLATE: schema */river_job
SET max_attempts = max_attempts_old;

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN max_attempts_old;

-- 
-- Changes `river_queue.updated_at` to revert the default of `CURRENT_TIMESTAMP`.
--

ALTER TABLE /* TEMPLATE: schema */river_queue
    RENAME COLUMN updated_at TO updated_at_old;

ALTER TABLE /* TEMPLATE: schema */river_queue
    ADD COLUMN updated_at timestamp NOT NULL;

UPDATE /* TEMPLATE: schema */river_queue
SET updated_at = updated_at_old;

ALTER TABLE /* TEMPLATE: schema */river_queue
    DROP COLUMN updated_at_old;