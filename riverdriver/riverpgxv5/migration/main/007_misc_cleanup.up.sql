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