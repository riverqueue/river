
--
-- Drop `river_job.unique_states` and its index.
--

DROP INDEX /* TEMPLATE: schema */river_job_unique_idx;

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN unique_states;

CREATE UNIQUE INDEX IF NOT EXISTS river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job (kind, unique_key) WHERE unique_key IS NOT NULL;

--
-- Drop `river_job_state_in_bitmask` function.
--
DROP FUNCTION /* TEMPLATE: schema */river_job_state_in_bitmask;
