--
-- Drop the functional unique index and unique_states column.
--

DROP INDEX river_job_unique_idx ON /* TEMPLATE: schema */river_job;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN unique_states;

-- Recreate the old unique index from migration 005.
CREATE UNIQUE INDEX river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job (kind, unique_key);
