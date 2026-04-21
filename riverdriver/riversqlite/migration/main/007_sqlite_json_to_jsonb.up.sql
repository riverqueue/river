--
-- Convert JSON text columns to JSONB binary format for more efficient storage
-- and processing, and update column defaults from json() to jsonb().
--
-- Uses a rename-and-replace strategy to avoid full table rebuilds: rename the
-- old column, add a new one with the correct default, copy data, drop the old.
--

--
-- river_job
--

ALTER TABLE /* TEMPLATE: schema */river_job RENAME COLUMN args TO _args_old;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN args blob NOT NULL DEFAULT (jsonb('{}'));
UPDATE /* TEMPLATE: schema */river_job SET args = jsonb(_args_old);
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN _args_old;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME COLUMN attempted_by TO _attempted_by_old;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN attempted_by blob;
UPDATE /* TEMPLATE: schema */river_job SET attempted_by = jsonb(_attempted_by_old) WHERE _attempted_by_old IS NOT NULL;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN _attempted_by_old;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME COLUMN errors TO _errors_old;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN errors blob;
UPDATE /* TEMPLATE: schema */river_job SET errors = jsonb(_errors_old) WHERE _errors_old IS NOT NULL;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN _errors_old;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME COLUMN metadata TO _metadata_old;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN metadata blob NOT NULL DEFAULT (jsonb('{}'));
UPDATE /* TEMPLATE: schema */river_job SET metadata = jsonb(_metadata_old);
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN _metadata_old;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME COLUMN tags TO _tags_old;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN tags blob NOT NULL DEFAULT (jsonb('[]'));
UPDATE /* TEMPLATE: schema */river_job SET tags = jsonb(_tags_old);
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN _tags_old;

--
-- river_queue
--

ALTER TABLE /* TEMPLATE: schema */river_queue RENAME COLUMN metadata TO _metadata_old;
ALTER TABLE /* TEMPLATE: schema */river_queue ADD COLUMN metadata blob NOT NULL DEFAULT (jsonb('{}'));
UPDATE /* TEMPLATE: schema */river_queue SET metadata = jsonb(_metadata_old);
ALTER TABLE /* TEMPLATE: schema */river_queue DROP COLUMN _metadata_old;

--
-- river_client
--

ALTER TABLE /* TEMPLATE: schema */river_client RENAME COLUMN metadata TO _metadata_old;
ALTER TABLE /* TEMPLATE: schema */river_client ADD COLUMN metadata blob NOT NULL DEFAULT (jsonb('{}'));
UPDATE /* TEMPLATE: schema */river_client SET metadata = jsonb(_metadata_old);
ALTER TABLE /* TEMPLATE: schema */river_client DROP COLUMN _metadata_old;

--
-- river_client_queue
--

ALTER TABLE /* TEMPLATE: schema */river_client_queue RENAME COLUMN metadata TO _metadata_old;
ALTER TABLE /* TEMPLATE: schema */river_client_queue ADD COLUMN metadata blob NOT NULL DEFAULT (jsonb('{}'));
UPDATE /* TEMPLATE: schema */river_client_queue SET metadata = jsonb(_metadata_old);
ALTER TABLE /* TEMPLATE: schema */river_client_queue DROP COLUMN _metadata_old;
