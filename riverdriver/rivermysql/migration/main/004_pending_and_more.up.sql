-- Make args NOT NULL with a default.
UPDATE /* TEMPLATE: schema */river_job SET args = JSON_OBJECT() WHERE args IS NULL;
ALTER TABLE /* TEMPLATE: schema */river_job MODIFY COLUMN args JSON NOT NULL DEFAULT (JSON_OBJECT());

-- Make metadata NOT NULL (it already had a default).
UPDATE /* TEMPLATE: schema */river_job SET metadata = JSON_OBJECT() WHERE metadata IS NULL;
ALTER TABLE /* TEMPLATE: schema */river_job MODIFY COLUMN metadata JSON NOT NULL DEFAULT (JSON_OBJECT());

-- Add 'pending' to the set of valid states. MySQL doesn't have enum types to
-- alter, so we update the CHECK constraint instead.
ALTER TABLE /* TEMPLATE: schema */river_job DROP CONSTRAINT state_valid;
ALTER TABLE /* TEMPLATE: schema */river_job ADD CONSTRAINT state_valid CHECK (
    state IN ('available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled')
);

-- Update the finalized_at constraint to use the inverted form (matching
-- Postgres migration 004).
ALTER TABLE /* TEMPLATE: schema */river_job DROP CONSTRAINT finalized_or_finalized_at_null;
ALTER TABLE /* TEMPLATE: schema */river_job ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
    (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
    (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
);

-- MySQL does not support triggers for LISTEN/NOTIFY, so river_job_notify
-- changes from Postgres are omitted.

--
-- Create table `river_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_queue (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

--
-- Alter `river_leader` to add a default value of 'default' to `name`.
-- MySQL supports ALTER TABLE for changing defaults and constraints.
--

ALTER TABLE /* TEMPLATE: schema */river_leader
    ALTER COLUMN name SET DEFAULT 'default';

ALTER TABLE /* TEMPLATE: schema */river_leader DROP CONSTRAINT name_length;
ALTER TABLE /* TEMPLATE: schema */river_leader ADD CONSTRAINT name_length CHECK (name = 'default');
