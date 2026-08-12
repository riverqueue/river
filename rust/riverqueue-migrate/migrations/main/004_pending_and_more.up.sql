-- The args column never had a NOT NULL constraint or default value at the
-- database level, though we tried to ensure one at the application level.
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN args SET DEFAULT '{}';
UPDATE /* TEMPLATE: schema */river_job SET args = '{}' WHERE args IS NULL;
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN args SET NOT NULL;
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN args DROP DEFAULT;

-- The metadata column never had a NOT NULL constraint or default value at the
-- database level, though we tried to ensure one at the application level.
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN metadata SET DEFAULT '{}';
UPDATE /* TEMPLATE: schema */river_job SET metadata = '{}' WHERE metadata IS NULL;
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN metadata SET NOT NULL;

-- The 'pending' job state will be used for upcoming functionality:
ALTER TYPE /* TEMPLATE: schema */river_job_state ADD VALUE IF NOT EXISTS 'pending' AFTER 'discarded';

ALTER TABLE /* TEMPLATE: schema */river_job DROP CONSTRAINT finalized_or_finalized_at_null;
ALTER TABLE /* TEMPLATE: schema */river_job ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
    (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
    (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
);

DROP TRIGGER river_notify ON /* TEMPLATE: schema */river_job;
DROP FUNCTION /* TEMPLATE: schema */river_job_notify;

--
-- Create table `river_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
    paused_at timestamptz,
    updated_at timestamptz NOT NULL
);

--
-- Alter `river_leader` to add a default value of 'default` to `name`.
--

ALTER TABLE /* TEMPLATE: schema */river_leader
    ALTER COLUMN name SET DEFAULT 'default',
    DROP CONSTRAINT name_length,
    ADD CONSTRAINT name_length CHECK (name = 'default');