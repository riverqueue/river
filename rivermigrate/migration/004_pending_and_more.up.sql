ALTER TABLE river_job ALTER COLUMN metadata SET DEFAULT '{}';
UPDATE river_job SET metadata = '{}' WHERE metadata IS NULL;
ALTER TABLE river_job ALTER COLUMN metadata SET NOT NULL;

ALTER TYPE river_job_state ADD VALUE 'pending' AFTER 'discarded';

ALTER TABLE river_job DROP CONSTRAINT finalized_or_finalized_at_null;
ALTER TABLE river_job ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
    (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
    (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
);
