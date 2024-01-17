ALTER TABLE river_job ALTER COLUMN metadata SET NULL;
ALTER TABLE river_job ALTER COLUMN metadata DROP DEFAULT;

ALTER TYPE river_job_state DROP VALUE 'pending';

ALTER TABLE river_job DROP CONSTRAINT finalized_or_finalized_at_null;
ALTER TABLE river_job ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
  (state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR finalized_at IS NULL
);
