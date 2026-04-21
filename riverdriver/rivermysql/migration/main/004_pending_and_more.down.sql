ALTER TABLE /* TEMPLATE: schema */river_job MODIFY COLUMN args JSON NULL;

ALTER TABLE /* TEMPLATE: schema */river_job MODIFY COLUMN metadata JSON NOT NULL DEFAULT (JSON_OBJECT());

-- Cannot safely remove 'pending' from the CHECK constraint if rows reference
-- it, but we restore the original constraint form.
ALTER TABLE /* TEMPLATE: schema */river_job DROP CONSTRAINT finalized_or_finalized_at_null;
ALTER TABLE /* TEMPLATE: schema */river_job ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
    (state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR
    finalized_at IS NULL
);

-- MySQL does not support triggers for LISTEN/NOTIFY, so no trigger changes.

DROP TABLE /* TEMPLATE: schema */river_queue;

ALTER TABLE /* TEMPLATE: schema */river_leader
    ALTER COLUMN name DROP DEFAULT;

ALTER TABLE /* TEMPLATE: schema */river_leader DROP CONSTRAINT name_length;
ALTER TABLE /* TEMPLATE: schema */river_leader ADD CONSTRAINT name_length CHECK (CHAR_LENGTH(name) > 0 AND CHAR_LENGTH(name) < 128);
