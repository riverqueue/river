--
-- Add `river_job.unique_states` and bring up an index on it.
--
-- MySQL 8.0+ supports functional indexes (index on an expression). We use one
-- to index `unique_key` only when the job's state matches the bitmask, which
-- is equivalent to Postgres's partial index with `river_job_state_in_bitmask`.
-- The expression evaluates to NULL when the constraint shouldn't be active,
-- and MySQL's UNIQUE allows multiple NULLs.
--

ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN unique_states SMALLINT NULL;

-- A "functional index" (feature specific to MySQL 8.0+) over `unique_key`,
-- `unique_states`, and `state`. The expression evaluates to `unique_key` when
-- the job's current state has its corresponding bit set in `unique_states`, and
-- to NULL otherwise. MySQL's UNIQUE indexes permit multiple NULL values, so
-- rows where the constraint is inactive (NULL result) never conflict with each
-- other, while rows where it's active are checked for uniqueness on
-- `unique_key`. This is the MySQL equivalent of Postgres's partial index with
-- `WHERE ... AND river_job_state_in_bitmask(unique_states, state)`.
--
-- Unlike Postgres's partial indexes which exclude non-matching rows from the
-- index entirely, MySQL's functional index still stores an entry for every row
-- (NULLs included), so the storage savings aren't equivalent. The uniqueness
-- semantics are the same though.
CREATE UNIQUE INDEX river_job_unique_idx ON /* TEMPLATE: schema */river_job ((
    CASE WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL AND
        CASE state
            WHEN 'available' THEN unique_states & (1 << 0)
            WHEN 'cancelled' THEN unique_states & (1 << 1)
            WHEN 'completed' THEN unique_states & (1 << 2)
            WHEN 'discarded' THEN unique_states & (1 << 3)
            WHEN 'pending'   THEN unique_states & (1 << 4)
            WHEN 'retryable' THEN unique_states & (1 << 5)
            WHEN 'running'   THEN unique_states & (1 << 6)
            WHEN 'scheduled' THEN unique_states & (1 << 7)
            ELSE 0
        END >= 1
    THEN unique_key
    ELSE NULL
    END
));

-- Remove the old unique index from migration 005.
DROP INDEX river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job;
