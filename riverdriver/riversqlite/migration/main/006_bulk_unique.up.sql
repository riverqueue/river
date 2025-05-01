-- Only drops the trivial `river_job` we created in 002 which puts a placeholder
-- in place so that the right tables exist in the right versions. We don't
-- bother migrating any job data because it's not possible to have had any real
-- jobs by that point because this version (006) preexists the addition of SQLite.
DROP TABLE /* TEMPLATE: schema */river_job;

CREATE TABLE /* TEMPLATE: schema */river_job (
    id integer PRIMARY KEY, -- SQLite makes this autoincrementing automatically
    args blob NOT NULL DEFAULT '{}',
    attempt integer NOT NULL DEFAULT 0,
    attempted_at timestamp,
    attempted_by blob, -- json
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    errors blob, -- json
    finalized_at timestamp,
    kind text NOT NULL,
    max_attempts integer NOT NULL,
    metadata blob NOT NULL DEFAULT (json('{}')),
    priority integer NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state text NOT NULL DEFAULT 'available',
    scheduled_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tags blob NOT NULL DEFAULT (json('[]')),
    unique_key blob,
    unique_states integer,
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (length(queue) > 0 AND length(queue) < 128),
    CONSTRAINT kind_length CHECK (length(kind) > 0 AND length(kind) < 128),
    CONSTRAINT state_valid CHECK (state IN ('available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled'))
);

-- All these indexes are normally brought up in version 002.
CREATE INDEX /* TEMPLATE: schema */river_job_kind ON river_job (kind);
CREATE INDEX /* TEMPLATE: schema */river_job_state_and_finalized_at_index ON river_job (state, finalized_at) WHERE finalized_at IS NOT NULL;
CREATE INDEX /* TEMPLATE: schema */river_job_prioritized_fetching_index ON river_job (state, queue, priority, scheduled_at, id);

-- Not raised because SQLite doesn't support Gin indexes. These aren't used in
-- River anyway.
-- CREATE INDEX river_job_args_index ON /* TEMPLATE: schema */river_job USING GIN(args);
-- CREATE INDEX river_job_metadata_index ON /* TEMPLATE: schema */river_job USING GIN(metadata);

-- SQLite doesn't support SQL functions, so where the bit extraction logic below
-- goes in the `river_job_state_in_bitmask` function in Postgres, here it's
-- baked right into the index. Use of helpers that don't exist in SQLite like
-- `get_bit` are also dropped by necessity.
CREATE UNIQUE INDEX /* TEMPLATE: schema */river_job_unique_idx ON river_job (unique_key)
    WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND CASE state
            WHEN 'available' THEN unique_states & (1 << 0)
            WHEN 'cancelled' THEN unique_states & (1 << 1)
            WHEN 'completed' THEN unique_states & (1 << 2)
            WHEN 'discarded' THEN unique_states & (1 << 3)
            WHEN 'pending'   THEN unique_states & (1 << 4)
            WHEN 'retryable' THEN unique_states & (1 << 5)
            WHEN 'running'   THEN unique_states & (1 << 6)
            WHEN 'scheduled' THEN unique_states & (1 << 7)
            ELSE 0
        END >= 1;