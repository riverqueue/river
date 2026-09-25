-- Rebuild river_job to restore SQLite's default ROWID allocation behavior.

DROP INDEX /* TEMPLATE: schema */river_job_kind;
DROP INDEX /* TEMPLATE: schema */river_job_state_and_finalized_at_index;
DROP INDEX /* TEMPLATE: schema */river_job_prioritized_fetching_index;
DROP INDEX /* TEMPLATE: schema */river_job_unique_idx;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME TO river_job_old;

CREATE TABLE /* TEMPLATE: schema */river_job (
    id integer PRIMARY KEY,
    args blob NOT NULL DEFAULT (jsonb('{}')),
    attempt integer NOT NULL DEFAULT 0,
    attempted_at timestamp,
    attempted_by blob, -- json
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    errors blob, -- json
    finalized_at timestamp,
    kind text NOT NULL,
    max_attempts integer NOT NULL DEFAULT 25,
    metadata blob NOT NULL DEFAULT (jsonb('{}')),
    priority integer NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state text NOT NULL DEFAULT 'available',
    scheduled_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tags blob NOT NULL DEFAULT (jsonb('[]')),
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

INSERT INTO /* TEMPLATE: schema */river_job (
    id,
    args,
    attempt,
    attempted_at,
    attempted_by,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    state,
    scheduled_at,
    tags,
    unique_key,
    unique_states
)
SELECT
    id,
    args,
    attempt,
    attempted_at,
    attempted_by,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    state,
    scheduled_at,
    tags,
    unique_key,
    unique_states
FROM /* TEMPLATE: schema */river_job_old;

DROP TABLE /* TEMPLATE: schema */river_job_old;

CREATE INDEX /* TEMPLATE: schema */river_job_kind ON river_job (kind);
CREATE INDEX /* TEMPLATE: schema */river_job_state_and_finalized_at_index ON river_job (state, finalized_at) WHERE finalized_at IS NOT NULL;
CREATE INDEX /* TEMPLATE: schema */river_job_prioritized_fetching_index ON river_job (state, queue, priority, scheduled_at, id);
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
