--
-- SQL cleanup rollback.
--

--
-- Add back unused tables `river_client` and `river_client_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (jsonb('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT name_length CHECK (length(id) > 0 AND length(id) < 128)
);

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id text NOT NULL REFERENCES river_client (id) ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    max_workers integer NOT NULL DEFAULT 0,
    metadata blob NOT NULL DEFAULT (jsonb('{}')),
    num_jobs_completed integer NOT NULL DEFAULT 0,
    num_jobs_running integer NOT NULL DEFAULT 0,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);

--
-- SQLite JSONB conversion rollback.
--
-- Convert JSONB binary columns back to JSON text format and restore json()
-- defaults. The `river_job` rebuild also reverts the addition of `DEFAULT 25`
-- to `river_job.max_attempts`.
--
-- SQLite doesn't allow `ALTER TABLE ADD COLUMN` with non-constant defaults like
-- `json('{}')`, so rebuild each affected table instead.
--

--
-- river_job
--

DROP INDEX /* TEMPLATE: schema */river_job_kind;
DROP INDEX /* TEMPLATE: schema */river_job_state_and_finalized_at_index;
DROP INDEX /* TEMPLATE: schema */river_job_prioritized_fetching_index;
DROP INDEX /* TEMPLATE: schema */river_job_unique_idx;

ALTER TABLE /* TEMPLATE: schema */river_job RENAME TO river_job_old;

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
    json(args),
    attempt,
    attempted_at,
    CASE WHEN attempted_by IS NULL THEN NULL ELSE json(attempted_by) END,
    created_at,
    CASE WHEN errors IS NULL THEN NULL ELSE json(errors) END,
    finalized_at,
    kind,
    max_attempts,
    json(metadata),
    priority,
    queue,
    state,
    scheduled_at,
    json(tags),
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

--
-- river_queue
--

ALTER TABLE /* TEMPLATE: schema */river_queue RENAME TO river_queue_old;

CREATE TABLE /* TEMPLATE: schema */river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL
);

INSERT INTO /* TEMPLATE: schema */river_queue (
    name,
    created_at,
    metadata,
    paused_at,
    updated_at
)
SELECT
    name,
    created_at,
    json(metadata),
    paused_at,
    updated_at
FROM /* TEMPLATE: schema */river_queue_old;

DROP TABLE /* TEMPLATE: schema */river_queue_old;

--
-- river_client
--

ALTER TABLE /* TEMPLATE: schema */river_client RENAME TO river_client_old;

CREATE TABLE /* TEMPLATE: schema */river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT name_length CHECK (length(id) > 0 AND length(id) < 128)
);

INSERT INTO /* TEMPLATE: schema */river_client (
    id,
    created_at,
    metadata,
    paused_at,
    updated_at
)
SELECT
    id,
    created_at,
    json(metadata),
    paused_at,
    updated_at
FROM /* TEMPLATE: schema */river_client_old;

--
-- river_client_queue
--

ALTER TABLE /* TEMPLATE: schema */river_client_queue RENAME TO river_client_queue_old;

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id text NOT NULL REFERENCES river_client (id) ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    max_workers integer NOT NULL DEFAULT 0,
    metadata blob NOT NULL DEFAULT (json('{}')),
    num_jobs_completed integer NOT NULL DEFAULT 0,
    num_jobs_running integer NOT NULL DEFAULT 0,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);

INSERT INTO /* TEMPLATE: schema */river_client_queue (
    river_client_id,
    name,
    created_at,
    max_workers,
    metadata,
    num_jobs_completed,
    num_jobs_running,
    updated_at
)
SELECT
    river_client_id,
    name,
    created_at,
    max_workers,
    json(metadata),
    num_jobs_completed,
    num_jobs_running,
    updated_at
FROM /* TEMPLATE: schema */river_client_queue_old;

DROP TABLE /* TEMPLATE: schema */river_client_queue_old;
DROP TABLE /* TEMPLATE: schema */river_client_old;

--
-- Notification outbox rollback.
--

DROP TABLE /* TEMPLATE: schema */river_notification;
