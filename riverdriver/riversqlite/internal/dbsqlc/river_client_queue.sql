CREATE TABLE river_client_queue (
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