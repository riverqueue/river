CREATE TABLE river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT name_length CHECK (length(id) > 0 AND length(id) < 128)
);