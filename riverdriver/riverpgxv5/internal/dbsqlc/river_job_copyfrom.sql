-- name: JobInsertFastManyCopyFrom :copyfrom
INSERT INTO river_job(
    args,
    created_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) VALUES (
    @args,
    @created_at,
    @kind,
    @max_attempts,
    @metadata,
    @priority,
    @queue,
    @scheduled_at,
    @state,
    @tags,
    @unique_key,
    @unique_states
);
