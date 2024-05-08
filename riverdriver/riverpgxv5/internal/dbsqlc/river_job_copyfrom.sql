-- name: JobInsertMany :copyfrom
INSERT INTO sqlc_schema_placeholder.river_job(
    args,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    @args,
    @finalized_at,
    @kind,
    @max_attempts,
    @metadata,
    @priority,
    @queue,
    @scheduled_at,
    @state,
    @tags
);
