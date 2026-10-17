-- A joined UPDATE doesn't guarantee assignment order. Materialize the old state
-- and cancellation flag before changing either state or metadata. The locking
-- read must see current values even in an existing REPEATABLE READ transaction.
UPDATE /*+ NO_MERGE(batch) */ /* TEMPLATE: schema */river_job AS job
JOIN (
    SELECT input.*, original.state = 'running' AS was_running,
        input.state IN ('available', 'retryable', 'scheduled')
            AND JSON_CONTAINS_PATH(original.metadata, 'one', '$.cancel_attempted_at') AS should_cancel
    FROM JSON_TABLE(?, '$[*]' COLUMNS (
        id BIGINT PATH '$.id' ERROR ON ERROR,
        attempt BIGINT PATH '$.attempt' ERROR ON ERROR,
        error LONGTEXT CHARACTER SET utf8mb4 PATH '$.error' ERROR ON ERROR,
        finalized_at DATETIME(6) PATH '$.finalized_at' ERROR ON ERROR,
        metadata LONGTEXT CHARACTER SET utf8mb4 PATH '$.metadata' ERROR ON ERROR,
        scheduled_at DATETIME(6) PATH '$.scheduled_at' ERROR ON ERROR,
        state LONGTEXT CHARACTER SET utf8mb4 PATH '$.state' ERROR ON ERROR
    )) AS input
    JOIN /* TEMPLATE: schema */river_job AS original ON original.id = input.id
    FOR UPDATE OF original
) AS batch ON batch.id = job.id
SET
    job.attempt = CASE WHEN batch.was_running AND NOT batch.should_cancel
        THEN COALESCE(batch.attempt, job.attempt) ELSE job.attempt END,
    job.errors = CASE WHEN batch.was_running AND batch.error IS NOT NULL
        THEN JSON_ARRAY_APPEND(COALESCE(job.errors, JSON_ARRAY()), '$', CAST(batch.error AS JSON))
        ELSE job.errors END,
    job.finalized_at = CASE
        WHEN batch.was_running AND batch.should_cancel
        THEN COALESCE(?, CONVERT_TZ(NOW(6), @@session.time_zone, '+00:00'))
        WHEN batch.was_running THEN COALESCE(batch.finalized_at, job.finalized_at)
        ELSE job.finalized_at END,
    job.scheduled_at = CASE WHEN batch.was_running AND NOT batch.should_cancel
        THEN COALESCE(batch.scheduled_at, job.scheduled_at) ELSE job.scheduled_at END,
    job.state = CASE
        WHEN batch.was_running AND batch.should_cancel THEN 'cancelled'
        WHEN batch.was_running THEN batch.state ELSE job.state END,
    job.metadata = CASE WHEN batch.metadata IS NOT NULL
        THEN CAST(batch.metadata AS JSON) ELSE job.metadata END;
