UPDATE /* TEMPLATE: schema */river_job AS job
JOIN JSON_TABLE(?, '$[*]' COLUMNS (
    id BIGINT PATH '$.id' ERROR ON ERROR,
    error LONGTEXT CHARACTER SET utf8mb4 PATH '$.error' ERROR ON ERROR,
    finalized_at DATETIME(6) PATH '$.finalized_at' ERROR ON ERROR,
    scheduled_at DATETIME(6) PATH '$.scheduled_at' ERROR ON ERROR,
    state LONGTEXT CHARACTER SET utf8mb4 PATH '$.state' ERROR ON ERROR
)) AS batch ON batch.id = job.id
SET
    job.errors = JSON_ARRAY_APPEND(COALESCE(job.errors, JSON_ARRAY()), '$', CAST(batch.error AS JSON)),
    job.finalized_at = batch.finalized_at,
    job.scheduled_at = batch.scheduled_at,
    job.metadata = JSON_SET(
        job.metadata,
        '$."river:rescue_count"',
        COALESCE(
            CASE JSON_TYPE(JSON_EXTRACT(job.metadata, '$."river:rescue_count"'))
                WHEN 'INTEGER' THEN JSON_EXTRACT(job.metadata, '$."river:rescue_count"')
                WHEN 'DOUBLE' THEN JSON_EXTRACT(job.metadata, '$."river:rescue_count"')
                ELSE NULL
            END,
            0
        ) + 1
    ),
    job.state = batch.state
WHERE job.state = 'running' AND job.attempted_at < ?;
