INSERT INTO /* TEMPLATE: schema */river_job (
    id, args, created_at, kind, max_attempts, metadata, priority, queue,
    scheduled_at, state, tags, unique_key, unique_states
)
SELECT
    batch.id, CAST(batch.args AS JSON),
    COALESCE(batch.created_at, CONVERT_TZ(NOW(6), @@session.time_zone, '+00:00')),
    batch.kind, batch.max_attempts, CAST(batch.metadata AS JSON), batch.priority, batch.queue,
    COALESCE(batch.scheduled_at, CONVERT_TZ(NOW(6), @@session.time_zone, '+00:00')),
    batch.state, CAST(batch.tags AS JSON), UNHEX(NULLIF(batch.unique_key, '')), NULLIF(batch.unique_states, 0)
FROM JSON_TABLE(?, '$[*]' COLUMNS (
    ordinal FOR ORDINALITY,
    id BIGINT PATH '$.id' ERROR ON ERROR,
    args LONGTEXT CHARACTER SET utf8mb4 PATH '$.args' ERROR ON ERROR,
    created_at DATETIME(6) PATH '$.created_at' ERROR ON ERROR,
    kind LONGTEXT CHARACTER SET utf8mb4 PATH '$.kind' ERROR ON ERROR,
    max_attempts BIGINT PATH '$.max_attempts' ERROR ON ERROR,
    metadata LONGTEXT CHARACTER SET utf8mb4 PATH '$.metadata' ERROR ON ERROR,
    priority BIGINT PATH '$.priority' ERROR ON ERROR,
    queue LONGTEXT CHARACTER SET utf8mb4 PATH '$.queue' ERROR ON ERROR,
    scheduled_at DATETIME(6) PATH '$.scheduled_at' ERROR ON ERROR,
    state LONGTEXT CHARACTER SET utf8mb4 PATH '$.state' ERROR ON ERROR,
    tags LONGTEXT CHARACTER SET utf8mb4 PATH '$.tags' ERROR ON ERROR,
    unique_active BOOLEAN PATH '$.unique_active' ERROR ON ERROR,
    unique_key LONGTEXT PATH '$.unique_key' ERROR ON ERROR,
    unique_states INT PATH '$.unique_states' ERROR ON ERROR
)) AS batch
ORDER BY batch.ordinal
/* TEMPLATE: on_duplicate_key */;
