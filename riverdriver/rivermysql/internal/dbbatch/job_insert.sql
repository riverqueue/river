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
    id BIGINT PATH '$.i' ERROR ON ERROR,
    args LONGTEXT CHARACTER SET utf8mb4 PATH '$.a' ERROR ON ERROR,
    created_at DATETIME(6) PATH '$.c' ERROR ON ERROR,
    kind LONGTEXT CHARACTER SET utf8mb4 PATH '$.k' ERROR ON ERROR,
    max_attempts BIGINT PATH '$.ma' ERROR ON ERROR,
    metadata LONGTEXT CHARACTER SET utf8mb4 PATH '$.m' ERROR ON ERROR,
    priority BIGINT PATH '$.p' ERROR ON ERROR,
    queue LONGTEXT CHARACTER SET utf8mb4 PATH '$.q' ERROR ON ERROR,
    scheduled_at DATETIME(6) PATH '$.s' ERROR ON ERROR,
    state LONGTEXT CHARACTER SET utf8mb4 PATH '$.st' ERROR ON ERROR,
    tags LONGTEXT CHARACTER SET utf8mb4 PATH '$.t' ERROR ON ERROR,
    unique_active BOOLEAN PATH '$.u' ERROR ON ERROR,
    unique_key LONGTEXT PATH '$.uk' ERROR ON ERROR,
    unique_states INT PATH '$.us' ERROR ON ERROR
)) AS batch
ORDER BY batch.ordinal
/* TEMPLATE: on_duplicate_key */;
