CREATE TABLE river_job (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    args JSON NOT NULL DEFAULT (JSON_OBJECT()),
    attempt INT NOT NULL DEFAULT 0,
    attempted_at DATETIME(6) NULL,
    attempted_by JSON NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    errors JSON NULL,
    finalized_at DATETIME(6) NULL,
    kind VARCHAR(128) NOT NULL,
    max_attempts INT NOT NULL,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    priority SMALLINT NOT NULL DEFAULT 1,
    queue VARCHAR(128) NOT NULL DEFAULT 'default',
    state VARCHAR(20) NOT NULL DEFAULT 'available',
    scheduled_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    tags JSON NOT NULL DEFAULT (JSON_ARRAY()),
    unique_key VARBINARY(255) NULL,
    unique_states SMALLINT NULL
);

-- name: JobGetByID :one
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = sqlc.arg('id')
LIMIT 1;

-- name: JobGetByIDMany :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id IN (sqlc.slice('id'))
ORDER BY id;

-- name: JobGetByKindMany :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE kind IN (sqlc.slice('kind'))
ORDER BY id;

-- name: JobCancelExec :execresult
UPDATE /* TEMPLATE: schema */river_job
SET
    state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
    finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE COALESCE(sqlc.narg('now'), NOW(6)) END,
    metadata = JSON_SET(metadata, '$.cancel_attempted_at', CAST(sqlc.arg('cancel_attempted_at') AS CHAR))
WHERE id = sqlc.arg('id')
    AND state NOT IN ('cancelled', 'completed', 'discarded')
    AND finalized_at IS NULL;

-- name: JobCountByAllStates :many
SELECT state, count(*) AS count
FROM /* TEMPLATE: schema */river_job
GROUP BY state;

-- name: JobCountByQueueAndState :many
WITH all_queues AS (
    SELECT DISTINCT river_job.queue
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.queue IN (sqlc.slice('queue_names'))
),

running_job_counts AS (
    SELECT river_job.queue, COUNT(*) AS count
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.queue IN (sqlc.slice('queue_names'))
        AND river_job.state = 'running'
    GROUP BY river_job.queue
),

available_job_counts AS (
    SELECT river_job.queue, COUNT(*) AS count
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.queue IN (sqlc.slice('queue_names'))
        AND river_job.state = 'available'
    GROUP BY river_job.queue
)

SELECT
    all_queues.queue,
    COALESCE(available_job_counts.count, 0) AS count_available,
    COALESCE(running_job_counts.count, 0) AS count_running
FROM all_queues
LEFT JOIN running_job_counts ON all_queues.queue = running_job_counts.queue
LEFT JOIN available_job_counts ON all_queues.queue = available_job_counts.queue
ORDER BY all_queues.queue ASC;

-- name: JobCountByState :one
SELECT count(*) AS count
FROM /* TEMPLATE: schema */river_job
WHERE state = sqlc.arg('state');

-- name: JobDeleteExec :execresult
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id = sqlc.arg('id')
    AND river_job.state != 'running';

-- name: JobDeleteBefore :execresult
DELETE FROM /* TEMPLATE: schema */river_job
WHERE
    id IN (
        SELECT id FROM (
            SELECT rj2.id
            FROM /* TEMPLATE: schema */river_job rj2
            WHERE
                (rj2.state = 'cancelled' AND rj2.finalized_at < sqlc.arg('cancelled_finalized_at_horizon')) OR
                (rj2.state = 'completed' AND rj2.finalized_at < sqlc.arg('completed_finalized_at_horizon')) OR
                (rj2.state = 'discarded' AND rj2.finalized_at < sqlc.arg('discarded_finalized_at_horizon'))
            ORDER BY rj2.id
            LIMIT ?
        ) AS tmp
    )
    AND (
        CAST(sqlc.arg('queues_excluded_empty') AS SIGNED)
        OR river_job.queue NOT IN (sqlc.slice('queues_excluded'))
    );

-- name: JobDeleteManySelect :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id IN (
    SELECT id FROM (
        SELECT id
        FROM /* TEMPLATE: schema */river_job
        WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
            AND state != 'running'
        ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
        LIMIT ?
    ) AS tmp
)
ORDER BY id;

-- name: JobDeleteManyExec :exec
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id IN (sqlc.slice('id'));

-- name: JobGetAvailableIDs :many
SELECT id
FROM /* TEMPLATE: schema */river_job
WHERE
    priority >= 0
    AND queue = sqlc.arg('queue')
    AND scheduled_at <= COALESCE(sqlc.narg('now'), NOW(6))
    AND state = 'available'
ORDER BY priority ASC, scheduled_at ASC, id ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;

-- name: JobGetAvailableUpdate :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt = attempt + 1,
    attempted_at = COALESCE(sqlc.narg('now'), NOW(6)),
    attempted_by = JSON_ARRAY_APPEND(
        CASE
            WHEN JSON_LENGTH(COALESCE(attempted_by, JSON_ARRAY())) < CAST(sqlc.arg('max_attempted_by') AS SIGNED)
                THEN COALESCE(attempted_by, JSON_ARRAY())
            WHEN CAST(sqlc.arg('max_attempted_by') AS SIGNED) <= 1
                THEN JSON_ARRAY()
            ELSE COALESCE(
                JSON_EXTRACT(
                    attempted_by,
                    CONCAT('$[last-', CAST(sqlc.arg('max_attempted_by') AS SIGNED) - 2, ' to last]')
                ),
                JSON_ARRAY()
            )
        END,
        '$',
        CAST(sqlc.arg('attempted_by') AS CHAR)
    ),
    state = 'running'
WHERE id IN (sqlc.slice('id'));

-- name: JobGetByIDManyOrdered :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id IN (sqlc.slice('id'))
ORDER BY priority ASC, scheduled_at ASC, id ASC;

-- name: JobGetStuck :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE state = 'running'
    AND attempted_at < sqlc.arg('stuck_horizon')
ORDER BY id
LIMIT ?;

-- name: JobInsertFast :execresult
INSERT INTO /* TEMPLATE: schema */river_job(
    id,
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
    sqlc.narg('id'),
    sqlc.arg('args'),
    COALESCE(sqlc.narg('created_at'), NOW(6)),
    sqlc.arg('kind'),
    sqlc.arg('max_attempts'),
    CAST(sqlc.arg('metadata') AS JSON),
    sqlc.arg('priority'),
    sqlc.arg('queue'),
    COALESCE(sqlc.narg('scheduled_at'), NOW(6)),
    sqlc.arg('state'),
    CAST(sqlc.arg('tags') AS JSON),
    sqlc.narg('unique_key'),
    sqlc.narg('unique_states')
)
ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), kind = VALUES(kind);

-- name: JobInsertFullExec :execlastid
INSERT INTO /* TEMPLATE: schema */river_job(
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
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) VALUES (
    sqlc.arg('args'),
    sqlc.arg('attempt'),
    sqlc.narg('attempted_at'),
    CAST(sqlc.narg('attempted_by') AS JSON),
    COALESCE(sqlc.narg('created_at'), NOW(6)),
    CAST(sqlc.narg('errors') AS JSON),
    sqlc.narg('finalized_at'),
    sqlc.arg('kind'),
    sqlc.arg('max_attempts'),
    CAST(sqlc.arg('metadata') AS JSON),
    sqlc.arg('priority'),
    sqlc.arg('queue'),
    COALESCE(sqlc.narg('scheduled_at'), NOW(6)),
    sqlc.arg('state'),
    CAST(sqlc.arg('tags') AS JSON),
    sqlc.narg('unique_key'),
    sqlc.narg('unique_states')
);

-- name: JobKindList :many
SELECT DISTINCT kind
FROM /* TEMPLATE: schema */river_job
WHERE (sqlc.arg('match') = '' OR LOWER(kind) COLLATE utf8mb4_general_ci LIKE CONCAT('%', LOWER(sqlc.arg('match')), '%'))
    AND (sqlc.arg('after') = '' OR kind COLLATE utf8mb4_general_ci > sqlc.arg('after'))
    AND kind NOT IN (sqlc.slice('exclude'))
ORDER BY kind ASC
LIMIT ?;

-- name: JobList :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
LIMIT ?;

-- name: JobRescue :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    errors = JSON_ARRAY_APPEND(COALESCE(errors, JSON_ARRAY()), '$', CAST(sqlc.arg('error') AS JSON)),
    finalized_at = sqlc.narg('finalized_at'),
    scheduled_at = sqlc.arg('scheduled_at'),
    metadata = JSON_SET(
        metadata,
        '$."river:rescue_count"',
        COALESCE(
            CASE JSON_TYPE(JSON_EXTRACT(metadata, '$."river:rescue_count"'))
                WHEN 'INTEGER' THEN JSON_EXTRACT(metadata, '$."river:rescue_count"')
                WHEN 'DOUBLE' THEN JSON_EXTRACT(metadata, '$."river:rescue_count"')
                ELSE NULL
            END,
            0
        ) + 1
    ),
    state = sqlc.arg('state')
WHERE id = sqlc.arg('id');

-- name: JobRetryExec :execresult
UPDATE /* TEMPLATE: schema */river_job
SET
    state = 'available',
    max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
    finalized_at = NULL,
    scheduled_at = COALESCE(sqlc.narg('now'), NOW(6))
WHERE id = sqlc.arg('id')
    AND state != 'running'
    AND (
        state <> 'available'
        OR scheduled_at > COALESCE(sqlc.narg('now'), NOW(6))
    );

-- name: JobSchedule :many
WITH eligible AS (
    SELECT river_job.id, river_job.unique_key, river_job.unique_states, river_job.priority, river_job.scheduled_at,
        CASE
            WHEN river_job.unique_key IS NOT NULL AND river_job.unique_states IS NOT NULL THEN
                ROW_NUMBER() OVER (PARTITION BY river_job.unique_key ORDER BY river_job.priority, river_job.scheduled_at, river_job.id)
            ELSE NULL
        END AS row_num
    FROM /* TEMPLATE: schema */river_job
    WHERE
        river_job.state IN ('retryable', 'scheduled')
        AND river_job.scheduled_at <= COALESCE(sqlc.narg('now'), NOW(6))
    ORDER BY
        river_job.priority,
        river_job.scheduled_at,
        river_job.id
    LIMIT ?
),
unique_conflicts AS (
    SELECT DISTINCT eligible.unique_key
    FROM /* TEMPLATE: schema */river_job
    JOIN eligible
        ON river_job.unique_key = eligible.unique_key
        AND river_job.id != eligible.id
    WHERE
        river_job.unique_key IS NOT NULL
        AND river_job.unique_states IS NOT NULL
        AND CASE river_job.state
                WHEN 'available' THEN river_job.unique_states & (1 << 0)
                WHEN 'cancelled' THEN river_job.unique_states & (1 << 1)
                WHEN 'completed' THEN river_job.unique_states & (1 << 2)
                WHEN 'discarded' THEN river_job.unique_states & (1 << 3)
                WHEN 'pending'   THEN river_job.unique_states & (1 << 4)
                WHEN 'retryable' THEN river_job.unique_states & (1 << 5)
                WHEN 'running'   THEN river_job.unique_states & (1 << 6)
                WHEN 'scheduled' THEN river_job.unique_states & (1 << 7)
                ELSE 0
            END >= 1
)
SELECT eligible.id,
    CASE
        WHEN eligible.unique_key IS NULL OR eligible.unique_states IS NULL THEN FALSE
        WHEN uc.unique_key IS NOT NULL THEN TRUE
        WHEN eligible.row_num > 1 THEN TRUE
        ELSE FALSE
    END AS conflict_discarded
FROM eligible
LEFT JOIN unique_conflicts uc ON eligible.unique_key = uc.unique_key
ORDER BY eligible.priority, eligible.scheduled_at, eligible.id;

-- name: JobScheduleSetAvailableExec :exec
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available'
WHERE id IN (sqlc.slice('id'));

-- name: JobScheduleSetDiscardedExec :exec
UPDATE /* TEMPLATE: schema */river_job
SET metadata = JSON_MERGE_PATCH(metadata, '{"unique_key_conflict": "scheduler_discarded"}'),
    finalized_at = COALESCE(sqlc.narg('now'), NOW(6)),
    state = 'discarded'
WHERE id IN (sqlc.slice('id'));

-- name: JobSetMetadataIfNotRunningExec :execresult
UPDATE /* TEMPLATE: schema */river_job
SET metadata = JSON_MERGE_PATCH(metadata, CAST(sqlc.arg('metadata_updates') AS JSON))
WHERE id = sqlc.arg('id')
    AND state != 'running';

-- name: JobSetStateIfRunningExec :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt      = CASE WHEN (CAST(sqlc.arg('state') AS CHAR) <> 'retryable' AND sqlc.arg('state') <> 'scheduled' OR JSON_EXTRACT(metadata, '$.cancel_attempted_at') IS NULL) AND CAST(sqlc.arg('attempt_do_update') AS SIGNED)
                    THEN sqlc.arg('attempt')
                    ELSE attempt END,
    errors       = CASE WHEN CAST(sqlc.arg('errors_do_update') AS SIGNED)
                    THEN JSON_ARRAY_APPEND(COALESCE(errors, JSON_ARRAY()), '$', CAST(sqlc.arg('error') AS JSON))
                    ELSE errors END,
    finalized_at = CASE WHEN ((sqlc.arg('state') = 'retryable' OR sqlc.arg('state') = 'scheduled') AND JSON_EXTRACT(metadata, '$.cancel_attempted_at') IS NOT NULL)
                    THEN COALESCE(sqlc.narg('now'), NOW(6))
                    WHEN CAST(sqlc.arg('finalized_at_do_update') AS SIGNED)
                    THEN sqlc.narg('finalized_at')
                    ELSE finalized_at END,
    metadata     = CASE WHEN CAST(sqlc.arg('metadata_do_merge') AS SIGNED)
                    THEN JSON_MERGE_PATCH(metadata, CAST(sqlc.arg('metadata_updates') AS JSON))
                    ELSE metadata END,
    scheduled_at = CASE WHEN (CAST(sqlc.arg('state') AS CHAR) <> 'retryable' AND sqlc.arg('state') <> 'scheduled' OR JSON_EXTRACT(metadata, '$.cancel_attempted_at') IS NULL) AND CAST(sqlc.arg('scheduled_at_do_update') AS SIGNED)
                    THEN sqlc.arg('scheduled_at')
                    ELSE scheduled_at END,
    state        = CASE WHEN ((sqlc.arg('state') = 'retryable' OR sqlc.arg('state') = 'scheduled') AND JSON_EXTRACT(metadata, '$.cancel_attempted_at') IS NOT NULL)
                    THEN 'cancelled'
                    ELSE sqlc.arg('state') END
WHERE id = sqlc.arg('id')
    AND state = 'running';

-- name: JobUpdateExec :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    metadata = CASE WHEN CAST(sqlc.arg('metadata_do_merge') AS SIGNED) THEN JSON_MERGE_PATCH(metadata, CAST(sqlc.arg('metadata') AS JSON)) ELSE metadata END
WHERE id = sqlc.arg('id');

-- name: JobUpdateFullExec :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt = CASE WHEN CAST(sqlc.arg('attempt_do_update') AS SIGNED) THEN sqlc.arg('attempt') ELSE attempt END,
    attempted_at = CASE WHEN CAST(sqlc.arg('attempted_at_do_update') AS SIGNED) THEN sqlc.narg('attempted_at') ELSE attempted_at END,
    attempted_by = CASE WHEN CAST(sqlc.arg('attempted_by_do_update') AS SIGNED) THEN CAST(sqlc.arg('attempted_by') AS JSON) ELSE attempted_by END,
    errors = CASE WHEN CAST(sqlc.arg('errors_do_update') AS SIGNED) THEN CAST(sqlc.arg('errors') AS JSON) ELSE errors END,
    finalized_at = CASE WHEN CAST(sqlc.arg('finalized_at_do_update') AS SIGNED) THEN sqlc.narg('finalized_at') ELSE finalized_at END,
    max_attempts = CASE WHEN CAST(sqlc.arg('max_attempts_do_update') AS SIGNED) THEN sqlc.arg('max_attempts') ELSE max_attempts END,
    metadata = CASE WHEN CAST(sqlc.arg('metadata_do_update') AS SIGNED) THEN CAST(sqlc.arg('metadata') AS JSON) ELSE metadata END,
    state = CASE WHEN CAST(sqlc.arg('state_do_update') AS SIGNED) THEN sqlc.arg('state') ELSE state END
WHERE id = sqlc.arg('id');
