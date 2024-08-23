CREATE TYPE river_job_state AS ENUM(
    'available',
    'cancelled',
    'completed',
    'discarded',
    'pending',
    'retryable',
    'running',
    'scheduled'
);

CREATE TABLE river_job(
    id bigserial PRIMARY KEY,
    args jsonb NOT NULL DEFAULT '{}',
    attempt smallint NOT NULL DEFAULT 0,
    attempted_at timestamptz,
    attempted_by text[],
    created_at timestamptz NOT NULL DEFAULT NOW(),
    errors jsonb[],
    finalized_at timestamptz,
    kind text NOT NULL,
    max_attempts smallint NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    priority smallint NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state river_job_state NOT NULL DEFAULT 'available',
    scheduled_at timestamptz NOT NULL DEFAULT NOW(),
    tags varchar(255)[] NOT NULL DEFAULT '{}',
    unique_key bytea,
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- name: JobCancel :one
WITH locked_job AS (
    SELECT
        id, queue, state, finalized_at
    FROM river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
notification AS (
    SELECT
        id,
        pg_notify(
            concat(current_schema(), '.', @control_topic::text),
            json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text
        )
    FROM
        locked_job
    WHERE
        state NOT IN ('cancelled', 'completed', 'discarded')
        AND finalized_at IS NULL
),
updated_job AS (
    UPDATE river_job
    SET
        -- If the job is actively running, we want to let its current client and
        -- producer handle the cancellation. Otherwise, immediately cancel it.
        state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
        finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE now() END,
        -- Mark the job as cancelled by query so that the rescuer knows not to
        -- rescue it, even if it gets stuck in the running state:
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], @cancel_attempted_at::jsonb, true),
        -- Similarly, zero a `unique_key` if the job is transitioning directly
        -- to cancelled. Otherwise, it'll be clear the job executor.
        unique_key = CASE WHEN state = 'running' THEN unique_key ELSE NULL END
    FROM notification
    WHERE river_job.id = notification.id
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- name: JobCountByState :one
SELECT count(*)
FROM river_job
WHERE state = @state;

-- name: JobDelete :one
WITH job_to_delete AS (
    SELECT id
    FROM river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
deleted_job AS (
    DELETE
    FROM river_job
    USING job_to_delete
    WHERE river_job.id = job_to_delete.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM deleted_job)
UNION
SELECT *
FROM deleted_job;

-- name: JobDeleteBefore :one
WITH deleted_jobs AS (
    DELETE FROM river_job
    WHERE id IN (
        SELECT id
        FROM river_job
        WHERE
            (state = 'cancelled' AND finalized_at < @cancelled_finalized_at_horizon::timestamptz) OR
            (state = 'completed' AND finalized_at < @completed_finalized_at_horizon::timestamptz) OR
            (state = 'discarded' AND finalized_at < @discarded_finalized_at_horizon::timestamptz)
        ORDER BY id
        LIMIT @max::bigint
    )
    RETURNING *
)
SELECT count(*)
FROM deleted_jobs;

-- name: JobGetAvailable :many
WITH locked_jobs AS (
    SELECT
        *
    FROM
        river_job
    WHERE
        state = 'available'
        AND queue = @queue::text
        AND scheduled_at <= now()
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT @max::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    river_job
SET
    state = 'running',
    attempt = river_job.attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(river_job.attempted_by, @attempted_by::text)
FROM
    locked_jobs
WHERE
    river_job.id = locked_jobs.id
RETURNING
    river_job.*;

-- name: JobGetByKindAndUniqueProperties :one
SELECT *
FROM river_job
WHERE kind = @kind
    AND CASE WHEN @by_args::boolean THEN args = @args ELSE true END
    AND CASE WHEN @by_created_at::boolean THEN tstzrange(@created_at_begin::timestamptz, @created_at_end::timestamptz, '[)') @> created_at ELSE true END
    AND CASE WHEN @by_queue::boolean THEN queue = @queue ELSE true END
    AND CASE WHEN @by_state::boolean THEN state::text = any(@state::text[]) ELSE true END;

-- name: JobGetByKindMany :many
SELECT *
FROM river_job
WHERE kind = any(@kind::text[])
ORDER BY id;

-- name: JobGetByID :one
SELECT *
FROM river_job
WHERE id = @id
LIMIT 1;

-- name: JobGetByIDMany :many
SELECT *
FROM river_job
WHERE id = any(@id::bigint[])
ORDER BY id;

-- name: JobGetStuck :many
SELECT *
FROM river_job
WHERE state = 'running'
    AND attempted_at < @stuck_horizon::timestamptz
ORDER BY id
LIMIT @max;

-- name: JobInsertFast :one
INSERT INTO river_job(
    args,
    created_at,
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
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @finalized_at,
    @kind,
    @max_attempts,
    coalesce(@metadata::jsonb, '{}'),
    @priority,
    @queue,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state,
    coalesce(@tags::varchar(255)[], '{}')
) RETURNING *;

-- name: JobInsertFastMany :execrows
INSERT INTO river_job(
    args,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) SELECT
    unnest(@args::jsonb[]),
    unnest(@kind::text[]),
    unnest(@max_attempts::smallint[]),
    unnest(@metadata::jsonb[]),
    unnest(@priority::smallint[]),
    unnest(@queue::text[]),
    unnest(@scheduled_at::timestamptz[]),
    unnest(@state::river_job_state[]),

    -- lib/pq really, REALLY does not play nicely with multi-dimensional arrays,
    -- so instead we pack each set of tags into a string, send them through,
    -- then unpack them here into an array to put in each row. This isn't
    -- necessary in the Pgx driver where copyfrom is used instead.
    string_to_array(unnest(@tags::text[]), ',');

-- name: JobInsertFull :one
INSERT INTO river_job(
    args,
    attempt,
    attempted_at,
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
    unique_key
) VALUES (
    @args::jsonb,
    coalesce(@attempt::smallint, 0),
    @attempted_at,
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @errors,
    @finalized_at,
    @kind,
    @max_attempts::smallint,
    coalesce(@metadata::jsonb, '{}'),
    @priority,
    @queue,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state,
    coalesce(@tags::varchar(255)[], '{}'),
    @unique_key
) RETURNING *;

-- name: JobInsertUnique :one
INSERT INTO river_job(
    args,
    created_at,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key
) VALUES (
    @args,
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @finalized_at,
    @kind,
    @max_attempts,
    coalesce(@metadata::jsonb, '{}'),
    @priority,
    @queue,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state,
    coalesce(@tags::varchar(255)[], '{}'),
    @unique_key
)
ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING sqlc.embed(river_job), (xmax != 0) AS unique_skipped_as_duplicate;

-- Run by the rescuer to queue for retry or discard depending on job state.
-- name: JobRescueMany :exec
UPDATE river_job
SET
    errors = array_append(errors, updated_job.error),
    finalized_at = updated_job.finalized_at,
    scheduled_at = updated_job.scheduled_at,
    state = updated_job.state
FROM (
    SELECT
        unnest(@id::bigint[]) AS id,
        unnest(@error::jsonb[]) AS error,
        nullif(unnest(@finalized_at::timestamptz[]), '0001-01-01 00:00:00 +0000') AS finalized_at,
        unnest(@scheduled_at::timestamptz[]) AS scheduled_at,
        unnest(@state::text[])::river_job_state AS state
) AS updated_job
WHERE river_job.id = updated_job.id;

-- name: JobRetry :one
WITH job_to_update AS (
    SELECT id
    FROM river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        state = 'available',
        scheduled_at = now(),
        max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
        finalized_at = NULL
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'
        -- If the job is already available with a prior scheduled_at, leave it alone.
        AND NOT (river_job.state = 'available' AND river_job.scheduled_at < now())
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- name: JobSchedule :many
WITH jobs_to_schedule AS (
    SELECT id
    FROM river_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND queue IS NOT NULL
        AND priority >= 0
        AND river_job.scheduled_at <= @now::timestamptz
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT @max::bigint
    FOR UPDATE
),
river_job_scheduled AS (
    UPDATE river_job
    SET state = 'available'
    FROM jobs_to_schedule
    WHERE river_job.id = jobs_to_schedule.id
    RETURNING river_job.id
)
SELECT *
FROM river_job
WHERE id IN (SELECT id FROM river_job_scheduled);

-- name: JobSetCompleteIfRunningMany :many
WITH job_to_finalized_at AS (
    SELECT
        unnest(@id::bigint[]) AS id,
        unnest(@finalized_at::timestamptz[]) AS finalized_at
),
job_to_update AS (
    SELECT river_job.id, job_to_finalized_at.finalized_at
    FROM river_job, job_to_finalized_at
    WHERE river_job.id = job_to_finalized_at.id
        AND river_job.state = 'running'
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        finalized_at = job_to_update.finalized_at,
        state = 'completed'
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id IN (SELECT id FROM job_to_finalized_at EXCEPT SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- name: JobSetStateIfRunning :one
WITH job_to_update AS (
    SELECT
        id,
        @state::river_job_state IN ('retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' AS should_cancel
    FROM river_job
    WHERE id = @id::bigint
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        state        = CASE WHEN should_cancel                                           THEN 'cancelled'::river_job_state
                            ELSE @state::river_job_state END,
        finalized_at = CASE WHEN should_cancel                                           THEN now()
                            WHEN @finalized_at_do_update::boolean                        THEN @finalized_at
                            ELSE finalized_at END,
        errors       = CASE WHEN @error_do_update::boolean                               THEN array_append(errors, @error::jsonb)
                            ELSE errors       END,
        max_attempts = CASE WHEN NOT should_cancel AND @max_attempts_update::boolean     THEN @max_attempts
                            ELSE max_attempts END,
        scheduled_at = CASE WHEN NOT should_cancel AND @scheduled_at_do_update::boolean  THEN sqlc.narg('scheduled_at')::timestamptz
                            ELSE scheduled_at END,
        unique_key   = CASE WHEN (@state IN ('cancelled', 'discarded') OR should_cancel) THEN NULL
                            ELSE unique_key END
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        AND river_job.state = 'running'
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- A generalized update for any property on a job. This brings in a large number
-- of parameters and therefore may be more suitable for testing than production.
-- name: JobUpdate :one
UPDATE river_job
SET
    attempt = CASE WHEN @attempt_do_update::boolean THEN @attempt ELSE attempt END,
    attempted_at = CASE WHEN @attempted_at_do_update::boolean THEN @attempted_at ELSE attempted_at END,
    errors = CASE WHEN @errors_do_update::boolean THEN @errors::jsonb[] ELSE errors END,
    finalized_at = CASE WHEN @finalized_at_do_update::boolean THEN @finalized_at ELSE finalized_at END,
    state = CASE WHEN @state_do_update::boolean THEN @state ELSE state END,
    unique_key = CASE WHEN @unique_key_do_update::boolean THEN @unique_key ELSE unique_key END
WHERE id = @id
RETURNING *;
