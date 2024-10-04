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
    unique_states bit(8),
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
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], @cancel_attempted_at::jsonb, true)
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
        AND scheduled_at <= coalesce(sqlc.narg('now')::timestamptz, now())
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

-- name: JobInsertFastMany :many
INSERT INTO river_job(
    args,
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
) SELECT
    unnest(@args::jsonb[]),
    unnest(@kind::text[]),
    unnest(@max_attempts::smallint[]),
    unnest(@metadata::jsonb[]),
    unnest(@priority::smallint[]),
    unnest(@queue::text[]),
    unnest(@scheduled_at::timestamptz[]),
    -- To avoid requiring pgx users to register the OID of the river_job_state[]
    -- type, we cast the array to text[] and then to river_job_state.
    unnest(@state::text[])::river_job_state,
    -- Unnest on a multi-dimensional array will fully flatten the array, so we
    -- encode the tag list as a comma-separated string and split it in the
    -- query.
    string_to_array(unnest(@tags::text[]), ','),

    unnest(@unique_key::bytea[]),
    unnest(@unique_states::bit(8)[])

ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state)
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING sqlc.embed(river_job), (xmax != 0) AS unique_skipped_as_duplicate;

-- name: JobInsertFastManyNoReturning :execrows
INSERT INTO river_job(
    args,
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
    string_to_array(unnest(@tags::text[]), ','),

    unnest(@unique_key::bytea[]),
    unnest(@unique_states::bit(8)[])

ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state)
DO NOTHING;

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
    unique_key,
    unique_states
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
    @unique_key,
    @unique_states
) RETURNING *;


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
    SELECT
        id,
        unique_key,
        unique_states,
        priority,
        scheduled_at
    FROM river_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND queue IS NOT NULL
        AND priority >= 0
        AND scheduled_at <= @now::timestamptz
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT @max::bigint
    FOR UPDATE
),
jobs_with_rownum AS (
    SELECT
        *,
        CASE
            WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL THEN
                ROW_NUMBER() OVER (
                    PARTITION BY unique_key
                    ORDER BY priority, scheduled_at, id
                )
            ELSE NULL
        END AS row_num
    FROM jobs_to_schedule
),
unique_conflicts AS (
    SELECT river_job.unique_key
    FROM river_job
    JOIN jobs_with_rownum
        ON river_job.unique_key = jobs_with_rownum.unique_key
        AND river_job.id != jobs_with_rownum.id
    WHERE
        river_job.unique_key IS NOT NULL
        AND river_job.unique_states IS NOT NULL
        AND river_job_state_in_bitmask(river_job.unique_states, river_job.state)
),
job_updates AS (
    SELECT
        job.id,
        job.unique_key,
        job.unique_states,
        CASE
            WHEN job.row_num IS NULL THEN 'available'::river_job_state
            WHEN uc.unique_key IS NOT NULL THEN 'discarded'::river_job_state
            WHEN job.row_num = 1 THEN 'available'::river_job_state
            ELSE 'discarded'::river_job_state
        END AS new_state,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS finalized_at_do_update,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS metadata_do_update
    FROM jobs_with_rownum job
    LEFT JOIN unique_conflicts uc ON job.unique_key = uc.unique_key
),
updated_jobs AS (
    UPDATE river_job
    SET
        state        = job_updates.new_state,
        finalized_at = CASE WHEN job_updates.finalized_at_do_update THEN @now::timestamptz
                            ELSE river_job.finalized_at END,
        metadata     = CASE WHEN job_updates.metadata_do_update THEN river_job.metadata || '{"unique_key_conflict": "scheduler_discarded"}'::jsonb
                            ELSE river_job.metadata END
    FROM job_updates
    WHERE river_job.id = job_updates.id
    RETURNING
        river_job.id,
        job_updates.new_state = 'discarded'::river_job_state AS conflict_discarded
)
SELECT
    sqlc.embed(river_job),
    updated_jobs.conflict_discarded
FROM river_job
JOIN updated_jobs ON river_job.id = updated_jobs.id;

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
                            ELSE scheduled_at END
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

-- name: JobSetStateIfRunningMany :many
WITH job_input AS (
    SELECT
        unnest(@ids::bigint[]) AS id,
        -- To avoid requiring pgx users to register the OID of the river_job_state[]
        -- type, we cast the array to text[] and then to river_job_state.
        unnest(@state::text[])::river_job_state AS state,
        unnest(@finalized_at_do_update::boolean[]) AS finalized_at_do_update,
        unnest(@finalized_at::timestamptz[]) AS finalized_at,
        unnest(@errors_do_update::boolean[]) AS errors_do_update,
        unnest(@errors::jsonb[]) AS errors,
        unnest(@max_attempts_do_update::boolean[]) AS max_attempts_do_update,
        unnest(@max_attempts::int[]) AS max_attempts,
        unnest(@scheduled_at_do_update::boolean[]) AS scheduled_at_do_update,
        unnest(@scheduled_at::timestamptz[]) AS scheduled_at
),
job_to_update AS (
    SELECT
        river_job.id,
        job_input.state,
        job_input.finalized_at,
        job_input.errors,
        job_input.max_attempts,
        job_input.scheduled_at,
        (job_input.state IN ('retryable', 'scheduled') AND river_job.metadata ? 'cancel_attempted_at') AS should_cancel,
        job_input.finalized_at_do_update,
        job_input.errors_do_update,
        job_input.max_attempts_do_update,
        job_input.scheduled_at_do_update
    FROM river_job
    JOIN job_input ON river_job.id = job_input.id
    WHERE river_job.state = 'running'
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        state        = CASE WHEN job_to_update.should_cancel THEN 'cancelled'::river_job_state
                            ELSE job_to_update.state END,
        finalized_at = CASE WHEN job_to_update.should_cancel THEN now()
                            WHEN job_to_update.finalized_at_do_update THEN job_to_update.finalized_at
                            ELSE river_job.finalized_at END,
        errors       = CASE WHEN job_to_update.errors_do_update THEN array_append(river_job.errors, job_to_update.errors)
                            ELSE river_job.errors END,
        max_attempts = CASE WHEN NOT job_to_update.should_cancel AND job_to_update.max_attempts_do_update THEN job_to_update.max_attempts
                            ELSE river_job.max_attempts END,
        scheduled_at = CASE WHEN NOT job_to_update.should_cancel AND job_to_update.scheduled_at_do_update THEN job_to_update.scheduled_at
                            ELSE river_job.scheduled_at END
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
    RETURNING river_job.*
)
SELECT *
FROM river_job
WHERE id IN (SELECT id FROM job_input)
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
    state = CASE WHEN @state_do_update::boolean THEN @state ELSE state END
WHERE id = @id
RETURNING *;
