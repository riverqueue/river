CREATE TABLE river_job (
    id integer PRIMARY KEY, -- SQLite makes this autoincrementing automatically
    args blob NOT NULL DEFAULT '{}',
    attempt integer NOT NULL DEFAULT 0,
    attempted_at timestamp,
    attempted_by blob, -- JSON array of strings
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    errors blob, -- JSON array of error objects
    finalized_at timestamp,
    kind text NOT NULL,
    max_attempts integer NOT NULL,
    metadata blob NOT NULL DEFAULT (json('{}')),
    priority integer NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state text NOT NULL DEFAULT 'available',
    scheduled_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tags blob NOT NULL DEFAULT (json('[]')), -- JSON array of strings
    unique_key blob,
    unique_states integer,
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (length(queue) > 0 AND length(queue) < 128),
    CONSTRAINT kind_length CHECK (length(kind) > 0 AND length(kind) < 128),
    CONSTRAINT state_valid CHECK (state IN ('available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled'))
);

-- Differs by necessity from other drivers because SQLite doesn't support
-- `UPDATE` inside CTEs so we can't retry if running but select otherwise.
-- Instead, the driver uses a transaction to optimisticaly try an update, but
-- perform a subsequent fetch on a not found to return the right status.
--
-- I had to invert the last 'AND' expression below (was an 'ANT NOT) due to an
-- sqlc bug. Something about sqlc's SQLite parser cannot detect a parameter
-- inside an `AND NOT`.
-- name: JobCancel :one
UPDATE /* TEMPLATE: schema */river_job
SET
    -- If the job is actively running, we want to let its current client and
    -- producer handle the cancellation. Otherwise, immediately cancel it.
    state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
    finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')) END,
    -- Mark the job as cancelled by query so that the rescuer knows not to
    -- rescue it, even if it gets stuck in the running state:
    metadata = json_set(metadata, '$.cancel_attempted_at', cast(@cancel_attempted_at AS text))
WHERE id = @id
    AND state NOT IN ('cancelled', 'completed', 'discarded')
    AND finalized_at IS NULL
RETURNING *;

-- name: JobCountByAllStates :many
SELECT state, count(*)
FROM /* TEMPLATE: schema */river_job
GROUP BY state;

-- name: JobCountByQueueAndState :many
WITH queue_stats AS (
    SELECT
        river_job.queue,
        COUNT(CASE WHEN river_job.state = 'available' THEN 1 END) AS count_available,
        COUNT(CASE WHEN river_job.state = 'running' THEN 1 END) AS count_running
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.queue IN (sqlc.slice('queue_names'))
    GROUP BY river_job.queue
)

SELECT
    cast(queue AS text) AS queue,
    count_available,
    count_running
FROM queue_stats
ORDER BY queue ASC;

-- name: JobCountByState :one
SELECT count(*)
FROM /* TEMPLATE: schema */river_job
WHERE state = @state;

-- Differs by necessity from other drivers because SQLite doesn't support
-- `DELETE` inside CTEs so we can't delete if running but select otherwise.
-- Instead, the driver uses a transaction to optimisticaly try a delete, but
-- perform a subsequent fetch on a not found to return the right status.
-- name: JobDelete :one
DELETE
FROM /* TEMPLATE: schema */river_job
WHERE id = @id
    -- Do not touch running jobs:
    AND river_job.state != 'running'
RETURNING *;

-- name: JobDeleteBefore :execresult
DELETE FROM /* TEMPLATE: schema */river_job
WHERE
    id IN (
        SELECT id
        FROM /* TEMPLATE: schema */river_job
        WHERE
            (state = 'cancelled' AND finalized_at < cast(@cancelled_finalized_at_horizon AS text)) OR
            (state = 'completed' AND finalized_at < cast(@completed_finalized_at_horizon AS text)) OR
            (state = 'discarded' AND finalized_at < cast(@discarded_finalized_at_horizon AS text))
        ORDER BY id
        LIMIT @max
    )
    -- This is really awful, but unless the `sqlc.slice` appears as the very
    -- last parameter in the query things will fail if it includes more than one
    -- element. The sqlc SQLite driver uses position-based placeholders (?1) for
    -- most parameters, but unnamed ones with `sqlc.slice` (?), and when
    -- positional parameters follow unnamed parameters great confusion is the
    -- result. Making sure `sqlc.slice` is last is the only workaround I could
    -- find, but it stops working if there are multiple clauses that need a
    -- positional placeholder plus `sqlc.slice` like this one (the Postgres
    -- driver supports a `queues_included` parameter that I couldn't support
    -- here). The non-workaround version is (unfortunately) to never, ever use
    -- the sqlc driver for SQLite -- it's not a little buggy, it's off the
    -- charts buggy, and there's little interest from the maintainers in fixing
    -- any of it. We already started using it though, so plough on.
    AND (
        cast(@queues_excluded_empty AS boolean)
        OR river_job.queue NOT IN (sqlc.slice('queues_excluded'))
    );

-- name: JobDeleteMany :many
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id IN (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
        AND state != 'running'
    ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
    LIMIT @max
)
RETURNING *;

-- Differs from the Postgres version in that we don't have `FOR UPDATE SKIP
-- LOCKED`. It doesn't exist in SQLite, but more aptly, there's only one writer
-- on SQLite at a time, so nothing else has the rows locked.
-- name: JobGetAvailable :many
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt = river_job.attempt + 1,
    attempted_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),

    -- This is replaced in the driver to work around sqlc bugs for SQLite. See
    -- comments there for more details.
    attempted_by = /* TEMPLATE_BEGIN: attempted_by_clause */ attempted_by /* TEMPLATE_END */,

    state = 'running'
WHERE id IN (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE
        priority >= 0
        AND river_job.queue = @queue
        AND scheduled_at <= coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
        AND state = 'available'
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT @max_to_lock
)
RETURNING *;

-- name: JobGetByID :one
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = @id
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

-- name: JobGetStuck :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE state = 'running'
    AND attempted_at < cast(@stuck_horizon AS text)
ORDER BY id
LIMIT @max;

-- Insert a job.
--
-- This is supposed to be a batch insert, but various limitations of the
-- combined SQLite + sqlc has left me unable to find a way of injecting many
-- arguments en masse (like how we slightly abuse arrays to pull it off for the
-- Postgres drivers), so we loop over many insert operations instead, with the
-- expectation that this may be fixable in the future. Because SQLite targets
-- will often be local and therefore with a very minimal round trip compared to
-- a network, looping over operations is probably okay performance-wise.
-- name: JobInsertFast :one
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
    cast(sqlc.narg('id') AS integer),
    @args,
    coalesce(cast(sqlc.narg('created_at') AS text), datetime('now', 'subsec')),
    @kind,
    @max_attempts,
    json(cast(@metadata AS blob)),
    @priority,
    @queue,
    coalesce(cast(sqlc.narg('scheduled_at') AS text), datetime('now', 'subsec')),
    @state,
    json(cast(@tags AS blob)),
    CASE WHEN length(cast(@unique_key AS blob)) = 0 THEN NULL ELSE @unique_key END,
    @unique_states
)
ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND CASE state
                WHEN 'available' THEN unique_states & (1 << 0)
                WHEN 'cancelled' THEN unique_states & (1 << 1)
                WHEN 'completed' THEN unique_states & (1 << 2)
                WHEN 'discarded' THEN unique_states & (1 << 3)
                WHEN 'pending'   THEN unique_states & (1 << 4)
                WHEN 'retryable' THEN unique_states & (1 << 5)
                WHEN 'running'   THEN unique_states & (1 << 6)
                WHEN 'scheduled' THEN unique_states & (1 << 7)
                ELSE 0
            END >= 1
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING *;

-- name: JobInsertFastNoReturning :execrows
INSERT INTO /* TEMPLATE: schema */river_job(
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
    coalesce(cast(sqlc.narg('created_at') AS text), datetime('now', 'subsec')),
    @kind,
    @max_attempts,
    json(cast(@metadata AS blob)),
    @priority,
    @queue,
    coalesce(cast(sqlc.narg('scheduled_at') AS text), datetime('now', 'subsec')),
    @state,
    json(cast(@tags AS blob)),
    CASE WHEN length(cast(@unique_key AS blob)) = 0 THEN NULL ELSE @unique_key END,
    @unique_states
)
ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND CASE state
                WHEN 'available' THEN unique_states & (1 << 0)
                WHEN 'cancelled' THEN unique_states & (1 << 1)
                WHEN 'completed' THEN unique_states & (1 << 2)
                WHEN 'discarded' THEN unique_states & (1 << 3)
                WHEN 'pending'   THEN unique_states & (1 << 4)
                WHEN 'retryable' THEN unique_states & (1 << 5)
                WHEN 'running'   THEN unique_states & (1 << 6)
                WHEN 'scheduled' THEN unique_states & (1 << 7)
                ELSE 0
            END >= 1
DO NOTHING;

-- name: JobInsertFull :one
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
    @args,
    @attempt,
    cast(sqlc.narg('attempted_at') as text),
    CASE WHEN length(cast(@attempted_by AS blob)) = 0 THEN NULL ELSE json(@attempted_by) END,
    coalesce(cast(sqlc.narg('created_at') AS text), datetime('now', 'subsec')),
    CASE WHEN length(cast(@errors AS blob)) = 0 THEN NULL ELSE @errors END,
    cast(sqlc.narg('finalized_at') as text),
    @kind,
    @max_attempts,
    json(cast(@metadata AS blob)),
    @priority,
    @queue,
    coalesce(cast(sqlc.narg('scheduled_at') AS text), datetime('now', 'subsec')),
    @state,
    json(cast(@tags AS blob)),
    CASE WHEN length(cast(@unique_key AS blob)) = 0 THEN NULL ELSE @unique_key END,
    @unique_states
) RETURNING *;

-- name: JobKindList :many
SELECT DISTINCT kind
FROM /* TEMPLATE: schema */river_job
WHERE (cast(@match AS text) = '' OR LOWER(kind) LIKE '%' || LOWER(cast(@match AS text)) || '%')
    AND (cast(@after AS text) = '' OR kind > cast(@after AS text))
    AND kind NOT IN (sqlc.slice('exclude'))
ORDER BY kind ASC
LIMIT @max;

-- name: JobList :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
LIMIT @max;

-- Rescue a job.
--
-- This is supposed to rescue jobs in batches, but various limitations of the
-- combined SQLite + sqlc has left me unable to find a way of injecting many
-- arguments en masse (like how we slightly abuse arrays to pull it off for the
-- Postgres drivers), and SQLite doesn't support `UPDATE` in CTEs, so we loop
-- over many insert operations instead, with the expectation that this may be
-- fixable in the future. Because SQLite targets will often be local and with a
-- very minimal round trip compared to a network, looping over operations is
-- probably okay performance-wise.
-- name: JobRescue :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    errors = json_insert(coalesce(errors, json('[]')), '$[#]', json(cast(@error AS blob))),
    finalized_at = cast(sqlc.narg('finalized_at') as text),
    scheduled_at = @scheduled_at,
    state = @state
WHERE id = @id;

-- Differs by necessity from other drivers because SQLite doesn't support
-- `UPDATE` inside CTEs so we can't retry if running but select otherwise.
-- Instead, the driver uses a transaction to optimisticaly try an update, but
-- perform a subsequent fetch on a not found to return the right status.
--
-- I had to invert the last 'AND' expression below (was an 'AND NOT') due to an
-- sqlc bug. Something about sqlc's SQLite parser cannot detect a parameter
-- inside an `AND NOT`. I'll try to get this fixed upstream at some point so we
-- can clean this up and keep it more like the Postgres version.
-- name: JobRetry :one
UPDATE /* TEMPLATE: schema */river_job
SET
    state = 'available',
    max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
    finalized_at = NULL,
    scheduled_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE id = @id
    -- Do not touch running jobs:
    AND state != 'running'
    -- If the job is already available with a prior scheduled_at, leave it alone.
    --
    -- I had to invert the original 'AND NOT' to 'AND'. Something about
    -- sqlc's SQLite parser cannot detect a parameter inside an `AND NOT`. An
    -- unfortunate bug that will hopefully be fixed in the future ...
    AND (
        state <> 'available'
        OR scheduled_at > coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
    )
RETURNING *;

-- name: JobScheduleGetEligible :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE
    state IN ('retryable', 'scheduled')
    AND scheduled_at <= coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
ORDER BY
    priority,
    scheduled_at,
    id
LIMIT @max;

-- name: JobScheduleGetCollision :one
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id <> @id
    AND unique_key = @unique_key
    AND unique_states IS NOT NULL
    AND CASE state
            WHEN 'available' THEN unique_states & (1 << 0)
            WHEN 'cancelled' THEN unique_states & (1 << 1)
            WHEN 'completed' THEN unique_states & (1 << 2)
            WHEN 'discarded' THEN unique_states & (1 << 3)
            WHEN 'pending'   THEN unique_states & (1 << 4)
            WHEN 'retryable' THEN unique_states & (1 << 5)
            WHEN 'running'   THEN unique_states & (1 << 6)
            WHEN 'scheduled' THEN unique_states & (1 << 7)
            ELSE 0
        END >= 1;

-- name: JobScheduleSetAvailable :many
UPDATE /* TEMPLATE: schema */river_job
SET
    state = 'available'
WHERE id IN (sqlc.slice('id'))
RETURNING *;

-- name: JobScheduleSetDiscarded :many
UPDATE /* TEMPLATE: schema */river_job
SET metadata = json_patch(metadata, json('{"unique_key_conflict": "scheduler_discarded"}')),
    finalized_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    state = 'discarded'
WHERE id IN (sqlc.slice('id'))
RETURNING *;

-- This doesn't exist under the Postgres driver, but needed as an extra query
-- for JobSetStateIfRunning to use when falling back to non-running jobs.
-- name: JobSetMetadataIfNotRunning :one
UPDATE /* TEMPLATE: schema */river_job
SET metadata = json_patch(metadata, json(cast(@metadata_updates AS blob)))
WHERE id = @id
    AND state != 'running'
RETURNING *;

-- Differs significantly from the Postgres version in that it can't do a bulk
-- update, and since sqlc doesn't support `UPDATE` in CTEs, we need separate
-- queries like JobSetMetadataIfNotRunning to do the fallback work.
-- name: JobSetStateIfRunning :one
UPDATE /* TEMPLATE: schema */river_job
SET
    -- should_cancel: (job_input.state IN ('retryable', 'scheduled') AND river_job.metadata ? 'cancel_attempted_at')
    --
    -- or inverted:   (cast(@state AS text) <> 'retryable' AND @state <> 'scheduled' OR NOT (metadata -> 'cancel_attempted_at'))
    attempt      = CASE WHEN /* NOT should_cancel */(cast(@state AS text) <> 'retryable' AND @state <> 'scheduled' OR (metadata -> 'cancel_attempted_at') IS NULL) AND cast(@attempt_do_update AS boolean)
                        THEN @attempt
                        ELSE attempt END,
    errors       = CASE WHEN cast(@errors_do_update AS boolean)
                        THEN json_insert(coalesce(errors, json('[]')), '$[#]', json(cast(@error AS blob)))
                        ELSE errors END,
    finalized_at = CASE WHEN /* should_cancel */((@state = 'retryable' OR @state = 'scheduled') AND (metadata -> 'cancel_attempted_at') iS NOT NULL)
                        THEN coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
                        WHEN cast(@finalized_at_do_update AS boolean)
                        THEN @finalized_at
                        ELSE finalized_at END,
    metadata     = CASE WHEN cast(@metadata_do_merge AS boolean)
                        THEN json_patch(metadata, json(cast(@metadata_updates AS blob)))
                        ELSE metadata END,
    scheduled_at = CASE WHEN /* NOT should_cancel */(cast(@state AS text) <> 'retryable' AND @state <> 'scheduled' OR (metadata -> 'cancel_attempted_at') IS NULL) AND cast(@scheduled_at_do_update AS boolean)
                        THEN @scheduled_at
                        ELSE scheduled_at END,
    state        = CASE WHEN /* should_cancel */((@state = 'retryable' OR @state = 'scheduled') AND (metadata -> 'cancel_attempted_at') IS NOT NULL)
                        THEN 'cancelled'
                        ELSE @state END
WHERE id = @id
    AND state = 'running'
RETURNING *;

-- A generalized update for any property on a job. This brings in a large number
-- of parameters and therefore may be more suitable for testing than production.
-- name: JobUpdate :one
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt = CASE WHEN cast(@attempt_do_update AS boolean) THEN @attempt ELSE attempt END,
    attempted_at = CASE WHEN cast(@attempted_at_do_update AS boolean) THEN @attempted_at ELSE attempted_at END,
    attempted_by = CASE WHEN cast(@attempted_by_do_update AS boolean) THEN @attempted_by ELSE attempted_by END,
    errors = CASE WHEN cast(@errors_do_update AS boolean) THEN @errors ELSE errors END,
    finalized_at = CASE WHEN cast(@finalized_at_do_update AS boolean) THEN @finalized_at ELSE finalized_at END,
    max_attempts = CASE WHEN cast(@max_attempts_do_update AS boolean) THEN @max_attempts ELSE max_attempts END,
    metadata = CASE WHEN cast(@metadata_do_update AS boolean) THEN json(cast(@metadata AS blob)) ELSE metadata END,
    state = CASE WHEN cast(@state_do_update AS boolean) THEN @state ELSE state END
WHERE id = @id
RETURNING *;
