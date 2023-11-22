CREATE TYPE river_job_state AS ENUM(
  'available',
  'cancelled',
  'completed',
  'discarded',
  'retryable',
  'running',
  'scheduled'
);

CREATE TABLE river_job(
  id bigserial PRIMARY KEY,
  args jsonb,
  attempt smallint NOT NULL DEFAULT 0,
  attempted_at timestamptz,
  attempted_by text[],
  created_at timestamptz NOT NULL DEFAULT NOW(),
  errors jsonb[],
  finalized_at timestamptz,
  kind text NOT NULL,
  max_attempts smallint NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
  priority smallint NOT NULL DEFAULT 1,
  queue text NOT NULL DEFAULT 'default' ::text,
  state river_job_state NOT NULL DEFAULT 'available' ::river_job_state,
  scheduled_at timestamptz NOT NULL DEFAULT NOW(),
  tags varchar(255)[] NOT NULL DEFAULT '{}' ::varchar(255)[],
  CONSTRAINT finalized_or_finalized_at_null CHECK ((state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR finalized_at IS NULL),
  CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
  CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
  CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- name: JobCompleteMany :exec
UPDATE river_job
SET
  finalized_at = updated.finalized_at,
  state = updated.state
FROM (
  SELECT
    unnest(@id::bigint[]) AS id,
    unnest(@finalized_at::timestamptz[]) AS finalized_at,
    'completed'::river_job_state AS state
) AS updated
WHERE river_job.id = updated.id;

-- name: JobCountRunning :one
SELECT
  count(*)
FROM
  river_job
WHERE
  state = 'running';

-- name: JobDeleteBefore :one
WITH deleted_jobs AS (
  DELETE FROM
    river_job
  WHERE
    id IN (
      SELECT
        id
      FROM
        river_job
      WHERE
        (state = 'cancelled' AND finalized_at < @cancelled_finalized_at_horizon::timestamptz) OR
        (state = 'completed' AND finalized_at < @completed_finalized_at_horizon::timestamptz) OR
        (state = 'discarded' AND finalized_at < @discarded_finalized_at_horizon::timestamptz)
      ORDER BY id
      LIMIT @max::bigint
    )
  RETURNING *
)
SELECT
  count(*)
FROM
  deleted_jobs;

-- name: JobGetAvailable :many
WITH locked_jobs AS (
  SELECT
    *
  FROM
    river_job
  WHERE
    state = 'available'::river_job_state
    AND queue = @queue::text
  ORDER BY
    priority ASC,
    scheduled_at ASC,
    id ASC
  LIMIT @limit_count::integer
  FOR UPDATE
    SKIP LOCKED)
UPDATE
  river_job
SET
  state = 'running'::river_job_state,
  attempt = river_job.attempt + 1,
  attempted_at = NOW(),
  attempted_by = array_append(river_job.attempted_by, @worker::text)
FROM
  locked_jobs
WHERE
  river_job.id = locked_jobs.id
RETURNING
  river_job.*;

-- name: JobGetByKind :many
SELECT *
FROM river_job
WHERE kind = @kind
ORDER BY id;

-- name: JobGetByKindMany :many
SELECT *
FROM river_job
WHERE kind = any(@kind::text[])
ORDER BY id;

-- name: JobGetByID :one
SELECT
  *
FROM
  river_job
WHERE
  id = @id
LIMIT 1;

-- name: JobGetByIDMany :many
SELECT
  *
FROM
  river_job
WHERE
  id = any(@id::bigint[]);

-- name: JobGetByKindAndUniqueProperties :one
SELECT *
FROM river_job
WHERE kind = @kind
  AND CASE WHEN @by_args::boolean THEN args = @args ELSE true END
  AND CASE WHEN @by_created_at::boolean THEN tstzrange(@created_at_start::timestamptz, @created_at_end::timestamptz, '[)') @> created_at ELSE true END
  AND CASE WHEN @by_queue::boolean THEN queue = @queue ELSE true END
  AND CASE WHEN @by_state::boolean THEN state::text = any(@state::text[]) ELSE true END;

-- name: JobGetStuck :many
SELECT
  *
FROM
  river_job
WHERE
  state = 'running'::river_job_state
  AND attempted_at < @stuck_horizon::timestamptz
LIMIT @limit_count::integer;

-- name: JobInsert :one
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
  tags
) VALUES (
  @args::jsonb,
  coalesce(@attempt::smallint, 0),
  @attempted_at,
  coalesce(sqlc.narg('created_at')::timestamptz, now()),
  @errors::jsonb[],
  @finalized_at,
  @kind::text,
  @max_attempts::smallint,
  coalesce(@metadata::jsonb, '{}'),
  @priority::smallint,
  @queue::text,
  coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
  @state::river_job_state,
  coalesce(@tags::varchar(255)[], '{}')
) RETURNING *;

-- name: JobInsertMany :copyfrom
INSERT INTO river_job(
  args,
  errors,
  kind,
  max_attempts,
  metadata,
  priority,
  queue,
  state,
  tags
) VALUES (
  @args,
  @errors,
  @kind,
  @max_attempts,
  @metadata,
  @priority,
  @queue,
  @state,
  @tags
);

-- name: JobSchedule :one
WITH jobs_to_schedule AS (
  SELECT id
  FROM river_job
  WHERE
    state IN ('scheduled', 'retryable')
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
river_job_scheduled AS (
  UPDATE river_job
  SET state = 'available'::river_job_state
  FROM jobs_to_schedule
  WHERE river_job.id = jobs_to_schedule.id
  RETURNING *
)
SELECT count(*)
FROM (
SELECT pg_notify('river_insert', json_build_object('queue', queue)::text)
FROM river_job_scheduled) AS notifications_sent;

-- name: JobSetCancelledIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = @id::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = @finalized_at::timestamptz,
    errors = array_append(errors, @error::jsonb),
    state = 'cancelled'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.*
)
(
  SELECT
    river_job.*
  FROM
    river_job
  WHERE
    river_job.id = @id::bigint
  UNION
  SELECT
    updated_job.*
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1;

-- name: JobSetCompleted :one
UPDATE
  river_job
SET
  finalized_at = @finalized_at::timestamptz,
  state = 'completed'::river_job_state
WHERE
  id = @id::bigint
RETURNING *;

-- name: JobSetCompletedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = @id::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = @finalized_at::timestamptz,
    state = 'completed'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.*
)
(
  SELECT
    river_job.*
  FROM
    river_job
  WHERE
    river_job.id = @id::bigint
  UNION
  SELECT
    updated_job.*
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1;

-- name: JobSetDiscarded :one
UPDATE
  river_job
SET
  finalized_at = @finalized_at::timestamptz,
  errors = array_append(errors, @error::jsonb),
  state = 'discarded'::river_job_state
WHERE
  id = @id::bigint
RETURNING *;

-- name: JobSetDiscardedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = @id::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = @finalized_at::timestamptz,
    errors = array_append(errors, @error::jsonb),
    state = 'discarded'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.*
)
(
  SELECT
    river_job.*
  FROM
    river_job
  WHERE
    river_job.id = @id::bigint
  UNION
  SELECT
    updated_job.*
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1;

-- name: JobSetErroredIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = @id::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    scheduled_at = @scheduled_at::timestamptz,
    errors = array_append(errors, @error::jsonb),
    state = 'retryable'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.*
)
(
  SELECT
    river_job.*
  FROM
    river_job
  WHERE
    river_job.id = @id::bigint
  UNION
  SELECT
    updated_job.*
  FROM
    updated_job
) ORDER BY scheduled_at DESC NULLS LAST LIMIT 1;

-- name: JobSetSnoozedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = @id::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    scheduled_at = @scheduled_at::timestamptz,
    state = 'scheduled'::river_job_state,
    max_attempts = max_attempts + 1
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.*
)
(
  SELECT
    river_job.*
  FROM
    river_job
  WHERE
    river_job.id = @id::bigint
  UNION
  SELECT
    updated_job.*
  FROM
    updated_job
) ORDER BY scheduled_at DESC NULLS LAST LIMIT 1;


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

-- A generalized update for any property on a job. This brings in a large number
-- of parameters and therefore may be more suitable for testing than production.
-- name: JobUpdate :one
UPDATE river_job
SET
  attempt = CASE WHEN @attempt_do_update::boolean THEN @attempt ELSE attempt END,
  attempted_at = CASE WHEN @attempted_at_do_update::boolean THEN @attempted_at ELSE attempted_at END,
  state = CASE WHEN @state_do_update::boolean THEN @state ELSE state END
WHERE id = @id
RETURNING *;

-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);
