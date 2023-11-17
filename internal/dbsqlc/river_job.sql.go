// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.22.0
// source: river_job.sql

package dbsqlc

import (
	"context"
	"time"
)

const jobCompleteMany = `-- name: JobCompleteMany :exec
UPDATE river_job
SET
  finalized_at = updated.finalized_at,
  state = updated.state
FROM (
  SELECT
    unnest($1::bigint[]) AS id,
    unnest($2::timestamptz[]) AS finalized_at,
    'completed'::river_job_state AS state
) AS updated
WHERE river_job.id = updated.id
`

type JobCompleteManyParams struct {
	ID          []int64
	FinalizedAt []time.Time
}

func (q *Queries) JobCompleteMany(ctx context.Context, db DBTX, arg JobCompleteManyParams) error {
	_, err := db.Exec(ctx, jobCompleteMany, arg.ID, arg.FinalizedAt)
	return err
}

const jobCountRunning = `-- name: JobCountRunning :one
SELECT
  count(*)
FROM
  river_job
WHERE
  state = 'running'
`

func (q *Queries) JobCountRunning(ctx context.Context, db DBTX) (int64, error) {
	row := db.QueryRow(ctx, jobCountRunning)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const jobDeleteBefore = `-- name: JobDeleteBefore :one
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
        (state = 'cancelled' AND finalized_at < $1::timestamptz) OR
        (state = 'completed' AND finalized_at < $2::timestamptz) OR
        (state = 'discarded' AND finalized_at < $3::timestamptz)
      ORDER BY id
      LIMIT $4::bigint
    )
  RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
)
SELECT
  count(*)
FROM
  deleted_jobs
`

type JobDeleteBeforeParams struct {
	CancelledFinalizedAtHorizon time.Time
	CompletedFinalizedAtHorizon time.Time
	DiscardedFinalizedAtHorizon time.Time
	Max                         int64
}

func (q *Queries) JobDeleteBefore(ctx context.Context, db DBTX, arg JobDeleteBeforeParams) (int64, error) {
	row := db.QueryRow(ctx, jobDeleteBefore,
		arg.CancelledFinalizedAtHorizon,
		arg.CompletedFinalizedAtHorizon,
		arg.DiscardedFinalizedAtHorizon,
		arg.Max,
	)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const jobGetAvailable = `-- name: JobGetAvailable :many
WITH locked_jobs AS (
  SELECT
    id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
  FROM
    river_job
  WHERE
    state = 'available'::river_job_state
    AND queue = $2::text
  ORDER BY
    priority ASC,
    scheduled_at ASC,
    id ASC
  LIMIT $3::integer
  FOR UPDATE
    SKIP LOCKED)
UPDATE
  river_job
SET
  state = 'running'::river_job_state,
  attempt = river_job.attempt + 1,
  attempted_at = NOW(),
  attempted_by = array_append(river_job.attempted_by, $1::text)
FROM
  locked_jobs
WHERE
  river_job.id = locked_jobs.id
RETURNING
  river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
`

type JobGetAvailableParams struct {
	Worker     string
	Queue      string
	LimitCount int32
}

func (q *Queries) JobGetAvailable(ctx context.Context, db DBTX, arg JobGetAvailableParams) ([]*RiverJob, error) {
	rows, err := db.Query(ctx, jobGetAvailable, arg.Worker, arg.Queue, arg.LimitCount)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetByID = `-- name: JobGetByID :one
SELECT
  id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM
  river_job
WHERE
  id = $1
LIMIT 1
`

func (q *Queries) JobGetByID(ctx context.Context, db DBTX, id int64) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobGetByID, id)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobGetByIDMany = `-- name: JobGetByIDMany :many
SELECT
  id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM
  river_job
WHERE
  id = any($1::bigint[])
`

func (q *Queries) JobGetByIDMany(ctx context.Context, db DBTX, id []int64) ([]*RiverJob, error) {
	rows, err := db.Query(ctx, jobGetByIDMany, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetByKind = `-- name: JobGetByKind :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE kind = $1
ORDER BY id
`

func (q *Queries) JobGetByKind(ctx context.Context, db DBTX, kind string) ([]*RiverJob, error) {
	rows, err := db.Query(ctx, jobGetByKind, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetByKindAndUniqueProperties = `-- name: JobGetByKindAndUniqueProperties :one
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE kind = $1
  AND CASE WHEN $2::boolean THEN args = $3 ELSE true END
  AND CASE WHEN $4::boolean THEN tstzrange($5::timestamptz, $6::timestamptz, '[)') @> created_at ELSE true END
  AND CASE WHEN $7::boolean THEN queue = $8 ELSE true END
  AND CASE WHEN $9::boolean THEN state::text = any($10::text[]) ELSE true END
`

type JobGetByKindAndUniquePropertiesParams struct {
	Kind           string
	ByArgs         bool
	Args           []byte
	ByCreatedAt    bool
	CreatedAtStart time.Time
	CreatedAtEnd   time.Time
	ByQueue        bool
	Queue          string
	ByState        bool
	State          []string
}

func (q *Queries) JobGetByKindAndUniqueProperties(ctx context.Context, db DBTX, arg JobGetByKindAndUniquePropertiesParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobGetByKindAndUniqueProperties,
		arg.Kind,
		arg.ByArgs,
		arg.Args,
		arg.ByCreatedAt,
		arg.CreatedAtStart,
		arg.CreatedAtEnd,
		arg.ByQueue,
		arg.Queue,
		arg.ByState,
		arg.State,
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobGetByKindMany = `-- name: JobGetByKindMany :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE kind = any($1::text[])
ORDER BY id
`

func (q *Queries) JobGetByKindMany(ctx context.Context, db DBTX, kind []string) ([]*RiverJob, error) {
	rows, err := db.Query(ctx, jobGetByKindMany, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetStuck = `-- name: JobGetStuck :many
SELECT
  id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM
  river_job
WHERE
  state = 'running'::river_job_state
  AND attempted_at < $1::timestamptz
LIMIT $2::integer
`

type JobGetStuckParams struct {
	StuckHorizon time.Time
	LimitCount   int32
}

func (q *Queries) JobGetStuck(ctx context.Context, db DBTX, arg JobGetStuckParams) ([]*RiverJob, error) {
	rows, err := db.Query(ctx, jobGetStuck, arg.StuckHorizon, arg.LimitCount)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobInsert = `-- name: JobInsert :one
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
  $1::jsonb,
  coalesce($2::smallint, 0),
  $3,
  coalesce($4::timestamptz, now()),
  $5::jsonb[],
  $6,
  $7::text,
  $8::smallint,
  coalesce($9::jsonb, '{}'),
  $10::smallint,
  $11::text,
  coalesce($12::timestamptz, now()),
  $13::river_job_state,
  $14::varchar(255)[]
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobInsertParams struct {
	Args        []byte
	Attempt     int16
	AttemptedAt *time.Time
	CreatedAt   *time.Time
	Errors      [][]byte
	FinalizedAt *time.Time
	Kind        string
	MaxAttempts int16
	Metadata    []byte
	Priority    int16
	Queue       string
	ScheduledAt *time.Time
	State       JobState
	Tags        []string
}

func (q *Queries) JobInsert(ctx context.Context, db DBTX, arg JobInsertParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobInsert,
		arg.Args,
		arg.Attempt,
		arg.AttemptedAt,
		arg.CreatedAt,
		arg.Errors,
		arg.FinalizedAt,
		arg.Kind,
		arg.MaxAttempts,
		arg.Metadata,
		arg.Priority,
		arg.Queue,
		arg.ScheduledAt,
		arg.State,
		arg.Tags,
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

type JobInsertManyParams struct {
	Args        []byte
	Errors      []AttemptError
	Kind        string
	MaxAttempts int16
	Metadata    []byte
	Priority    int16
	Queue       string
	State       JobState
	Tags        []string
}

const jobRescueMany = `-- name: JobRescueMany :exec
UPDATE river_job
SET
  errors = array_append(errors, updated_job.error),
  finalized_at = updated_job.finalized_at,
  scheduled_at = updated_job.scheduled_at,
  state = updated_job.state
FROM (
  SELECT
    unnest($1::bigint[]) AS id,
    unnest($2::jsonb[]) AS error,
    nullif(unnest($3::timestamptz[]), '0001-01-01 00:00:00 +0000') AS finalized_at,
    unnest($4::timestamptz[]) AS scheduled_at,
    unnest($5::text[])::river_job_state AS state
) AS updated_job
WHERE river_job.id = updated_job.id
`

type JobRescueManyParams struct {
	ID          []int64
	Error       [][]byte
	FinalizedAt []time.Time
	ScheduledAt []time.Time
	State       []string
}

// Run by the rescuer to queue for retry or discard depending on job state.
func (q *Queries) JobRescueMany(ctx context.Context, db DBTX, arg JobRescueManyParams) error {
	_, err := db.Exec(ctx, jobRescueMany,
		arg.ID,
		arg.Error,
		arg.FinalizedAt,
		arg.ScheduledAt,
		arg.State,
	)
	return err
}

const jobSchedule = `-- name: JobSchedule :one
WITH jobs_to_schedule AS (
  SELECT id
  FROM river_job
  WHERE
    state IN ('scheduled', 'retryable')
    AND queue IS NOT NULL
    AND priority >= 0
    AND scheduled_at <= $1::timestamptz
  ORDER BY
    priority,
    scheduled_at,
    id
  LIMIT $2::bigint
  FOR UPDATE
),
river_job_scheduled AS (
  UPDATE river_job
  SET state = 'available'::river_job_state
  FROM jobs_to_schedule
  WHERE river_job.id = jobs_to_schedule.id
  RETURNING jobs_to_schedule.id, river_job.id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
)
SELECT count(*)
FROM (
SELECT pg_notify('river_insert', json_build_object('queue', queue)::text)
FROM river_job_scheduled) AS notifications_sent
`

type JobScheduleParams struct {
	Now time.Time
	Max int64
}

func (q *Queries) JobSchedule(ctx context.Context, db DBTX, arg JobScheduleParams) (int64, error) {
	row := db.QueryRow(ctx, jobSchedule, arg.Now, arg.Max)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const jobSetCancelledIfRunning = `-- name: JobSetCancelledIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = $1::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = $2::timestamptz,
    errors = array_append(errors, $3::jsonb),
    state = 'cancelled'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
(
  SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
  FROM
    river_job
  WHERE
    river_job.id = $1::bigint
  UNION
  SELECT
    updated_job.id, updated_job.args, updated_job.attempt, updated_job.attempted_at, updated_job.attempted_by, updated_job.created_at, updated_job.errors, updated_job.finalized_at, updated_job.kind, updated_job.max_attempts, updated_job.metadata, updated_job.priority, updated_job.queue, updated_job.state, updated_job.scheduled_at, updated_job.tags
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1
`

type JobSetCancelledIfRunningParams struct {
	ID          int64
	FinalizedAt time.Time
	Error       []byte
}

func (q *Queries) JobSetCancelledIfRunning(ctx context.Context, db DBTX, arg JobSetCancelledIfRunningParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetCancelledIfRunning, arg.ID, arg.FinalizedAt, arg.Error)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetCompleted = `-- name: JobSetCompleted :one
UPDATE
  river_job
SET
  finalized_at = $1::timestamptz,
  state = 'completed'::river_job_state
WHERE
  id = $2::bigint
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobSetCompletedParams struct {
	FinalizedAt time.Time
	ID          int64
}

func (q *Queries) JobSetCompleted(ctx context.Context, db DBTX, arg JobSetCompletedParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetCompleted, arg.FinalizedAt, arg.ID)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetCompletedIfRunning = `-- name: JobSetCompletedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = $1::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = $2::timestamptz,
    state = 'completed'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
(
  SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
  FROM
    river_job
  WHERE
    river_job.id = $1::bigint
  UNION
  SELECT
    updated_job.id, updated_job.args, updated_job.attempt, updated_job.attempted_at, updated_job.attempted_by, updated_job.created_at, updated_job.errors, updated_job.finalized_at, updated_job.kind, updated_job.max_attempts, updated_job.metadata, updated_job.priority, updated_job.queue, updated_job.state, updated_job.scheduled_at, updated_job.tags
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1
`

type JobSetCompletedIfRunningParams struct {
	ID          int64
	FinalizedAt time.Time
}

func (q *Queries) JobSetCompletedIfRunning(ctx context.Context, db DBTX, arg JobSetCompletedIfRunningParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetCompletedIfRunning, arg.ID, arg.FinalizedAt)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetDiscarded = `-- name: JobSetDiscarded :one
UPDATE
  river_job
SET
  finalized_at = $1::timestamptz,
  errors = array_append(errors, $2::jsonb),
  state = 'discarded'::river_job_state
WHERE
  id = $3::bigint
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobSetDiscardedParams struct {
	FinalizedAt time.Time
	Error       []byte
	ID          int64
}

func (q *Queries) JobSetDiscarded(ctx context.Context, db DBTX, arg JobSetDiscardedParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetDiscarded, arg.FinalizedAt, arg.Error, arg.ID)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetDiscardedIfRunning = `-- name: JobSetDiscardedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = $1::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    finalized_at = $2::timestamptz,
    errors = array_append(errors, $3::jsonb),
    state = 'discarded'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
(
  SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
  FROM
    river_job
  WHERE
    river_job.id = $1::bigint
  UNION
  SELECT
    updated_job.id, updated_job.args, updated_job.attempt, updated_job.attempted_at, updated_job.attempted_by, updated_job.created_at, updated_job.errors, updated_job.finalized_at, updated_job.kind, updated_job.max_attempts, updated_job.metadata, updated_job.priority, updated_job.queue, updated_job.state, updated_job.scheduled_at, updated_job.tags
  FROM
    updated_job
) ORDER BY finalized_at DESC NULLS LAST LIMIT 1
`

type JobSetDiscardedIfRunningParams struct {
	ID          int64
	FinalizedAt time.Time
	Error       []byte
}

func (q *Queries) JobSetDiscardedIfRunning(ctx context.Context, db DBTX, arg JobSetDiscardedIfRunningParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetDiscardedIfRunning, arg.ID, arg.FinalizedAt, arg.Error)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetErroredIfRunning = `-- name: JobSetErroredIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = $1::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    scheduled_at = $2::timestamptz,
    errors = array_append(errors, $3::jsonb),
    state = 'retryable'::river_job_state
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
(
  SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
  FROM
    river_job
  WHERE
    river_job.id = $1::bigint
  UNION
  SELECT
    updated_job.id, updated_job.args, updated_job.attempt, updated_job.attempted_at, updated_job.attempted_by, updated_job.created_at, updated_job.errors, updated_job.finalized_at, updated_job.kind, updated_job.max_attempts, updated_job.metadata, updated_job.priority, updated_job.queue, updated_job.state, updated_job.scheduled_at, updated_job.tags
  FROM
    updated_job
) ORDER BY scheduled_at DESC NULLS LAST LIMIT 1
`

type JobSetErroredIfRunningParams struct {
	ID          int64
	ScheduledAt time.Time
	Error       []byte
}

func (q *Queries) JobSetErroredIfRunning(ctx context.Context, db DBTX, arg JobSetErroredIfRunningParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetErroredIfRunning, arg.ID, arg.ScheduledAt, arg.Error)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobSetSnoozedIfRunning = `-- name: JobSetSnoozedIfRunning :one
WITH job_to_update AS (
  SELECT
    id,
    finalized_at,
    state
  FROM
    river_job
  WHERE
    id = $1::bigint
  FOR UPDATE
),
updated_job AS (
  UPDATE
    river_job
  SET
    scheduled_at = $2::timestamptz,
    state = 'scheduled'::river_job_state,
    max_attempts = max_attempts + 1
  FROM
    job_to_update
  WHERE
    river_job.id = job_to_update.id
    AND river_job.state = 'running'::river_job_state
  RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
(
  SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
  FROM
    river_job
  WHERE
    river_job.id = $1::bigint
  UNION
  SELECT
    updated_job.id, updated_job.args, updated_job.attempt, updated_job.attempted_at, updated_job.attempted_by, updated_job.created_at, updated_job.errors, updated_job.finalized_at, updated_job.kind, updated_job.max_attempts, updated_job.metadata, updated_job.priority, updated_job.queue, updated_job.state, updated_job.scheduled_at, updated_job.tags
  FROM
    updated_job
) ORDER BY scheduled_at DESC NULLS LAST LIMIT 1
`

type JobSetSnoozedIfRunningParams struct {
	ID          int64
	ScheduledAt time.Time
}

func (q *Queries) JobSetSnoozedIfRunning(ctx context.Context, db DBTX, arg JobSetSnoozedIfRunningParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobSetSnoozedIfRunning, arg.ID, arg.ScheduledAt)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const jobUpdate = `-- name: JobUpdate :one
UPDATE river_job
SET
  attempt = CASE WHEN $1::boolean THEN $2 ELSE attempt END,
  attempted_at = CASE WHEN $3::boolean THEN $4 ELSE attempted_at END,
  state = CASE WHEN $5::boolean THEN $6 ELSE state END
WHERE id = $7
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobUpdateParams struct {
	AttemptDoUpdate     bool
	Attempt             int16
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	StateDoUpdate       bool
	State               JobState
	ID                  int64
}

// A generalized update for any property on a job. This brings in a large number
// of parameters and therefore may be more suitable for testing than production.
func (q *Queries) JobUpdate(ctx context.Context, db DBTX, arg JobUpdateParams) (*RiverJob, error) {
	row := db.QueryRow(ctx, jobUpdate,
		arg.AttemptDoUpdate,
		arg.Attempt,
		arg.AttemptedAtDoUpdate,
		arg.AttemptedAt,
		arg.StateDoUpdate,
		arg.State,
		arg.ID,
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		&i.AttemptedBy,
		&i.CreatedAt,
		&i.Errors,
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		&i.Tags,
	)
	return &i, err
}

const pGAdvisoryXactLock = `-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock($1)
`

func (q *Queries) PGAdvisoryXactLock(ctx context.Context, db DBTX, key int64) error {
	_, err := db.Exec(ctx, pGAdvisoryXactLock, key)
	return err
}
