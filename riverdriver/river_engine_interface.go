package riverdriver

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

type Engine[TTx any] interface {
	JobGetAvailable(ctx context.Context, params *JobGetAvailableParams) ([]*rivertype.JobRow, error)
}

type StandardEngine[TTx any] struct {
	driver Driver[TTx]
}

func NewStandardEngine[TTx any](driver Driver[TTx]) *StandardEngine[TTx] {
	return &StandardEngine[TTx]{driver: driver}
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
        AND scheduled_at <= now()
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT $3::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    river_job
SET
    state = 'running'::river_job_state,
    attempt = river_job.attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(river_job.attempted_by, $1::text)
FROM
    locked_jobs
WHERE
    river_job.id = locked_jobs.id
RETURNING
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
`

func (e *StandardEngine[TTx]) JobGetAvailable(ctx context.Context, params *JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	rows, err := e.driver.GetExecutor().Query(ctx, jobGetAvailable, params.AttemptedBy, params.Queue, params.Max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return e.driver.RowsToJobs(rows)
}
