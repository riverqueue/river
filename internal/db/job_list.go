package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river/internal/dbsqlc"
)

const jobList = `-- name: JobList :many
SELECT
  id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM
  river_job
WHERE
  state = @state::river_job_state%s
ORDER BY
  %s
LIMIT @count::integer
`

type SortOrder int

const (
	SortOrderUnspecified SortOrder = iota
	SortOrderAsc
	SortOrderDesc
)

type JobListOrderBy struct {
	Expr  string
	Order SortOrder
}

type JobListParams struct {
	State      dbsqlc.JobState
	Priorities []int16
	Conditions string
	OrderBy    []JobListOrderBy
	NamedArgs  map[string]any
	LimitCount int32
}

func JobList(ctx context.Context, tx pgx.Tx, arg JobListParams) ([]*dbsqlc.RiverJob, error) {
	namedArgs := make(pgx.NamedArgs)
	for k, v := range arg.NamedArgs {
		namedArgs[k] = v
	}
	if arg.State == "" {
		return nil, errors.New("missing required argument 'State' in JobList")
	}
	namedArgs["state"] = arg.State
	if arg.LimitCount < 1 {
		return nil, errors.New("required argument 'Count' in JobList must be greater than zero")
	}
	namedArgs["count"] = arg.LimitCount

	if len(arg.OrderBy) == 0 {
		return nil, errors.New("sort order is required")
	}

	var orderByBuilder strings.Builder

	for i, orderBy := range arg.OrderBy {
		orderByBuilder.WriteString(orderBy.Expr)
		if orderBy.Order == SortOrderAsc {
			orderByBuilder.WriteString(" ASC")
		} else if orderBy.Order == SortOrderDesc {
			orderByBuilder.WriteString(" DESC")
		}
		if i < len(arg.OrderBy)-1 {
			orderByBuilder.WriteString(", ")
		}
	}

	var conditions string
	if arg.Conditions != "" {
		conditions = "\n  AND " + arg.Conditions
	}

	query := fmt.Sprintf(jobList, conditions, orderByBuilder.String())
	rows, err := tx.Query(ctx, query, namedArgs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*dbsqlc.RiverJob
	for rows.Next() {
		var i dbsqlc.RiverJob
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
