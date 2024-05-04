package dblist

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

const jobList = `-- name: JobList :many
SELECT
  %s
FROM
  river_job
%s
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
	Conditions string
	Kinds      []string
	LimitCount int32
	NamedArgs  map[string]any
	OrderBy    []JobListOrderBy
	Priorities []int16
	Queues     []string
	States     []rivertype.JobState
}

func JobList(ctx context.Context, exec riverdriver.Executor, params *JobListParams) ([]*rivertype.JobRow, error) {
	var conditionsBuilder strings.Builder

	orderBy := make([]JobListOrderBy, len(params.OrderBy))
	for i, o := range params.OrderBy {
		orderBy[i] = JobListOrderBy{
			Expr:  o.Expr,
			Order: o.Order,
		}
	}

	namedArgs := params.NamedArgs
	if namedArgs == nil {
		namedArgs = make(map[string]any)
	}

	writeWhereOrAnd := func() {
		if conditionsBuilder.Len() == 0 {
			conditionsBuilder.WriteString("WHERE\n	")
		} else {
			conditionsBuilder.WriteString("\n  AND ")
		}
	}

	if len(params.Kinds) > 0 {
		writeWhereOrAnd()
		conditionsBuilder.WriteString("kind = any(@kinds::text[])")
		namedArgs["kinds"] = params.Kinds
	}

	if len(params.Queues) > 0 {
		writeWhereOrAnd()
		conditionsBuilder.WriteString("queue = any(@queues::text[])")
		namedArgs["queues"] = params.Queues
	}

	if len(params.States) > 0 {
		writeWhereOrAnd()
		conditionsBuilder.WriteString("state = any(@states::river_job_state[])")
		namedArgs["states"] = sliceutil.Map(params.States, func(s rivertype.JobState) string { return string(s) })
	}

	if params.Conditions != "" {
		writeWhereOrAnd()
		conditionsBuilder.WriteString(params.Conditions)
	}

	if params.LimitCount < 1 {
		return nil, errors.New("required parameter 'Count' in JobList must be greater than zero")
	}
	namedArgs["count"] = params.LimitCount

	if len(params.OrderBy) == 0 {
		return nil, errors.New("sort order is required")
	}

	var orderByBuilder strings.Builder

	for i, orderBy := range params.OrderBy {
		orderByBuilder.WriteString(orderBy.Expr)
		if orderBy.Order == SortOrderAsc {
			orderByBuilder.WriteString(" ASC")
		} else if orderBy.Order == SortOrderDesc {
			orderByBuilder.WriteString(" DESC")
		}
		if i < len(params.OrderBy)-1 {
			orderByBuilder.WriteString(", ")
		}
	}

	sql := fmt.Sprintf(jobList, exec.JobListFields(), conditionsBuilder.String(), orderByBuilder.String())

	return exec.JobList(ctx, sql, namedArgs)
}
