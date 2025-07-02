package dblist

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

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
	IDs        []int64
	Kinds      []string
	LimitCount int32
	OrderBy    []JobListOrderBy
	Priorities []int16
	Queues     []string
	Schema     string
	States     []rivertype.JobState
	Where      []WherePredicate
}

type WherePredicate struct {
	NamedArgs map[string]any
	SQL       string
}

// JobMakeDriverParams converts client-level parameters for job and delete to
// driver-level parameters for use with an executor, which generally goes by
// converting typed fields for IDs, kinds, queues, etc. to lower-level SQL.
//
// This was originally implemented for listing jobs, but since the logic is so
// similar, it also performs the same function for JobDeleteMany. This works
// because `riverdriver.JobListParams` is identical to `JobDeleteMany` and
// therefore pointer-level converts to it.
func JobMakeDriverParams(ctx context.Context, params *JobListParams, sqlFragmentColumnIn func(column string, values any) (string, any, error)) (*riverdriver.JobListParams, error) {
	var (
		namedArgs    = make(map[string]any)
		whereBuilder strings.Builder
	)

	orderBy := make([]JobListOrderBy, len(params.OrderBy))
	for i, o := range params.OrderBy {
		orderBy[i] = JobListOrderBy{
			Expr:  o.Expr,
			Order: o.Order,
		}
	}

	// Writes an `AND` to connect SQL predicates as long as this isn't the first
	// predicate.
	writeAndAfterFirst := func() {
		if whereBuilder.Len() != 0 {
			whereBuilder.WriteString("\n  AND ")
		}
	}

	if len(params.IDs) > 0 {
		writeAndAfterFirst()

		const column = "id"
		sqlFragment, arg, err := sqlFragmentColumnIn(column, params.IDs)
		if err != nil {
			return nil, fmt.Errorf("error building SQL fragment for %q: %w", column, err)
		}
		whereBuilder.WriteString(sqlFragment)
		namedArgs[column] = arg
	}

	if len(params.Kinds) > 0 {
		writeAndAfterFirst()

		const column = "kind"
		sqlFragment, arg, err := sqlFragmentColumnIn(column, params.Kinds)
		if err != nil {
			return nil, fmt.Errorf("error building SQL fragment for %q: %w", column, err)
		}
		whereBuilder.WriteString(sqlFragment)
		namedArgs[column] = arg
	}

	if len(params.Priorities) > 0 {
		writeAndAfterFirst()

		const column = "priority"
		sqlFragment, arg, err := sqlFragmentColumnIn(column, params.Priorities)
		if err != nil {
			return nil, fmt.Errorf("error building SQL fragment for %q: %w", column, err)
		}
		whereBuilder.WriteString(sqlFragment)
		namedArgs[column] = arg
	}

	if len(params.Queues) > 0 {
		writeAndAfterFirst()

		const column = "queue"
		sqlFragment, arg, err := sqlFragmentColumnIn(column, params.Queues)
		if err != nil {
			return nil, fmt.Errorf("error building SQL fragment for %q: %w", column, err)
		}
		whereBuilder.WriteString(sqlFragment)
		namedArgs[column] = arg
	}

	if len(params.States) > 0 {
		writeAndAfterFirst()

		const column = "state"
		sqlFragment, arg, err := sqlFragmentColumnIn(column,
			sliceutil.Map(params.States, func(v rivertype.JobState) string { return string(v) }))
		if err != nil {
			return nil, fmt.Errorf("error building SQL fragment for %q: %w", column, err)
		}
		whereBuilder.WriteString(sqlFragment)
		namedArgs[column] = arg
	}

	for _, where := range params.Where {
		writeAndAfterFirst()

		whereBuilder.WriteString(where.SQL)
		for name, val := range where.NamedArgs {
			expectedSymbol := "@" + name
			if !strings.Contains(where.SQL, expectedSymbol) {
				return nil, fmt.Errorf("expected %q to contain named arg symbol %s", where.SQL, expectedSymbol)
			}

			if _, ok := namedArgs[name]; ok {
				return nil, fmt.Errorf("named argument %s already registered", expectedSymbol)
			}

			namedArgs[name] = val
		}
	}

	// A condition of some kind is needed, so given no others write one that'll
	// always return true.
	if whereBuilder.Len() < 1 {
		whereBuilder.WriteString("true")
	}

	if params.LimitCount < 1 {
		return nil, errors.New("required parameter 'Count' in JobList must be greater than zero")
	}

	if len(params.OrderBy) == 0 {
		return nil, errors.New("sort order is required")
	}

	var orderByBuilder strings.Builder

	for i, orderBy := range params.OrderBy {
		orderByBuilder.WriteString(orderBy.Expr)
		switch orderBy.Order {
		case SortOrderAsc:
			orderByBuilder.WriteString(" ASC")
		case SortOrderDesc:
			orderByBuilder.WriteString(" DESC")
		case SortOrderUnspecified:
			return nil, errors.New("should not have gotten SortOrderUnspecified by this point before executing list (bug?)")
		}
		if i < len(params.OrderBy)-1 {
			orderByBuilder.WriteString(", ")
		}
	}

	return &riverdriver.JobListParams{
		Max:           params.LimitCount,
		NamedArgs:     namedArgs,
		OrderByClause: orderByBuilder.String(),
		Schema:        params.Schema,
		WhereClause:   whereBuilder.String(),
	}, nil
}
