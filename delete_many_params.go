package river

import (
	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/rivertype"
)

// JobDeleteManyParams specifies the parameters for a JobDeleteMany query. It
// must be initialized with NewJobDeleteManyParams. Params can be built by
// chaining methods on the JobDeleteManyParams object:
//
//	params := NewJobDeleteManyParams().First(100).States(river.JobStateCompleted)
type JobDeleteManyParams struct {
	ids        []int64
	kinds      []string
	limit      int32
	priorities []int16
	queues     []string
	schema     string
	states     []rivertype.JobState
	unsafeAll  bool
}

// NewJobDeleteManyParams creates a new JobDeleteManyParams to delete jobs
// sorted by ID in ascending order, deleting 100 jobs at most.
func NewJobDeleteManyParams() *JobDeleteManyParams {
	return &JobDeleteManyParams{
		limit: 100,
	}
}

func (p *JobDeleteManyParams) copy() *JobDeleteManyParams {
	return &JobDeleteManyParams{
		ids:        append([]int64(nil), p.ids...),
		kinds:      append([]string(nil), p.kinds...),
		limit:      p.limit,
		priorities: append([]int16(nil), p.priorities...),
		queues:     append([]string(nil), p.queues...),
		schema:     p.schema,
		states:     append([]rivertype.JobState(nil), p.states...),
		unsafeAll:  p.unsafeAll,
	}
}

func (p *JobDeleteManyParams) filtersEmpty() bool {
	return len(p.ids) < 1 &&
		len(p.kinds) < 1 &&
		len(p.priorities) < 1 &&
		len(p.queues) < 1 &&
		len(p.states) < 1
}

func (p *JobDeleteManyParams) toDBParams() *dblist.JobListParams {
	return &dblist.JobListParams{
		IDs:        p.ids,
		Kinds:      p.kinds,
		LimitCount: p.limit,
		OrderBy:    []dblist.JobListOrderBy{{Expr: "id", Order: dblist.SortOrderAsc}},
		Priorities: p.priorities,
		Queues:     p.queues,
		Schema:     p.schema,
		States:     p.states,
	}
}

// First returns an updated filter set that will only delete the first
// count jobs.
//
// Count must be between 1 and 10_000, inclusive, or this will panic.
func (p *JobDeleteManyParams) First(count int) *JobDeleteManyParams {
	if count <= 0 {
		panic("count must be > 0")
	}
	if count > 10000 {
		panic("count must be <= 10000")
	}
	paramsCopy := p.copy()
	paramsCopy.limit = int32(count)
	return paramsCopy
}

// IDs returns an updated filter set that will only delete jobs with the given
// IDs.
func (p *JobDeleteManyParams) IDs(ids ...int64) *JobDeleteManyParams {
	paramsCopy := p.copy()
	paramsCopy.ids = make([]int64, len(ids))
	copy(paramsCopy.ids, ids)
	return paramsCopy
}

// Kinds returns an updated filter set that will only delete jobs of the given
// kinds.
func (p *JobDeleteManyParams) Kinds(kinds ...string) *JobDeleteManyParams {
	paramsCopy := p.copy()
	paramsCopy.kinds = make([]string, len(kinds))
	copy(paramsCopy.kinds, kinds)
	return paramsCopy
}

// Priorities returns an updated filter set that will only delete jobs with the
// given priorities.
func (p *JobDeleteManyParams) Priorities(priorities ...int16) *JobDeleteManyParams {
	paramsCopy := p.copy()
	paramsCopy.priorities = make([]int16, len(priorities))
	copy(paramsCopy.priorities, priorities)
	return paramsCopy
}

// Queues returns an updated filter set that will only delete jobs from the
// given queues.
func (p *JobDeleteManyParams) Queues(queues ...string) *JobDeleteManyParams {
	paramsCopy := p.copy()
	paramsCopy.queues = make([]string, len(queues))
	copy(paramsCopy.queues, queues)
	return paramsCopy
}

// States returns an updated filter set that will only delete jobs in the given
// states.
func (p *JobDeleteManyParams) States(states ...rivertype.JobState) *JobDeleteManyParams {
	paramsCopy := p.copy()
	paramsCopy.states = make([]rivertype.JobState, len(states))
	copy(paramsCopy.states, states)
	return paramsCopy
}

// UnsafeAll is a special directive that allows unbounded job deletion without
// any filters. Normally, filters like IDs or Kinds is required to scope down
// the deletion so that the caller doesn't accidentally delete all non-running
// jobs. Invoking UnsafeAll removes this safety guard so that all jobs can be
// removed arbitrarily.
//
// Example of use:
//
//	deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().UnsafeAll())
//	if err != nil {
//		// handle error
//	}
//
// It only makes sense to call this function if no filters have yet been applied
// on the parameters object. If some have already, calling it will panic.
func (p *JobDeleteManyParams) UnsafeAll() *JobDeleteManyParams {
	if !p.filtersEmpty() {
		panic("UnsafeAll no longer meaningful with non-default filters applied")
	}

	paramsCopy := p.copy()
	paramsCopy.unsafeAll = true
	return paramsCopy
}
