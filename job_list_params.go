package river

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/rivertype"
)

// JobListCursor is used to specify a starting point for a paginated
// job list query.
type JobListCursor struct {
	id        int64
	kind      string
	queue     string
	sortField JobListOrderByField
	time      time.Time
}

// JobListCursorFromJob creates a JobListCursor from a JobRow.
func JobListCursorFromJob(job *rivertype.JobRow, sortField JobListOrderByField) *JobListCursor {
	time := job.CreatedAt
	switch sortField {
	case JobListOrderByTime:
		time = jobListTimeValue(job)
	case JobListOrderByFinalizedAt:
		if job.FinalizedAt != nil {
			time = *job.FinalizedAt
		}
	case JobListOrderByScheduledAt:
		time = job.ScheduledAt
	default:
		panic("invalid sort field")
	}
	return &JobListCursor{
		id:        job.ID,
		kind:      job.Kind,
		queue:     job.Queue,
		sortField: sortField,
		time:      time,
	}
}

// UnmarshalText implements encoding.TextUnmarshaler to decode the cursor from
// a previously marshaled string.
func (c *JobListCursor) UnmarshalText(text []byte) error {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
	n, err := base64.StdEncoding.Decode(dst, text)
	if err != nil {
		return err
	}
	dst = dst[:n]

	wrapperValue := jobListPaginationCursorJSON{}
	if err := json.Unmarshal(dst, &wrapperValue); err != nil {
		return err
	}
	*c = JobListCursor{
		id:        wrapperValue.ID,
		kind:      wrapperValue.Kind,
		queue:     wrapperValue.Queue,
		sortField: JobListOrderByField(wrapperValue.SortField),
		time:      wrapperValue.Time,
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler to encode the cursor as an
// opaque string.
func (c JobListCursor) MarshalText() ([]byte, error) {
	wrapperValue := jobListPaginationCursorJSON{
		ID:        c.id,
		Kind:      c.kind,
		Queue:     c.queue,
		SortField: string(c.sortField),
		Time:      c.time,
	}
	data, err := json.Marshal(wrapperValue)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, base64.URLEncoding.EncodedLen(len(data)))
	base64.URLEncoding.Encode(dst, data)
	return dst, nil
}

type jobListPaginationCursorJSON struct {
	ID        int64     `json:"id"`
	Kind      string    `json:"kind"`
	Queue     string    `json:"queue"`
	SortField string    `json:"sort_field"`
	Time      time.Time `json:"time"`
}

// SortOrder specifies the direction of a sort.
type SortOrder int

const (
	// SortOrderAsc specifies that the sort should in ascending order.
	SortOrderAsc SortOrder = iota
	// SortOrderDesc specifies that the sort should in descending order.
	SortOrderDesc
)

// JobListOrderByField specifies the field to sort by.
type JobListOrderByField string

const (
	// JobListOrderByFinalizedAt specifies that the sort should be by
	// finalized_at.
	//
	// This option must be used in conjunction with filtering by only finalized
	// job states.
	JobListOrderByFinalizedAt JobListOrderByField = "finalized_at"
	// JobListOrderByScheduledAt specifies that the sort should be by scheduled_at.
	JobListOrderByScheduledAt JobListOrderByField = "scheduled_at"
	// JobListOrderByTime specifies that the sort should be by time. The specific
	// time field used will vary by the first specified job state.
	JobListOrderByTime JobListOrderByField = "time"
)

// JobListParams specifies the parameters for a JobList query. It must be
// initialized with NewJobListParams. Params can be built by chaining methods on
// the JobListParams object:
//
//	params := NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).First(100)
type JobListParams struct {
	after            *JobListCursor
	kinds            []string
	metadataFragment string
	overrodeState    bool
	paginationCount  int32
	queues           []string
	sortField        JobListOrderByField
	sortOrder        SortOrder
	states           []rivertype.JobState
}

// NewJobListParams creates a new JobListParams to return available jobs sorted
// by time in ascending order, returning 100 jobs at most.
func NewJobListParams() *JobListParams {
	return &JobListParams{
		paginationCount: 100,
		sortField:       JobListOrderByTime,
		sortOrder:       SortOrderAsc,
		states: []rivertype.JobState{
			JobStateAvailable,
			JobStateCancelled,
			JobStateCompleted,
			JobStateDiscarded,
			JobStateRetryable,
			JobStateRunning,
			JobStateScheduled,
		},
	}
}

func (p *JobListParams) copy() *JobListParams {
	return &JobListParams{
		after:            p.after,
		kinds:            append([]string(nil), p.kinds...),
		metadataFragment: p.metadataFragment,
		overrodeState:    p.overrodeState,
		paginationCount:  p.paginationCount,
		queues:           append([]string(nil), p.queues...),
		sortField:        p.sortField,
		sortOrder:        p.sortOrder,
		states:           append([]rivertype.JobState(nil), p.states...),
	}
}

func (p *JobListParams) toDBParams() (*dblist.JobListParams, error) {
	conditionsBuilder := &strings.Builder{}
	conditions := make([]string, 0, 10)
	namedArgs := make(map[string]any)
	orderBy := []dblist.JobListOrderBy{}

	var sortOrder dblist.SortOrder
	switch p.sortOrder {
	case SortOrderAsc:
		sortOrder = dblist.SortOrderAsc
	case SortOrderDesc:
		sortOrder = dblist.SortOrderDesc
	default:
		return nil, errors.New("invalid sort order")
	}

	if p.sortField == JobListOrderByFinalizedAt {
		currentNonFinalizedStates := make([]rivertype.JobState, 0, len(p.states))
		for _, state := range p.states {
			//nolint:exhaustive
			switch state {
			case JobStateCancelled, JobStateCompleted, JobStateDiscarded:
			default:
				currentNonFinalizedStates = append(currentNonFinalizedStates, state)
			}
		}
		// This indicates the user overrode the States list with only non-finalized
		// states prior to then requesting FinalizedAt ordering.
		if len(currentNonFinalizedStates) == 0 {
			return nil, fmt.Errorf("cannot order by finalized_at with non-finalized state filters %+v", currentNonFinalizedStates)
		}
	}

	var timeField string
	if len(p.states) > 0 && p.sortField == JobListOrderByTime {
		timeField = jobListTimeFieldForState(p.states[0])
	} else {
		timeField = string(p.sortField)
	}

	orderBy = append(orderBy, []dblist.JobListOrderBy{
		{Expr: timeField, Order: sortOrder},
		{Expr: "id", Order: sortOrder},
	}...)

	if p.metadataFragment != "" {
		conditions = append(conditions, `metadata @> @metadata_fragment::jsonb`)
		namedArgs["metadata_fragment"] = p.metadataFragment
	}

	if p.after != nil {
		if sortOrder == dblist.SortOrderAsc {
			conditions = append(conditions, fmt.Sprintf(`("%s" > @cursor_time OR ("%s" = @cursor_time AND "id" > @after_id))`, timeField, timeField))
		} else {
			conditions = append(conditions, fmt.Sprintf(`("%s" < @cursor_time OR ("%s" = @cursor_time AND "id" < @after_id))`, timeField, timeField))
		}
		namedArgs["cursor_time"] = p.after.time
		namedArgs["after_id"] = p.after.id
	}

	for i, condition := range conditions {
		if i > 0 {
			conditionsBuilder.WriteString("\n  AND ")
		}
		conditionsBuilder.WriteString(condition)
	}

	dbParams := &dblist.JobListParams{
		Conditions: conditionsBuilder.String(),
		Kinds:      p.kinds,
		LimitCount: p.paginationCount,
		NamedArgs:  namedArgs,
		OrderBy:    orderBy,
		Priorities: nil,
		Queues:     p.queues,
		States:     p.states,
	}

	return dbParams, nil
}

// After returns an updated filter set that will only return jobs
// after the given cursor.
func (p *JobListParams) After(cursor *JobListCursor) *JobListParams {
	result := p.copy()
	result.after = cursor
	return result
}

// First returns an updated filter set that will only return the first
// count jobs.
//
// Count must be between 1 and 10000, inclusive, or this will panic.
func (p *JobListParams) First(count int) *JobListParams {
	if count <= 0 {
		panic("count must be > 0")
	}
	if count > 10000 {
		panic("count must be <= 10000")
	}
	result := p.copy()
	result.paginationCount = int32(count)
	return result
}

// Kinds returns an updated filter set that will only return jobs of the given
// kinds.
func (p *JobListParams) Kinds(kinds ...string) *JobListParams {
	result := p.copy()
	result.kinds = make([]string, len(kinds))
	copy(result.kinds, kinds)
	return result
}

func (p *JobListParams) Metadata(json string) *JobListParams {
	result := p.copy()
	result.metadataFragment = json
	return result
}

// Queues returns an updated filter set that will only return jobs from the
// given queues.
func (p *JobListParams) Queues(queues ...string) *JobListParams {
	result := p.copy()
	result.queues = make([]string, len(queues))
	copy(result.queues, queues)
	return result
}

// OrderBy returns an updated filter set that will sort the results using the
// specified field and direction.
//
// If ordering by FinalizedAt, the States filter will be set to only include
// finalized job states unless it has already been overridden.
func (p *JobListParams) OrderBy(field JobListOrderByField, direction SortOrder) *JobListParams {
	result := p.copy()
	switch field {
	case JobListOrderByTime, JobListOrderByScheduledAt:
		result.sortField = field
	case JobListOrderByFinalizedAt:
		result.sortField = field
		if !p.overrodeState {
			result.states = []rivertype.JobState{
				JobStateCancelled,
				JobStateCompleted,
				JobStateDiscarded,
			}
		}
	default:
		panic("invalid order by field")
	}
	result.sortField = field
	result.sortOrder = direction
	return result
}

// States returns an updated filter set that will only return jobs in the given
// states.
func (p *JobListParams) States(states ...rivertype.JobState) *JobListParams {
	result := p.copy()
	result.states = make([]rivertype.JobState, len(states))
	result.overrodeState = true
	copy(result.states, states)
	return result
}

func jobListTimeFieldForState(state rivertype.JobState) string {
	switch state {
	case rivertype.JobStateAvailable, rivertype.JobStateRetryable, rivertype.JobStateScheduled:
		return "scheduled_at"
	case rivertype.JobStateRunning:
		return "attempted_at"
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		return "finalized_at"
	default:
		return "created_at" // should never happen
	}
}

func jobListTimeValue(job *rivertype.JobRow) time.Time {
	switch job.State {
	case rivertype.JobStateAvailable, rivertype.JobStateRetryable, rivertype.JobStateScheduled:
		return job.ScheduledAt
	case rivertype.JobStateRunning:
		if job.AttemptedAt == nil {
			// This should never happen unless a job has been manually manipulated.
			return job.CreatedAt
		}
		return *job.AttemptedAt
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		if job.FinalizedAt == nil {
			// This should never happen unless a job has been manually manipulated.
			return job.CreatedAt
		}
		return *job.FinalizedAt
	default:
		return job.CreatedAt // should never happen
	}
}
