package river

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// JobListCursor is used to specify a starting point for a paginated
// job list query.
type JobListCursor struct {
	id        int64
	job       *rivertype.JobRow // used for JobListCursorFromJob path; not serialized
	kind      string
	queue     string
	sortField JobListOrderByField
	time      time.Time // may be empty
}

// JobListCursorFromJob creates a JobListCursor from a JobRow.
func JobListCursorFromJob(job *rivertype.JobRow) *JobListCursor {
	// Other fields are initialized when the cursor is used in After below.
	return &JobListCursor{job: job}
}

func jobListCursorFromJobAndParams(job *rivertype.JobRow, listParams *JobListParams) *JobListCursor {
	// A pointer so that we can detect a condition where we accidentally left
	// this value unset.
	var cursorTime *time.Time

	// Don't include a `default` so `exhaustive` lint can detect omissions.
	switch listParams.sortField {
	case JobListOrderByID:
		cursorTime = ptrutil.Ptr(time.Time{})
	case JobListOrderByTime:
		cursorTime = ptrutil.Ptr(jobListTimeValue(job))
	case JobListOrderByFinalizedAt:
		if job.FinalizedAt != nil {
			cursorTime = job.FinalizedAt
		}
	case JobListOrderByScheduledAt:
		cursorTime = &job.ScheduledAt
	}

	if cursorTime == nil {
		panic("invalid sort field")
	}

	return &JobListCursor{
		id:        job.ID,
		kind:      job.Kind,
		queue:     job.Queue,
		sortField: listParams.sortField,
		time:      *cursorTime,
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
	if c.job != nil {
		return nil, errors.New("cursor initialized with only a job can't be marshaled; try a cursor from JobListResult instead")
	}

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
	// JobListOrderByID specifies that the sort should be by job ID.
	JobListOrderByID JobListOrderByField = "id"

	// JobListOrderByFinalizedAt specifies that the sort should be by
	// `finalized_at`.
	//
	// This option must be used in conjunction with filtering by only finalized
	// job states.
	JobListOrderByFinalizedAt JobListOrderByField = "finalized_at"

	// JobListOrderByScheduledAt specifies that the sort should be by
	// `scheduled_at`.
	JobListOrderByScheduledAt JobListOrderByField = "scheduled_at"

	// JobListOrderByTime specifies that the sort should be by the "best fit"
	// time field based on listed state. The best fit is determined by looking
	// at the first value given to JobListParams.States. If multiple states are
	// specified, the ones after the first will be ignored.
	//
	// The specific time field used for sorting depends on requested state:
	//
	// * States `available`, `retryable`, or `scheduled` use `scheduled_at`.
	// * State `running` uses `attempted_at`.
	// * States `cancelled`, `completed`, or `discarded` use `finalized_at`.
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
		sortField:       JobListOrderByID,
		sortOrder:       SortOrderAsc,
		states: []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			rivertype.JobStateRetryable,
			rivertype.JobStateRunning,
			rivertype.JobStateScheduled,
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
	orderBy := make([]dblist.JobListOrderBy, 0, 2)

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
			case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
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
	switch {
	case p.sortField == JobListOrderByID:
		// no time field

	case len(p.states) > 0 && p.sortField == JobListOrderByTime:
		timeField = jobListTimeFieldForState(p.states[0])
		orderBy = append(orderBy, dblist.JobListOrderBy{Expr: timeField, Order: sortOrder})

	default:
		timeField = string(p.sortField)
		orderBy = append(orderBy, dblist.JobListOrderBy{Expr: timeField, Order: sortOrder})
	}

	orderBy = append(orderBy, dblist.JobListOrderBy{Expr: "id", Order: sortOrder})

	if p.metadataFragment != "" {
		conditions = append(conditions, `metadata @> @metadata_fragment::jsonb`)
		namedArgs["metadata_fragment"] = p.metadataFragment
	}

	if p.after != nil {
		if p.after.time.IsZero() { // order by ID only
			if sortOrder == dblist.SortOrderAsc {
				conditions = append(conditions, "(id > @after_id)")
			} else {
				conditions = append(conditions, "(id < @after_id)")
			}
		} else {
			if sortOrder == dblist.SortOrderAsc {
				conditions = append(conditions, fmt.Sprintf(`("%s" > @cursor_time OR ("%s" = @cursor_time AND "id" > @after_id))`, timeField, timeField))
			} else {
				conditions = append(conditions, fmt.Sprintf(`("%s" < @cursor_time OR ("%s" = @cursor_time AND "id" < @after_id))`, timeField, timeField))
			}
			namedArgs["cursor_time"] = p.after.time
		}
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
	paramsCopy := p.copy()
	if cursor.job == nil {
		paramsCopy.after = cursor
	} else {
		paramsCopy.after = jobListCursorFromJobAndParams(cursor.job, paramsCopy)
	}
	return paramsCopy
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
	paramsCopy := p.copy()
	paramsCopy.paginationCount = int32(count)
	return paramsCopy
}

// Kinds returns an updated filter set that will only return jobs of the given
// kinds.
func (p *JobListParams) Kinds(kinds ...string) *JobListParams {
	paramsCopy := p.copy()
	paramsCopy.kinds = make([]string, len(kinds))
	copy(paramsCopy.kinds, kinds)
	return paramsCopy
}

func (p *JobListParams) Metadata(json string) *JobListParams {
	paramsCopy := p.copy()
	paramsCopy.metadataFragment = json
	return paramsCopy
}

// Queues returns an updated filter set that will only return jobs from the
// given queues.
func (p *JobListParams) Queues(queues ...string) *JobListParams {
	paramsCopy := p.copy()
	paramsCopy.queues = make([]string, len(queues))
	copy(paramsCopy.queues, queues)
	return paramsCopy
}

// OrderBy returns an updated filter set that will sort the results using the
// specified field and direction.
//
// If ordering by FinalizedAt, the States filter will be set to only include
// finalized job states unless it has already been overridden.
func (p *JobListParams) OrderBy(field JobListOrderByField, direction SortOrder) *JobListParams {
	paramsCopy := p.copy()
	switch field {
	case JobListOrderByID, JobListOrderByTime, JobListOrderByScheduledAt:
		paramsCopy.sortField = field
	case JobListOrderByFinalizedAt:
		paramsCopy.sortField = field
		if !p.overrodeState {
			paramsCopy.states = []rivertype.JobState{
				rivertype.JobStateCancelled,
				rivertype.JobStateCompleted,
				rivertype.JobStateDiscarded,
			}
		}
	default:
		panic("invalid order by field")
	}
	paramsCopy.sortField = field
	paramsCopy.sortOrder = direction
	return paramsCopy
}

// States returns an updated filter set that will only return jobs in the given
// states.
func (p *JobListParams) States(states ...rivertype.JobState) *JobListParams {
	paramsCopy := p.copy()
	paramsCopy.states = make([]rivertype.JobState, len(states))
	paramsCopy.overrodeState = true
	copy(paramsCopy.states, states)
	return paramsCopy
}

func jobListTimeFieldForState(state rivertype.JobState) string {
	// Don't include a `default` so `exhaustive` lint can detect omissions.
	switch state {
	case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateScheduled:
		return "scheduled_at"
	case rivertype.JobStateRunning:
		return "attempted_at"
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		return "finalized_at"
	}

	return "created_at" // should never happen
}

func jobListTimeValue(job *rivertype.JobRow) time.Time {
	// Don't include a `default` so `exhaustive` lint can detect omissions.
	switch job.State {
	case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateScheduled:
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
	}

	return job.CreatedAt // should never happen
}
