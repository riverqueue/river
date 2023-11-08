package river

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
)

// Job represents a single unit of work, holding both the arguments and
// information for a job with args of type T.
type Job[T JobArgs] struct {
	*JobRow

	// Args are the arguments for the job.
	Args T
}

// JobArgs is an interface that represents the arguments for a job of type T.
// These arguments are serialized into JSON and stored in the database.
type JobArgs interface {
	// Kind is a string that uniquely identifies the type of job. This must be
	// provided on your job arguments struct.
	Kind() string
}

// JobArgsWithInsertOpts is an extra interface that a job may implement on top
// of JobArgs to provide insertion-time options for all jobs of this type.
type JobArgsWithInsertOpts interface {
	// InsertOpts returns options for all jobs of this job type, overriding any
	// system defaults. These can also be overridden at insertion time.
	InsertOpts() InsertOpts
}

// JobRow contains the properties of a job that are persisted to the database.
// Use of `Job[T]` will generally be preferred in user-facing code like worker
// interfaces.
type JobRow struct {
	// ID of the job. Generated as part of a Postgres sequence and generally
	// ascending in nature, but there may be gaps in it as transactions roll
	// back.
	ID int64

	// Attempt is the attempt number of the job. Jobs are inserted at 0, the
	// number is incremented to 1 the first time work its worked, and may
	// increment further if it's either snoozed or errors.
	Attempt int

	// AttemptedAt is the time that the job was last worked. Starts out as `nil`
	// on a new insert.
	AttemptedAt *time.Time

	// AttemptedBy is the set of worker IDs that have worked this job. A worker
	// ID differs between different programs, but is shared by all executors
	// within any given one. (i.e. Different Go processes have different IDs,
	// but IDs are shared within any given process.) A process generates a new
	// ULID (an ordered UUID) worker ID when it starts up.
	AttemptedBy []string

	// CreatedAt is when the job record was created.
	CreatedAt time.Time

	// EncodedArgs is the job's JobArgs encoded as JSON.
	EncodedArgs []byte

	// Errors is a set of errors that occurred when the job was worked, one for
	// each attempt. Ordered from earliest error to the latest error.
	Errors []AttemptError

	// FinalizedAt is the time at which the job was "finalized", meaning it was
	// either completed successfully or errored for the last time such that
	// it'll no longer be retried.
	FinalizedAt *time.Time

	// Kind uniquely identifies the type of job and instructs which worker
	// should work it. It is set at insertion time via `Kind()` on the
	// `JobArgs`.
	Kind string

	// MaxAttempts is the maximum number of attempts that the job will be tried
	// before it errors for the last time and will no longer be worked.
	//
	// Extracted (in order of precedence) from job-specific InsertOpts
	// on Insert, from the worker level InsertOpts from JobArgsWithInsertOpts,
	// or from a client's default value.
	MaxAttempts int

	// Priority is the priority of the job, with 1 being the highest priority and
	// 4 being the lowest. When fetching available jobs to work, the highest
	// priority jobs will always be fetched before any lower priority jobs are
	// fetched. Note that if your workers are swamped with more high-priority jobs
	// then they can handle, lower priority jobs may not be fetched.
	Priority int

	// Queue is the name of the queue where the job will be worked. Queues can
	// be configured independently and be used to isolate jobs.
	//
	// Extracted from either specific InsertOpts on Insert, or InsertOpts from
	// JobArgsWithInsertOpts, or a client's default value.
	Queue string

	// ScheduledAt is when the job is scheduled to become available to be
	// worked. Jobs default to running immediately, but may be scheduled
	// for the future when they're inserted. They may also be scheduled for
	// later because they were snoozed or because they errored and have
	// additional retry attempts remaining.
	ScheduledAt time.Time

	// State is the state of job like `available` or `completed`. Jobs are
	// `available` when they're first inserted.
	State JobState

	// Tags are an arbitrary list of keywords to add to the job. They have no
	// functional behavior and are meant entirely as a user-specified construct
	// to help group and categorize jobs.
	Tags []string

	// metadata is a field that'll eventually be used to store arbitrary data on
	// a job for flexible use and use with plugins. It's currently unexported
	// until we get a chance to more fully flesh out this feature.
	metadata []byte
}

// WARNING!!!!!
//
// !!! When updating this function, the equivalent in `./rivertest/rivertest.go`
// must also be updated!!!
//
// This is obviously not ideal, but since JobRow is at the top-level package,
// there's no way to put a helper in a shared package that can produce one,
// which is why we have this copy/pasta. There are some potential alternatives
// to this, but none of them are great.
func jobRowFromInternal(internal *dbsqlc.RiverJob) *JobRow {
	tags := internal.Tags
	if tags == nil {
		tags = []string{}
	}
	return &JobRow{
		ID:          internal.ID,
		Attempt:     max(int(internal.Attempt), 0),
		AttemptedAt: internal.AttemptedAt,
		AttemptedBy: internal.AttemptedBy,
		CreatedAt:   internal.CreatedAt,
		EncodedArgs: internal.Args,
		Errors:      sliceutil.Map(internal.Errors, func(e dbsqlc.AttemptError) AttemptError { return attemptErrorFromInternal(&e) }),
		FinalizedAt: internal.FinalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: max(int(internal.MaxAttempts), 0),
		Priority:    max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(), // TODO(brandur): Very weird this is the only place a UTC conversion happens.
		State:       JobState(internal.State),
		Tags:        tags,

		metadata: internal.Metadata,
	}
}

// JobCompleteTx marks the job as completed as part of transaction tx. If tx is
// rolled back, the completion will be as well.
//
// The function needs to know the type of the River database driver, which is
// the same as the one in use by Client, but the other generic parameters can be
// inferred. An invocation should generally look like:
//
//	_, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
//	if err != nil {
//		// handle error
//	}
//
// Returns the updated, completed job.
func JobCompleteTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs]) (*Job[TArgs], error) {
	if job.State != JobStateRunning {
		return nil, errors.New("job must be running")
	}

	var (
		driver  TDriver
		queries = &dbsqlc.Queries{}
	)

	internal, err := queries.JobSetCompleted(ctx, driver.UnwrapTx(tx), dbsqlc.JobSetCompletedParams{ID: job.ID, FinalizedAt: time.Now()})
	if err != nil {
		return nil, err
	}

	updatedJob := &Job[TArgs]{JobRow: jobRowFromInternal(internal)}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}

type JobState string

const (
	JobStateAvailable JobState = JobState(dbsqlc.JobStateAvailable)
	JobStateCancelled JobState = JobState(dbsqlc.JobStateCancelled)
	JobStateCompleted JobState = JobState(dbsqlc.JobStateCompleted)
	JobStateDiscarded JobState = JobState(dbsqlc.JobStateDiscarded)
	JobStateRetryable JobState = JobState(dbsqlc.JobStateRetryable)
	JobStateRunning   JobState = JobState(dbsqlc.JobStateRunning)
	JobStateScheduled JobState = JobState(dbsqlc.JobStateScheduled)
)

var jobStateAll = []JobState{ //nolint:gochecknoglobals
	JobStateAvailable,
	JobStateCancelled,
	JobStateCompleted,
	JobStateDiscarded,
	JobStateRetryable,
	JobStateRunning,
	JobStateScheduled,
}

type AttemptError struct {
	At    time.Time `json:"at"`
	Error string    `json:"error"`
	Num   int       `json:"num"`
	Trace string    `json:"trace"`
}

func attemptErrorFromInternal(e *dbsqlc.AttemptError) AttemptError {
	return AttemptError{
		At:    e.At,
		Error: e.Error,
		Num:   int(e.Num),
		Trace: e.Trace,
	}
}
