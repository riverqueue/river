// Package rivertype stores some of the lowest level River primitives so they
// can be shared amongst a number of packages including the top-level river
// package, database drivers, and internal utilities.
package rivertype

import (
	"time"
)

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

	// Metadata is a field for storing arbitrary metadata on a job. It should
	// always be a valid JSON object payload, and users should not overwrite or
	// remove anything stored in this field by River.
	Metadata []byte

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
}

// JobState is the state of a job. Jobs start as `available` or `scheduled`, and
// if all goes well eventually transition to `completed` as they're worked.
type JobState string

const (
	JobStateAvailable JobState = "available"
	JobStateCancelled JobState = "cancelled"
	JobStateCompleted JobState = "completed"
	JobStateDiscarded JobState = "discarded"
	JobStateRetryable JobState = "retryable"
	JobStateRunning   JobState = "running"
	JobStateScheduled JobState = "scheduled"
)

// AttemptError is an error from a single job attempt that failed due to an
// error or a panic.
type AttemptError struct {
	// At is the time at which the error occurred.
	At time.Time `json:"at"`

	// Attempt is the attempt number on which the error occurred (maps to
	// Attempt on a job row).
	Attempt int `json:"attempt"`

	// Error contains the stringified error of an error returned from a job or a
	// panic value in case of a panic.
	Error string `json:"error"`

	// Trace contains a stack trace from a job that panicked. The trace is
	// produced by invoking `debug.Trace()`.
	Trace string `json:"trace"`
}
