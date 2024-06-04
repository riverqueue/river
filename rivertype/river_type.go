// Package rivertype stores some of the lowest level River primitives so they
// can be shared amongst a number of packages including the top-level river
// package, database drivers, and internal utilities.
package rivertype

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a query by ID does not match any existing
// rows. For example, attempting to cancel a job that doesn't exist will
// return this error.
var ErrNotFound = errors.New("not found")

// JobInsertResult is the result of a job insert, containing the inserted job
// along with some other useful metadata.
type JobInsertResult struct {
	// Job is a struct containing the database persisted properties of the
	// inserted job.
	Job *JobRow

	// UniqueSkippedAsDuplicate is true if for a unique job, the insertion was
	// skipped due to an equivalent job matching unique property already being
	// present.
	UniqueSkippedAsDuplicate bool
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

	// AttemptedBy is the set of client IDs that have worked this job.
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

// JobState is the state of a job. Jobs start their lifecycle as either
// JobStateAvailable or JobStateScheduled, and if all goes well, transition to
// JobStateCompleted after they're worked.
type JobState string

const (
	// JobStateAvailable is the state for jobs that are immediately eligible to
	// be worked.
	JobStateAvailable JobState = "available"

	// JobStateCancelled is the state for jobs that have been manually cancelled
	// by user request.
	//
	// Cancelled jobs are reaped by the job cleaner service after a configured
	// amount of time (default 24 hours).
	JobStateCancelled JobState = "cancelled"

	// JobStateCompleted is the state for jobs that have successfully run to
	// completion.
	//
	// Completed jobs are reaped by the job cleaner service after a configured
	// amount of time (default 24 hours).
	JobStateCompleted JobState = "completed"

	// JobStateDiscarded is the state for jobs that have errored enough times
	// that they're no longer eligible to be retried. Manual user invention
	// is required for them to be tried again.
	//
	// Discarded jobs are reaped by the job cleaner service after a configured
	// amount of time (default 7 days).
	JobStateDiscarded JobState = "discarded"

	// JobStatePending is a state for jobs to be parked while waiting for some
	// external action before they can be worked. Jobs in pending will never be
	// worked or deleted unless moved out of this state by the user.
	JobStatePending JobState = "pending"

	// JobStateRetryable is the state for jobs that have errored, but will be
	// retried.
	//
	// The job scheduler service changes them to JobStateAvailable when they're
	// ready to be worked (their `scheduled_at` timestamp comes due).
	//
	// Jobs that will be retried very soon in the future may be changed to
	// JobStateAvailable immediately instead of JobStateRetryable so that they
	// don't have to wait for the job scheduler to run.
	JobStateRetryable JobState = "retryable"

	// JobStateRunning are jobs which are actively running.
	//
	// If River can't update state of a running job (in the case of a program
	// crash, underlying hardware failure, or job that doesn't return from its
	// Work function), that job will be left as JobStateRunning, and will
	// require a pass by the job rescuer service to be set back to
	// JobStateAvailable and be eligible for another run attempt.
	JobStateRunning JobState = "running"

	// JobStateScheduled is the state for jobs that are scheduled for the
	// future.
	//
	// The job scheduler service changes them to JobStateAvailable when they're
	// ready to be worked (their `scheduled_at` timestamp comes due).
	JobStateScheduled JobState = "scheduled"
)

// JobStates returns all possible job states.
func JobStates() []JobState {
	return []JobState{
		JobStateAvailable,
		JobStateCancelled,
		JobStateCompleted,
		JobStateDiscarded,
		JobStatePending,
		JobStateRetryable,
		JobStateRunning,
		JobStateScheduled,
	}
}

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

type MaintenanceService interface {
	Run(ctx context.Context)
}

// PeriodicJobHandle is a reference to a dynamically added periodic job
// (returned by the use of `Client.PeriodicJobs().Add()`) which can be used to
// subsequently remove the periodic job with `Remove()`.
type PeriodicJobHandle int

// Queue is a configuration for a queue that is currently (or recently was) in
// use by a client.
type Queue struct {
	// CreatedAt is the time at which the queue first began being worked by a
	// client. Unused queues are deleted after a retention period, so this only
	// reflects the most recent time the queue was created if there was a long
	// gap.
	CreatedAt time.Time
	// Metadata is a field for storing arbitrary metadata on a queue. It is
	// currently reserved for River's internal use and should not be modified by
	// users.
	Metadata []byte
	// Name is the name of the queue.
	Name string
	// PausedAt is the time the queue was paused, if any. When a paused queue is
	// resumed, this field is set to nil.
	PausedAt *time.Time
	// UpdatedAt is the last time the queue was updated. This field is updated
	// periodically any time an active Client is configured to work the queue,
	// even if the queue is paused.
	//
	// If UpdatedAt has not been updated for awhile, the queue record will be
	// deleted from the table by a maintenance process.
	UpdatedAt time.Time
}
