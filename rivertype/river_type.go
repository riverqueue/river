// Package rivertype stores some of the lowest level River primitives so they
// can be shared amongst a number of packages including the top-level river
// package, database drivers, and internal utilities.
package rivertype

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// MetadataKeyOutput is the metadata key used to store recorded job output.
const MetadataKeyOutput = "output"

// ErrNotFound is returned when a query by ID does not match any existing
// rows. For example, attempting to cancel a job that doesn't exist will
// return this error.
var ErrNotFound = errors.New("not found")

// ErrJobRunning is returned when a job is attempted to be deleted while it's
// running.
var ErrJobRunning = errors.New("running jobs cannot be deleted")

// JobArgs is an interface that should be implemented by the arguments to a job.
// This definition duplicates the JobArgs interface in the river package so that
// it can be used in other packages without creating a circular dependency.
type JobArgs interface {
	// Kind returns a unique string that identifies the type of job. It's used to
	// determine which worker should work the job.
	Kind() string
}

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
	// increment further if it errors. Attempt will decrement on snooze so that
	// repeated snoozes don't increment this value.
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

	// UniqueKey is a unique key for the job within its kind that's used for
	// unique job insertions. It's generated by hashing an inserted job's unique
	// opts configuration.
	UniqueKey []byte

	// UniqueStates is the set of states where uniqueness is enforced for this
	// job. Equivalent to the default set of unique states unless
	// UniqueOpts.ByState was assigned a custom value.
	UniqueStates []JobState
}

// Output returns the previously recorded output for the job, if any. The return
// value is a raw JSON payload from the output that was recorded by the job, or
// nil if no output was recorded.
func (j *JobRow) Output() []byte {
	type metadataWithOutput struct {
		Output json.RawMessage `json:"output"`
	}

	var metadata metadataWithOutput
	if err := json.Unmarshal(j.Metadata, &metadata); err != nil {
		return nil
	}

	return metadata.Output
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
	// that they're no longer eligible to be retried. Manual user intervention
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
	//
	// In the case of a non-panic or an error produced as a stuck job was
	// rescued, this value will be an empty string.
	Trace string `json:"trace"`
}

type JobInsertParams struct {
	ID           *int64
	Args         JobArgs
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Kind         string
	MaxAttempts  int
	Metadata     []byte
	Priority     int
	Queue        string
	ScheduledAt  *time.Time
	State        JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

// Hook is an arbitrary interface for a plugin "hook" which will execute some
// arbitrary code at a predefined step in the job lifecycle.
//
// This interface is left purposely non-specific. Hook structs should embed
// river.HookDefaults to inherit an IsHook implementation, then implement one
// of the more specific hook interfaces like HookInsertBegin or HookWorkBegin. A
// hook struct may also implement multiple specific hook interfaces which are
// logically related and benefit from being grouped together.
//
// Hooks differ from middleware in that they're invoked at a specific lifecycle
// phase, but finish immediately instead of wrapping an inner call like a
// middleware does. One of the main ramifications of this different is that a
// hook cannot modify context in any useful way to pass down into the stack.
// Like a normal function, any changes it makes to its context are discarded on
// return.
//
// All else equal, hooks should generally be preferred over middleware because
// they don't add anything to the call stack. Call stacks that get overly deep
// can become a bit of an operational nightmare because they get hard to read.
//
// In a language with more specific type capabilities, this interface would be a
// union type. In Go we implement it somewhat awkwardly so that we can get
// future extensibility, but also some typing guarantees to prevent misuse (i.e.
// if Hook was an empty interface, then any object could be passed as a hook,
// but having a single function to implement forces the caller to make some
// token motions in the direction of implementing hooks).
//
// List of hook interfaces that may be implemented:
// - HookInsertBegin
// - HookWorkBegin
// - HookWorkEnd
//
// More operation-specific interfaces may be added in future versions.
type Hook interface {
	// IsHook is a sentinel function to check that a type is implementing Hook
	// on purpose and not by accident (Hook would otherwise be an empty
	// interface). Hooks should embed river.HookDefaults to pick up an
	// implementation for this function automatically.
	IsHook() bool
}

// HookInsertBegin is an interface to a hook that runs before job insertion.
type HookInsertBegin interface {
	Hook

	// InsertBegin is invoked just before a job is inserted to the database.
	InsertBegin(ctx context.Context, params *JobInsertParams) error
}

// HookWorkBegin is an interface to a hook that runs after a job has been locked
// for work and before it's worked.
type HookWorkBegin interface {
	Hook

	// WorkBegin is invoked after a job has been locked and assigned to a
	// particular executor for work and just before the job is actually worked.
	//
	// Returning an error from any HookWorkBegin hook will abort the job early
	// such that it has an error set and doesn't work, with a retry scheduled
	// according to its retry policy.
	//
	// This function doesn't return a context so any context set in WorkBegin is
	// discarded after the function returns. If persistent context needs to be
	// set, middleware should be used instead.
	WorkBegin(ctx context.Context, job *JobRow) error
}

// HookWorkEnd is an interface to a hook that runs after a job has been worked.
type HookWorkEnd interface {
	Hook

	// WorkEnd is invoked after a job has been worked with the error result of
	// the worked job. It's invoked after any middleware has already run.
	//
	// WorkEnd may modify a returned work error or pass it through unchanged.
	// Each returned error is passed through to the next hook and the final
	// error result is returned from the job executor:
	//
	// 	err := e.WorkUnit.Work(ctx)
	// 	for _, hook := range hooks {
	// 		err = hook.(rivertype.HookWorkEnd).WorkEnd(ctx, e.JobRow, err)
	// 	}
	// 	return err
	//
	// If a hook does not want to modify an error result, it should make sure to
	// return whatever error value it received as its argument whether that
	// error is nil or not.
	//
	// The JobRow received by WorkEnd is the same one passed to HookWorkBegin's
	// WorkBegin. Its state, errors, next scheduled at time, etc. have not yet
	// been updated based on the latest work result.
	//
	// Will not receive a common context related to HookWorkBegin because
	// WorkBegin doesn't return a context. Middleware should be used for this
	// sort of shared context instead.
	WorkEnd(ctx context.Context, job *JobRow, err error) error
}

// Middleware is an arbitrary interface for a struct which will execute some
// arbitrary code at a predefined step in the job lifecycle.
//
// This interface is left purposely non-specific. Middleware structs should
// embed river.MiddlewareDefaults to inherit an IsMiddleware implementation,
// then implement a more specific hook interface like JobInsertMiddleware or
// WorkerMiddleware. A middleware struct may also implement multiple specific
// hook interfaces which are logically related and benefit from being grouped
// together.
//
// Hooks differ from middleware in that they're invoked at a specific lifecycle
// phase, but finish immediately instead of wrapping an inner call like a
// middleware does. One of the main ramifications of this different is that a
// hook cannot modify context in any useful way to pass down into the stack.
// Like a normal function, any changes it makes to its context are discarded on
// return.
//
// Middleware differs from hooks in that they wrap a specific lifecycle phase,
// staying on the callstack for the duration of the step while they call into a
// doInner function that executes the step and the rest of the middleware stack.
// The main ramification of this difference is that middleware can modify
// context for the step and any other middleware inner relative to it.
//
// All else equal, hooks should generally be preferred over middleware because
// they don't add anything to the call stack. Call stacks that get overly deep
// can become a bit of an operational nightmare because they get hard to read.
//
// In a language with more specific type capabilities, this interface would be a
// union type. In Go we implement it somewhat awkwardly so that we can get
// future extensibility, but also some typing guarantees to prevent misuse (i.e.
// if Hook was an empty interface, then any object could be passed as a hook,
// but having a single function to implement forces the caller to make some
// token motions in the direction of implementing hooks).
//
// List of middleware interfaces that may be implemented:
// - JobInsertMiddleware
// - WorkerMiddleware
//
// More operation-specific interfaces may be added in future versions.
type Middleware interface {
	// IsMiddleware is a sentinel function to check that a type is implementing
	// Middleware on purpose and not by accident (Middleware would otherwise be
	// an empty interface). Middleware should embed river.MiddlewareDefaults to
	// pick up an implementation for this function automatically.
	IsMiddleware() bool
}

// JobInsertMiddleware provides an interface for middleware that integrations
// can use to encapsulate common logic around job insertion.
//
// Implementations should embed river.JobMiddlewareDefaults to inherit default
// implementations for phases where no custom code is needed, and for forward
// compatibility in case new functions are added to this interface.
type JobInsertMiddleware interface {
	Middleware

	// InsertMany is invoked around a batch insert operation. Implementations
	// must always include a call to doInner to call down the middleware stack
	// and perform the batch insertion, and may run custom code before and after.
	//
	// Returning an error from this function will fail the overarching insert
	// operation, even if the inner insertion originally succeeded.
	InsertMany(ctx context.Context, manyParams []*JobInsertParams, doInner func(context.Context) ([]*JobInsertResult, error)) ([]*JobInsertResult, error)
}

// WorkerMiddleware provides an interface for middleware that integrations can
// use to encapsulate common logic when a job is worked.
type WorkerMiddleware interface {
	Middleware

	// Work is invoked after a job's JSON args being unmarshaled and before the
	// job is worked. Implementations must always include a call to doInner to
	// call down the middleware stack and perform the batch insertion, and may
	// run custom code before and after.
	//
	// Returning an error from this function will fail the overarching work
	// operation, even if the inner work originally succeeded.
	Work(ctx context.Context, job *JobRow, doInner func(context.Context) error) error
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

// UniqueOptsByStateDefault is the set of job states that are used to determine
// uniqueness unless unique job states have been overridden with
// UniqueOpts.ByState. So for example, with this default set a new unique job
// may be inserted even if another job already exists, as long as that other job
// is set `cancelled` or `discarded`.
func UniqueOptsByStateDefault() []JobState {
	return []JobState{
		JobStateAvailable,
		JobStateCompleted,
		JobStatePending,
		JobStateRetryable,
		JobStateRunning,
		JobStateScheduled,
	}
}

// WorkerMetadata is metadata about workers registered with a client.
type WorkerMetadata struct {
	// JobArgHooks are job args specific hooks returned from a JobArgsWithHooks
	// implementation.
	JobArgHooks []Hook

	// Kind is the kind returned from job args and recognized by worker to work.
	Kind string
}
