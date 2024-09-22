package river

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// Regular expression to which the format of tags must comply. Mainly, no
// special characters, and with hyphens in the middle.
//
// A key property here (in case this is relaxed in the future) is that commas
// must never be allowed because they're used as a delimiter during batch job
// insertion for the `riverdatabasesql` driver.
var tagRE = regexp.MustCompile(`\A[\w][\w\-]+[\w]\z`)

// InsertOpts are optional settings for a new job which can be provided at job
// insertion time. These will override any default InsertOpts settings provided
// by JobArgsWithInsertOpts, as well as any global defaults.
type InsertOpts struct {
	// MaxAttempts is the maximum number of total attempts (including both the
	// original run and all retries) before a job is abandoned and set as
	// discarded.
	MaxAttempts int

	// Metadata is a JSON object blob of arbitrary data that will be stored with
	// the job. Users should not overwrite or remove anything stored in this
	// field by River.
	Metadata []byte

	// Pending indicates that the job should be inserted in the `pending` state.
	// Pending jobs are not immediately available to be worked and are never
	// deleted, but they can be used to indicate work which should be performed in
	// the future once they are made available (or scheduled) by some external
	// update.
	Pending bool

	// Priority is the priority of the job, with 1 being the highest priority and
	// 4 being the lowest. When fetching available jobs to work, the highest
	// priority jobs will always be fetched before any lower priority jobs are
	// fetched. Note that if your workers are swamped with more high-priority jobs
	// then they can handle, lower priority jobs may not be fetched.
	//
	// Defaults to PriorityDefault.
	Priority int

	// Queue is the name of the job queue in which to insert the job.
	//
	// Defaults to QueueDefault.
	Queue string

	// ScheduledAt is a time in future at which to schedule the job (i.e. in
	// cases where it shouldn't be run immediately). The job is guaranteed not
	// to run before this time, but may run slightly after depending on the
	// number of other scheduled jobs and how busy the queue is.
	//
	// Use of this option generally only makes sense when passing options into
	// Insert rather than when a job args struct is implementing
	// JobArgsWithInsertOpts, however, it will work in both cases.
	ScheduledAt time.Time

	// Tags are an arbitrary list of keywords to add to the job. They have no
	// functional behavior and are meant entirely as a user-specified construct
	// to help group and categorize jobs.
	//
	// Tags should conform to the regex `\A[\w][\w\-]+[\w]\z` and be a maximum
	// of 255 characters long. No special characters are allowed.
	//
	// If tags are specified from both a job args override and from options on
	// Insert, the latter takes precedence. Tags are not merged.
	Tags []string

	// UniqueOpts returns options relating to job uniqueness. An empty struct
	// avoids setting any worker-level unique options.
	UniqueOpts UniqueOpts
}

// UniqueOpts contains parameters for uniqueness for a job.
//
// When the options struct is uninitialized (its zero value) no uniqueness at is
// enforced. As each property is initialized, it's added as a dimension on the
// uniqueness matrix, and with any property on, the job's kind always counts
// toward uniqueness.
//
// So for example, if only ByQueue is on, then for the given job kind, only a
// single instance is allowed in any given queue, regardless of other properties
// on the job. If both ByArgs and ByQueue are on, then for the given job kind, a
// single instance is allowed for each combination of args and queues. If either
// args or queue is changed on a new job, it's allowed to be inserted as a new
// job.
//
// Uniqueness relies on a hash of the job kind and any unique properties along
// with a database unique constraint. See the note on ByState for more details
// including about the fallback to a deprecated advisory lock method.
type UniqueOpts struct {
	// ByArgs indicates that uniqueness should be enforced for any specific
	// instance of encoded args for a job.
	//
	// Default is false, meaning that as long as any other unique property is
	// enabled, uniqueness will be enforced for a kind regardless of input args.
	//
	// When set to true, the entire encoded args field will be included in the
	// uniqueness hash, which requires care to ensure that no irrelevant args are
	// factored into the uniqueness check. It is also possible to use a subset of
	// the args by indicating on the `JobArgs` struct which fields should be
	// included in the uniqueness check using struct tags:
	//
	// 	type MyJobArgs struct {
	// 		CustomerID string `json:"customer_id" river:"unique"`
	// 		TraceID string `json:"trace_id"
	// 	}
	//
	// In this example, only the encoded `customer_id` key will be included in the
	// uniqueness check and the `trace_id` key will be ignored.
	//
	// All keys are sorted alphabetically before hashing to ensure consistent
	// results.
	ByArgs bool

	// ByPeriod defines uniqueness within a given period. On an insert time is
	// rounded down to the nearest multiple of the given period, and a job is
	// only inserted if there isn't an existing job that will run between then
	// and the next multiple of the period.
	//
	// Default is no unique period, meaning that as long as any other unique
	// property is enabled, uniqueness will be enforced across all jobs of the
	// kind in the database, regardless of when they were scheduled.
	ByPeriod time.Duration

	// ByQueue indicates that uniqueness should be enforced within each queue.
	//
	// Default is false, meaning that as long as any other unique property is
	// enabled, uniqueness will be enforced for a kind across all queues.
	ByQueue bool

	// ByState indicates that uniqueness should be enforced across any of the
	// states in the given set. Unlike other unique options, ByState gets a
	// default when it's not set for user convenience. The default is equivalent
	// to:
	//
	// 	ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateRetryable, rivertype.JobStateScheduled}
	//
	// With this setting, any jobs of the same kind that have been completed or
	// discarded, but not yet cleaned out by the system, will still prevent a
	// duplicate unique job from being inserted. For example, with the default
	// states, if a unique job is actively `running`, a duplicate cannot be
	// inserted. Likewise, if a unique job has `completed`, you still can't
	// insert a duplicate, at least not until the job cleaner maintenance process
	// eventually removes the completed job from the `river_job` table.
	//
	// The list may be safely customized to _add_ additional states (`cancelled`
	// or `discarded`), though only `retryable` may be safely _removed_ from the
	// list.
	//
	// Warning: Removing any states from the default list (other than `retryable`
	// forces a fallback to a slower insertion path that takes an advisory lock
	// and performs a look up before insertion. This path is deprecated and should
	// be avoided if possible.
	ByState []rivertype.JobState

	// ExcludeKind indicates that the job kind should not be included in the
	// uniqueness check. This is useful when you want to enforce uniqueness
	// across all jobs regardless of kind.
	ExcludeKind bool
}

// isEmpty returns true for an empty, uninitialized options struct.
//
// This is required because we can't check against `JobUniqueOpts{}` because
// slices aren't comparable. Unfortunately it makes things a little more brittle
// comparatively because any new options must also be considered here for things
// to work.
func (o *UniqueOpts) isEmpty() bool {
	return !o.ByArgs &&
		o.ByPeriod == time.Duration(0) &&
		!o.ByQueue &&
		o.ByState == nil
}

func (o *UniqueOpts) isV1() bool {
	requiredV3states := []rivertype.JobState{
		rivertype.JobStatePending,
		rivertype.JobStateScheduled,
		rivertype.JobStateAvailable,
		rivertype.JobStateRunning,
	}
	if len(o.ByState) == 0 {
		return false
	}

	for _, state := range requiredV3states {
		if !slices.Contains(o.ByState, state) {
			return true
		}
	}
	return false
}

var jobStateAll = rivertype.JobStates() //nolint:gochecknoglobals

func (o *UniqueOpts) validate() error {
	if o.isEmpty() {
		return nil
	}

	if o.ByPeriod != time.Duration(0) && o.ByPeriod < 1*time.Second {
		return errors.New("JobUniqueOpts.ByPeriod should not be less than 1 second")
	}

	// Job states are typed, but since the underlying type is a string, users
	// can put anything they want in there.
	for _, state := range o.ByState {
		// This could be turned to a map lookup, but last I checked the speed
		// difference for tiny slice sizes is negligible, and map lookup might
		// even be slower.
		if !slices.Contains(jobStateAll, state) {
			return fmt.Errorf("JobUniqueOpts.ByState contains invalid state %q", state)
		}
	}

	return nil
}
