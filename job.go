package river

import (
	"github.com/riverqueue/river/rivertype"
)

// Job represents a single unit of work, holding both the arguments and
// information for a job with args of type T.
type Job[T JobArgs] struct {
	*rivertype.JobRow

	// Args are the arguments for the job.
	Args T
}

// JobArgs is an interface that represents the arguments for a job of type T.
// These arguments are serialized into JSON and stored in the database.
//
// The struct is serialized using `encoding/json`. All exported fields are
// serialized, unless skipped with a struct field tag.
type JobArgs interface {
	// Kind is a string that uniquely identifies the type of job. This must be
	// provided on your job arguments struct.
	Kind() string
}

// JobArgsWithHooks is an interface that job args can implement to attach
// specific hooks (i.e. other than those globally installed to a client) to
// certain kinds of jobs.
type JobArgsWithHooks interface {
	// Hooks returns specific hooks to run for this job type. These will run
	// after the global hooks configured on the client.
	//
	// Warning: Hooks returned should be based on the job type only and be
	// invariant of the specific contents of a job. Hooks are extracted by
	// instantiating a generic instance of the job even when a specific instance
	// is available, so any conditional logic within will be ignored. This is
	// done because although specific job information may be available in some
	// hook contexts like on InsertBegin, it won't be in others like WorkBegin.
	Hooks() []rivertype.Hook
}

// JobArgsWithInsertOpts is an extra interface that a job may implement on top
// of JobArgs to provide insertion-time options for all jobs of this type.
type JobArgsWithInsertOpts interface {
	// InsertOpts returns options for all jobs of this job type, overriding any
	// system defaults. These can also be overridden at insertion time.
	InsertOpts() InsertOpts
}
