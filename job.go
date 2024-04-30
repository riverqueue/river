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
