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
	// provided on your job arguments struct. Jobs are identified by a string
	// instead of being based on type names so that previously inserted jobs
	// can be worked across deploys even if job/worker types are renamed.
	//
	// Kinds should be formatted without spaces like `my_custom_job`,
	// `mycustomjob`, or `my-custom-job`. Many special characters like colons,
	// dots, hyphens, and underscores are allowed, but those like spaces and
	// commas, which would interfere with UI functionality, are invalid.
	//
	// After initially deploying a job, it's generally not safe to rename its
	// kind (unless the database is completely empty) because River won't know
	// which worker should work the old kind. Job kinds can be renamed safely
	// over multiple deploys using the JobArgsWithKindAliases interface.
	Kind() string
}

// JobArgsWithKindAliases  is an interface that jobs args can implement to
// provide an alternate kind which a worker will be registered under in addition
// to the primary kind. This is useful for renaming a job kind in a safe manner
// so that any jobs already in the database aren't orphaned.
//
// Renaming a job is a three part process. To begin, a job args with its
// original name:
//
//	type jobArgsBeingRenamed struct{}
//
//	func (a jobArgsBeingRenamed) Kind() string { return "old_name" }
//
// Rename by putting the new name in Kind and moving the old name to
// KindAliases:
//
//	type jobArgsBeingRenamed struct{}
//
//	func (a jobArgsBeingRenamed) Kind() string          { return "new_name" }
//	func (a jobArgsBeingRenamed) KindAliases() []string { return []string{"old_name"} }
//
// After all jobs inserted under the original name have finished working
// (including all their possible retries, which notably might take up to three
// weeks on the default retry policy), remove KindAliases:
//
//	type jobArgsBeingRenamed struct{}
//
//	func (a jobArgsBeingRenamed) Kind() string { return "new_name" }
type JobArgsWithKindAliases interface {
	// KindAliases returns alias kinds that an associated job args worker will
	// respond to.
	KindAliases() []string
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
