package rivercommon

import (
	"errors"
	"regexp"
)

// These constants are made available in rivercommon so that they're accessible
// by internal packages, but the top-level river package re-exports them, and
// all user code must use that set instead.
const (
	// AllQueuesString is a special string that can be used to indicate all
	// queues in some operations, particularly pause and resume.
	AllQueuesString    = "*"
	MaxAttemptsDefault = 25
	PriorityDefault    = 1
	QueueDefault       = "default"
)

const (
	// MetadataKeyPeriodicJobID is a metadata key inserted with a periodic job
	// when a configured periodic job has its ID property set. This lets
	// inserted jobs easily be traced back to the periodic job that created
	// them.
	MetadataKeyPeriodicJobID = "river:periodic_job_id"

	// MetadataKeyUniqueNonce is a special metadata key used by the SQLite driver to
	// determine whether an upsert is was skipped or not because the `(xmax != 0)`
	// trick we use in Postgres doesn't work in SQLite.
	MetadataKeyUniqueNonce = "river:unique_nonce"
)

type ContextKeyClient struct{}

// ErrStop is a special error injected by the client into its fetch and work
// CancelCauseFuncs when it's stopping. It may be used by components for such
// cases like avoiding logging an error during a normal shutdown procedure.
var ErrStop = errors.New("stop initiated")

// UserSpecifiedIDOrKindRE is a regular expression to which the format of job
// kinds and some other user-specified IDs (e.g. periodic job names) must
// comply. Mainly, minimal special characters, and excluding spaces and commas
// which are problematic for the search UI.
var UserSpecifiedIDOrKindRE = regexp.MustCompile(`\A[\w][\w\-\[\]<>\/.·:+]+\z`)
