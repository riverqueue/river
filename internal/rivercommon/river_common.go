package rivercommon

import (
	"errors"
	"regexp"
	"time"
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

// HotOperationTimeout attempts to standardize timeouts for some "hot"
// operations like locking available jobs or completing finished jobs. It's
// somewhat questionable whether it makes sense to share timing on these
// queries, but for the time being it makes more sense than each part of the
// code randomly choosing its own timing.
//
// We probably want to have another look at this in the not-too-distant future
// to make sure we can't do anything a bit smarter when it comes to timeouts.
const HotOperationTimeout = 10 * time.Second

const (
	// MetadataKeyPeriodicJobID is a metadata key inserted with a periodic job
	// when a configured periodic job has its ID property set. This lets
	// inserted jobs easily be traced back to the periodic job that created
	// them.
	MetadataKeyPeriodicJobID = "river:periodic_job_id"

	// MetadataKeyResumableStep records the last successfully completed step for
	// a resumable job so later attempts can skip ahead.
	MetadataKeyResumableStep = "river:resumable_step"

	// MetadataKeyResumableCursor records a resumable step cursor so a later
	// attempt can resume a partially completed step.
	MetadataKeyResumableCursor = "river:resumable_cursor"

	// MetadataKeyRescueCount records how many times the job has been rescued.
	MetadataKeyRescueCount = "river:rescue_count"

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
