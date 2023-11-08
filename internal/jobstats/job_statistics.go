package jobstats

import "time"

// JobStatistics contains information about a single execution of a job.
//
// This type has an identical one in the top-level package. The reason for that
// is so that we can use statistics from subpackages, but can reveal all
// public-facing River types in a single public-facing package.
type JobStatistics struct {
	CompleteDuration  time.Duration // Time it took to set the job completed, discarded, or errored.
	QueueWaitDuration time.Duration // Time the job spent waiting in available state before starting execution.
	RunDuration       time.Duration // Time job spent running (measured around job worker.)
}
