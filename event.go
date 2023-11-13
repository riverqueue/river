package river

import (
	"time"

	"github.com/riverqueue/river/internal/jobstats"
)

// EventKind is a kind of event to subscribe to from a client.
type EventKind string

const (
	// EventKindJobCancelled occurs when a job is cancelled.
	EventKindJobCancelled EventKind = "job_cancelled"

	// EventKindJobCompleted occurs when a job is completed.
	EventKindJobCompleted EventKind = "job_completed"

	// EventKindJobFailed occurs when a job fails. Occurs both when a job fails
	// and will be retried and when a job fails for the last time and will be
	// discarded. Callers can use job fields like `Attempt` and `State` to
	// differentiate each type of occurrence.
	EventKindJobFailed EventKind = "job_failed"

	// EventKindJobSnoozed occurs when a job is snoozed.
	EventKindJobSnoozed EventKind = "job_snoozed"
)

// All known event kinds, used to validate incoming kinds. This is purposely not
// exported because end users should have no way of subscribing to all known
// kinds for forward compatibility reasons.
var allKinds = map[EventKind]struct{}{ //nolint:gochecknoglobals
	EventKindJobCancelled: {},
	EventKindJobCompleted: {},
	EventKindJobFailed:    {},
	EventKindJobSnoozed:   {},
}

// Event wraps an event that occurred within a River client, like a job being
// completed.
type Event struct {
	// Kind is the kind of event. Receivers should read this field and respond
	// accordingly. Subscriptions will only receive event kinds that they
	// requested when creating a subscription with Subscribe.
	Kind EventKind

	// Job contains job-related information.
	Job *JobRow

	// JobStats are statistics about the run of a job.
	JobStats *JobStatistics
}

// JobStatistics contains information about a single execution of a job.
type JobStatistics struct {
	CompleteDuration  time.Duration // Time it took to set the job completed, discarded, or errored.
	QueueWaitDuration time.Duration // Time the job spent waiting in available state before starting execution.
	RunDuration       time.Duration // Time job spent running (measured around job worker.)
}

func jobStatisticsFromInternal(stats *jobstats.JobStatistics) *JobStatistics {
	return &JobStatistics{
		CompleteDuration:  stats.CompleteDuration,
		QueueWaitDuration: stats.QueueWaitDuration,
		RunDuration:       stats.RunDuration,
	}
}

// The maximum size of the subscribe channel. Events that would overflow it will
// be dropped.
const subscribeChanSize = 100

// eventSubscription is an active subscription for events being produced by a
// client, created with Client.Subscribe.
type eventSubscription struct {
	Chan  chan *Event
	Kinds map[EventKind]struct{}
}

func (s *eventSubscription) ListensFor(kind EventKind) bool {
	_, ok := s.Kinds[kind]
	return ok
}
