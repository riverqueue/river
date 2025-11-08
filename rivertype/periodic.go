package rivertype

import "time"

// PeriodicSchedule is a schedule for a periodic job. Periodic jobs should
// generally have an interval of at least 1 minute, and never less than one
// second.
type PeriodicSchedule interface {
	// Next returns the next time at which the job should be run given the
	// current time.
	Next(current time.Time) time.Time
}

// PeriodicJobConstructor is a function that gets called each time the paired
// PeriodicSchedule is triggered.
//
// A constructor must never block. It may return nil to indicate that no job
// should be inserted.
type PeriodicJobConstructor func() (JobArgs, *InsertOpts)
