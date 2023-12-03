package river

import (
	"time"
)

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

// PeriodicJob is a configuration for a periodic job.
type PeriodicJob struct {
	constructorFunc PeriodicJobConstructor
	opts            *PeriodicJobOpts
	scheduleFunc    PeriodicSchedule
}

// PeriodicJobOpts are options for a periodic job.
type PeriodicJobOpts struct {
	// RunOnStart can be used to indicate that a periodic job should insert an
	// initial job as a new scheduler is started. This can be used as a hedge
	// for jobs with longer scheduled durations that may not get to expiry
	// before a new scheduler is elected.
	RunOnStart bool
}

// NewPeriodicJob returns a new PeriodicJob given a schedule and a constructor
// function.
//
// The schedule returns a time until the next time the periodic job should run.
// The helper PeriodicInterval is available for jobs that should run on simple,
// fixed intervals (e.g. every 15 minutes), and a custom schedule or third party
// cron package can be used for more complex scheduling (see the cron example).
// The constructor function is invoked each time a periodic job's schedule
// elapses, returning job arguments to insert along with optional insertion
// options.
//
// The periodic job scheduler is approximate and doesn't guarantee strong
// durability. It's started by the elected leader in a River cluster, and each
// periodic job is assigned an initial run time when that occurs. New run times
// are scheduled each time a job's target run time is reached and a new job
// inserted. However, each scheduler only retains in-memory state, so anytime a
// process quits or a new leader is elected, the whole process starts over
// without regard for the state of the last scheduler. The RunOnStart option
// can be used as a hedge to make sure that jobs with long run durations are
// guaranteed to occasionally run.
func NewPeriodicJob(scheduleFunc PeriodicSchedule, constructorFunc PeriodicJobConstructor, opts *PeriodicJobOpts) *PeriodicJob {
	return &PeriodicJob{
		constructorFunc: constructorFunc,
		opts:            opts,
		scheduleFunc:    scheduleFunc,
	}
}

type periodicIntervalSchedule struct {
	interval time.Duration
}

// PeriodicInterval returns a simple PeriodicSchedule that runs at the given
// interval.
func PeriodicInterval(interval time.Duration) PeriodicSchedule {
	return &periodicIntervalSchedule{interval}
}

func (s *periodicIntervalSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}
