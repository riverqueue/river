package river

import (
	"time"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
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
	//
	// RunOnStart also applies when a new periodic job is added dynamically with
	// `PeriodicJobs().Add` or `PeriodicJobs().AddMany`. Jobs added this way
	// with RunOnStart set to true are inserted once, then continue with their
	// normal run schedule.
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

// PeriodicJobBundle is a bundle of currently configured periodic jobs. It's
// made accessible through Client, where new periodic jobs can be configured,
// and only ones removed.
type PeriodicJobBundle struct {
	clientConfig        *Config
	periodicJobEnqueuer *maintenance.PeriodicJobEnqueuer
}

func newPeriodicJobBundle(config *Config, periodicJobEnqueuer *maintenance.PeriodicJobEnqueuer) *PeriodicJobBundle {
	return &PeriodicJobBundle{
		clientConfig:        config,
		periodicJobEnqueuer: periodicJobEnqueuer,
	}
}

// Adds a new periodic job to the client. The job is queued immediately if
// RunOnStart is enabled, and then scheduled normally.
//
// Returns a periodic job handle which can be used to subsequently remove the
// job if desired.
//
// Adding or removing periodic jobs has no effect unless this client is elected
// leader because only the leader enqueues periodic jobs. To make sure that a
// new periodic job is fully enabled or disabled, it should be added or removed
// from _every_ active River client across all processes.
func (b *PeriodicJobBundle) Add(periodicJob *PeriodicJob) rivertype.PeriodicJobHandle {
	return b.periodicJobEnqueuer.Add(b.toInternal(periodicJob))
}

// AddMany adds many new periodic jobs to the client. The jobs are queued
// immediately if their RunOnStart is enabled, and then scheduled normally.
//
// Returns a periodic job handle which can be used to subsequently remove the
// job if desired.
//
// Adding or removing periodic jobs has no effect unless this client is elected
// leader because only the leader enqueues periodic jobs. To make sure that a
// new periodic job is fully enabled or disabled, it should be added or removed
// from _every_ active River client across all processes.
func (b *PeriodicJobBundle) AddMany(periodicJobs []*PeriodicJob) []rivertype.PeriodicJobHandle {
	return b.periodicJobEnqueuer.AddMany(sliceutil.Map(periodicJobs, b.toInternal))
}

// Clear clears all periodic jobs, cancelling all scheduled runs.
//
// Adding or removing periodic jobs has no effect unless this client is elected
// leader because only the leader enqueues periodic jobs. To make sure that a
// new periodic job is fully enabled or disabled, it should be added or removed
// from _every_ active River client across all processes.
func (b *PeriodicJobBundle) Clear() {
	b.periodicJobEnqueuer.Clear()
}

// Remove removes a periodic job, cancelling all scheduled runs.
//
// Requires the use of the periodic job handle that was returned when the job
// was added.
//
// Adding or removing periodic jobs has no effect unless this client is elected
// leader because only the leader enqueues periodic jobs. To make sure that a
// new periodic job is fully enabled or disabled, it should be added or removed
// from _every_ active River client across all processes.
func (b *PeriodicJobBundle) Remove(periodicJobHandle rivertype.PeriodicJobHandle) {
	b.periodicJobEnqueuer.Remove(periodicJobHandle)
}

// RemoveMany removes many periodic jobs, cancelling all scheduled runs.
//
// Requires the use of the periodic job handles that were returned when the jobs
// were added.
//
// Adding or removing periodic jobs has no effect unless this client is elected
// leader because only the leader enqueues periodic jobs. To make sure that a
// new periodic job is fully enabled or disabled, it should be added or removed
// from _every_ active River client across all processes.
func (b *PeriodicJobBundle) RemoveMany(periodicJobHandles []rivertype.PeriodicJobHandle) {
	b.periodicJobEnqueuer.RemoveMany(periodicJobHandles)
}

// An empty set of periodic job opts used as a default when none are specified.
var periodicJobEmptyOpts PeriodicJobOpts //nolint:gochecknoglobals

// There are two separate periodic job structs so that the top-level River
// package can expose one while still containing most periodic job logic in a
// subpackage. This function converts a top-level periodic job struct (used for
// configuration) to an internal one.
func (b *PeriodicJobBundle) toInternal(periodicJob *PeriodicJob) *maintenance.PeriodicJob {
	opts := &periodicJobEmptyOpts
	if periodicJob.opts != nil {
		opts = periodicJob.opts
	}
	return &maintenance.PeriodicJob{
		ConstructorFunc: func() (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error) {
			args, options := periodicJob.constructorFunc()
			return insertParamsFromConfigArgsAndOptions(&b.periodicJobEnqueuer.Archetype, b.clientConfig, args, options)
		},
		RunOnStart:   opts.RunOnStart,
		ScheduleFunc: periodicJob.scheduleFunc.Next,
	}
}
