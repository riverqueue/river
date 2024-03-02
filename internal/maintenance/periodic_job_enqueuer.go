package maintenance

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
)

// ErrNoJobToInsert can be returned by a PeriodicJob's JobToInsertFunc to
// signal that there's no job to insert at this time.
var ErrNoJobToInsert = errors.New("a nil job was returned, nothing to insert")

// Test-only properties.
type PeriodicJobEnqueuerTestSignals struct {
	EnteredLoop  rivercommon.TestSignal[struct{}] // notifies when the enqueuer finishes start up and enters its initial run loop
	InsertedJobs rivercommon.TestSignal[struct{}] // notifies when a batch of jobs is inserted
	SkippedJob   rivercommon.TestSignal[struct{}] // notifies when a job is skipped because of nil JobInsertParams
}

func (ts *PeriodicJobEnqueuerTestSignals) Init() {
	ts.EnteredLoop.Init()
	ts.InsertedJobs.Init()
	ts.SkippedJob.Init()
}

// PeriodicJob is a periodic job to be run. It's similar to the top-level
// river.PeriodicJobArgs, but needs a separate type because the enqueuer is in a
// subpackage.
type PeriodicJob struct {
	ConstructorFunc func() (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error)
	RunOnStart      bool
	ScheduleFunc    func(time.Time) time.Time

	nextRunAt time.Time // set on service start
}

func (j *PeriodicJob) mustValidate() *PeriodicJob {
	if j.ScheduleFunc == nil {
		panic("PeriodicJob.ScheduleFunc must be set")
	}
	if j.ConstructorFunc == nil {
		panic("PeriodicJob.ConstructorFunc must be set")
	}

	return j
}

type PeriodicJobEnqueuerConfig struct {
	AdvisoryLockPrefix int32

	// PeriodicJobs are the periodic jobs with which to configure the enqueuer.
	PeriodicJobs []*PeriodicJob
}

func (c *PeriodicJobEnqueuerConfig) mustValidate() *PeriodicJobEnqueuerConfig {
	return c
}

// PeriodicJobEnqueuer inserts jobs configured to run periodically as unique
// jobs to make sure they'll run as frequently as their period dictates.
type PeriodicJobEnqueuer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	Config      *PeriodicJobEnqueuerConfig
	TestSignals PeriodicJobEnqueuerTestSignals

	exec           riverdriver.Executor
	periodicJobs   []*PeriodicJob
	uniqueInserter *dbunique.UniqueInserter
}

func NewPeriodicJobEnqueuer(archetype *baseservice.Archetype, config *PeriodicJobEnqueuerConfig, exec riverdriver.Executor) *PeriodicJobEnqueuer {
	svc := baseservice.Init(archetype, &PeriodicJobEnqueuer{
		Config: (&PeriodicJobEnqueuerConfig{
			AdvisoryLockPrefix: config.AdvisoryLockPrefix,
			PeriodicJobs:       config.PeriodicJobs,
		}).mustValidate(),

		exec:           exec,
		periodicJobs:   config.PeriodicJobs,
		uniqueInserter: baseservice.Init(archetype, &dbunique.UniqueInserter{AdvisoryLockPrefix: config.AdvisoryLockPrefix}),
	})

	return svc
}

type insertParamsAndUniqueOpts struct {
	InsertParams *riverdriver.JobInsertFastParams
	UniqueOpts   *dbunique.UniqueOpts
}

func (s *PeriodicJobEnqueuer) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	// Jitter start up slightly so services don't all perform their first run at
	// exactly the same time.
	s.CancellableSleepRandomBetween(ctx, JitterMin, JitterMax)

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		s.Logger.InfoContext(ctx, s.Name+logPrefixRunLoopStarted)
		defer s.Logger.InfoContext(ctx, s.Name+logPrefixRunLoopStopped)

		// An initial loop to assign next runs for every configured job and
		// queues any jobs that should run immediately.
		{
			var (
				insertParamsMany   []*riverdriver.JobInsertFastParams
				insertParamsUnique []*insertParamsAndUniqueOpts
			)
			now := s.TimeNowUTC()

			for _, periodicJob := range s.periodicJobs {
				// Expect client to have validated any user input in a safer way
				// already, but do a second pass for internal uses.
				periodicJob.mustValidate()

				periodicJob.nextRunAt = periodicJob.ScheduleFunc(now)

				if periodicJob.RunOnStart {
					if insertParams, uniqueOpts, ok := s.insertParamsFromConstructor(ctx, periodicJob.ConstructorFunc); ok {
						if !uniqueOpts.IsEmpty() {
							insertParamsUnique = append(insertParamsUnique, &insertParamsAndUniqueOpts{insertParams, uniqueOpts})
						} else {
							insertParamsMany = append(insertParamsMany, insertParams)
						}
					}
				}
			}

			s.insertBatch(ctx, insertParamsMany, insertParamsUnique)
		}

		s.TestSignals.EnteredLoop.Signal(struct{}{})

		timerUntilNextRun := time.NewTimer(s.timeUntilNextRun())

	loop:
		for {
			select {
			case <-timerUntilNextRun.C:
				var (
					insertParamsMany   []*riverdriver.JobInsertFastParams
					insertParamsUnique []*insertParamsAndUniqueOpts
				)

				now := s.TimeNowUTC()

				// Add a small margin to the current time so we're not only
				// running jobs that are already ready, but also ones ready at
				// this exact moment or ready in the very near future.
				nowWithMargin := now.Add(10 * time.Millisecond)

				for _, periodicJob := range s.periodicJobs {
					if !periodicJob.nextRunAt.Before(nowWithMargin) {
						continue
					}

					periodicJob.nextRunAt = periodicJob.ScheduleFunc(now)

					if insertParams, uniqueOpts, ok := s.insertParamsFromConstructor(ctx, periodicJob.ConstructorFunc); ok {
						if !uniqueOpts.IsEmpty() {
							insertParamsUnique = append(insertParamsUnique, &insertParamsAndUniqueOpts{insertParams, uniqueOpts})
						} else {
							insertParamsMany = append(insertParamsMany, insertParams)
						}
					}
				}

				s.insertBatch(ctx, insertParamsMany, insertParamsUnique)

				// Reset the timer after the insert loop has finished so it's
				// paused during work. Makes its firing more deterministic.
				timerUntilNextRun.Reset(s.timeUntilNextRun())

			case <-ctx.Done():
				// Clean up timer resources. We know it has _not_ received from the
				// timer since its last reset because that would have led us to the case
				// above instead of here.
				if !timerUntilNextRun.Stop() {
					<-timerUntilNextRun.C
				}
				break loop
			}
		}
	}()

	return nil
}

func (s *PeriodicJobEnqueuer) insertBatch(ctx context.Context, insertParamsMany []*riverdriver.JobInsertFastParams, insertParamsUnique []*insertParamsAndUniqueOpts) {
	if len(insertParamsMany) > 0 {
		if _, err := s.exec.JobInsertFastMany(ctx, insertParamsMany); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error inserting periodic jobs",
				"error", err.Error(), "num_jobs", len(insertParamsMany))
		}
	}

	// Unique periodic jobs must be inserted one at a time because bulk insert
	// doesn't respect uniqueness. Unique jobs are rare compared to non-unique,
	// so we still maintain an insert many fast path above for programs that
	// aren't inserting any unique jobs periodically (which we expect is most).
	if len(insertParamsUnique) > 0 {
		for _, params := range insertParamsUnique {
			if _, err := s.uniqueInserter.JobInsert(ctx, s.exec, params.InsertParams, params.UniqueOpts); err != nil {
				s.Logger.ErrorContext(ctx, s.Name+": Error inserting unique periodic job",
					"error", err.Error(), "kind", params.InsertParams.Kind)
			}
		}
	}

	if len(insertParamsMany) > 0 || len(insertParamsUnique) > 0 {
		s.TestSignals.InsertedJobs.Signal(struct{}{})
	}
}

func (s *PeriodicJobEnqueuer) insertParamsFromConstructor(ctx context.Context, constructorFunc func() (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error)) (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, bool) {
	insertParams, uniqueOpts, err := constructorFunc()
	if err != nil {
		if errors.Is(err, ErrNoJobToInsert) {
			s.Logger.InfoContext(ctx, s.Name+": nil returned from periodic job constructor, skipping")
			s.TestSignals.SkippedJob.Signal(struct{}{})
			return nil, nil, false
		}
		s.Logger.ErrorContext(ctx, s.Name+": Internal error generating periodic job", "error", err.Error())
		return nil, nil, false
	}

	return insertParams, uniqueOpts, true
}

func (s *PeriodicJobEnqueuer) timeUntilNextRun() time.Duration {
	// With no configured jobs, just return a big duration for the loop to block
	// on.
	if len(s.periodicJobs) < 1 {
		return 24 * time.Hour
	}

	var (
		firstNextRunAt time.Time
		now            = s.TimeNowUTC()
	)

	for _, periodicJob := range s.periodicJobs {
		// In case we detect a job that should've run before now, immediately short
		// circuit with a 0 duration. This avoids needlessly iterating through the
		// rest of the loop when we already know we're overdue for the next job.
		if periodicJob.nextRunAt.Before(now) {
			return 0
		}

		if firstNextRunAt.IsZero() || periodicJob.nextRunAt.Before(firstNextRunAt) {
			firstNextRunAt = periodicJob.nextRunAt
		}
	}

	return firstNextRunAt.Sub(now)
}
