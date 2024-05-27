package maintenance

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/maputil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// ErrNoJobToInsert can be returned by a PeriodicJob's JobToInsertFunc to
// signal that there's no job to insert at this time.
var ErrNoJobToInsert = errors.New("a nil job was returned, nothing to insert")

// Test-only properties.
type PeriodicJobEnqueuerTestSignals struct {
	EnteredLoop    rivercommon.TestSignal[struct{}] // notifies when the enqueuer finishes start up and enters its initial run loop
	InsertedJobs   rivercommon.TestSignal[struct{}] // notifies when a batch of jobs is inserted
	NotifiedQueues rivercommon.TestSignal[[]string] // notifies when queues are sent an insert notification
	SkippedJob     rivercommon.TestSignal[struct{}] // notifies when a job is skipped because of nil JobInsertParams
}

func (ts *PeriodicJobEnqueuerTestSignals) Init() {
	ts.EnteredLoop.Init()
	ts.InsertedJobs.Init()
	ts.NotifiedQueues.Init()
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

	// NotifyInsert is a function to call to emit notifications for queues
	// where jobs were scheduled.
	NotifyInsert NotifyInsertFunc

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

	// exported for test purposes
	Config      *PeriodicJobEnqueuerConfig
	TestSignals PeriodicJobEnqueuerTestSignals

	exec               riverdriver.Executor
	mu                 sync.RWMutex
	nextHandle         rivertype.PeriodicJobHandle
	periodicJobs       map[rivertype.PeriodicJobHandle]*PeriodicJob
	recalculateNextRun chan struct{}
	uniqueInserter     *dbunique.UniqueInserter
}

func NewPeriodicJobEnqueuer(archetype *baseservice.Archetype, config *PeriodicJobEnqueuerConfig, exec riverdriver.Executor) *PeriodicJobEnqueuer {
	var (
		nextHandle   rivertype.PeriodicJobHandle
		periodicJobs = make(map[rivertype.PeriodicJobHandle]*PeriodicJob, len(config.PeriodicJobs))
	)

	for _, periodicJob := range config.PeriodicJobs {
		periodicJob.mustValidate()

		periodicJobs[nextHandle] = periodicJob
		nextHandle++
	}

	svc := baseservice.Init(archetype, &PeriodicJobEnqueuer{
		Config: (&PeriodicJobEnqueuerConfig{
			AdvisoryLockPrefix: config.AdvisoryLockPrefix,
			NotifyInsert:       config.NotifyInsert,
			PeriodicJobs:       config.PeriodicJobs,
		}).mustValidate(),

		exec:               exec,
		nextHandle:         nextHandle,
		periodicJobs:       periodicJobs,
		recalculateNextRun: make(chan struct{}, 1),
		uniqueInserter:     baseservice.Init(archetype, &dbunique.UniqueInserter{AdvisoryLockPrefix: config.AdvisoryLockPrefix}),
	})

	return svc
}

// Add adds a new periodic job to the enqueuer. The service's run loop is woken
// immediately so that the job is scheduled appropriately, and inserted if its
// RunOnStart flag is set to true.
func (s *PeriodicJobEnqueuer) Add(periodicJob *PeriodicJob) rivertype.PeriodicJobHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	periodicJob.mustValidate()

	handle := s.nextHandle
	s.periodicJobs[handle] = periodicJob
	s.nextHandle++

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handle
}

// AddMany adds many new periodic job to the enqueuer. The service's run loop is
// woken immediately so that the job is scheduled appropriately, and inserted if
// any RunOnStart flags are set to true.
func (s *PeriodicJobEnqueuer) AddMany(periodicJobs []*PeriodicJob) []rivertype.PeriodicJobHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	handles := make([]rivertype.PeriodicJobHandle, len(periodicJobs))

	for i, periodicJob := range periodicJobs {
		periodicJob.mustValidate()

		handles[i] = s.nextHandle
		s.periodicJobs[handles[i]] = periodicJob
		s.nextHandle++
	}

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handles
}

// Clear clears all periodic jobs from the enqueuer.
func (s *PeriodicJobEnqueuer) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// `nextHandle` is _not_ reset so that even across multiple generations of
	// jobs, handles aren't reused.
	s.periodicJobs = make(map[rivertype.PeriodicJobHandle]*PeriodicJob)
}

// Remove removes a periodic job from the enqueuer. Its current target run time
// and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) Remove(periodicJobHandle rivertype.PeriodicJobHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.periodicJobs, periodicJobHandle)
}

// RemoveMany removes many periodic jobs from the enqueuer. Their current target
// run time and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) RemoveMany(periodicJobHandles []rivertype.PeriodicJobHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, handle := range periodicJobHandles {
		delete(s.periodicJobs, handle)
	}
}

type insertParamsAndUniqueOpts struct {
	InsertParams *riverdriver.JobInsertFastParams
	UniqueOpts   *dbunique.UniqueOpts
}

func (s *PeriodicJobEnqueuer) Run(ctx context.Context) {
	s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStarted)
	defer s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStopped)

	// Drain the signal to recalculate next run if it's been sent (i.e. Add
	// or AddMany called before Start). We're about to schedule jobs from
	// scratch, and therefore don't need to immediately do so again.
	select {
	case <-s.recalculateNextRun:
	default:
	}

	var lastHandleSeen rivertype.PeriodicJobHandle = -1 // so handle 0 is considered

	validateInsertRunOnStartAndScheduleNewlyAdded := func() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		var (
			insertParamsMany   []*riverdriver.JobInsertFastParams
			insertParamsUnique []*insertParamsAndUniqueOpts

			now = s.TimeNowUTC()
		)

		// Handle periodic jobs in sorted order so we can correctly account
		// for the most recently added one that we've seen.
		sortedPeriodicJobHandles := maputil.Keys(s.periodicJobs)
		slices.Sort(sortedPeriodicJobHandles)

		for _, handle := range sortedPeriodicJobHandles {
			if handle <= lastHandleSeen {
				continue
			}

			lastHandleSeen = handle

			periodicJob := s.periodicJobs[handle].mustValidate()

			periodicJob.nextRunAt = periodicJob.ScheduleFunc(now)

			if !periodicJob.RunOnStart {
				continue
			}

			if insertParams, uniqueOpts, ok := s.insertParamsFromConstructor(ctx, periodicJob.ConstructorFunc, now); ok {
				if !uniqueOpts.IsEmpty() {
					insertParamsUnique = append(insertParamsUnique, &insertParamsAndUniqueOpts{insertParams, uniqueOpts})
				} else {
					insertParamsMany = append(insertParamsMany, insertParams)
				}
			}
		}

		s.insertBatch(ctx, insertParamsMany, insertParamsUnique)

		if len(insertParamsMany) > 0 {
			s.Logger.DebugContext(ctx, s.Name+": Inserted RunOnStart jobs", "num_jobs", len(insertParamsMany)+len(insertParamsUnique))
		}
	}

	// Run any jobs that need to run on start and calculate initial runs.
	validateInsertRunOnStartAndScheduleNewlyAdded()

	s.TestSignals.EnteredLoop.Signal(struct{}{})

	timerUntilNextRun := time.NewTimer(s.timeUntilNextRun())

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
			nowWithMargin := now.Add(100 * time.Millisecond)

			func() {
				s.mu.RLock()
				defer s.mu.RUnlock()

				for _, periodicJob := range s.periodicJobs {
					if !periodicJob.nextRunAt.Before(nowWithMargin) {
						continue
					}

					if insertParams, uniqueOpts, ok := s.insertParamsFromConstructor(ctx, periodicJob.ConstructorFunc, periodicJob.nextRunAt); ok {
						if !uniqueOpts.IsEmpty() {
							insertParamsUnique = append(insertParamsUnique, &insertParamsAndUniqueOpts{insertParams, uniqueOpts})
						} else {
							insertParamsMany = append(insertParamsMany, insertParams)
						}
					}

					// Although we may have inserted a new job a little
					// preemptively due to the margin applied above, try to stay
					// as true as possible to the original schedule by using the
					// original run time when calculating the next one.
					periodicJob.nextRunAt = periodicJob.ScheduleFunc(periodicJob.nextRunAt)
				}
			}()

			s.insertBatch(ctx, insertParamsMany, insertParamsUnique)

		case <-s.recalculateNextRun:
			if !timerUntilNextRun.Stop() {
				<-timerUntilNextRun.C
			}

		case <-ctx.Done():
			// Clean up timer resources. We know it has _not_ received from the
			// timer since its last reset because that would have led us to the case
			// above instead of here.
			if !timerUntilNextRun.Stop() {
				<-timerUntilNextRun.C
			}
			return
		}

		// Insert any RunOnStart initial runs for new jobs that've been
		// added since the last run loop.
		validateInsertRunOnStartAndScheduleNewlyAdded()

		// Reset the timer after the insert loop has finished so it's
		// paused during work. Makes its firing more deterministic.
		timerUntilNextRun.Reset(s.timeUntilNextRun())
	}
}

func (s *PeriodicJobEnqueuer) insertBatch(ctx context.Context, insertParamsMany []*riverdriver.JobInsertFastParams, insertParamsUnique []*insertParamsAndUniqueOpts) {
	if len(insertParamsMany) == 0 && len(insertParamsUnique) == 0 {
		return
	}

	tx, err := s.exec.Begin(ctx)
	if err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error starting transaction", "error", err.Error())
		return
	}
	defer tx.Rollback(ctx)

	queues := make([]string, 0, len(insertParamsMany)+len(insertParamsUnique))

	if len(insertParamsMany) > 0 {
		if _, err := tx.JobInsertFastMany(ctx, insertParamsMany); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error inserting periodic jobs",
				"error", err.Error(), "num_jobs", len(insertParamsMany))
			return
		}
		for _, params := range insertParamsMany {
			queues = append(queues, params.Queue)
		}
	}

	// Unique periodic jobs must be inserted one at a time because bulk insert
	// doesn't respect uniqueness. Unique jobs are rare compared to non-unique,
	// so we still maintain an insert many fast path above for programs that
	// aren't inserting any unique jobs periodically (which we expect is most).
	if len(insertParamsUnique) > 0 {
		for _, params := range insertParamsUnique {
			res, err := s.uniqueInserter.JobInsert(ctx, tx, params.InsertParams, params.UniqueOpts)
			if err != nil {
				s.Logger.ErrorContext(ctx, s.Name+": Error inserting unique periodic job",
					"error", err.Error(), "kind", params.InsertParams.Kind)
				continue
			}
			if !res.UniqueSkippedAsDuplicate {
				queues = append(queues, params.InsertParams.Queue)
			}
		}
	}

	if len(queues) > 0 {
		if err := s.Config.NotifyInsert(ctx, tx, queues); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error notifying insert", "error", err.Error())
			return
		}
		s.TestSignals.NotifiedQueues.Signal(queues)
	}

	if err := tx.Commit(ctx); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error committing transaction", "error", err.Error())
		return
	}
	s.TestSignals.InsertedJobs.Signal(struct{}{})
}

func (s *PeriodicJobEnqueuer) insertParamsFromConstructor(ctx context.Context, constructorFunc func() (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error), scheduledAt time.Time) (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, bool) {
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

	if insertParams.ScheduledAt == nil {
		insertParams.ScheduledAt = &scheduledAt
	}

	return insertParams, uniqueOpts, true
}

func (s *PeriodicJobEnqueuer) timeUntilNextRun() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
