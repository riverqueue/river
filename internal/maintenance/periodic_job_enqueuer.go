package maintenance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

// ErrNoJobToInsert can be returned by a PeriodicJob's JobToInsertFunc to
// signal that there's no job to insert at this time.
var ErrNoJobToInsert = errors.New("a nil job was returned, nothing to insert")

// Test-only properties.
type PeriodicJobEnqueuerTestSignals struct {
	EnteredLoop                 testsignal.TestSignal[struct{}] // notifies when the enqueuer finishes start up and enters its initial run loop
	InsertedJobs                testsignal.TestSignal[struct{}] // notifies when a batch of jobs is inserted
	PeriodicJobKeepAliveAndReap testsignal.TestSignal[struct{}] // notifies when the background services that runs keep alive and reap on periodic jobs ticks
	PeriodicJobUpserted         testsignal.TestSignal[struct{}] // notifies when a batch of periodic job records are upserted to pilot
	SkippedJob                  testsignal.TestSignal[struct{}] // notifies when a job is skipped because of nil JobInsertParams
}

func (ts *PeriodicJobEnqueuerTestSignals) Init(tb testutil.TestingTB) {
	ts.EnteredLoop.Init(tb)
	ts.InsertedJobs.Init(tb)
	ts.PeriodicJobKeepAliveAndReap.Init(tb)
	ts.PeriodicJobUpserted.Init(tb)
	ts.SkippedJob.Init(tb)
}

// PeriodicJob is a periodic job to be run. It's similar to the top-level
// river.PeriodicJobArgs, but needs a separate type because the enqueuer is in a
// subpackage.
type PeriodicJob struct {
	ID              string
	ConstructorFunc func() (*rivertype.JobInsertParams, error)
	RunOnStart      bool
	ScheduleFunc    func(time.Time) time.Time

	nextRunAt time.Time // set on service start
}

func (j *PeriodicJob) mustValidate() *PeriodicJob {
	if err := j.validate(); err != nil {
		panic(err)
	}
	return j
}

func (j *PeriodicJob) validate() error {
	if j.ID != "" {
		if len(j.ID) >= 128 {
			return errors.New("PeriodicJob.ID must be less than 128 characters")
		}
		if !rivercommon.UserSpecifiedIDOrKindRE.MatchString(j.ID) {
			return fmt.Errorf("PeriodicJob.ID %q should match regex %s", j.ID, rivercommon.UserSpecifiedIDOrKindRE.String())
		}
	}
	if j.ConstructorFunc == nil {
		return errors.New("PeriodicJob.ConstructorFunc must be set")
	}
	if j.ScheduleFunc == nil {
		return errors.New("PeriodicJob.ScheduleFunc must be set")
	}

	return nil
}

type InsertFunc func(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error)

type PeriodicJobEnqueuerConfig struct {
	AdvisoryLockPrefix int32

	// Insert is the function to call to insert jobs into the database.
	Insert InsertFunc

	// PeriodicJobs are the periodic jobs with which to configure the enqueuer.
	PeriodicJobs []*PeriodicJob

	// Pilot is a plugin module providing additional non-standard functionality.
	Pilot riverpilot.PilotPeriodicJob

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *PeriodicJobEnqueuerConfig) mustValidate() *PeriodicJobEnqueuerConfig {
	// no validations currently
	return c
}

// PeriodicJobEnqueuer inserts jobs configured to run periodically as unique
// jobs to make sure they'll run as frequently as their period dictates.
type PeriodicJobEnqueuer struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *PeriodicJobEnqueuerConfig
	TestSignals PeriodicJobEnqueuerTestSignals

	exec               riverdriver.Executor
	mu                 sync.RWMutex
	nextHandle         rivertype.PeriodicJobHandle
	periodicJobIDs     map[string]struct{}
	periodicJobs       map[rivertype.PeriodicJobHandle]*PeriodicJob
	recalculateNextRun chan struct{}
}

func NewPeriodicJobEnqueuer(archetype *baseservice.Archetype, config *PeriodicJobEnqueuerConfig, exec riverdriver.Executor) (*PeriodicJobEnqueuer, error) {
	var (
		nextHandle     rivertype.PeriodicJobHandle
		periodicJobIDs = make(map[string]struct{})
		periodicJobs   = make(map[rivertype.PeriodicJobHandle]*PeriodicJob, len(config.PeriodicJobs))
	)

	for _, periodicJob := range config.PeriodicJobs {
		if err := periodicJob.validate(); err != nil {
			return nil, err
		}

		if err := addUniqueID(periodicJobIDs, periodicJob.ID); err != nil {
			return nil, err
		}

		periodicJobs[nextHandle] = periodicJob
		nextHandle++
	}

	pilot := config.Pilot
	if pilot == nil {
		pilot = &riverpilot.StandardPilot{}
	}

	svc := baseservice.Init(archetype, &PeriodicJobEnqueuer{
		Config: (&PeriodicJobEnqueuerConfig{
			AdvisoryLockPrefix: config.AdvisoryLockPrefix,
			Insert:             config.Insert,
			PeriodicJobs:       config.PeriodicJobs,
			Pilot:              pilot,
			Schema:             config.Schema,
		}).mustValidate(),

		exec:               exec,
		nextHandle:         nextHandle,
		periodicJobIDs:     periodicJobIDs,
		periodicJobs:       periodicJobs,
		recalculateNextRun: make(chan struct{}, 1),
	})

	return svc, nil
}

// AddSafely adds a new periodic job to the enqueuer. The service's run loop is
// woken immediately so that the job is scheduled appropriately, and inserted if
// its RunOnStart flag is set to true.
func (s *PeriodicJobEnqueuer) AddSafely(periodicJob *PeriodicJob) (rivertype.PeriodicJobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := periodicJob.validate(); err != nil {
		return 0, err
	}

	if err := addUniqueID(s.periodicJobIDs, periodicJob.ID); err != nil {
		return 0, err
	}

	handle := s.nextHandle
	s.periodicJobs[handle] = periodicJob
	s.nextHandle++

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handle, nil
}

// AddManySafely adds many new periodic job to the enqueuer. The service's run loop is
// woken immediately so that the job is scheduled appropriately, and inserted if
// any RunOnStart flags are set to true.
func (s *PeriodicJobEnqueuer) AddManySafely(periodicJobs []*PeriodicJob) ([]rivertype.PeriodicJobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handles := make([]rivertype.PeriodicJobHandle, len(periodicJobs))

	for i, periodicJob := range periodicJobs {
		if err := periodicJob.validate(); err != nil {
			return nil, err
		}

		if err := addUniqueID(s.periodicJobIDs, periodicJob.ID); err != nil {
			return nil, err
		}

		handles[i] = s.nextHandle
		s.periodicJobs[handles[i]] = periodicJob
		s.nextHandle++
	}

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handles, nil
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

func (s *PeriodicJobEnqueuer) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	subServices := []startstop.Service{
		startstop.StartStopFunc(s.periodicJobKeepAliveAndReapPeriodically),
	}
	if err := startstop.StartAll(ctx, subServices...); err != nil {
		return err
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		defer startstop.StopAllParallel(subServices...)

		// Drain the signal to recalculate next run if it's been sent (i.e. Add
		// or AddMany called before Start). We're about to schedule jobs from
		// scratch, and therefore don't need to immediately do so again.
		select {
		case <-s.recalculateNextRun:
		default:
		}

		// Initial set of periodic job IDs mapped to next run at times fetched
		// from a configured pilot. Not used in most cases.
		initialPeriodicJobsMap := func() map[string]time.Time {
			initialPeriodicJobs, err := s.Config.Pilot.PeriodicJobGetAll(ctx, s.exec, &riverpilot.PeriodicJobGetAllParams{
				Schema: s.Config.Schema,
			})
			if err != nil {
				s.Logger.ErrorContext(ctx, s.Name+": Error fetching initial periodic jobs", "error", err)
				return make(map[string]time.Time)
			}

			return sliceutil.KeyBy(initialPeriodicJobs,
				func(j *riverpilot.PeriodicJob) (string, time.Time) { return j.ID, j.NextRunAt })
		}()

		var lastHandleSeen rivertype.PeriodicJobHandle = -1 // so handle 0 is considered

		validateInsertRunOnStartAndScheduleNewlyAdded := func() {
			s.mu.RLock()
			defer s.mu.RUnlock()

			var (
				insertParamsMany        []*rivertype.JobInsertParams
				now                     = s.Time.NowUTC()
				periodicJobUpsertParams = &riverpilot.PeriodicJobUpsertManyParams{Schema: s.Config.Schema}
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

				if nextRunAt, ok := initialPeriodicJobsMap[periodicJob.ID]; periodicJob.ID != "" && ok {
					periodicJob.nextRunAt = nextRunAt
					delete(initialPeriodicJobsMap, periodicJob.ID)
				} else {
					periodicJob.nextRunAt = periodicJob.ScheduleFunc(now)
				}

				if periodicJob.ID != "" {
					periodicJobUpsertParams.Jobs = append(periodicJobUpsertParams.Jobs, &riverpilot.PeriodicJobUpsertParams{
						ID:        periodicJob.ID,
						NextRunAt: periodicJob.nextRunAt,
					})
				}

				if !periodicJob.RunOnStart {
					continue
				}

				if insertParams, ok := s.insertParamsFromConstructor(ctx, periodicJob.ID, periodicJob.ConstructorFunc, now); ok {
					insertParamsMany = append(insertParamsMany, insertParams)
				}
			}

			s.insertBatch(ctx, insertParamsMany, periodicJobUpsertParams)

			if len(insertParamsMany) > 0 {
				s.Logger.DebugContext(ctx, s.Name+": Inserted RunOnStart jobs", "num_jobs", len(insertParamsMany))
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
					insertParamsMany        []*rivertype.JobInsertParams
					periodicJobUpsertParams = &riverpilot.PeriodicJobUpsertManyParams{Schema: s.Config.Schema}
				)

				now := s.Time.NowUTC()

				// Add a small margin to the current time so we're not only
				// running jobs that are already ready, but also ones ready at
				// this exact moment or ready in the very near future.
				nowWithMargin := now.Add(100 * time.Millisecond)

				func() {
					s.mu.RLock()
					defer s.mu.RUnlock()

					for _, periodicJob := range s.periodicJobs {
						if periodicJob.nextRunAt.IsZero() || !periodicJob.nextRunAt.Before(nowWithMargin) {
							continue
						}

						if insertParams, ok := s.insertParamsFromConstructor(ctx, periodicJob.ID, periodicJob.ConstructorFunc, periodicJob.nextRunAt); ok {
							insertParamsMany = append(insertParamsMany, insertParams)
						}

						// Although we may have inserted a new job a little
						// preemptively due to the margin applied above, try to stay
						// as true as possible to the original schedule by using the
						// original run time when calculating the next one.
						periodicJob.nextRunAt = periodicJob.ScheduleFunc(periodicJob.nextRunAt)

						if periodicJob.ID != "" {
							periodicJobUpsertParams.Jobs = append(periodicJobUpsertParams.Jobs, &riverpilot.PeriodicJobUpsertParams{
								ID:        periodicJob.ID,
								NextRunAt: periodicJob.nextRunAt,
							})
						}
					}
				}()

				s.insertBatch(ctx, insertParamsMany, periodicJobUpsertParams)

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
	}()

	return nil
}

func (s *PeriodicJobEnqueuer) insertBatch(ctx context.Context, insertParamsMany []*rivertype.JobInsertParams, periodicJobUpsertParams *riverpilot.PeriodicJobUpsertManyParams) {
	if len(insertParamsMany) < 1 && len(periodicJobUpsertParams.Jobs) < 1 {
		return
	}

	tx, err := s.exec.Begin(ctx)
	if err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error starting transaction", "error", err.Error())
		return
	}
	defer tx.Rollback(ctx)

	if len(insertParamsMany) > 0 {
		if _, err := s.Config.Insert(ctx, tx, insertParamsMany); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error inserting periodic jobs",
				"error", err.Error(), "num_jobs", len(insertParamsMany))
		}
	}

	if len(periodicJobUpsertParams.Jobs) > 0 {
		if _, err = s.Config.Pilot.PeriodicJobUpsertMany(ctx, tx, periodicJobUpsertParams); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error upserting periodic job next run times",
				"error", err.Error(), "num_jobs", len(insertParamsMany), "num_next_run_at_upserts", len(periodicJobUpsertParams.Jobs))
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error committing transaction", "error", err.Error())
		return
	}

	if len(insertParamsMany) > 0 {
		s.TestSignals.InsertedJobs.Signal(struct{}{})
	}
	if len(periodicJobUpsertParams.Jobs) > 0 {
		s.TestSignals.PeriodicJobUpserted.Signal(struct{}{})
	}
}

func (s *PeriodicJobEnqueuer) insertParamsFromConstructor(ctx context.Context, periodicJobID string, constructorFunc func() (*rivertype.JobInsertParams, error), scheduledAt time.Time) (*rivertype.JobInsertParams, bool) {
	insertParams, err := constructorFunc()
	if err != nil {
		if errors.Is(err, ErrNoJobToInsert) {
			s.Logger.InfoContext(ctx, s.Name+": nil returned from periodic job constructor, skipping")
			s.TestSignals.SkippedJob.Signal(struct{}{})
			return nil, false
		}
		s.Logger.ErrorContext(ctx, s.Name+": Internal error generating periodic job", "error", err.Error())
		return nil, false
	}

	if insertParams.ScheduledAt == nil {
		insertParams.ScheduledAt = &scheduledAt
	}

	if periodicJobID != "" {
		var err error
		if insertParams.Metadata, err = sjson.SetBytes(insertParams.Metadata, rivercommon.MetadataKeyPeriodicJobID, periodicJobID); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error setting periodic metadata", "error", err.Error())
		}
	}

	if insertParams.Metadata, err = sjson.SetBytes(insertParams.Metadata, "periodic", true); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error setting periodic metadata", "error", err.Error())
		return nil, false
	}

	return insertParams, true
}

func (s *PeriodicJobEnqueuer) periodicJobKeepAliveAndReapPeriodically(ctx context.Context, shouldStart bool, started, stopped func()) error {
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		ticker := timeutil.NewTickerWithInitialTick(ctx, 10*time.Minute)
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				func() {
					s.mu.RLock()
					defer s.mu.RUnlock()

					if len(s.periodicJobIDs) > 0 {
						if _, err := s.Config.Pilot.PeriodicJobKeepAliveAndReap(ctx, s.exec, &riverpilot.PeriodicJobKeepAliveAndReapParams{
							ID:     maputil.Keys(s.periodicJobIDs),
							Schema: s.Config.Schema,
						}); err != nil {
							s.Logger.ErrorContext(ctx, s.Name+": Error executing periodic job keep alive and reap", "error", err.Error())
							return
						}
					}

					s.TestSignals.PeriodicJobKeepAliveAndReap.Signal(struct{}{})
				}()
			}
		}
	}()

	return nil
}

const periodicJobEnqueuerVeryLongDuration = 24 * time.Hour

func (s *PeriodicJobEnqueuer) timeUntilNextRun() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// With no configured jobs, just return a big duration for the loop to block
	// on.
	if len(s.periodicJobs) < 1 {
		return periodicJobEnqueuerVeryLongDuration
	}

	var (
		firstNextRunAt time.Time
		now            = s.Time.NowUTC()
	)

	for _, periodicJob := range s.periodicJobs {
		// Jobs may have been added after service start, but before this
		// function runs for the first time. They're not scheduled properly yet,
		// but they will be soon, at which point this function will run again.
		// Skip them for now.
		if periodicJob.nextRunAt.IsZero() {
			continue
		}

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

	// Only encountered unscheduled jobs (see comment above). Don't schedule
	// anything for now.
	if firstNextRunAt.IsZero() {
		return periodicJobEnqueuerVeryLongDuration
	}

	return firstNextRunAt.Sub(now)
}

// Adds a unique ID to known periodic job IDs, erroring in case of a duplicate.
func addUniqueID(periodicJobIDs map[string]struct{}, id string) error {
	if id == "" {
		return nil
	}

	if _, ok := periodicJobIDs[id]; ok {
		return errors.New("periodic job with ID already registered: " + id)
	}

	periodicJobIDs[id] = struct{}{}
	return nil
}
