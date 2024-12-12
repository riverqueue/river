package maintenance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

type noOpArgs struct{}

func (noOpArgs) Kind() string { return "no_op" }

func TestPeriodicJobEnqueuer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec                 riverdriver.Executor
		notificationsByQueue map[string]int
		waitChan             chan (struct{})
	}

	stubSvc := &riversharedtest.TimeStub{}
	stubSvc.StubNowUTC(time.Now().UTC())

	jobConstructorWithQueueFunc := func(name string, unique bool, queue string) func() (*rivertype.JobInsertParams, error) {
		return func() (*rivertype.JobInsertParams, error) {
			params := &rivertype.JobInsertParams{
				Args:        noOpArgs{},
				EncodedArgs: []byte("{}"),
				Kind:        name,
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       queue,
				State:       rivertype.JobStateAvailable,
			}
			if unique {
				uniqueOpts := &dbunique.UniqueOpts{ByArgs: true}
				var err error
				params.UniqueKey, err = dbunique.UniqueKey(stubSvc, uniqueOpts, params)
				if err != nil {
					return nil, err
				}

				params.UniqueStates = uniqueOpts.StateBitmask()
			}

			return params, nil
		}
	}

	jobConstructorFunc := func(name string, unique bool) func() (*rivertype.JobInsertParams, error) {
		return jobConstructorWithQueueFunc(name, unique, rivercommon.QueueDefault)
	}

	periodicIntervalSchedule := func(d time.Duration) func(time.Time) time.Time {
		return func(t time.Time) time.Time {
			return t.Add(d)
		}
	}

	// A simplified version of `Client.insertMany` that only inserts jobs directly
	// via the driver instead of using the pilot.
	insertFunc := func(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
		finalInsertParams := sliceutil.Map(insertParams, func(params *rivertype.JobInsertParams) *riverdriver.JobInsertFastParams {
			return (*riverdriver.JobInsertFastParams)(params)
		})
		results, err := tx.JobInsertFastMany(ctx, finalInsertParams)
		if err != nil {
			return nil, err
		}
		return sliceutil.Map(results,
			func(result *riverdriver.JobInsertFastResult) *rivertype.JobInsertResult {
				return (*rivertype.JobInsertResult)(result)
			},
		), nil
	}

	setup := func(t *testing.T) (*PeriodicJobEnqueuer, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			exec:                 riverpgxv5.New(riverinternaltest.TestDB(ctx, t)).GetExecutor(),
			notificationsByQueue: make(map[string]int),
			waitChan:             make(chan struct{}),
		}

		svc := NewPeriodicJobEnqueuer(riversharedtest.BaseServiceArchetype(t), &PeriodicJobEnqueuerConfig{Insert: insertFunc}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init()

		return svc, bundle
	}

	requireNJobs := func(t *testing.T, exec riverdriver.Executor, kind string, n int) []*rivertype.JobRow {
		t.Helper()

		jobs, err := exec.JobGetByKindMany(ctx, []string{kind})
		require.NoError(t, err)
		require.Len(t, jobs, n, fmt.Sprintf("Expected to find exactly %d job(s) of kind: %s, but found %d", n, kind, len(jobs)))

		return jobs
	}

	startService := func(t *testing.T, svc *PeriodicJobEnqueuer) {
		t.Helper()

		require.NoError(t, svc.Start(ctx))
		t.Cleanup(svc.Stop)

		riversharedtest.WaitOrTimeout(t, startstop.WaitAllStartedC(svc))
	}

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)
		svc.Logger = riversharedtest.LoggerWarn(t)         // loop started/stop log is very noisy; suppress
		svc.TestSignals = PeriodicJobEnqueuerTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, svc)
	})

	// This test run is somewhat susceptible to the "ready margin" applied on
	// enqueuer loops to find jobs that aren't quite ready yet, but close
	// enough. The 500 ms/1500 ms job types can have their ready times diverge
	// slightly as they're enqueued separately. Usually they're ~identical, but
	// a large enough divergence which can occur with `-race` and a hundred test
	// iterations can cause the test to fail as an expected job wasn't enqueued
	// on the expected loop. The ready margin is currently high enough (100 ms)
	// that this problem won't occur, but in case it's ever substantially
	// lowered, this test will need to be rewritten.
	t.Run("EnqueuesPeriodicJobs", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
		})

		startService(t, svc)

		// Should be no jobs to start.
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 1)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 2)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 3)
		requireNJobs(t, bundle.exec, "periodic_job_1500ms", 1)
	})

	t.Run("SetsScheduledAtAccordingToExpectedNextRunAt", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), RunOnStart: true},
		})

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		job1 := requireNJobs(t, bundle.exec, "periodic_job_500ms", 1)[0]
		require.Equal(t, rivertype.JobStateAvailable, job1.State)
		require.WithinDuration(t, time.Now(), job1.ScheduledAt, 1*time.Second)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		job2 := requireNJobs(t, bundle.exec, "periodic_job_500ms", 2)[1] // ordered by ID

		// The new `scheduled_at` is *exactly* the original `scheduled_at` plus
		// 500 milliseconds because the enqueuer used the target next run time
		// to calculate the new `scheduled_at`.
		require.Equal(t, job1.ScheduledAt.Add(500*time.Millisecond), job2.ScheduledAt)

		require.Equal(t, rivertype.JobStateAvailable, job2.State)
	})

	t.Run("RespectsJobUniqueness", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("unique_periodic_job_500ms", true)},
		})

		startService(t, svc)

		// Should be no jobs to start.
		requireNJobs(t, bundle.exec, "unique_periodic_job_500ms", 0)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "unique_periodic_job_500ms", 1)

		// Another insert was attempted, but there's still only one job due to
		// uniqueness conditions.
		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "unique_periodic_job_500ms", 1)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "unique_periodic_job_500ms", 1)
	})

	t.Run("RunOnStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s", false), RunOnStart: true},
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("unique_periodic_job_5s", true), RunOnStart: true},
		})

		start := time.Now()
		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_5s", 1)
		requireNJobs(t, bundle.exec, "unique_periodic_job_5s", 1)

		// Should've happened quite quickly.
		require.WithinDuration(t, time.Now(), start, 1*time.Second)
	})

	t.Run("ErrNoJobToInsert", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.AddMany([]*PeriodicJob{
			// skip this insert when it returns nil:
			{ScheduleFunc: periodicIntervalSchedule(time.Second), ConstructorFunc: func() (*rivertype.JobInsertParams, error) {
				return nil, ErrNoJobToInsert
			}, RunOnStart: true},
		})

		startService(t, svc)

		svc.TestSignals.SkippedJob.WaitOrTimeout()
	})

	t.Run("InitialScheduling", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		now := svc.Time.StubNowUTC(time.Now())

		svc.periodicJobs = make(map[rivertype.PeriodicJobHandle]*PeriodicJob)
		periodicJobHandles := svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s", false)},
			{ScheduleFunc: periodicIntervalSchedule(15 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_15m", false)},
			{ScheduleFunc: periodicIntervalSchedule(3 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_3h", false)},
			{ScheduleFunc: periodicIntervalSchedule(7 * 24 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_7d", false)},
		})

		startService(t, svc)

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		require.Equal(t, now.Add(500*time.Millisecond), svc.periodicJobs[periodicJobHandles[0]].nextRunAt)
		require.Equal(t, now.Add(1500*time.Millisecond), svc.periodicJobs[periodicJobHandles[1]].nextRunAt)
		require.Equal(t, now.Add(5*time.Second), svc.periodicJobs[periodicJobHandles[2]].nextRunAt)
		require.Equal(t, now.Add(15*time.Minute), svc.periodicJobs[periodicJobHandles[3]].nextRunAt)
		require.Equal(t, now.Add(3*time.Hour), svc.periodicJobs[periodicJobHandles[4]].nextRunAt)
		require.Equal(t, now.Add(7*24*time.Hour), svc.periodicJobs[periodicJobHandles[5]].nextRunAt)

		// Schedules a job for the distant future. This is so that we can remove
		// jobs from the running and verify that waiting until each successive
		// periodic job really works.
		scheduleDistantFuture := func(periodicJob *PeriodicJob) {
			// It may feel a little heavy-handed to stop and start the service
			// for each job we check, but the alternative is an internal locking
			// system needed for the tests only because modifying a job while
			// the service is running will be detected by `-race`.
			svc.Stop()

			periodicJob.ScheduleFunc = periodicIntervalSchedule(365 * 24 * time.Hour)

			require.NoError(t, svc.Start(ctx))
			svc.TestSignals.EnteredLoop.WaitOrTimeout()
		}

		require.Equal(t, 500*time.Millisecond, svc.timeUntilNextRun())

		scheduleDistantFuture(svc.periodicJobs[periodicJobHandles[0]])
		require.Equal(t, 1500*time.Millisecond, svc.timeUntilNextRun())

		scheduleDistantFuture(svc.periodicJobs[periodicJobHandles[1]])
		require.Equal(t, 5*time.Second, svc.timeUntilNextRun())

		scheduleDistantFuture(svc.periodicJobs[periodicJobHandles[2]])
		require.Equal(t, 15*time.Minute, svc.timeUntilNextRun())

		scheduleDistantFuture(svc.periodicJobs[periodicJobHandles[3]])
		require.Equal(t, 3*time.Hour, svc.timeUntilNextRun())

		scheduleDistantFuture(svc.periodicJobs[periodicJobHandles[4]])
		require.Equal(t, 7*24*time.Hour, svc.timeUntilNextRun())
	})

	// To ensure we are protected against runs that are supposed to have already happened,
	// this test uses a totally-not-safe schedule to enqueue every 0.5ms.
	t.Run("RapidScheduling", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.Add(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Microsecond), ConstructorFunc: jobConstructorFunc("periodic_job_1us", false)})
		// make a longer list of jobs so the loop has to run for longer
		for i := 1; i < 100; i++ {
			svc.Add(&PeriodicJob{
				ScheduleFunc:    periodicIntervalSchedule(time.Duration(i) * time.Hour),
				ConstructorFunc: jobConstructorFunc(fmt.Sprintf("periodic_job_%dh", i), false),
			})
		}

		startService(t, svc)

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		for i := 0; i < 100; i++ {
			svc.TestSignals.InsertedJobs.WaitOrTimeout()
		}
	})

	t.Run("ConfigurableViaConstructor", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		svc := NewPeriodicJobEnqueuer(
			riversharedtest.BaseServiceArchetype(t),
			&PeriodicJobEnqueuerConfig{
				Insert: insertFunc,
				PeriodicJobs: []*PeriodicJob{
					{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), RunOnStart: true},
					{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false), RunOnStart: true},
				},
			}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init()

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 1)
		requireNJobs(t, bundle.exec, "periodic_job_1500ms", 1)
	})

	t.Run("AddAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		svc.Add(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
		)
		svc.Add(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_start", 1)
	})

	t.Run("AddManyAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_start", 1)
	})

	t.Run("ClearAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles := svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_start", 1)

		svc.Clear()

		require.Empty(t, svc.periodicJobs)

		handleAfterClear := svc.Add(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_new", false)},
		)

		// Handles are not reused.
		require.NotEqual(t, handles[0], handleAfterClear)
		require.NotEqual(t, handles[1], handleAfterClear)
	})

	t.Run("RemoveAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles := svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_start", 1)

		svc.Remove(handles[1])

		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveManyAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles := svc.AddMany([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_other", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.exec, "periodic_job_500ms", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_other", 0)
		requireNJobs(t, bundle.exec, "periodic_job_500ms_start", 1)

		svc.RemoveMany([]rivertype.PeriodicJobHandle{handles[1], handles[2]})

		require.Len(t, svc.periodicJobs, 1)
	})

	// To suss out any race conditions in the add/remove/clear/run loop code,
	// and interactions between them.
	t.Run("AddRemoveStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		var wg sync.WaitGroup

		randomSleep := func() {
			time.Sleep(time.Duration(randutil.IntBetween(1, 5)) * time.Millisecond)
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)

			jobBaseName := fmt.Sprintf("periodic_job_1ms_%02d", i)

			go func() {
				defer wg.Done()

				for j := 0; j < 50; j++ {
					handle := svc.Add(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Millisecond), ConstructorFunc: jobConstructorFunc(jobBaseName, false)})
					randomSleep()

					svc.Add(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Millisecond), ConstructorFunc: jobConstructorFunc(jobBaseName+"_second", false)})
					randomSleep()

					svc.Remove(handle)
					randomSleep()

					svc.Clear()
					randomSleep()
				}
			}()
		}

		wg.Wait()
	})

	t.Run("NoJobsConfigured", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		startService(t, svc)

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		require.LessOrEqual(t, svc.timeUntilNextRun(), 24*time.Hour)
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		startService(t, svc)
		svc.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		ctx, cancelFunc := context.WithCancel(ctx)
		require.NoError(t, svc.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := svc.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("TimeUntilNextRun", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		now := svc.Time.StubNowUTC(time.Now())

		// no jobs
		require.Equal(t, periodicJobEnqueuerVeryLongDuration, svc.timeUntilNextRun())

		svc.periodicJobs = map[rivertype.PeriodicJobHandle]*PeriodicJob{
			1: {nextRunAt: now.Add(2 * time.Hour)},
			2: {nextRunAt: now.Add(1 * time.Hour)},
			3: {nextRunAt: now.Add(3 * time.Hour)},
		}

		// pick job with soonest next run
		require.Equal(t, 1*time.Hour, svc.timeUntilNextRun())

		svc.periodicJobs = map[rivertype.PeriodicJobHandle]*PeriodicJob{
			1: {nextRunAt: now.Add(2 * time.Hour)},
			2: {nextRunAt: now.Add(-1 * time.Hour)},
			3: {nextRunAt: now.Add(3 * time.Hour)},
		}

		// job is already behind so time until next run is 0
		require.Equal(t, time.Duration(0), svc.timeUntilNextRun())

		svc.periodicJobs = map[rivertype.PeriodicJobHandle]*PeriodicJob{
			1: {},
			2: {},
		}

		// jobs not scheduled yet
		require.Equal(t, periodicJobEnqueuerVeryLongDuration, svc.timeUntilNextRun())

		svc.periodicJobs = map[rivertype.PeriodicJobHandle]*PeriodicJob{
			1: {},
			2: {nextRunAt: now.Add(1 * time.Hour)},
			3: {},
		}

		// pick job with soonest next run amongst some not scheduled yet
		require.Equal(t, 1*time.Hour, svc.timeUntilNextRun())
	})
}
