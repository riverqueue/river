package maintenance

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
)

func TestPeriodicJobEnqueuer(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		dbPool   *pgxpool.Pool
		waitChan chan (struct{})
	}

	jobConstructorFunc := func(name string) func() (*dbadapter.JobInsertParams, error) {
		return func() (*dbadapter.JobInsertParams, error) {
			return &dbadapter.JobInsertParams{
				EncodedArgs: []byte("{}"),
				Kind:        name,
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       dbsqlc.JobStateAvailable,
			}, nil
		}
	}

	periodicIntervalSchedule := func(d time.Duration) func(time.Time) time.Time {
		return func(t time.Time) time.Time {
			return t.Add(d)
		}
	}

	setup := func(t *testing.T) (*PeriodicJobEnqueuer, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			dbPool:   riverinternaltest.TestDB(ctx, t),
			waitChan: make(chan struct{}),
		}

		archetype := riverinternaltest.BaseServiceArchetype(t)

		svc := NewPeriodicJobEnqueuer(
			archetype,
			&PeriodicJobEnqueuerConfig{
				PeriodicJobs: []*PeriodicJob{
					{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms")},
					{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms")},
				},
			}, dbadapter.NewStandardAdapter(archetype, &dbadapter.StandardAdapterConfig{Executor: bundle.dbPool}))
		svc.TestSignals.Init()
		t.Cleanup(svc.Stop)

		return svc, bundle
	}

	requireNJobs := func(t *testing.T, pool *pgxpool.Pool, kind string, n int) {
		t.Helper()

		jobs, err := queries.JobGetByKind(ctx, pool, kind)
		require.NoError(t, err)
		require.Len(t, jobs, n, fmt.Sprintf("Expected to find exactly %d job(s) of kind: %s, but found %d", n, kind, len(jobs)))
	}

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)
		svc.Logger = riverinternaltest.LoggerWarn(t)       // loop started/stop log is very noisy; suppress
		svc.TestSignals = PeriodicJobEnqueuerTestSignals{} // deinit so channels don't fill

		runStartStopStress(ctx, t, svc)
	})

	t.Run("EnqueuesPeriodicJobs", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		require.NoError(t, svc.Start(ctx))

		// Should be no jobs to start.
		requireNJobs(t, bundle.dbPool, "periodic_job_500ms", 0)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.dbPool, "periodic_job_500ms", 1)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.dbPool, "periodic_job_500ms", 2)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.dbPool, "periodic_job_500ms", 3)
		requireNJobs(t, bundle.dbPool, "periodic_job_1500ms", 1)
	})

	t.Run("RunOnStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.periodicJobs = []*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s"), RunOnStart: true},
		}

		start := time.Now()
		require.NoError(t, svc.Start(ctx))

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle.dbPool, "periodic_job_5s", 1)

		// Should've happened quite quickly.
		require.WithinDuration(t, time.Now(), start, 1*time.Second)
	})

	t.Run("ErrNoJobToInsert", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.periodicJobs = []*PeriodicJob{
			// skip this insert when it returns nil:
			{ScheduleFunc: periodicIntervalSchedule(time.Second), ConstructorFunc: func() (*dbadapter.JobInsertParams, error) { return nil, ErrNoJobToInsert }, RunOnStart: true},
		}

		require.NoError(t, svc.Start(ctx))

		svc.TestSignals.SkippedJob.WaitOrTimeout()
	})

	t.Run("InitialScheduling", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		now := time.Now()
		svc.TimeNowUTC = func() time.Time { return now }

		svc.periodicJobs = []*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms")},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms")},
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s")},
			{ScheduleFunc: periodicIntervalSchedule(15 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_15m")},
			{ScheduleFunc: periodicIntervalSchedule(3 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_3h")},
			{ScheduleFunc: periodicIntervalSchedule(7 * 24 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_7d")},
		}

		// Randomize the order of jobs so that we can make sure that scheduling
		// behavior is correct regardless of how they're input.
		rand.Shuffle(len(svc.periodicJobs), func(i, j int) {
			svc.periodicJobs[i], svc.periodicJobs[j] = svc.periodicJobs[j], svc.periodicJobs[i]
		})

		require.NoError(t, svc.Start(ctx))

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		sortedPeriodicJobs := make([]*PeriodicJob, len(svc.periodicJobs))
		copy(sortedPeriodicJobs, svc.periodicJobs)
		slices.SortFunc(sortedPeriodicJobs, func(a, b *PeriodicJob) int { return a.ScheduleFunc(now).Compare(b.ScheduleFunc(now)) })

		require.Equal(t, now.Add(500*time.Millisecond), sortedPeriodicJobs[0].nextRunAt)
		require.Equal(t, now.Add(1500*time.Millisecond), sortedPeriodicJobs[1].nextRunAt)
		require.Equal(t, now.Add(5*time.Second), sortedPeriodicJobs[2].nextRunAt)
		require.Equal(t, now.Add(15*time.Minute), sortedPeriodicJobs[3].nextRunAt)
		require.Equal(t, now.Add(3*time.Hour), sortedPeriodicJobs[4].nextRunAt)
		require.Equal(t, now.Add(7*24*time.Hour), sortedPeriodicJobs[5].nextRunAt)

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

		scheduleDistantFuture(sortedPeriodicJobs[0])
		require.Equal(t, 1500*time.Millisecond, svc.timeUntilNextRun())

		scheduleDistantFuture(sortedPeriodicJobs[1])
		require.Equal(t, 5*time.Second, svc.timeUntilNextRun())

		scheduleDistantFuture(sortedPeriodicJobs[2])
		require.Equal(t, 15*time.Minute, svc.timeUntilNextRun())

		scheduleDistantFuture(sortedPeriodicJobs[3])
		require.Equal(t, 3*time.Hour, svc.timeUntilNextRun())

		scheduleDistantFuture(sortedPeriodicJobs[4])
		require.Equal(t, 7*24*time.Hour, svc.timeUntilNextRun())
	})

	// To ensure we are protected against runs that are supposed to have already happened,
	// this test uses a totally-not-safe schedule to enqueue every 0.5ms.
	t.Run("RapidScheduling", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.periodicJobs = []*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(time.Microsecond), ConstructorFunc: jobConstructorFunc("periodic_job_1us")},
		}
		// make a longer list of jobs so the loop has to run for longer
		for i := 1; i < 100; i++ {
			svc.periodicJobs = append(svc.periodicJobs,
				&PeriodicJob{
					ScheduleFunc:    periodicIntervalSchedule(time.Duration(i) * time.Hour),
					ConstructorFunc: jobConstructorFunc(fmt.Sprintf("periodic_job_%dh", i)),
				},
			)
		}

		require.NoError(t, svc.Start(ctx))

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		periodicJobs := make([]*PeriodicJob, len(svc.periodicJobs))
		copy(periodicJobs, svc.periodicJobs)

		for i := 0; i < 100; i++ {
			svc.TestSignals.InsertedJobs.WaitOrTimeout()
		}
	})

	t.Run("NoJobsConfigured", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.periodicJobs = []*PeriodicJob{}

		require.NoError(t, svc.Start(ctx))

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		require.LessOrEqual(t, svc.timeUntilNextRun(), 24*time.Hour)
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		require.NoError(t, svc.Start(ctx))
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
		riverinternaltest.WaitOrTimeout(t, stopped)
	})
}
