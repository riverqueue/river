package maintenance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

func TestPeriodicJob(t *testing.T) {
	t.Parallel()

	validPeriodicJob := func() *PeriodicJob {
		return &PeriodicJob{
			ConstructorFunc: func() (*rivertype.JobInsertParams, error) { return nil, nil },
			ScheduleFunc:    func(t time.Time) time.Time { return time.Time{} },
		}
	}

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, validPeriodicJob().validate())
	})

	t.Run("IDTooLong", func(t *testing.T) {
		t.Parallel()

		periodicJob := validPeriodicJob()
		periodicJob.ID = strings.Repeat("a", 128)
		require.EqualError(t, periodicJob.validate(), "PeriodicJob.ID must be less than 128 characters")
	})

	t.Run("IDIllegalCharacters", func(t *testing.T) {
		t.Parallel()

		periodicJob := validPeriodicJob()
		periodicJob.ID = "shouldn't have spaces and stuff"
		require.EqualError(t, periodicJob.validate(), `PeriodicJob.ID "shouldn't have spaces and stuff" should match regex `+rivercommon.UserSpecifiedIDOrKindRE.String())
	})

	t.Run("ConstructorFuncMissing", func(t *testing.T) {
		t.Parallel()

		periodicJob := validPeriodicJob()
		periodicJob.ConstructorFunc = nil
		require.EqualError(t, periodicJob.validate(), "PeriodicJob.ConstructorFunc must be set")
	})

	t.Run("ScheduleFuncMissing", func(t *testing.T) {
		t.Parallel()

		periodicJob := validPeriodicJob()
		periodicJob.ScheduleFunc = nil
		require.EqualError(t, periodicJob.validate(), "PeriodicJob.ScheduleFunc must be set")
	})
}

type noOpArgs struct{}

func (noOpArgs) Kind() string { return "no_op" }

func TestPeriodicJobEnqueuer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec                 riverdriver.Executor
		notificationsByQueue map[string]int
		pilotMock            *PilotPeriodicJobMock
		schema               string
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
	makeInsertFunc := func(schema string) func(ctx context.Context, execTx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
		return func(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
			_, err := tx.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: sliceutil.Map(insertParams, func(params *rivertype.JobInsertParams) *riverdriver.JobInsertFastParams {
					return (*riverdriver.JobInsertFastParams)(params)
				}),
				Schema: schema,
			})
			return nil, err
		}
	}

	setup := func(t *testing.T) (*PeriodicJobEnqueuer, *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		bundle := &testBundle{
			exec:                 riverpgxv5.New(dbPool).GetExecutor(),
			notificationsByQueue: make(map[string]int),
			pilotMock:            NewPilotPeriodicJobMock(),
			schema:               schema,
			waitChan:             make(chan struct{}),
		}

		svc, err := NewPeriodicJobEnqueuer(riversharedtest.BaseServiceArchetype(t), &PeriodicJobEnqueuerConfig{
			Insert: makeInsertFunc(schema),
			Pilot:  bundle.pilotMock,
			Schema: schema,
		}, bundle.exec)
		require.NoError(t, err)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)

		return svc, bundle
	}

	requireNJobs := func(t *testing.T, bundle *testBundle, kind string, expectedNumJobs int) []*rivertype.JobRow {
		t.Helper()

		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{kind},
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, expectedNumJobs, "Expected to find exactly %d job(s) of kind: %s, but found %d", expectedNumJobs, kind, len(jobs))

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

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
		})
		require.NoError(t, err)

		startService(t, svc)

		// Should be no jobs to start.
		requireNJobs(t, bundle, "periodic_job_500ms", 0)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		insertedPeriodicJobs := requireNJobs(t, bundle, "periodic_job_500ms", 1)

		require.Empty(t, gjson.GetBytes(insertedPeriodicJobs[0].Metadata, rivercommon.MetadataKeyPeriodicJobID).Str) // no metadata set without periodic job ID

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 2)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 3)
		requireNJobs(t, bundle, "periodic_job_1500ms", 1)
	})

	t.Run("SetsPeriodicMetadataAttribute", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		jobConstructorWithMetadata := func(name string, metadata []byte) func() (*rivertype.JobInsertParams, error) {
			return func() (*rivertype.JobInsertParams, error) {
				params, err := jobConstructorFunc(name, false)()
				if err != nil {
					return nil, err
				}
				params.Metadata = metadata
				return params, nil
			}
		}

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorWithMetadata("p_md_nil", nil)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorWithMetadata("p_md_empty_string", []byte(""))},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorWithMetadata("p_md_empty_obj", []byte("{}"))},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorWithMetadata("p_md_existing", []byte(`{"key": "value"}`))},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		assertMetadata := func(name string, expected string) {
			job := requireNJobs(t, bundle, name, 1)[0]
			require.JSONEq(t, expected, string(job.Metadata))
		}

		assertMetadata("p_md_nil", `{"periodic": true}`)
		assertMetadata("p_md_empty_string", `{"periodic": true}`)
		assertMetadata("p_md_empty_obj", `{"periodic": true}`)
		assertMetadata("p_md_existing", `{"key": "value", "periodic": true}`)
	})

	t.Run("SetsScheduledAtAccordingToExpectedNextRunAt", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), RunOnStart: true},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		job1 := requireNJobs(t, bundle, "periodic_job_500ms", 1)[0]
		require.Equal(t, rivertype.JobStateAvailable, job1.State)
		require.WithinDuration(t, time.Now(), job1.ScheduledAt, 1*time.Second)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		job2 := requireNJobs(t, bundle, "periodic_job_500ms", 2)[1] // ordered by ID

		// The new `scheduled_at` is *exactly* the original `scheduled_at` plus
		// 500 milliseconds because the enqueuer used the target next run time
		// to calculate the new `scheduled_at`.
		require.Equal(t, job1.ScheduledAt.Add(500*time.Millisecond), job2.ScheduledAt)

		require.Equal(t, rivertype.JobStateAvailable, job2.State)
	})

	t.Run("RespectsJobUniqueness", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("unique_periodic_job_500ms", true)},
		})
		require.NoError(t, err)

		startService(t, svc)

		// Should be no jobs to start.
		requireNJobs(t, bundle, "unique_periodic_job_500ms", 0)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "unique_periodic_job_500ms", 1)

		// Another insert was attempted, but there's still only one job due to
		// uniqueness conditions.
		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "unique_periodic_job_500ms", 1)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "unique_periodic_job_500ms", 1)
	})

	t.Run("RunOnStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s", false), RunOnStart: true},
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("unique_periodic_job_5s", true), RunOnStart: true},
		})
		require.NoError(t, err)

		start := time.Now()
		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_5s", 1)
		requireNJobs(t, bundle, "unique_periodic_job_5s", 1)

		// Should've happened quite quickly.
		require.WithinDuration(t, time.Now(), start, 1*time.Second)
	})

	t.Run("ErrNoJobToInsert", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			// skip this insert when it returns nil:
			{ScheduleFunc: periodicIntervalSchedule(time.Second), ConstructorFunc: func() (*rivertype.JobInsertParams, error) {
				return nil, ErrNoJobToInsert
			}, RunOnStart: true},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.SkippedJob.WaitOrTimeout()
	})

	t.Run("InitialScheduling", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		now := svc.Time.StubNowUTC(time.Now())

		svc.periodicJobs = make(map[rivertype.PeriodicJobHandle]*PeriodicJob)
		periodicJobHandles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(5 * time.Second), ConstructorFunc: jobConstructorFunc("periodic_job_5s", false)},
			{ScheduleFunc: periodicIntervalSchedule(15 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_15m", false)},
			{ScheduleFunc: periodicIntervalSchedule(3 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_3h", false)},
			{ScheduleFunc: periodicIntervalSchedule(7 * 24 * time.Hour), ConstructorFunc: jobConstructorFunc("periodic_job_7d", false)},
		})
		require.NoError(t, err)

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

		_, err := svc.AddSafely(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Microsecond), ConstructorFunc: jobConstructorFunc("periodic_job_1us", false)})
		require.NoError(t, err)

		// make a longer list of jobs so the loop has to run for longer
		for i := 1; i < 100; i++ {
			_, err := svc.AddSafely(&PeriodicJob{
				ScheduleFunc:    periodicIntervalSchedule(time.Duration(i) * time.Hour),
				ConstructorFunc: jobConstructorFunc(fmt.Sprintf("periodic_job_%dh", i), false),
			})
			require.NoError(t, err)
		}

		startService(t, svc)

		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		for range 100 {
			svc.TestSignals.InsertedJobs.WaitOrTimeout()
		}
	})

	t.Run("ConfigurableViaConstructor", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		svc, err := NewPeriodicJobEnqueuer(
			riversharedtest.BaseServiceArchetype(t),
			&PeriodicJobEnqueuerConfig{
				Insert: makeInsertFunc(bundle.schema),
				PeriodicJobs: []*PeriodicJob{
					{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), RunOnStart: true},
					{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false), RunOnStart: true},
				},
			}, bundle.exec)
		require.NoError(t, err)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 1)
		requireNJobs(t, bundle, "periodic_job_1500ms", 1)
	})

	t.Run("AddAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		_, err := svc.AddSafely(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
		)
		require.NoError(t, err)
		_, err = svc.AddSafely(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		)
		require.NoError(t, err)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_start", 1)
	})

	t.Run("AddManyAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})
		require.NoError(t, err)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_start", 1)
	})

	t.Run("ClearAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})
		require.NoError(t, err)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_start", 1)

		svc.Clear()

		require.Empty(t, svc.periodicJobIDs)
		require.Empty(t, svc.periodicJobs)

		handleAfterClear, err := svc.AddSafely(
			&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_new", false)},
		)
		require.NoError(t, err)

		// Handles are not reused.
		require.NotEqual(t, handles[0], handleAfterClear)
		require.NotEqual(t, handles[1], handleAfterClear)
	})

	t.Run("RemoveAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})
		require.NoError(t, err)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_start", 1)

		svc.Remove(handles[1])

		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveWithID", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), ID: "periodic_job_500ms"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), ID: "periodic_job_500ms_start", RunOnStart: true},
		})
		require.NoError(t, err)

		svc.Remove(handles[1])

		require.Len(t, svc.periodicJobIDs, 1)
		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveByID", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), ID: "periodic_job_500ms"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), ID: "periodic_job_500ms_start", RunOnStart: true},
		})
		require.NoError(t, err)

		require.True(t, svc.RemoveByID("periodic_job_500ms_start"))
		require.False(t, svc.RemoveByID("does_not_exist"))

		require.Contains(t, svc.periodicJobIDs, "periodic_job_500ms")
		require.NotContains(t, svc.periodicJobIDs, "periodic_job_500ms_start")

		require.Len(t, svc.periodicJobIDs, 1)
		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveManyAfterStart", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		startService(t, svc)

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_other", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), RunOnStart: true},
		})
		require.NoError(t, err)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_other", 0)
		requireNJobs(t, bundle, "periodic_job_500ms_start", 1)

		svc.RemoveMany([]rivertype.PeriodicJobHandle{handles[1], handles[2]})

		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveManyWithID", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), ID: "periodic_job_500ms"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_other", false), ID: "periodic_job_500ms_other"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), ID: "periodic_job_500ms_start", RunOnStart: true},
		})
		require.NoError(t, err)

		svc.RemoveMany([]rivertype.PeriodicJobHandle{handles[1], handles[2]})

		require.Contains(t, svc.periodicJobIDs, "periodic_job_500ms")
		require.NotContains(t, svc.periodicJobIDs, "periodic_job_500ms_other")
		require.NotContains(t, svc.periodicJobIDs, "periodic_job_500ms_start")

		require.Len(t, svc.periodicJobIDs, 1)
		require.Len(t, svc.periodicJobs, 1)
	})

	t.Run("RemoveManyByID", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false), ID: "periodic_job_500ms"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_other", false), ID: "periodic_job_500ms_other"},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms_start", false), ID: "periodic_job_500ms_start", RunOnStart: true},
		})
		require.NoError(t, err)

		svc.RemoveManyByID([]string{"periodic_job_500ms_other", "periodic_job_500ms_start", "does_not_exist"})

		require.Contains(t, svc.periodicJobIDs, "periodic_job_500ms")
		require.NotContains(t, svc.periodicJobIDs, "periodic_job_500ms_other")
		require.NotContains(t, svc.periodicJobIDs, "periodic_job_500ms_start")

		require.Len(t, svc.periodicJobIDs, 1)
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

		for i := range 10 {
			wg.Add(1)

			jobBaseName := fmt.Sprintf("periodic_job_1ms_%02d", i)

			go func() {
				defer wg.Done()

				for range 50 {
					handle, err := svc.AddSafely(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Millisecond), ConstructorFunc: jobConstructorFunc(jobBaseName, false)})
					require.NoError(t, err)
					randomSleep()

					_, err = svc.AddSafely(&PeriodicJob{ScheduleFunc: periodicIntervalSchedule(time.Millisecond), ConstructorFunc: jobConstructorFunc(jobBaseName+"_second", false)})
					require.NoError(t, err)
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

	t.Run("InvokesPilot", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		now := time.Now()

		bundle.pilotMock.PeriodicJobGetAllMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
			require.Equal(t, bundle.schema, params.Schema)
			return []*riverpilot.PeriodicJob{
				{ID: "periodic_job_500ms", NextRunAt: now.Add(1 * time.Hour)},
				{ID: "periodic_job_1500ms", NextRunAt: now.Add(2 * time.Hour)},
				{ID: "periodic_job_999ms", NextRunAt: now.Add(3 * time.Hour)},
			}, nil
		}

		var periodicJobKeepAliveAndReapMockCalled bool
		bundle.pilotMock.PeriodicJobKeepAliveAndReapMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
			periodicJobKeepAliveAndReapMockCalled = true
			require.ElementsMatch(t, []string{"periodic_job_100ms", "periodic_job_500ms", "periodic_job_1500ms"}, params.ID)
			require.Equal(t, bundle.schema, params.Schema)
			return nil, nil
		}

		var insertedPeriodicJobIDs [][]string
		bundle.pilotMock.PeriodicJobUpsertManyMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
			insertedPeriodicJobIDs = append(insertedPeriodicJobIDs, sliceutil.Map(params.Jobs, func(j *riverpilot.PeriodicJobUpsertParams) string { return j.ID }))
			require.Equal(t, bundle.schema, params.Schema)
			for _, job := range params.Jobs {
				require.NotZero(t, job.NextRunAt)
				require.NotZero(t, job.UpdatedAt)
			}
			return nil, nil
		}

		handles, err := svc.AddManySafely([]*PeriodicJob{
			{ID: "periodic_job_100ms", ScheduleFunc: periodicIntervalSchedule(100 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_100ms", false)},
			{ID: "periodic_job_500ms", ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ID: "periodic_job_1500ms", ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		// periodic_job_100ms runs immediately because it didn't have a
		// persisted record from PeriodicJobGetAllMock
		insertedPeriodicJobs := requireNJobs(t, bundle, "periodic_job_100ms", 1)
		requireNJobs(t, bundle, "periodic_job_500ms", 0)
		requireNJobs(t, bundle, "periodic_job_1500ms", 0)

		require.Equal(t, "periodic_job_100ms", gjson.GetBytes(insertedPeriodicJobs[0].Metadata, rivercommon.MetadataKeyPeriodicJobID).Str)

		// During the first invocation periodic job records for all three jobs
		// are inserted (this happens on start up), then after one run we expect
		// only an insertion for the job that actually ran.
		require.Equal(t, [][]string{
			{"periodic_job_100ms", "periodic_job_500ms", "periodic_job_1500ms"},
			{"periodic_job_100ms"},
		}, insertedPeriodicJobIDs)

		svc.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
		require.True(t, periodicJobKeepAliveAndReapMockCalled)

		svc.Stop()

		require.WithinDuration(t, now.Add(1*time.Hour), svc.periodicJobs[handles[1]].nextRunAt, time.Microsecond)
		require.WithinDuration(t, now.Add(2*time.Hour), svc.periodicJobs[handles[2]].nextRunAt, time.Microsecond)
	})

	t.Run("PilotNotInvokedWithoutID", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		bundle.pilotMock.PeriodicJobGetAllMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, nil
		}

		var periodicJobKeepAliveAndReapMockCalled bool
		bundle.pilotMock.PeriodicJobKeepAliveAndReapMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
			periodicJobKeepAliveAndReapMockCalled = true
			return nil, nil
		}

		var periodicJobUpsertManyMockCalled bool
		bundle.pilotMock.PeriodicJobUpsertManyMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
			periodicJobUpsertManyMockCalled = true
			return nil, nil
		}

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ScheduleFunc: periodicIntervalSchedule(100 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_100ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
			{ScheduleFunc: periodicIntervalSchedule(1500 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_1500ms", false)},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()
		requireNJobs(t, bundle, "periodic_job_100ms", 1)
		require.False(t, periodicJobUpsertManyMockCalled)

		svc.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
		require.False(t, periodicJobKeepAliveAndReapMockCalled)
	})

	t.Run("PeriodicJobsWithIDAlwaysUpserted", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		bundle.pilotMock.PeriodicJobGetAllMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
			return []*riverpilot.PeriodicJob{}, nil
		}

		bundle.pilotMock.PeriodicJobKeepAliveAndReapMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, nil
		}

		var insertedPeriodicJobIDs [][]string
		bundle.pilotMock.PeriodicJobUpsertManyMock = func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
			insertedPeriodicJobIDs = append(insertedPeriodicJobIDs, sliceutil.Map(params.Jobs, func(j *riverpilot.PeriodicJobUpsertParams) string { return j.ID }))
			return nil, nil
		}

		_, err := svc.AddManySafely([]*PeriodicJob{
			{ID: "periodic_job_10m", ScheduleFunc: periodicIntervalSchedule(10 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_10m", false)},
			{ID: "periodic_job_20m", ScheduleFunc: periodicIntervalSchedule(20 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_20m", false)},

			// this one doesn't have an ID and won't get an initial insert
			{ScheduleFunc: periodicIntervalSchedule(30 * time.Minute), ConstructorFunc: jobConstructorFunc("periodic_job_30m", false)},
		})
		require.NoError(t, err)

		startService(t, svc)

		svc.TestSignals.PeriodicJobUpserted.WaitOrTimeout()

		require.Equal(t, [][]string{
			{"periodic_job_10m", "periodic_job_20m"},
		}, insertedPeriodicJobIDs)
	})

	t.Run("DuplicateIDError", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		periodicJobs := []*PeriodicJob{
			{ID: "periodic_job_100ms", ScheduleFunc: periodicIntervalSchedule(100 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_100ms", false)},
			{ID: "periodic_job_100ms", ScheduleFunc: periodicIntervalSchedule(100 * time.Millisecond), ConstructorFunc: jobConstructorFunc("periodic_job_500ms", false)},
		}

		_, err := NewPeriodicJobEnqueuer(riversharedtest.BaseServiceArchetype(t), &PeriodicJobEnqueuerConfig{
			Insert:       makeInsertFunc(bundle.schema),
			PeriodicJobs: periodicJobs,
			Pilot:        bundle.pilotMock,
			Schema:       bundle.schema,
		}, bundle.exec)
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_100ms")

		_, err = svc.AddManySafely(periodicJobs)
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_100ms")
	})
}

type PilotPeriodicJobMock struct {
	PeriodicJobGetAllMock           func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error)
	PeriodicJobKeepAliveAndReapMock func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error)
	PeriodicJobUpsertManyMock       func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error)
}

func NewPilotPeriodicJobMock() *PilotPeriodicJobMock {
	return &PilotPeriodicJobMock{
		PeriodicJobGetAllMock: func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, nil
		},
		PeriodicJobKeepAliveAndReapMock: func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, nil
		},
		PeriodicJobUpsertManyMock: func(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, nil
		},
	}
}

func (p *PilotPeriodicJobMock) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
	return p.PeriodicJobGetAllMock(ctx, exec, params)
}

func (p *PilotPeriodicJobMock) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
	return p.PeriodicJobKeepAliveAndReapMock(ctx, exec, params)
}

func (p *PilotPeriodicJobMock) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
	return p.PeriodicJobUpsertManyMock(ctx, exec, params)
}
