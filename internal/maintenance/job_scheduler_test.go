package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

type jobSchedulerExecutorMock struct {
	riverdriver.ExecutorTx

	TestSignals     jobSchedulerExecutorMockTestSignals
	jobScheduleFunc func(context.Context, *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error)
}

func (e *jobSchedulerExecutorMock) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	return e, nil
}

func (e *jobSchedulerExecutorMock) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	e.TestSignals.JobScheduleStarted.Signal(struct{}{})
	return e.jobScheduleFunc(ctx, params)
}

func (e *jobSchedulerExecutorMock) Rollback(ctx context.Context) error {
	return nil
}

type jobSchedulerExecutorMockTestSignals struct {
	JobScheduleStarted testsignal.TestSignal[struct{}]
}

func (ts *jobSchedulerExecutorMockTestSignals) Init(t *testing.T) {
	t.Helper()

	ts.JobScheduleStarted.Init(t)
}

func TestJobScheduler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec                 riverdriver.Executor
		notificationsByQueue map[string]int
	}

	type testOpts struct {
		exec   riverdriver.Executor
		schema string // for use when using a non-TestTx
	}

	setup := func(t *testing.T, opts *testOpts) (*JobScheduler, *testBundle) {
		t.Helper()

		archetype := riversharedtest.BaseServiceArchetype(t)

		bundle := &testBundle{
			exec:                 opts.exec,
			notificationsByQueue: make(map[string]int),
		}

		scheduler := NewJobScheduler(
			archetype,
			&JobSchedulerConfig{
				Interval: JobSchedulerIntervalDefault,
				NotifyInsert: func(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error {
					for _, queue := range queues {
						bundle.notificationsByQueue[queue]++
					}
					return nil
				},
				Schema: opts.schema,
			},
			bundle.exec)
		scheduler.TestSignals.Init(t)
		t.Cleanup(scheduler.Stop)

		return scheduler, bundle
	}

	setupTx := func(t *testing.T) (*JobScheduler, *testBundle) {
		t.Helper()
		tx := riverdbtest.TestTxPgx(ctx, t)
		return setup(t, &testOpts{exec: riverpgxv5.New(nil).UnwrapExecutor(tx)})
	}

	requireJobStateUnchanged := func(t *testing.T, scheduler *JobScheduler, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.config.Schema})
		require.NoError(t, err)
		require.Equal(t, job.State, newJob.State)
		return newJob
	}
	requireJobStateAvailable := func(t *testing.T, scheduler *JobScheduler, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.config.Schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, newJob.State)
		return newJob
	}
	requireJobStateDiscardedWithMeta := func(t *testing.T, scheduler *JobScheduler, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.config.Schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, newJob.State)
		require.NotNil(t, newJob.FinalizedAt)
		require.Equal(t, "scheduler_discarded", gjson.GetBytes(newJob.Metadata, "unique_key_conflict").String())
		return newJob
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		scheduler := NewJobScheduler(riversharedtest.BaseServiceArchetype(t), &JobSchedulerConfig{}, nil)

		require.Equal(t, JobSchedulerIntervalDefault, scheduler.config.Interval)
		require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.config.Default)
		require.Equal(t, riversharedmaintenance.BatchSizeReduced, scheduler.config.Reduced)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.Logger = riversharedtest.LoggerWarn(t)  // loop started/stop log is very noisy; suppress
		scheduler.TestSignals = JobSchedulerTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, scheduler)
	})

	t.Run("SchedulesScheduledAndRetryableJobs", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		now := time.Now().UTC()

		// none of these should get updated
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: new(now), State: new(rivertype.JobStateCompleted)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: new(now), State: new(rivertype.JobStateCancelled)})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: new(now), State: new(rivertype.JobStateDiscarded)})
		job5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateAvailable)})

		scheduledJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateScheduled), ScheduledAt: new(now.Add(-1 * time.Hour))})
		scheduledJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateScheduled), ScheduledAt: new(now.Add(-5 * time.Second))})
		scheduledJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateScheduled), ScheduledAt: new(now.Add(scheduler.config.Interval - time.Millisecond))}) // won't be scheduled
		scheduledJob4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateScheduled), ScheduledAt: new(now.Add(30 * time.Second))})                             // won't be scheduled

		retryableJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-1 * time.Hour))})
		retryableJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))})
		retryableJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(30 * time.Second))}) // won't be scheduled

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateUnchanged(t, scheduler, bundle.exec, job1)
		requireJobStateUnchanged(t, scheduler, bundle.exec, job2)
		requireJobStateUnchanged(t, scheduler, bundle.exec, job3)
		requireJobStateUnchanged(t, scheduler, bundle.exec, job4)
		requireJobStateUnchanged(t, scheduler, bundle.exec, job5)

		requireJobStateAvailable(t, scheduler, bundle.exec, scheduledJob1)
		requireJobStateAvailable(t, scheduler, bundle.exec, scheduledJob2)
		requireJobStateAvailable(t, scheduler, bundle.exec, scheduledJob3)
		requireJobStateUnchanged(t, scheduler, bundle.exec, scheduledJob4) // still scheduled

		requireJobStateAvailable(t, scheduler, bundle.exec, retryableJob1)
		requireJobStateAvailable(t, scheduler, bundle.exec, retryableJob2)
		requireJobStateUnchanged(t, scheduler, bundle.exec, retryableJob3) // still retryable
	})

	t.Run("MovesUniqueKeyConflictingJobsToDiscarded", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		now := time.Now().UTC()

		// The list of default states, but without retryable to allow for dupes in that state:
		uniqueStates := []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCompleted,
			rivertype.JobStatePending,
			rivertype.JobStateRunning,
			rivertype.JobStateScheduled,
		}
		uniqueMap := uniquestates.UniqueStatesToBitmask(uniqueStates)

		retryableJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("1"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-1 * time.Hour))})
		retryableJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("2"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))})
		retryableJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("3"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))}) // dupe
		retryableJob4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("4"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))}) // dupe
		retryableJob5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("5"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))}) // dupe
		retryableJob6 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("6"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))}) // dupe
		retryableJob7 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("7"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-5 * time.Second))}) // dupe

		// Will cause conflicts with above jobs when retried:
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("3"), UniqueStates: uniqueMap, State: new(rivertype.JobStateAvailable)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("4"), UniqueStates: uniqueMap, State: new(rivertype.JobStateCompleted)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("5"), UniqueStates: uniqueMap, State: new(rivertype.JobStatePending)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("6"), UniqueStates: uniqueMap, State: new(rivertype.JobStateRunning)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("7"), UniqueStates: uniqueMap, State: new(rivertype.JobStateScheduled)})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateAvailable(t, scheduler, bundle.exec, retryableJob1)
		requireJobStateAvailable(t, scheduler, bundle.exec, retryableJob2)
		requireJobStateDiscardedWithMeta(t, scheduler, bundle.exec, retryableJob3)
		requireJobStateDiscardedWithMeta(t, scheduler, bundle.exec, retryableJob4)
		requireJobStateDiscardedWithMeta(t, scheduler, bundle.exec, retryableJob5)
		requireJobStateDiscardedWithMeta(t, scheduler, bundle.exec, retryableJob6)
		requireJobStateDiscardedWithMeta(t, scheduler, bundle.exec, retryableJob7)
	})

	t.Run("SchedulesInBatches", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		scheduler.config.Default = 10 // reduced size for test speed

		now := time.Now().UTC()

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := scheduler.config.Default + 1

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := range numJobs {
			jobState := rivertype.JobStateScheduled
			if i%2 == 0 {
				jobState = rivertype.JobStateRetryable
			}
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				Queue:       new("scheduler_test"),
				State:       &jobState,
				ScheduledAt: new(now.Add(-1 * time.Hour)),
			})
			jobs[i] = job
		}

		require.NoError(t, scheduler.Start(ctx))

		// See comment above. Exactly two batches are expected.
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		for _, job := range jobs {
			requireJobStateAvailable(t, scheduler, bundle.exec, job)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.config.Interval = 1 * time.Microsecond

		require.NoError(t, scheduler.Start(ctx))

		// This should trigger ~immediately every time:
		for i := range 5 {
			t.Logf("Iteration %d", i)
			scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()
		}
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run

		require.NoError(t, scheduler.Start(ctx))
		scheduler.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run

		ctx, cancelFunc := context.WithCancel(ctx)

		require.NoError(t, scheduler.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := scheduler.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run
		now := time.Now().UTC()

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateScheduled), ScheduledAt: new(now.Add(-1 * time.Hour))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		scheduler.Stop()

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRetryable), ScheduledAt: new(now.Add(-1 * time.Minute))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateAvailable(t, scheduler, bundle.exec, job1)
		requireJobStateAvailable(t, scheduler, bundle.exec, job2)
	})

	t.Run("TriggersNotificationsOnEachQueueWithNewlyAvailableJobs", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			exec   = driver.GetExecutor()
		)
		notifyCh := make(chan []string, 10)

		scheduler, _ := setup(t, &testOpts{exec: exec, schema: schema})
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run
		scheduler.config.NotifyInsert = func(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error {
			notifyCh <- queues
			return nil
		}
		now := time.Now().UTC()

		addJob := func(queue string, fromNow time.Duration, state rivertype.JobState) {
			t.Helper()
			var finalizedAt *time.Time
			switch state {
			case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateRunning, rivertype.JobStateScheduled:
			case rivertype.JobStateCompleted, rivertype.JobStateCancelled, rivertype.JobStateDiscarded:
				finalizedAt = new(now.Add(fromNow))
			}
			testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: finalizedAt,
				Queue:       &queue,
				Schema:      schema,
				State:       &state,
				ScheduledAt: new(now.Add(fromNow)),
			})
		}

		addJob("queue1", -1*time.Hour, rivertype.JobStateScheduled)
		addJob("queue2", -1*time.Minute, rivertype.JobStateScheduled)
		// deduplication is handled in client, so this dupe should appear:
		addJob("queue2", -5*time.Second, rivertype.JobStateScheduled)
		addJob("queue3", -30*time.Second, rivertype.JobStateRetryable)
		// This one is scheduled only a millisecond in the future, so it should
		// trigger a notification:
		addJob("queue4", time.Millisecond, rivertype.JobStateRetryable)

		// these shouldn't cause notifications:
		addJob("future_queue", 2*time.Minute, rivertype.JobStateScheduled)     // it's in the future
		addJob("other_status_queue", time.Minute, rivertype.JobStateCancelled) // it's cancelled
		// This one is scheduled in the future, just barely before the next run, so it should
		// be scheduled but shouldn't trigger a notification:
		addJob("queue5", scheduler.config.Interval-time.Millisecond, rivertype.JobStateRetryable)

		// Run the scheduler and wait for it to execute once:
		require.NoError(t, scheduler.Start(ctx))
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		notifiedQueues := riversharedtest.WaitOrTimeout(t, notifyCh)
		sort.Strings(notifiedQueues)
		require.Equal(t, []string{"queue1", "queue2", "queue2", "queue3", "queue4"}, notifiedQueues)
	})

	t.Run("ReducedBatchSizeBreakerTrips", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)

		ctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()

		// Starts at default batch size.
		require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())

		for range scheduler.reducedBatchSizeBreaker.Limit() - 1 {
			_, err := scheduler.runOnce(ctx)
			require.Error(t, err)

			// Circuit not broken yet so we stay at default batch size.
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())
		}

		_, err := scheduler.runOnce(ctx)
		require.Error(t, err)

		// Circuit now broken. Reduced batch size.
		require.Equal(t, riversharedmaintenance.BatchSizeReduced, scheduler.batchSize())
	})

	t.Run("ReducedBatchSizeBreakerResetsOnSuccess", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		scheduler, _ := setupTx(t)

		{
			ctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
			defer cancel()

			// Starts at default batch size.
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())

			for range scheduler.reducedBatchSizeBreaker.Limit() - 1 {
				_, err := scheduler.runOnce(ctx)
				require.Error(t, err)

				// Circuit not broken yet so we stay at default batch size.
				require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())
			}
		}

		// Context has not been cancelled for this call so it succeeds.
		_, err := scheduler.runOnce(ctx)
		require.NoError(t, err)

		require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())

		// Because of the success above, the circuit breaker resets. N - 1
		// failures are allowed again before it breaks.
		{
			ctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
			defer cancel()

			// Starts at default batch size.
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())

			for range scheduler.reducedBatchSizeBreaker.Limit() - 1 {
				_, err := scheduler.runOnce(ctx)
				require.Error(t, err)

				// Circuit not broken yet so we stay at default batch size.
				require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.batchSize())
			}
		}
	})
}

func TestJobScheduler_ErrorLogging(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		exec      *jobSchedulerExecutorMock
		logOutput bytes.Buffer
		scheduler *JobScheduler
	}

	// pgx can return a write timeout instead of context.Canceled when shutdown
	// interrupts a query write. Preserve that error shape in the regression.
	writeErr := fmt.Errorf("write failed: %w", os.ErrDeadlineExceeded)
	require.NotErrorIs(t, writeErr, context.Canceled)

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		bundle := &testBundle{exec: &jobSchedulerExecutorMock{}}
		bundle.exec.TestSignals.Init(t)
		bundle.exec.jobScheduleFunc = func(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
			<-ctx.Done()
			return nil, writeErr
		}

		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Logger = slog.New(slog.NewTextHandler(&bundle.logOutput, &slog.HandlerOptions{Level: slog.LevelError}))
		bundle.scheduler = NewJobScheduler(archetype, &JobSchedulerConfig{Interval: time.Millisecond}, bundle.exec)
		bundle.scheduler.StaggerStartupDisable(true)
		t.Cleanup(bundle.scheduler.Stop)

		return bundle
	}

	t.Run("ContextCancellationWithoutLogging", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		require.NoError(t, bundle.scheduler.Start(ctx))
		bundle.exec.TestSignals.JobScheduleStarted.WaitOrTimeout()

		cancelFunc()
		bundle.scheduler.Stop()
		require.Empty(t, bundle.logOutput.String())
	})

	t.Run("LogsDatabaseErrors", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var numCalls int
		bundle.exec.jobScheduleFunc = func(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
			numCalls++
			if numCalls > 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, writeErr
		}

		require.NoError(t, bundle.scheduler.Start(context.Background()))
		bundle.exec.TestSignals.JobScheduleStarted.WaitOrTimeout()
		// The next query starts only after the first error has been logged.
		bundle.exec.TestSignals.JobScheduleStarted.WaitOrTimeout()
		bundle.scheduler.Stop()
		require.Contains(t, bundle.logOutput.String(), "Error scheduling jobs")
		require.Contains(t, bundle.logOutput.String(), writeErr.Error())
	})

	t.Run("StopsWithoutLogging", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		require.NoError(t, bundle.scheduler.Start(context.Background()))
		bundle.exec.TestSignals.JobScheduleStarted.WaitOrTimeout()

		bundle.scheduler.Stop()
		require.Empty(t, bundle.logOutput.String())
	})
}
