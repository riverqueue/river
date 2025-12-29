package maintenance

import (
	"context"
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
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

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
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.Config.Schema})
		require.NoError(t, err)
		require.Equal(t, job.State, newJob.State)
		return newJob
	}
	requireJobStateAvailable := func(t *testing.T, scheduler *JobScheduler, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.Config.Schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, newJob.State)
		return newJob
	}
	requireJobStateDiscardedWithMeta := func(t *testing.T, scheduler *JobScheduler, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: scheduler.Config.Schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, newJob.State)
		require.NotNil(t, newJob.FinalizedAt)
		require.Equal(t, "scheduler_discarded", gjson.GetBytes(newJob.Metadata, "unique_key_conflict").String())
		return newJob
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		scheduler := NewJobScheduler(riversharedtest.BaseServiceArchetype(t), &JobSchedulerConfig{}, nil)

		require.Equal(t, JobSchedulerIntervalDefault, scheduler.Config.Interval)
		require.Equal(t, riversharedmaintenance.BatchSizeDefault, scheduler.Config.Default)
		require.Equal(t, riversharedmaintenance.BatchSizeReduced, scheduler.Config.Reduced)
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
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: ptrutil.Ptr(now), State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: ptrutil.Ptr(now), State: ptrutil.Ptr(rivertype.JobStateCancelled)})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{FinalizedAt: ptrutil.Ptr(now), State: ptrutil.Ptr(rivertype.JobStateDiscarded)})
		job5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		scheduledJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		scheduledJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		scheduledJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(scheduler.Config.Interval - time.Millisecond))}) // won't be scheduled
		scheduledJob4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))})                             // won't be scheduled

		retryableJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		retryableJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		retryableJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))}) // won't be scheduled

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

		retryableJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("1"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		retryableJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("2"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		retryableJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("3"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))}) // dupe
		retryableJob4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("4"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))}) // dupe
		retryableJob5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("5"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))}) // dupe
		retryableJob6 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("6"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))}) // dupe
		retryableJob7 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("7"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))}) // dupe

		// Will cause conflicts with above jobs when retried:
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("3"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("4"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("5"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStatePending)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("6"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{UniqueKey: []byte("7"), UniqueStates: uniqueMap, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

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
		scheduler.Config.Default = 10 // reduced size for test speed

		now := time.Now().UTC()

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := scheduler.Config.Default + 1

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := range numJobs {
			jobState := rivertype.JobStateScheduled
			if i%2 == 0 {
				jobState = rivertype.JobStateRetryable
			}
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				Queue:       ptrutil.Ptr("scheduler_test"),
				State:       &jobState,
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
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
		scheduler.Config.Interval = 1 * time.Microsecond

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
		scheduler.Config.Interval = time.Minute // should only trigger once for the initial run

		require.NoError(t, scheduler.Start(ctx))
		scheduler.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.Config.Interval = time.Minute // should only trigger once for the initial run

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
		scheduler.Config.Interval = time.Minute // should only trigger once for the initial run
		now := time.Now().UTC()

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		scheduler.Stop()

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})

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
		scheduler.Config.Interval = time.Minute // should only trigger once for the initial run
		scheduler.Config.NotifyInsert = func(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error {
			notifyCh <- queues
			return nil
		}
		now := time.Now().UTC()

		addJob := func(queue string, fromNow time.Duration, state rivertype.JobState) {
			t.Helper()
			var finalizedAt *time.Time
			switch state { //nolint:exhaustive
			case rivertype.JobStateCompleted, rivertype.JobStateCancelled, rivertype.JobStateDiscarded:
				finalizedAt = ptrutil.Ptr(now.Add(fromNow))
			}
			testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: finalizedAt,
				Queue:       &queue,
				Schema:      schema,
				State:       &state,
				ScheduledAt: ptrutil.Ptr(now.Add(fromNow)),
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
		addJob("queue5", scheduler.Config.Interval-time.Millisecond, rivertype.JobStateRetryable)

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

	t.Run("QueuesIncluded", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)

		var (
			now = time.Now().UTC()

			notIncludedJob1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
			notIncludedJob2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

			includedQueue1 = "queue1"
			includedQueue2 = "queue2"

			includedJob1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: &includedQueue1, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
			includedJob2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: &includedQueue2, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		)

		scheduler.Config.QueuesIncluded = []string{includedQueue1, includedQueue2}

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateUnchanged(t, scheduler, bundle.exec, notIncludedJob1)
		requireJobStateUnchanged(t, scheduler, bundle.exec, notIncludedJob2)

		requireJobStateAvailable(t, scheduler, bundle.exec, includedJob1)
		requireJobStateAvailable(t, scheduler, bundle.exec, includedJob2)
	})
}
