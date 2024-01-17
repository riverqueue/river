package maintenance

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/internal/componentstatus"
	"weavelab.xyz/river/internal/dbsqlc"
	"weavelab.xyz/river/internal/notifier"
	"weavelab.xyz/river/internal/rivercommon"
	"weavelab.xyz/river/internal/riverinternaltest"
	"weavelab.xyz/river/internal/util/dbutil"
	"weavelab.xyz/river/internal/util/ptrutil"
	"weavelab.xyz/river/internal/util/valutil"
)

func TestScheduler(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		ex dbutil.Executor
	}

	type insertJobParams struct {
		Queue       string
		ScheduledAt *time.Time
		State       dbsqlc.JobState
	}

	insertJob := func(ctx context.Context, dbtx dbsqlc.DBTX, params insertJobParams) *dbsqlc.RiverJob {
		job, err := queries.JobInsert(ctx, dbtx, dbsqlc.JobInsertParams{
			Kind:        "test_kind",
			MaxAttempts: int16(rivercommon.MaxAttemptsDefault),
			Priority:    int16(rivercommon.PriorityDefault),
			Queue:       valutil.ValOrDefault(params.Queue, "default"),
			ScheduledAt: params.ScheduledAt,
			State:       params.State,
		})
		require.NoError(t, err)
		return job
	}

	setup := func(t *testing.T, ex dbutil.Executor) (*Scheduler, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			ex: ex,
		}

		scheduler := NewScheduler(
			riverinternaltest.BaseServiceArchetype(t),
			&SchedulerConfig{
				Interval: SchedulerIntervalDefault,
				Limit:    10,
			},
			bundle.ex)
		scheduler.TestSignals.Init()
		t.Cleanup(scheduler.Stop)

		return scheduler, bundle
	}

	setupTx := func(t *testing.T) (*Scheduler, *testBundle) {
		t.Helper()
		return setup(t, riverinternaltest.TestTx(ctx, t))
	}

	requireJobStateUnchanged := func(t *testing.T, ex dbutil.Executor, job *dbsqlc.RiverJob) *dbsqlc.RiverJob {
		t.Helper()
		newJob, err := queries.JobGetByID(ctx, ex, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.State, newJob.State)
		return newJob
	}
	requireJobStateAvailable := func(t *testing.T, ex dbutil.Executor, job *dbsqlc.RiverJob) *dbsqlc.RiverJob {
		t.Helper()
		newJob, err := queries.JobGetByID(ctx, ex, job.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateAvailable, newJob.State)
		return newJob
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		scheduler := NewScheduler(riverinternaltest.BaseServiceArchetype(t), &SchedulerConfig{}, nil)

		require.Equal(t, SchedulerIntervalDefault, scheduler.config.Interval)
		require.Equal(t, SchedulerLimitDefault, scheduler.config.Limit)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		scheduler.TestSignals = SchedulerTestSignals{}     // deinit so channels don't fill

		runStartStopStress(ctx, t, scheduler)
	})

	t.Run("SchedulesScheduledAndRetryableJobs", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)

		// none of these should get updated
		job1 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateCompleted})
		job2 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateRunning})
		job3 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateCancelled})
		job4 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateDiscarded})
		job5 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateAvailable})

		now := time.Now().UTC()

		scheduledJob1 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		scheduledJob2 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		scheduledJob3 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))}) // won't be scheduled

		retryableJob1 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateRetryable, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		retryableJob2 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateRetryable, ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		retryableJob3 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateRetryable, ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))}) // won't be scheduled

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateUnchanged(t, bundle.ex, job1)
		requireJobStateUnchanged(t, bundle.ex, job2)
		requireJobStateUnchanged(t, bundle.ex, job3)
		requireJobStateUnchanged(t, bundle.ex, job4)
		requireJobStateUnchanged(t, bundle.ex, job5)

		requireJobStateAvailable(t, bundle.ex, scheduledJob1)
		requireJobStateAvailable(t, bundle.ex, scheduledJob2)
		requireJobStateUnchanged(t, bundle.ex, scheduledJob3) // still scheduled

		requireJobStateAvailable(t, bundle.ex, retryableJob1)
		requireJobStateAvailable(t, bundle.ex, retryableJob2)
		requireJobStateUnchanged(t, bundle.ex, retryableJob3) // still retryable
	})

	t.Run("SchedulesInBatches", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		scheduler.config.Limit = 10 // reduced size for test speed

		now := time.Now().UTC()

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := scheduler.config.Limit + 1

		jobs := make([]*dbsqlc.RiverJob, numJobs)

		for i := 0; i < numJobs; i++ {
			jobState := dbsqlc.JobStateScheduled
			if i%2 == 0 {
				jobState = dbsqlc.JobStateRetryable
			}
			job := insertJob(ctx, bundle.ex, insertJobParams{State: jobState, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, scheduler.Start(ctx))

		// See comment above. Exactly two batches are expected.
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		for _, job := range jobs {
			requireJobStateAvailable(t, bundle.ex, job)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.config.Interval = 1 * time.Microsecond

		require.NoError(t, scheduler.Start(ctx))

		// This should trigger ~immediately every time:
		for i := 0; i < 5; i++ {
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
		riverinternaltest.WaitOrTimeout(t, stopped)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run
		now := time.Now().UTC()

		job1 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		scheduler.Stop()

		job2 := insertJob(ctx, bundle.ex, insertJobParams{State: dbsqlc.JobStateRetryable, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateAvailable(t, bundle.ex, job1)
		requireJobStateAvailable(t, bundle.ex, job2)
	})

	t.Run("TriggersNotificationsOnEachQueueWithNewlyAvailableJobs", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)
		scheduler, _ := setup(t, dbPool)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run
		now := time.Now().UTC()

		statusUpdateCh := make(chan componentstatus.Status, 10)
		statusUpdate := func(status componentstatus.Status) {
			statusUpdateCh <- status
		}
		notify := notifier.New(&scheduler.Archetype, dbPool.Config().ConnConfig, statusUpdate, riverinternaltest.Logger(t))

		// Scope in so we can reuse ctx without the cancel embedded.
		{
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go notify.Run(ctx)
		}

		type insertPayload struct {
			Queue string `json:"queue"`
		}
		type notification struct {
			topic   notifier.NotificationTopic
			payload insertPayload
		}
		notifyCh := make(chan notification, 10)
		handleNotification := func(topic notifier.NotificationTopic, payload string) {
			notif := notification{topic: topic}
			require.NoError(t, json.Unmarshal([]byte(payload), &notif.payload))
			notifyCh <- notif
		}
		sub := notify.Listen(notifier.NotificationTopicInsert, handleNotification)
		defer sub.Unlisten()

		for {
			status := riverinternaltest.WaitOrTimeout(t, statusUpdateCh)
			if status == componentstatus.Healthy {
				break
			}
		}

		addJob := func(queue string, fromNow time.Duration, state dbsqlc.JobState) {
			t.Helper()
			insertJob(ctx, dbPool, insertJobParams{Queue: queue, State: state, ScheduledAt: ptrutil.Ptr(now.Add(fromNow))})
		}

		addJob("queue1", -1*time.Hour, dbsqlc.JobStateScheduled)
		addJob("queue2", -1*time.Minute, dbsqlc.JobStateScheduled)
		addJob("queue3", -30*time.Second, dbsqlc.JobStateRetryable)

		// these shouldn't cause notifications:
		addJob("queue2", -5*time.Second, dbsqlc.JobStateScheduled)          // it's a duplicate
		addJob("future_queue", time.Minute, dbsqlc.JobStateScheduled)       // it's in the future
		addJob("other_status_queue", time.Minute, dbsqlc.JobStateCancelled) // it's cancelled

		// Run the scheduler and wait for it to execute once:
		require.NoError(t, scheduler.Start(ctx))
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		expectedQueues := []string{"queue1", "queue2", "queue3"}
		expectedQueueMap := make(map[string]bool)
		for _, queue := range expectedQueues {
			expectedQueueMap[queue] = true
		}

		receivedQueues := make([]string, 0)
		for i := 0; i < len(expectedQueues); i++ {
			notification := riverinternaltest.WaitOrTimeout(t, notifyCh)
			require.Equal(t, notifier.NotificationTopicInsert, notification.topic)
			require.NotEmpty(t, notification.payload.Queue)

			_, isExpected := expectedQueueMap[notification.payload.Queue]
			require.True(t, isExpected, "unexpected queue %s", notification.payload.Queue)
			t.Logf("received notification for queue %s", notification.payload.Queue)
			receivedQueues = append(receivedQueues, notification.payload.Queue)
		}

		select {
		case notification := <-notifyCh:
			// no more should have been queued
			t.Fatalf("unexpected notification %+v", notification)
		case <-time.After(50 * time.Millisecond):
		}
		sort.Strings(receivedQueues)
		require.Equal(t, expectedQueues, receivedQueues)
	})
}
