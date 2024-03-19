package maintenance

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func TestJobScheduler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
	}

	setup := func(t *testing.T, ex riverdriver.Executor) (*JobScheduler, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			exec: ex,
		}

		scheduler := NewScheduler(
			riverinternaltest.BaseServiceArchetype(t),
			&JobSchedulerConfig{
				Interval: JobSchedulerIntervalDefault,
				Limit:    10,
			},
			bundle.exec)
		scheduler.TestSignals.Init()
		t.Cleanup(scheduler.Stop)

		return scheduler, bundle
	}

	setupTx := func(t *testing.T) (*JobScheduler, *testBundle) {
		t.Helper()
		tx := riverinternaltest.TestTx(ctx, t)
		return setup(t, riverpgxv5.New(nil).UnwrapExecutor(tx))
	}

	requireJobStateUnchanged := func(t *testing.T, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.State, newJob.State)
		return newJob
	}
	requireJobStateAvailable := func(t *testing.T, exec riverdriver.Executor, job *rivertype.JobRow) *rivertype.JobRow {
		t.Helper()
		newJob, err := exec.JobGetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, newJob.State)
		return newJob
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		scheduler := NewScheduler(riverinternaltest.BaseServiceArchetype(t), &JobSchedulerConfig{}, nil)

		require.Equal(t, JobSchedulerIntervalDefault, scheduler.config.Interval)
		require.Equal(t, JobSchedulerLimitDefault, scheduler.config.Limit)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		scheduler, _ := setupTx(t)
		scheduler.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		scheduler.TestSignals = JobSchedulerTestSignals{}  // deinit so channels don't fill

		startstoptest.Stress(ctx, t, scheduler)
	})

	t.Run("SchedulesScheduledAndRetryableJobs", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)

		// none of these should get updated
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled)})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded)})
		job5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		now := time.Now().UTC()

		scheduledJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		scheduledJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		scheduledJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))}) // won't be scheduled

		retryableJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		retryableJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		retryableJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(30 * time.Second))}) // won't be scheduled

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateUnchanged(t, bundle.exec, job1)
		requireJobStateUnchanged(t, bundle.exec, job2)
		requireJobStateUnchanged(t, bundle.exec, job3)
		requireJobStateUnchanged(t, bundle.exec, job4)
		requireJobStateUnchanged(t, bundle.exec, job5)

		requireJobStateAvailable(t, bundle.exec, scheduledJob1)
		requireJobStateAvailable(t, bundle.exec, scheduledJob2)
		requireJobStateUnchanged(t, bundle.exec, scheduledJob3) // still scheduled

		requireJobStateAvailable(t, bundle.exec, retryableJob1)
		requireJobStateAvailable(t, bundle.exec, retryableJob2)
		requireJobStateUnchanged(t, bundle.exec, retryableJob3) // still retryable
	})

	t.Run("SchedulesInBatches", func(t *testing.T) {
		t.Parallel()

		scheduler, bundle := setupTx(t)
		scheduler.config.Limit = 10 // reduced size for test speed

		now := time.Now().UTC()

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := scheduler.config.Limit + 1

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := 0; i < numJobs; i++ {
			jobState := rivertype.JobStateScheduled
			if i%2 == 0 {
				jobState = rivertype.JobStateRetryable
			}
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: &jobState, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, scheduler.Start(ctx))

		// See comment above. Exactly two batches are expected.
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		for _, job := range jobs {
			requireJobStateAvailable(t, bundle.exec, job)
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

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		scheduler.Stop()

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})

		require.NoError(t, scheduler.Start(ctx))

		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobStateAvailable(t, bundle.exec, job1)
		requireJobStateAvailable(t, bundle.exec, job2)
	})

	t.Run("TriggersNotificationsOnEachQueueWithNewlyAvailableJobs", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)
		driver := riverpgxv5.New(dbPool)
		exec := driver.GetExecutor()
		listener := driver.GetListener()

		scheduler, _ := setup(t, exec)
		scheduler.config.Interval = time.Minute // should only trigger once for the initial run
		now := time.Now().UTC()

		statusUpdateCh := make(chan componentstatus.Status, 10)
		statusUpdate := func(status componentstatus.Status) {
			statusUpdateCh <- status
		}

		notify := notifier.New(&scheduler.Archetype, listener, statusUpdate)
		require.NoError(t, notify.Start(ctx))
		t.Cleanup(notify.Stop)

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
		sub, err := notify.Listen(ctx, notifier.NotificationTopicInsert, handleNotification)
		require.NoError(t, err)
		defer sub.Unlisten(ctx)

		for {
			status := riverinternaltest.WaitOrTimeout(t, statusUpdateCh)
			if status == componentstatus.Healthy {
				break
			}
		}

		addJob := func(queue string, fromNow time.Duration, state rivertype.JobState) {
			t.Helper()
			testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: &queue, State: &state, ScheduledAt: ptrutil.Ptr(now.Add(fromNow))})
		}

		addJob("queue1", -1*time.Hour, rivertype.JobStateScheduled)
		addJob("queue2", -1*time.Minute, rivertype.JobStateScheduled)
		addJob("queue3", -30*time.Second, rivertype.JobStateRetryable)

		// these shouldn't cause notifications:
		addJob("queue2", -5*time.Second, rivertype.JobStateScheduled)          // it's a duplicate
		addJob("future_queue", time.Minute, rivertype.JobStateScheduled)       // it's in the future
		addJob("other_status_queue", time.Minute, rivertype.JobStateCancelled) // it's cancelled

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
