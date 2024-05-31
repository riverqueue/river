package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

func Test_SubscriptionManager(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec        riverdriver.Executor
		subscribeCh chan []jobcompleter.CompleterJobUpdated
		tx          pgx.Tx
	}

	setup := func(t *testing.T) (*subscriptionManager, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
		exec := riverpgxv5.New(nil).UnwrapExecutor(tx)

		subscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 1)
		manager := newSubscriptionManager(riverinternaltest.BaseServiceArchetype(t), subscribeCh)

		require.NoError(t, manager.Start(ctx))
		t.Cleanup(manager.Stop)

		return manager, &testBundle{
			exec:        exec,
			subscribeCh: subscribeCh,
			tx:          tx,
		}
	}

	t.Run("DistributesRequestedEventsToSubscribers", func(t *testing.T) {
		t.Parallel()

		manager, bundle := setup(t)
		t.Cleanup(func() { close(bundle.subscribeCh) })

		sub, cancelSub := manager.SubscribeConfig(&SubscribeConfig{ChanSize: 10, Kinds: []EventKind{EventKindJobCompleted, EventKindJobSnoozed}})
		t.Cleanup(cancelSub)

		// Send some events
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(time.Now())})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(time.Now())})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable)})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		makeStats := func(complete, wait, run time.Duration) *jobstats.JobStatistics {
			return &jobstats.JobStatistics{
				CompleteDuration:  complete,
				QueueWaitDuration: wait,
				RunDuration:       run,
			}
		}

		bundle.subscribeCh <- []jobcompleter.CompleterJobUpdated{
			{Job: job1, JobStats: makeStats(101, 102, 103)}, // completed, should be sent
			{Job: job2, JobStats: makeStats(201, 202, 203)}, // cancelled, should be skipped
		}
		bundle.subscribeCh <- []jobcompleter.CompleterJobUpdated{
			{Job: job3, JobStats: makeStats(301, 302, 303)}, // retryable, should be skipped
			{Job: job4, JobStats: makeStats(401, 402, 403)}, // snoozed/scheduled, should be sent
		}

		received := riverinternaltest.WaitOrTimeoutN(t, sub, 2)
		require.Equal(t, job1.ID, received[0].Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, received[0].Job.State)
		require.Equal(t, time.Duration(101), received[0].JobStats.CompleteDuration)
		require.Equal(t, time.Duration(102), received[0].JobStats.QueueWaitDuration)
		require.Equal(t, time.Duration(103), received[0].JobStats.RunDuration)
		require.Equal(t, job4.ID, received[1].Job.ID)
		require.Equal(t, rivertype.JobStateScheduled, received[1].Job.State)
		require.Equal(t, time.Duration(401), received[1].JobStats.CompleteDuration)
		require.Equal(t, time.Duration(402), received[1].JobStats.QueueWaitDuration)
		require.Equal(t, time.Duration(403), received[1].JobStats.RunDuration)

		cancelSub()
		select {
		case value, stillOpen := <-sub:
			require.False(t, stillOpen, "subscription channel should be closed")
			require.Nil(t, value, "subscription channel should be closed")
		default:
			require.Fail(t, "subscription channel should have been closed")
		}
	})

	t.Run("StartStopRepeatedly", func(t *testing.T) {
		// This service does not use the typical `startstoptest.Stress()` test
		// because there are some additional steps required after a `Stop` for the
		// subsequent `Start` to succeed. It's also not friendly for multiple
		// concurrent calls to `Start` and `Stop`, but this is fine because the only
		// usage within `Client` is already protected by a mutex.
		t.Parallel()

		manager, bundle := setup(t)

		subscribeCh := bundle.subscribeCh
		for i := 0; i < 100; i++ {
			go func() { close(subscribeCh) }()
			manager.Stop()

			subscribeCh = make(chan []jobcompleter.CompleterJobUpdated, 1)
			manager.ResetSubscribeChan(subscribeCh)

			require.NoError(t, manager.Start(ctx))
		}
		close(subscribeCh)
	})
}
