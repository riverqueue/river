package riverdrivertest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

func exerciseClientMaintenanceStartRecovery[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], schema string, pollOnly bool) {
	t.Helper()

	var startAttempts atomic.Int32
	var attemptLeaders testsignal.TestSignal[*riverdriver.Leader]
	attemptLeaders.Init(t)

	config := newTestConfig(t, schema)
	config.PollOnly = pollOnly
	config.Hooks = []rivertype.Hook{
		river.HookPeriodicJobsStartFunc(func(ctx context.Context, _ *rivertype.HookPeriodicJobsStartParams) error {
			leader, err := driver.GetExecutor().LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{Schema: schema})
			if err != nil {
				return err
			}

			attempt := startAttempts.Add(1)
			attemptLeaders.Signal(leader)
			if attempt <= 3 {
				return errors.New("maintenance start error")
			}
			return nil
		}),
	}
	config.PeriodicJobs = []*river.PeriodicJob{
		river.NewPeriodicJob(river.PeriodicInterval(time.Hour), func() (river.JobArgs, *river.InsertOpts) {
			return noOpArgs{}, nil
		}, &river.PeriodicJobOpts{RunOnStart: true}),
	}

	client, err := river.NewClient(driver, config)
	require.NoError(t, err)

	events := subscribe(t, client)
	startClient(ctx, t, client)

	first := attemptLeaders.WaitOrTimeout()
	require.NotNil(t, first)
	require.Equal(t, client.ID(), first.LeaderID)
	for range 2 {
		attemptLeader := attemptLeaders.WaitOrTimeout()
		require.NotNil(t, attemptLeader)
		require.Equal(t, first.LeaderID, attemptLeader.LeaderID)
		require.Equal(t, first.ElectedAt, attemptLeader.ElectedAt)
	}

	// Exhausting startup retries must end the term. With only one client, it
	// wins again and retries maintenance in a fresh term without a notification.
	recovered := attemptLeaders.WaitOrTimeout()
	require.NotNil(t, recovered)
	require.Equal(t, client.ID(), recovered.LeaderID)
	require.True(t, recovered.ElectedAt.After(first.ElectedAt))

	// Prove maintenance actually recovered, rather than just receiving a
	// resignation request: its run-on-start periodic job must be worked.
	event := riversharedtest.WaitOrTimeout(t, events)
	require.Equal(t, river.EventKindJobCompleted, event.Kind)
	require.Equal(t, (noOpArgs{}).Kind(), event.Job.Kind)
	require.Equal(t, int32(4), startAttempts.Load())
}
