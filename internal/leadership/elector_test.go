package leadership

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func TestAttemptElectOrReelect(t *testing.T) {
	t.Parallel()

	const (
		clientID           = "client-id"
		leaderInstanceName = "default"
		leaderTTL          = 10 * time.Second
	)

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := riverpgxv5.New(nil)

		return &testBundle{
			exec: driver.UnwrapExecutor(riverinternaltest.TestTx(ctx, t)),
		}
	}

	t.Run("ElectsLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		elected, err := attemptElectOrReelect(ctx, bundle.exec, false, &riverdriver.LeaderElectParams{
			LeaderID: clientID,
			Name:     leaderInstanceName,
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.True(t, elected) // won election

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
	})

	t.Run("ReelectsSameLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		leader := testfactory.Leader(ctx, t, bundle.exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
			Name:     ptrutil.Ptr(leaderInstanceName),
		})

		// Re-elect the same leader. Use a larger TTL to see if time is updated,
		// because we are in a test transaction and the time is frozen at the start of
		// the transaction.
		elected, err := attemptElectOrReelect(ctx, bundle.exec, true, &riverdriver.LeaderElectParams{
			LeaderID: clientID,
			Name:     leaderInstanceName,
			TTL:      30 * time.Second,
		})
		require.NoError(t, err)
		require.True(t, elected) // won re-election

		// expires_at should be incremented because this is the same leader that won
		// previously and we specified that we're already elected:
		updatedLeader, err := bundle.exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
		require.NoError(t, err)
		require.Greater(t, updatedLeader.ExpiresAt, leader.ExpiresAt)
	})

	t.Run("CannotElectDifferentLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		leader := testfactory.Leader(ctx, t, bundle.exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
			Name:     ptrutil.Ptr(leaderInstanceName),
		})

		elected, err := attemptElectOrReelect(ctx, bundle.exec, true, &riverdriver.LeaderElectParams{
			LeaderID: "different-client-id",
			Name:     leaderInstanceName,
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.False(t, elected) // lost election

		// The time should not have changed because we specified that we were not
		// already elected, and the elect query is a no-op if there's already a
		// updatedLeader:
		updatedLeader, err := bundle.exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
		require.NoError(t, err)
		require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
	})
}
