package leadership

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestElector_PollOnly(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		driver = riverpgxv5.New(nil)
	)

	type electorBundle struct {
		tx pgx.Tx
	}

	testElector(ctx, t,
		func(t *testing.T) *electorBundle {
			t.Helper()

			tx := riverinternaltest.TestTx(ctx, t)

			// We'll put multiple electors on one transaction. Make sure they can
			// live with each other in relative harmony.
			tx = sharedtx.NewSharedTx(tx)

			return &electorBundle{
				tx: tx,
			}
		},
		func(t *testing.T, electorBundle *electorBundle) *Elector {
			t.Helper()

			return NewElector(
				riversharedtest.BaseServiceArchetype(t),
				driver.UnwrapExecutor(electorBundle.tx),
				nil,
				&Config{ClientID: "test_client_id"},
			)
		})
}

func TestElector_WithNotifier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type electorBundle struct {
		archetype *baseservice.Archetype
		exec      riverdriver.Executor
		notifier  *notifier.Notifier
	}

	testElector(ctx, t,
		func(t *testing.T) *electorBundle {
			t.Helper()

			var (
				archetype = riversharedtest.BaseServiceArchetype(t)
				dbPool    = riverinternaltest.TestDB(ctx, t)
				driver    = riverpgxv5.New(dbPool)
			)

			notifier := notifier.New(archetype, driver.GetListener())
			{
				require.NoError(t, notifier.Start(ctx))
				t.Cleanup(notifier.Stop)
			}

			return &electorBundle{
				archetype: archetype,
				exec:      driver.GetExecutor(),
				notifier:  notifier,
			}
		},
		func(t *testing.T, electorBundle *electorBundle) *Elector {
			t.Helper()

			return NewElector(
				electorBundle.archetype,
				electorBundle.exec,
				electorBundle.notifier,
				&Config{ClientID: "test_client_id"},
			)
		})
}

// This system of "elector bundles" may appear to be a little convoluted, but
// it's built so that we can initialize multiple electors against a single
// database or transaction.
func testElector[TElectorBundle any](
	ctx context.Context,
	t *testing.T,
	makeElectorBundle func(t *testing.T) TElectorBundle,
	makeElector func(t *testing.T, bundle TElectorBundle) *Elector,
) {
	t.Helper()

	type testBundle struct {
		electorBundle TElectorBundle
		exec          riverdriver.Executor
	}

	setup := func(t *testing.T) (*Elector, *testBundle) {
		t.Helper()

		electorBundle := makeElectorBundle(t)

		elector := makeElector(t, electorBundle)
		elector.testSignals.Init()

		return elector, &testBundle{
			electorBundle: electorBundle,
			exec:          elector.exec,
		}
	}

	startElector := func(ctx context.Context, t *testing.T, elector *Elector) {
		t.Helper()
		t.Logf("Starting %s", elector.config.ClientID)
		require.NoError(t, elector.Start(ctx))
		t.Cleanup(elector.Stop)
	}

	t.Run("StartsGainsLeadershipAndStops", func(t *testing.T) {
		t.Parallel()

		elector, bundle := setup(t)

		startElector(ctx, t, elector)

		elector.testSignals.GainedLeadership.WaitOrTimeout()

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx)
		require.NoError(t, err)
		require.Equal(t, elector.config.ClientID, leader.LeaderID)

		elector.Stop()

		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err = bundle.exec.LeaderGetElectedLeader(ctx)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("NotifiesSubscribers", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		sub := elector.Listen()
		t.Cleanup(func() { elector.unlisten(sub) })

		// Drain an initial notification that occurs on Listen.
		notification := riversharedtest.WaitOrTimeout(t, sub.ch)
		require.False(t, notification.IsLeader)

		startElector(ctx, t, elector)

		elector.testSignals.GainedLeadership.WaitOrTimeout()

		notification = riversharedtest.WaitOrTimeout(t, sub.ch)
		require.True(t, notification.IsLeader)

		elector.Stop()

		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		notification = riversharedtest.WaitOrTimeout(t, sub.ch)
		require.False(t, notification.IsLeader)
	})

	t.Run("SustainsLeadership", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		startElector(ctx, t, elector)

		elector.testSignals.GainedLeadership.WaitOrTimeout()

		// The leadership maintenance loop also listens on the leadership
		// notification channel. Take advantage of that to cause an
		// immediate reelect attempt with no sleep.
		elector.leadershipNotificationChan <- struct{}{}
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()

		elector.leadershipNotificationChan <- struct{}{}
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()

		elector.leadershipNotificationChan <- struct{}{}
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()

		elector.Stop()

		elector.testSignals.ResignedLeadership.WaitOrTimeout()
	})

	t.Run("LosesLeadership", func(t *testing.T) {
		t.Parallel()

		elector, bundle := setup(t)

		startElector(ctx, t, elector)

		elector.testSignals.GainedLeadership.WaitOrTimeout()

		t.Logf("Force resigning %s", elector.config.ClientID)

		// Artificially force resign the elector and add a new leader record
		// so that it can't be elected again.
		_, err := bundle.exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
			LeaderID:        elector.config.ClientID,
			LeadershipTopic: string(notifier.NotificationTopicLeadership),
		})
		require.NoError(t, err)

		_ = testfactory.Leader(ctx, t, bundle.exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr("other-client-id"),
		})

		elector.leadershipNotificationChan <- struct{}{}
		elector.testSignals.LostLeadership.WaitOrTimeout()

		// Wait for the elector to try and fail to gain leadership so we
		// don't finish the test while it's still operating.
		elector.testSignals.DeniedLeadership.WaitOrTimeout()

		elector.Stop()
	})

	t.Run("CompetingElectors", func(t *testing.T) {
		t.Parallel()

		elector1, bundle := setup(t)
		elector1.config.ClientID = "elector1"

		{
			startElector(ctx, t, elector1)

			// next to avoid any raciness.
			t.Logf("Waiting for %s to gain leadership", elector1.config.ClientID)
			elector1.testSignals.GainedLeadership.WaitOrTimeout()

			leader, err := bundle.exec.LeaderGetElectedLeader(ctx)
			require.NoError(t, err)
			require.Equal(t, elector1.config.ClientID, leader.LeaderID)
		}

		// Make another elector and make sure it's using the same executor.
		elector2 := makeElector(t, bundle.electorBundle)
		elector2.config.ClientID = "elector2"
		elector2.exec = elector1.exec
		elector2.testSignals.Init()

		{
			startElector(ctx, t, elector2)

			elector2.testSignals.DeniedLeadership.WaitOrTimeout()

			t.Logf("Stopping %s", elector1.config.ClientID)
			elector1.Stop()
			elector1.testSignals.ResignedLeadership.WaitOrTimeout()

			// Cheat if we're in poll only by notifying leadership channel to
			// wake the elector from sleep.
			if elector2.notifier == nil {
				elector2.leadershipNotificationChan <- struct{}{}
			}

			t.Logf("Waiting for %s to gain leadership", elector2.config.ClientID)
			elector2.testSignals.GainedLeadership.WaitOrTimeout()

			t.Logf("Stopping %s", elector2.config.ClientID)
			elector2.Stop()
			elector2.testSignals.ResignedLeadership.WaitOrTimeout()
		}

		_, err := bundle.exec.LeaderGetElectedLeader(ctx)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)
		elector.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		elector.testSignals = electorTestSignals{}     // deinit so channels don't fill

		startstoptest.Stress(ctx, t, elector)
	})
}

func TestAttemptElectOrReelect(t *testing.T) {
	t.Parallel()

	const (
		clientID           = "client-id"
		leaderInstanceName = "default"
		leaderTTL          = 10 * time.Second
	)

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		logger *slog.Logger
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := riverpgxv5.New(nil)

		return &testBundle{
			exec:   driver.UnwrapExecutor(riverinternaltest.TestTx(ctx, t)),
			logger: riversharedtest.Logger(t),
		}
	}

	t.Run("ElectsLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		elected, err := attemptElectOrReelect(ctx, bundle.exec, false, &riverdriver.LeaderElectParams{
			LeaderID: clientID,
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.True(t, elected) // won election

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
	})

	t.Run("ReelectsSameLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		leader := testfactory.Leader(ctx, t, bundle.exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
		})

		// Re-elect the same leader. Use a larger TTL to see if time is updated,
		// because we are in a test transaction and the time is frozen at the start of
		// the transaction.
		elected, err := attemptElectOrReelect(ctx, bundle.exec, true, &riverdriver.LeaderElectParams{
			LeaderID: clientID,
			TTL:      30 * time.Second,
		})
		require.NoError(t, err)
		require.True(t, elected) // won re-election

		// expires_at should be incremented because this is the same leader that won
		// previously and we specified that we're already elected:
		updatedLeader, err := bundle.exec.LeaderGetElectedLeader(ctx)
		require.NoError(t, err)
		require.Greater(t, updatedLeader.ExpiresAt, leader.ExpiresAt)
	})

	t.Run("CannotElectDifferentLeader", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		leader := testfactory.Leader(ctx, t, bundle.exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
		})

		elected, err := attemptElectOrReelect(ctx, bundle.exec, true, &riverdriver.LeaderElectParams{
			LeaderID: "different-client-id",
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.False(t, elected) // lost election

		// The time should not have changed because we specified that we were not
		// already elected, and the elect query is a no-op if there's already a
		// updatedLeader:
		updatedLeader, err := bundle.exec.LeaderGetElectedLeader(ctx)
		require.NoError(t, err)
		require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
	})
}

func TestElectorHandleLeadershipNotification(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		driver = riverpgxv5.New(nil)
	)

	type testBundle struct{}

	setup := func(t *testing.T) (*Elector, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)

		elector := NewElector(
			riversharedtest.BaseServiceArchetype(t),
			driver.UnwrapExecutor(tx),
			nil,
			&Config{ClientID: "test_client_id"},
		)

		// This channel is normally only initialized on start, so we need to
		// create it manually here.
		elector.leadershipNotificationChan = make(chan struct{}, 1)

		return elector, &testBundle{}
	}

	mustMarshalJSON := func(t *testing.T, val any) []byte {
		t.Helper()

		data, err := json.Marshal(val)
		require.NoError(t, err)
		return data
	}

	validLeadershipChange := func() *dbLeadershipNotification {
		t.Helper()

		return &dbLeadershipNotification{
			Action:   "resigned",
			LeaderID: "other-client-id",
		}
	}

	t.Run("SignalsLeadershipChange", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, validLeadershipChange())))

		riversharedtest.WaitOrTimeout(t, elector.leadershipNotificationChan)
	})

	t.Run("StopsOnContextDone", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, validLeadershipChange())))

		require.Empty(t, elector.leadershipNotificationChan)
	})

	t.Run("IgnoresNonResignedAction", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		change := validLeadershipChange()
		change.Action = "not_resigned"

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, change)))

		require.Empty(t, elector.leadershipNotificationChan)
	})

	t.Run("IgnoresSameClientID", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		change := validLeadershipChange()
		change.LeaderID = elector.config.ClientID

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, change)))

		require.Empty(t, elector.leadershipNotificationChan)
	})
}
