package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseLeader[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return exec, &testBundle{
			driver: driver,
		}
	}

	const leaderTTL = 10 * time.Second

	// For use in test cases whera non-clock "now" is _not_ injected. This can
	// normally be very tight, but we see huge variance especially in GitHub
	// Actions, and given it's really not necessary to assert that this is
	// anything except within reasonable recent history, it's okay if it's big.
	const veryGenerousTimeCompareTolerance = 5 * time.Minute

	t.Run("LeaderAttemptElect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			leader, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: testClientID,
				Now:      &now,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, testClientID, leader.LeaderID)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, now, leaderFromDB.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leaderFromDB.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, testClientID, leaderFromDB.LeaderID)
		})

		t.Run("CannotElectTwiceInARow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(testClientID),
			})

			leaderAttempt, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: "different-client-id",
				TTL:      leaderTTL,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, leaderAttempt)

			// The time should not have changed because we specified that we were not
			// already elected, and the elect query is a no-op if there's already a
			// updatedLeader:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
		})

		t.Run("WithoutNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: testClientID,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.Equal(t, testClientID, leader.LeaderID)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leaderFromDB.ElectedAt, veryGenerousTimeCompareTolerance)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leaderFromDB.ExpiresAt, veryGenerousTimeCompareTolerance)
		})
	})

	t.Run("LeaderAttemptReelect", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotReelectDifferentLeaderID", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr("other-client-id"),
			})

			updatedLeader, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
				ElectedAt: leader.ElectedAt,
				LeaderID:  testClientID,
				TTL:       leaderTTL,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, updatedLeader)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.LeaderID, leaderFromDB.LeaderID)
			require.Equal(t, leader.ElectedAt, leaderFromDB.ElectedAt)
		})

		t.Run("ReelectsSameLeader", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(testClientID),
			})

			// Re-elect the same leader. Use a larger TTL to see if time is updated,
			// because we are in a test transaction and the time is frozen at the start of
			// the transaction.
			updatedLeader, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
				ElectedAt: leader.ElectedAt,
				LeaderID:  testClientID,
				TTL:       30 * time.Second,
			})
			require.NoError(t, err)
			require.Equal(t, testClientID, updatedLeader.LeaderID)
			require.WithinDuration(t, leader.ElectedAt, updatedLeader.ElectedAt, bundle.driver.TimePrecision())

			// expires_at should be incremented because this is the same leader that won
			// previously and we specified that we're already elected:
			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Greater(t, leaderFromDB.ExpiresAt, leader.ExpiresAt)
			require.WithinDuration(t, updatedLeader.ElectedAt, leaderFromDB.ElectedAt, bundle.driver.TimePrecision())
		})

		t.Run("DoesNotReelectExpiredRowThatIsNotYetDeleted", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				LeaderID:  ptrutil.Ptr(testClientID),
			})

			updatedLeader, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
				ElectedAt: leader.ElectedAt,
				LeaderID:  testClientID,
				TTL:       30 * time.Second,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, updatedLeader)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.ElectedAt, leaderFromDB.ElectedAt)
			require.Equal(t, leader.ExpiresAt, leaderFromDB.ExpiresAt)
		})

		t.Run("DoesNotReelectMismatchedElectedAt", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(testClientID),
			})

			updatedLeader, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
				ElectedAt: leader.ElectedAt.Add(-time.Second),
				LeaderID:  testClientID,
				TTL:       30 * time.Second,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, updatedLeader)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.ElectedAt, leaderFromDB.ElectedAt)
		})

		t.Run("WithoutNow", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(testClientID),
			})

			updatedLeader, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
				ElectedAt: leader.ElectedAt,
				LeaderID:  testClientID,
				TTL:       leaderTTL,
			})
			require.NoError(t, err)
			require.Equal(t, testClientID, updatedLeader.LeaderID)
			require.WithinDuration(t, leader.ElectedAt, updatedLeader.ElectedAt, bundle.driver.TimePrecision())

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, leader.ElectedAt, leaderFromDB.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, time.Now().Add(leaderTTL), leaderFromDB.ExpiresAt, veryGenerousTimeCompareTolerance)
		})
	})

	t.Run("LeaderDeleteExpired", func(t *testing.T) {
		t.Parallel()

		t.Run("DeletesExpired", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			{
				numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{})
				require.NoError(t, err)
				require.Zero(t, numDeleted)
			}

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				LeaderID:  ptrutil.Ptr(testClientID),
			})

			{
				numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{})
				require.NoError(t, err)
				require.Equal(t, 1, numDeleted)
			}
		})

		t.Run("WithInjectedNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			// Elected in the future.
			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(2 * time.Hour)),
				LeaderID:  ptrutil.Ptr(testClientID),
			})

			numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{
				Now: ptrutil.Ptr(now.Add(2*time.Hour + 1*time.Second)),
			})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)
		})
	})

	t.Run("LeaderInsert", func(t *testing.T) {
		t.Parallel()

		t.Run("InsertsLeader", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			var (
				now       = time.Now().UTC()
				electedAt = now.Add(1 * time.Second)
				expiresAt = now.Add(4*time.Hour + 3*time.Minute + 2*time.Second)
			)

			leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
				ElectedAt: &electedAt,
				ExpiresAt: &expiresAt,
				LeaderID:  testClientID,
				TTL:       leaderTTL,
			})
			require.NoError(t, err)
			require.WithinDuration(t, electedAt, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, expiresAt, leader.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, testClientID, leader.LeaderID)
		})

		t.Run("WithNow", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-1 * time.Minute) // subtract a minute to make sure it'not coincidentally working using wall time

			leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
				LeaderID: testClientID,
				Now:      &now,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, testClientID, leader.LeaderID)
		})
	})

	t.Run("LeaderGetElectedLeader", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		now := time.Now().UTC()

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(testClientID),
			Now:      &now,
		})

		leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
		require.NoError(t, err)
		require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
		require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
		require.Equal(t, testClientID, leader.LeaderID)
	})

	t.Run("LeaderResign", func(t *testing.T) {
		t.Parallel()

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					ElectedAt:       time.Now().UTC(),
					LeaderID:        testClientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.False(t, resigned)
			}

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(testClientID),
			})

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					ElectedAt:       leader.ElectedAt,
					LeaderID:        testClientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.True(t, resigned)
			}
		})

		t.Run("DoesNotResignWithoutLeadership", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr("other-client-id"),
			})

			resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				ElectedAt:       leader.ElectedAt,
				LeaderID:        testClientID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
			})
			require.NoError(t, err)
			require.False(t, resigned)
		})

		t.Run("DoesNotResignNewerTermForSameLeaderID", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			oldLeader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				LeaderID:  ptrutil.Ptr(testClientID),
			})

			numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{Now: &now})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)

			newLeader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now),
				LeaderID:  ptrutil.Ptr(testClientID),
			})

			resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				ElectedAt:       oldLeader.ElectedAt,
				LeaderID:        testClientID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
			})
			require.NoError(t, err)
			require.False(t, resigned)

			leaderFromDB, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, newLeader.LeaderID, leaderFromDB.LeaderID)
			require.Equal(t, newLeader.ElectedAt, leaderFromDB.ElectedAt)
		})
	})
}
