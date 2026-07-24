package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

func TestSQLiteNotificationCleaner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		schema string
	}

	setup := func(t *testing.T) (*SQLiteNotificationCleaner, *testBundle) {
		t.Helper()

		driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
		tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)

		bundle := &testBundle{
			exec:   driver.UnwrapExecutor(tx),
			schema: schema,
		}

		cleaner := NewSQLiteNotificationCleaner(
			riversharedtest.BaseServiceArchetype(t),
			&SQLiteNotificationCleanerConfig{
				Interval:        time.Hour,
				RetentionPeriod: time.Hour,
				Schema:          bundle.schema,
				Timeout:         time.Second,
			},
			bundle.exec,
		)
		cleaner.StaggerStartupDisable(true)
		t.Cleanup(cleaner.Stop)

		return cleaner, bundle
	}

	notificationCount := func(t *testing.T, exec riverdriver.Executor) int {
		t.Helper()

		var count int
		require.NoError(t, exec.QueryRow(ctx, "SELECT count(*) FROM river_notification").Scan(&count))
		return count
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewSQLiteNotificationCleaner(
			riversharedtest.BaseServiceArchetype(t),
			&SQLiteNotificationCleanerConfig{},
			nil,
		)

		require.Equal(t, SQLiteNotificationCleanerIntervalDefault, cleaner.Config.Interval)
		require.Equal(t, SQLiteNotificationCleanerRetentionPeriodDefault, cleaner.Config.RetentionPeriod)
	})

	t.Run("DeletesExpiredNotifications", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.TestSignals.Init(t)

		now := time.Now()
		require.NoError(t, bundle.exec.Exec(ctx, `
			INSERT INTO river_notification (created_at, payload, topic)
			VALUES
				($1, 'old_payload_1', 'topic'),
				($2, 'old_payload_2', 'topic'),
				($3, 'new_payload', 'topic')
		`, now.Add(-2*time.Hour), now.Add(-61*time.Minute), now.Add(-30*time.Minute)))

		res, err := cleaner.runOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, res.NumNotificationsDeleted)
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		require.Equal(t, 1, notificationCount(t, bundle.exec))
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress

		startstoptest.Stress(ctx, t, cleaner)
	})

	t.Run("TimeoutErrorIncludesOperation", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Timeout = time.Nanosecond

		_, err := cleaner.runOnce(ctx)
		require.ErrorContains(t, err, cleaner.Name+".runOnce timed out after 1ns")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
