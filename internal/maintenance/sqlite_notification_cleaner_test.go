package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

type sqliteNotificationCleanerExecutor struct {
	riverdriver.Executor

	notificationDeleteBeforeFunc func(context.Context, *riverdriver.NotificationDeleteBeforeParams) (int, error)
}

func (e *sqliteNotificationCleanerExecutor) NotificationDeleteBefore(ctx context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
	return e.notificationDeleteBeforeFunc(ctx, params)
}

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

	t.Run("CancelsBetweenBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Default = 2
		cleaner.TestSignals.Init(t)

		require.NoError(t, bundle.exec.Exec(ctx, `
			INSERT INTO river_notification (created_at, payload, topic)
			SELECT $1, 'old_payload', 'topic' FROM generate_series(1, 5)
		`, time.Now().Add(-2*time.Hour)))

		ctx, cancelFunc := context.WithCancel(ctx)
		defer cancelFunc()
		cleaner.exec = &sqliteNotificationCleanerExecutor{
			Executor: bundle.exec,
			notificationDeleteBeforeFunc: func(ctx context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
				numDeleted, err := bundle.exec.NotificationDeleteBefore(ctx, params)
				cancelFunc()
				return numDeleted, err
			},
		}

		_, err := cleaner.runOnce(ctx)
		require.ErrorIs(t, err, context.Canceled)
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		cleaner.TestSignals.DeletedBatch.RequireEmpty()
		require.Equal(t, 3, notificationCount(t, bundle.exec))
	})

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewSQLiteNotificationCleaner(
			riversharedtest.BaseServiceArchetype(t),
			&SQLiteNotificationCleanerConfig{},
			nil,
		)

		require.Equal(t, riversharedmaintenance.BatchSizeDefault, cleaner.Config.Default)
		require.Equal(t, riversharedmaintenance.BatchSizeReduced, cleaner.Config.Reduced)
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

	t.Run("DeletesMultipleBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Default = 2
		cleaner.TestSignals.Init(t)

		require.NoError(t, bundle.exec.Exec(ctx, `
			INSERT INTO river_notification (created_at, payload, topic)
			SELECT $1, 'old_payload', 'topic' FROM generate_series(1, 5)
		`, time.Now().Add(-2*time.Hour)))
		require.NoError(t, bundle.exec.Exec(ctx, `
			INSERT INTO river_notification (payload, topic) VALUES ('new_payload', 'topic')
		`))

		res, err := cleaner.runOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, res.NumNotificationsDeleted)
		for range 3 { // Two full batches followed by a partial batch.
			cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		}
		cleaner.TestSignals.DeletedBatch.RequireEmpty()
		require.Equal(t, 1, notificationCount(t, bundle.exec))
	})

	t.Run("ReducedBatchSizeBreakerIgnoresOtherErrors", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)

		for _, queryErr := range []error{context.Canceled, errors.New("notification delete failed")} {
			cleaner.exec = &sqliteNotificationCleanerExecutor{
				Executor: bundle.exec,
				notificationDeleteBeforeFunc: func(context.Context, *riverdriver.NotificationDeleteBeforeParams) (int, error) {
					return 0, queryErr
				},
			}

			for range cleaner.reducedBatchSizeBreaker.Limit() {
				_, err := cleaner.runOnce(ctx)
				require.ErrorIs(t, err, queryErr)
			}
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, cleaner.batchSize())
		}
	})

	t.Run("ReducedBatchSizeBreakerResetsOnSuccess", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		var queryErr error
		cleaner.exec = &sqliteNotificationCleanerExecutor{
			Executor: bundle.exec,
			notificationDeleteBeforeFunc: func(_ context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
				require.Equal(t, riversharedmaintenance.BatchSizeDefault, params.Max)
				return 0, queryErr
			},
		}

		for range 2 {
			queryErr = context.DeadlineExceeded
			for range cleaner.reducedBatchSizeBreaker.Limit() - 1 {
				_, err := cleaner.runOnce(ctx)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, riversharedmaintenance.BatchSizeDefault, cleaner.batchSize())
			}

			queryErr = nil
			_, err := cleaner.runOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, cleaner.batchSize())
		}
	})

	t.Run("ReducedBatchSizeBreakerTrips", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		expectedMax := riversharedmaintenance.BatchSizeDefault
		queryErr := context.DeadlineExceeded
		cleaner.exec = &sqliteNotificationCleanerExecutor{
			Executor: bundle.exec,
			notificationDeleteBeforeFunc: func(_ context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
				require.Equal(t, expectedMax, params.Max)
				return 0, queryErr
			},
		}

		for range cleaner.reducedBatchSizeBreaker.Limit() - 1 {
			_, err := cleaner.runOnce(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, riversharedmaintenance.BatchSizeDefault, cleaner.batchSize())
		}

		_, err := cleaner.runOnce(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, riversharedmaintenance.BatchSizeReduced, cleaner.batchSize())

		// Once tripped, successful deletes keep using the reduced batch size.
		expectedMax = riversharedmaintenance.BatchSizeReduced
		queryErr = nil
		for range 2 {
			_, err := cleaner.runOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, riversharedmaintenance.BatchSizeReduced, cleaner.batchSize())
		}
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
