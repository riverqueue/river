package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
)

func exerciseNotification[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		exec    riverdriver.Executor
		horizon time.Time
	}

	setup := func(ctx context.Context, t *testing.T) *testBundle {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		insertQuery := `
			INSERT INTO river_notification (created_at, payload, topic)
			VALUES
				($1, $2, $3),
				($4, $5, $6),
				($7, $8, $9),
				($10, $11, $12)
		`
		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			insertQuery = `
				INSERT INTO river_notification (created_at, payload, topic)
				VALUES
					(?, ?, ?),
					(?, ?, ?),
					(?, ?, ?),
					(?, ?, ?)
			`
		}
		createdAtFunc := func(t time.Time) any { return t }
		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			// Keep this in the same format that the SQLite driver uses for
			// CreatedAtHorizon so SQLite's text comparison stays chronological.
			createdAtFunc = func(t time.Time) any {
				const sqliteFormat = "2006-01-02 15:04:05.000"
				return t.UTC().Round(time.Millisecond).Format(sqliteFormat)
			}
		}

		// Include a trailing fractional zero to exercise SQLite's fixed-width format.
		now := time.Now().UTC().Truncate(time.Second).Add(120 * time.Millisecond)
		require.NoError(t, exec.Exec(ctx, insertQuery,
			createdAtFunc(now.Add(-61*time.Minute)), "old_payload", "topic",
			createdAtFunc(now.Add(-2*time.Hour)), "oldest_payload", "topic",
			createdAtFunc(now.Add(-time.Hour)), "horizon_payload", "topic",
			createdAtFunc(now.Add(-30*time.Minute)), "new_payload", "topic",
		))

		return &testBundle{
			exec:    exec,
			horizon: now.Add(-time.Hour),
		}
	}

	t.Run("NotificationDeleteBefore", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		numDeleted, err := bundle.exec.NotificationDeleteBefore(ctx, &riverdriver.NotificationDeleteBeforeParams{
			CreatedAtHorizon: bundle.horizon,
			Max:              10,
		})
		require.NoError(t, err)
		require.Equal(t, 2, numDeleted)

		var count int
		require.NoError(t, bundle.exec.QueryRow(ctx, "SELECT count(*) FROM river_notification").Scan(&count))
		require.Equal(t, 2, count)
	})

	t.Run("NotificationDeleteBefore_Limited", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)
		params := &riverdriver.NotificationDeleteBeforeParams{
			CreatedAtHorizon: bundle.horizon,
			Max:              1,
		}

		numDeleted, err := bundle.exec.NotificationDeleteBefore(ctx, params)
		require.NoError(t, err)
		require.Equal(t, 1, numDeleted)

		// Delete by age, even when the oldest notification was inserted later.
		var oldestRemaining string
		require.NoError(t, bundle.exec.QueryRow(ctx, "SELECT payload FROM river_notification ORDER BY created_at LIMIT 1").Scan(&oldestRemaining))
		require.Equal(t, "old_payload", oldestRemaining)

		numDeleted, err = bundle.exec.NotificationDeleteBefore(ctx, params)
		require.NoError(t, err)
		require.Equal(t, 1, numDeleted)

		numDeleted, err = bundle.exec.NotificationDeleteBefore(ctx, params)
		require.NoError(t, err)
		require.Zero(t, numDeleted)

		var count int
		require.NoError(t, bundle.exec.QueryRow(ctx, "SELECT count(*) FROM river_notification").Scan(&count))
		require.Equal(t, 2, count) // Includes the notification exactly at the horizon.
	})
}
