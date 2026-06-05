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

	t.Run("NotificationDeleteBefore", func(t *testing.T) {
		t.Parallel()

		exec, driver := executorWithTx(ctx, t)

		insertQuery := `
			INSERT INTO river_notification (created_at, payload, topic)
			VALUES
				($1, $2, $3),
				($4, $5, $6),
				($7, $8, $9)
		`
		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			insertQuery = `
				INSERT INTO river_notification (created_at, payload, topic)
				VALUES
					(?, ?, ?),
					(?, ?, ?),
					(?, ?, ?)
			`
		}
		createdAt := func(t time.Time) any { return t }
		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			// Keep this in the same format that the SQLite driver uses for
			// CreatedAtHorizon so SQLite's text comparison stays chronological.
			createdAt = func(t time.Time) any {
				const sqliteFormat = "2006-01-02 15:04:05.999"
				return t.UTC().Round(time.Millisecond).Format(sqliteFormat)
			}
		}

		now := time.Now().UTC()
		require.NoError(t, exec.Exec(ctx, insertQuery,
			createdAt(now.Add(-2*time.Hour)), "old_payload_1", "topic",
			createdAt(now.Add(-61*time.Minute)), "old_payload_2", "topic",
			createdAt(now.Add(-30*time.Minute)), "new_payload", "topic",
		))

		numDeleted, err := exec.NotificationDeleteBefore(ctx, &riverdriver.NotificationDeleteBeforeParams{
			CreatedAtHorizon: now.Add(-time.Hour),
		})
		require.NoError(t, err)
		require.Equal(t, 2, numDeleted)

		var count int
		require.NoError(t, exec.QueryRow(ctx, "SELECT count(*) FROM river_notification").Scan(&count))
		require.Equal(t, 1, count)
	})
}
