package riversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestDurationAsString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "3.000 seconds", durationAsString(3*time.Second))
	require.Equal(t, "3.255 seconds", durationAsString(3*time.Second+255*time.Millisecond))
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

func TestJobCancelWritesNotification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "river.sqlite"))
	require.NoError(t, err)
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	driver := New(pool)
	migrator, err := rivermigrate.New(driver, nil)
	require.NoError(t, err)
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	require.NoError(t, err)
	exec := driver.GetExecutor()
	job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		State: new(rivertype.JobStateRunning),
	})

	_, err = exec.JobCancel(ctx, &riverdriver.JobCancelParams{
		CancelAttemptedAt: time.Now(),
		ControlTopic:      "river_control",
		ID:                job.ID,
	})
	require.NoError(t, err)
	var payload, topic string
	require.NoError(t, pool.QueryRowContext(ctx,
		"SELECT payload, topic FROM river_notification",
	).Scan(&payload, &topic))
	require.Equal(t, "river_control", topic)
	require.JSONEq(t, fmt.Sprintf(`{"action":"cancel","job_id":%d,"queue":"default"}`, job.ID), payload)
}

func TestTimeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "2025-04-30 13:26:39.100", timeString(time.Date(2025, 4, 30, 13, 26, 39, 100000000, time.UTC)))
	require.Equal(t, "2025-04-30 13:26:39.123", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC)))
	require.Equal(t, "2025-04-30 13:26:39.124", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC))) // test rounding
}

func TestTimeStringNullable(t *testing.T) {
	t.Parallel()

	require.Nil(t, timeStringNullable(nil))
	require.Equal(t, "2025-04-30 13:26:39.100", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 100000000, time.UTC))))
	require.Equal(t, "2025-04-30 13:26:39.123", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC))))
	require.Equal(t, "2025-04-30 13:26:39.124", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC)))) // test rounding
}

func TestSQLiteTimeWritesCanonical(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	type testBundle struct {
		exec riverdriver.Executor
		pool *sql.DB
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		pool, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "river.sqlite"))
		require.NoError(t, err)
		pool.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, pool.Close()) })
		driver := New(pool)
		migrator, err := rivermigrate.New(driver, nil)
		require.NoError(t, err)
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)
		return &testBundle{exec: driver.GetExecutor(), pool: pool}
	}
	batch := func(params *riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
		return &riverdriver.JobSetStateIfRunningManyParams{
			Attempt:         []*int{params.Attempt},
			ErrData:         [][]byte{params.ErrData},
			FinalizedAt:     []*time.Time{params.FinalizedAt},
			ID:              []int64{params.ID},
			MetadataDoMerge: []bool{params.MetadataDoMerge},
			MetadataUpdates: [][]byte{params.MetadataUpdates},
			ScheduledAt:     []*time.Time{params.ScheduledAt},
			State:           []rivertype.JobState{params.State},
		}
	}
	rawTimes := func(t *testing.T, bundle *testBundle, id int64) (sql.NullString, sql.NullString, sql.NullString) {
		t.Helper()

		var attemptedAt, finalizedAt, scheduledAt sql.NullString
		err := bundle.pool.QueryRowContext(ctx,
			"SELECT CAST(attempted_at AS TEXT), CAST(finalized_at AS TEXT), CAST(scheduled_at AS TEXT) FROM river_job WHERE id = ?", id,
		).Scan(&attemptedAt, &finalizedAt, &scheduledAt)
		require.NoError(t, err)
		return attemptedAt, finalizedAt, scheduledAt
	}
	runningJob := func(t *testing.T, bundle *testBundle) *rivertype.JobRow {
		t.Helper()

		return testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateRunning),
		})
	}

	t.Run("CompletionAndScheduling", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		completedAt := time.Now()
		completed := runningJob(t, bundle)
		_, err := bundle.exec.JobSetStateIfRunningMany(ctx,
			batch(riverdriver.JobSetStateCompleted(completed.ID, completedAt, nil)))
		require.NoError(t, err)
		_, rawFinalizedAt, _ := rawTimes(t, bundle, completed.ID)
		require.Equal(t, timeString(completedAt), rawFinalizedAt.String)

		retryAt := time.Now().Add(90 * time.Second)
		retryable := runningJob(t, bundle)
		_, err = bundle.exec.JobSetStateIfRunningMany(ctx,
			batch(riverdriver.JobSetStateErrorRetryable(retryable.ID, retryAt, []byte(`{"error":"retry"}`), nil)))
		require.NoError(t, err)
		_, _, rawRetryAt := rawTimes(t, bundle, retryable.ID)
		require.Equal(t, timeString(retryAt), rawRetryAt.String)

		snoozeAt := time.Now().Add(2 * time.Minute)
		snoozed := runningJob(t, bundle)
		_, err = bundle.exec.JobSetStateIfRunningMany(ctx,
			batch(riverdriver.JobSetStateSnoozed(snoozed.ID, snoozeAt, 1, nil)))
		require.NoError(t, err)
		_, _, rawSnoozeAt := rawTimes(t, bundle, snoozed.ID)
		require.Equal(t, timeString(snoozeAt), rawSnoozeAt.String)
	})

	t.Run("Rescue", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		job := runningJob(t, bundle)
		finalizedAt := time.Now()
		scheduledAt := finalizedAt.Add(time.Minute)
		_, err := bundle.exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			Error:       [][]byte{[]byte(`{"error":"rescue"}`)},
			FinalizedAt: []*time.Time{&finalizedAt},
			ID:          []int64{job.ID},
			ScheduledAt: []time.Time{scheduledAt},
			State:       []string{string(rivertype.JobStateDiscarded)},
		})
		require.NoError(t, err)
		_, rawFinalizedAt, rawScheduledAt := rawTimes(t, bundle, job.ID)
		require.Equal(t, timeString(finalizedAt), rawFinalizedAt.String)
		require.Equal(t, timeString(scheduledAt), rawScheduledAt.String)
	})

	t.Run("UpdateFull", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		job := runningJob(t, bundle)
		attemptedAt := time.Now()
		finalizedAt := attemptedAt.Add(time.Second)
		_, err := bundle.exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			AttemptedAt:         &attemptedAt,
			AttemptedAtDoUpdate: true,
			FinalizedAt:         &finalizedAt,
			FinalizedAtDoUpdate: true,
			ID:                  job.ID,
			State:               rivertype.JobStateCompleted,
			StateDoUpdate:       true,
		})
		require.NoError(t, err)
		rawAttemptedAt, rawFinalizedAt, _ := rawTimes(t, bundle, job.ID)
		require.Equal(t, timeString(attemptedAt), rawAttemptedAt.String)
		require.Equal(t, timeString(finalizedAt), rawFinalizedAt.String)
	})
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *Driver
	}

	setup := func(t *testing.T) (*sqlctemplate.Replacer, *testBundle) {
		t.Helper()

		return &sqlctemplate.Replacer{}, &testBundle{
			driver: New(nil),
		}
	}

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "custom_schema"),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1 FROM "custom_schema".river_job`, updatedSQL)
	})

	t.Run("SchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1 FROM "my_schema".river_job`, updatedSQL1)

		// Second call with same SQL + same schema produces identical result.
		// Because schema is marked Stable, the Replacer caches the output
		// after the first call and short-circuits regex on subsequent calls.
		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})

	t.Run("EmptySchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL1)

		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})
}
