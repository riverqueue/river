package riverdrivertest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

func TestDriverYugabyteNotifications(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, testCase := range []struct {
		enabled *bool
		name    string
	}{
		{enabled: new(false), name: "Disabled"},
		{enabled: new(true), name: "Enabled"},
		{name: "Unavailable"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for _, driverName := range []string{"DatabaseSQL", "DatabaseSQLWithListener", "Pgx"} {
				t.Run(driverName, func(t *testing.T) {
					t.Parallel()

					basePool := riversharedtest.DBPool(ctx, t)
					schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(basePool), nil)
					pool := riversharedtest.DBPoolWithYugabyteVersion(ctx, t, schema, testCase.enabled)
					enabled := testCase.enabled != nil && *testCase.enabled
					if driverName == "Pgx" {
						exerciseYugabyteNotifications(ctx, t, schema, true, enabled, func() riverdriver.Driver[pgx.Tx] {
							return riverpgxv5.New(pool)
						})
					} else {
						sqlPool := stdlib.OpenDBFromPool(pool)
						t.Cleanup(func() { require.NoError(t, sqlPool.Close()) })
						withListener := driverName == "DatabaseSQLWithListener"
						exerciseYugabyteNotifications(ctx, t, schema, withListener, enabled, func() riverdriver.Driver[*sql.Tx] {
							if withListener {
								return riverdatabasesql.NewWithPgxListener(sqlPool, pool)
							}
							return riverdatabasesql.New(sqlPool)
						})
					}
				})
			}
		})
	}
}

func exerciseYugabyteNotifications[TTx any](ctx context.Context, t *testing.T, schema string, withListener, enabled bool, newDriverFunc func() riverdriver.Driver[TTx]) {
	t.Helper()

	driver := newDriverFunc()
	require.Equal(t, withListener, driver.SupportsListener())
	require.True(t, driver.SupportsListenNotify())

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, driver.GetExecutor().InitDriver(cancelledCtx), context.Canceled)
	require.NoError(t, driver.GetExecutor().InitDriver(ctx))
	// Successful detection is cached, even when the next context is cancelled.
	require.NoError(t, driver.GetExecutor().InitDriver(cancelledCtx))
	require.Equal(t, withListener && enabled, driver.SupportsListener())
	require.Equal(t, enabled, driver.SupportsListenNotify())

	// These executors haven't been initialized explicitly, as with insert-only
	// clients. Each must detect support before attempting a notification.
	notifyDriver := newDriverFunc()
	require.NoError(t, notifyDriver.GetExecutor().NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{`{"action":"pause","queue":"default"}`},
		Schema:  schema,
		Topic:   "river_control",
	}))
	require.Equal(t, enabled, notifyDriver.SupportsListenNotify())

	cancelExec := newDriverFunc().GetExecutor()
	job := testfactory.Job(ctx, t, cancelExec, &testfactory.JobOpts{Schema: schema})
	cancelledJob, err := cancelExec.JobCancel(ctx, &riverdriver.JobCancelParams{
		ID:                job.ID,
		CancelAttemptedAt: time.Now(),
		ControlTopic:      "river_control",
		Schema:            schema,
	})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)

	leaderExec := newDriverFunc().GetExecutor()
	leader, err := leaderExec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
		LeaderID: "yugabyte-test",
		Schema:   schema,
		TTL:      time.Minute,
	})
	require.NoError(t, err)
	resigned, err := leaderExec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
		ElectedAt:       leader.ElectedAt,
		LeaderID:        leader.LeaderID,
		LeadershipTopic: "river_leadership",
		Schema:          schema,
	})
	require.NoError(t, err)
	require.True(t, resigned)
	_, err = leaderExec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{Schema: schema})
	require.ErrorIs(t, err, rivertype.ErrNotFound)

	// Default PollOnly=false must still cancel a running job from another
	// client when startup detection disables LISTEN/NOTIFY.
	t.Run("CancelRunningJob", func(t *testing.T) {
		// Sequential: these clients share a schema and must stop before the
		// maintenance recovery test can elect its own leader.
		exerciseClientCancelRunningJob(ctx, t, newDriverFunc(), schema, false, true)
	})
	t.Run("MaintenanceStartRecovery", func(t *testing.T) {
		// Sequential because the cancellation client must release leadership.
		exerciseClientMaintenanceStartRecovery(ctx, t, newDriverFunc(), schema, false)
	})
}
