package riverdrivertest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

type maintenanceRecoveryDriver struct {
	riverdriver.Driver[pgx.Tx]

	commitDelay    time.Duration
	commitDelays   atomic.Int32
	resignAttempts atomic.Int32
	resignTimeouts int32
}

func (d *maintenanceRecoveryDriver) GetExecutor() riverdriver.Executor {
	return &maintenanceRecoveryExecutor{
		Executor: d.Driver.GetExecutor(),
		driver:   d,
	}
}

type maintenanceRecoveryExecutor struct {
	riverdriver.Executor

	driver *maintenanceRecoveryDriver
}

func (e *maintenanceRecoveryExecutor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.Executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &maintenanceRecoveryExecutorTx{ExecutorTx: tx, driver: e.driver}, nil
}

func (e *maintenanceRecoveryExecutor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	if e.driver.resignAttempts.Add(1) <= e.driver.resignTimeouts {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return e.Executor.LeaderResign(ctx, params)
}

type maintenanceRecoveryExecutorTx struct {
	riverdriver.ExecutorTx

	driver         *maintenanceRecoveryDriver
	insertNotified bool
}

func (e *maintenanceRecoveryExecutorTx) Commit(ctx context.Context) error {
	if e.insertNotified && e.driver.commitDelay > 0 {
		e.driver.commitDelays.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.driver.commitDelay):
		}
	}
	return e.ExecutorTx.Commit(ctx)
}

func (e *maintenanceRecoveryExecutorTx) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	if params.Topic == "river_insert" {
		e.insertNotified = true
	}
	return e.ExecutorTx.NotifyMany(ctx, params)
}

func TestMaintenanceStartRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *maintenanceRecoveryDriver
		schema string
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := &maintenanceRecoveryDriver{Driver: riverpgxv5.New(riversharedtest.DBPool(ctx, t))}
		return &testBundle{
			driver: driver,
			schema: riverdbtest.TestSchema(ctx, t, driver, nil),
		}
	}

	t.Run("ResignTimeouts", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.driver.resignTimeouts = 3

		exerciseClientMaintenanceStartRecovery(ctx, t, bundle.driver, bundle.schema, false)
		require.GreaterOrEqual(t, bundle.driver.resignAttempts.Load(), int32(3))
	})

	t.Run("SlowPeriodicCommit", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		// Exceed the ordinary 10-second CI event wait while remaining within
		// the enqueuer's 30-second transaction timeout.
		bundle.driver.commitDelay = 11 * time.Second

		exerciseClientMaintenanceStartRecovery(ctx, t, bundle.driver, bundle.schema, false)
		require.Equal(t, int32(1), bundle.driver.commitDelays.Load())
	})
}

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
	t.Cleanup(func() {
		if !t.Failed() || driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			return
		}

		// Register after startClient so diagnostics run before it stops the
		// client and cancels the operations that explain a recovery timeout.
		diagnosticCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		var activity string
		err := riversharedtest.DBPool(diagnosticCtx, t).QueryRow(diagnosticCtx, `
SELECT coalesce(json_agg(activity)::text, '[]')
FROM (
    SELECT pid, state, wait_event_type, wait_event,
        pg_blocking_pids(pid) AS blocking_pids,
        clock_timestamp() - query_start AS query_age,
        left(query, 2000) AS query
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle'
) AS activity`).Scan(&activity)
		if err != nil {
			t.Logf("Unable to capture maintenance recovery database activity: %v", err)
			return
		}
		t.Logf("Maintenance recovery database activity before shutdown: %s", activity)
	})

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
	// Three resignation attempts can take 1+2+3 seconds, plus about 1+2+4
	// seconds of backoff. If all fail, the 15-second lease must expire before
	// a new term can start. Both paths can exceed the usual test wait.
	var recovered *riverdriver.Leader
	select {
	case recovered = <-attemptLeaders.WaitC():
	case <-time.After(30 * time.Second):
		t.Fatal("maintenance did not recover in a new leadership term within 30s")
	}
	require.NotNil(t, recovered)
	require.Equal(t, client.ID(), recovered.LeaderID)
	require.True(t, recovered.ElectedAt.After(first.ElectedAt))

	// Prove maintenance actually recovered, rather than just receiving a
	// resignation request: its run-on-start periodic job must be worked.
	// A run-on-start insertion has its own maintenance transaction timeout.
	// Allow that operation to finish before applying the normal event margin.
	var event *river.Event
	select {
	case event = <-events:
	case <-time.After(riversharedmaintenance.TimeoutDefault + riversharedtest.WaitTimeout()):
		t.Fatal("maintenance recovery periodic job did not finish within its transaction timeout and event margin")
	}
	require.Equal(t, river.EventKindJobCompleted, event.Kind)
	require.Equal(t, (noOpArgs{}).Kind(), event.Job.Kind)
	require.Equal(t, int32(4), startAttempts.Load())
}
