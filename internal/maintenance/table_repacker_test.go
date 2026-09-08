package maintenance

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

type tableRepackerExecutorMock struct {
	riverdriver.Executor

	tableRepackCalls atomic.Int32
	tableRepackFunc  func(ctx context.Context, params *riverdriver.TableRepackParams) error
}

func newTableRepackerExecutorMock(exec riverdriver.Executor) *tableRepackerExecutorMock {
	return &tableRepackerExecutorMock{
		Executor:        exec,
		tableRepackFunc: exec.TableRepack,
	}
}

func (m *tableRepackerExecutorMock) TableRepack(ctx context.Context, params *riverdriver.TableRepackParams) error {
	m.tableRepackCalls.Add(1)
	return m.tableRepackFunc(ctx, params)
}

func TestTableRepacker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		schema string
	}

	setup := func(t *testing.T) (*TableRepacker, *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		bundle := &testBundle{
			exec:   riverpgxv5.New(dbPool).GetExecutor(),
			schema: schema,
		}

		archetype := riversharedtest.BaseServiceArchetype(t)

		fromNow := func(d time.Duration) func(time.Time) time.Time {
			return func(t time.Time) time.Time {
				return t.Add(d)
			}
		}

		svc := NewTableRepacker(archetype, &TableRepackerConfig{
			ScheduleFunc: fromNow(500 * time.Millisecond),
			Schema:       schema,
		}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)
		t.Cleanup(svc.Stop)

		return svc, bundle
	}

	runImmediatelyThenOnceAnHour := func() func(time.Time) time.Time {
		alreadyRan := false
		return func(t time.Time) time.Time {
			if alreadyRan {
				return t.Add(time.Hour)
			}
			alreadyRan = true
			return t.Add(time.Millisecond)
		}
	}

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)
		svc.Logger = riversharedtest.LoggerWarn(t)   // loop started/stop log is very noisy; suppress
		svc.TestSignals = TableRepackerTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, svc)
	})

	t.Run("RepackSuccess", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		mockExec := newTableRepackerExecutorMock(bundle.exec)
		mockExec.tableRepackFunc = func(ctx context.Context, params *riverdriver.TableRepackParams) error {
			return nil
		}
		svc.exec = mockExec

		svc.Config.ScheduleFunc = runImmediatelyThenOnceAnHour()

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.Repacked.WaitOrTimeout()

		require.GreaterOrEqual(t, int(mockExec.tableRepackCalls.Load()), 1)
	})

	t.Run("RepackUsesVacuumFullWhenDetected", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		var repackedWithVacuumFull atomic.Bool
		mockExec := newTableRepackerExecutorMock(bundle.exec)
		mockExec.tableRepackFunc = func(ctx context.Context, params *riverdriver.TableRepackParams) error {
			if params.UseVacuumFull {
				repackedWithVacuumFull.Store(true)
			}
			return nil
		}
		svc.exec = mockExec
		svc.useVacuumFull = true // simulate Postgres < 19

		svc.Config.ScheduleFunc = runImmediatelyThenOnceAnHour()

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.Repacked.WaitOrTimeout()

		require.True(t, repackedWithVacuumFull.Load())
	})

	t.Run("RepackPassesSchemaAndTable", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		var capturedParams atomic.Pointer[riverdriver.TableRepackParams]
		mockExec := newTableRepackerExecutorMock(bundle.exec)
		mockExec.tableRepackFunc = func(ctx context.Context, params *riverdriver.TableRepackParams) error {
			capturedParams.Store(params)
			return nil
		}
		svc.exec = mockExec

		svc.Config.ScheduleFunc = runImmediatelyThenOnceAnHour()

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.Repacked.WaitOrTimeout()

		params := capturedParams.Load()
		require.NotNil(t, params)
		require.Equal(t, "river_job", params.Table)
		require.Equal(t, bundle.schema, params.Schema)
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		require.NoError(t, svc.Start(ctx))
		svc.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		ctx, cancelFunc := context.WithCancel(ctx)
		require.NoError(t, svc.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := svc.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("AppliesDefaultScheduleAndTimeout", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)
		now := time.Now().UTC()
		svc = NewTableRepacker(&svc.Archetype, &TableRepackerConfig{}, bundle.exec)

		require.Equal(t, TableRepackerTimeoutDefault, svc.Config.Timeout)
		require.Equal(t, svc.Config.ScheduleFunc(now), (&DefaultTableRepackerSchedule{}).Next(now))
	})
}

func TestDefaultTableRepackerSchedule(t *testing.T) {
	t.Parallel()

	t.Run("WithMidnightInputReturns1AM24HoursLater", func(t *testing.T) {
		t.Parallel()

		schedule := &DefaultTableRepackerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 0, 0, 0, 0, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC), result)
	})

	t.Run("With1NanosecondBeforeMidnightItReturnsUpcoming1AM", func(t *testing.T) {
		t.Parallel()

		schedule := &DefaultTableRepackerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 23, 59, 59, 999999999, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC), result)
	})
}
