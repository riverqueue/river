package maintenance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

type periodicKeepAliveExecutor struct {
	riverdriver.Executor

	beginErr      error
	commitBarrier <-chan struct{}
	failCommit    atomic.Bool
	testSignals   periodicKeepAliveExecutorTestSignals
}

func (e *periodicKeepAliveExecutor) Begin(context.Context) (riverdriver.ExecutorTx, error) {
	if e.beginErr != nil {
		return nil, e.beginErr
	}
	return &periodicKeepAliveTx{executor: e}, nil
}

type periodicKeepAliveExecutorTestSignals struct {
	Commit        testsignal.TestSignal[error]
	CommitStarted testsignal.TestSignal[struct{}]
}

func (s *periodicKeepAliveExecutorTestSignals) Init(tb testing.TB) {
	tb.Helper()

	s.Commit.Init(tb)
	s.CommitStarted.Init(tb)
}

type periodicKeepAliveTx struct {
	riverdriver.ExecutorTx

	executor *periodicKeepAliveExecutor
}

func (tx *periodicKeepAliveTx) Commit(ctx context.Context) error {
	tx.executor.testSignals.CommitStarted.Signal(struct{}{})
	if tx.executor.commitBarrier != nil {
		select {
		case <-tx.executor.commitBarrier:
		case <-ctx.Done():
		}
	}
	err := ctx.Err()
	if err == nil && tx.executor.failCommit.Load() {
		err = errors.New("commit failed")
	}
	tx.executor.testSignals.Commit.Signal(err)
	return err
}

func (*periodicKeepAliveTx) Rollback(context.Context) error { return nil }

func TestPeriodicJobEnqueuerKeepAliveAfterPersistence(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		blockSchedule       bool
		executor            *periodicKeepAliveExecutor
		initialRows         []*riverpilot.PeriodicJob
		keepAliveIDs        testsignal.TestSignal[[]string]
		now                 time.Time
		pendingScheduleFunc func(time.Time) time.Time
		releaseSchedule     func()
		scheduleEntered     testsignal.TestSignal[struct{}]
		service             *PeriodicJobEnqueuer
		upsertErr           error
	}

	setup := func(t *testing.T) (*testBundle, context.Context) {
		t.Helper()

		ctx, cancel := context.WithCancel(context.Background())
		bundle := &testBundle{executor: &periodicKeepAliveExecutor{}, now: time.Now().UTC()}
		bundle.executor.testSignals.Init(t)
		bundle.keepAliveIDs.Init(t)
		bundle.scheduleEntered.Init(t)
		bundle.initialRows = []*riverpilot.PeriodicJob{{ID: "fresh", NextRunAt: bundle.now.Add(2 * time.Hour)}}

		// Hold a schedule callback while the actual keep-alive loop runs.
		scheduleBarrier := make(chan struct{})
		var releaseOnce sync.Once
		bundle.releaseSchedule = func() { releaseOnce.Do(func() { close(scheduleBarrier) }) }
		bundle.pendingScheduleFunc = func(now time.Time) time.Time {
			if bundle.blockSchedule {
				bundle.scheduleEntered.Signal(struct{}{})
				<-scheduleBarrier
			}
			return now.Add(time.Hour)
		}

		pilot := NewPilotPeriodicJobMock()
		pilot.PeriodicJobGetAllMock = func(context.Context, riverdriver.Executor, *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
			return bundle.initialRows, nil
		}
		pilot.PeriodicJobKeepAliveAndReapMock = func(_ context.Context, _ riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
			bundle.keepAliveIDs.Signal(params.ID)
			return nil, nil
		}
		pilot.PeriodicJobUpsertManyMock = func(context.Context, riverdriver.Executor, *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
			return nil, bundle.upsertErr
		}
		constructor := func() (*rivertype.JobInsertParams, error) { return nil, ErrNoJobToInsert }
		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Time.StubNow(bundle.now)
		var err error
		bundle.service, err = NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
			PeriodicJobs: []*PeriodicJob{
				{ID: "fresh", ConstructorFunc: constructor, ScheduleFunc: func(now time.Time) time.Time { return now.Add(2 * time.Hour) }},
				{ID: "pending", ConstructorFunc: constructor, ScheduleFunc: bundle.pendingScheduleFunc},
			},
			Pilot: pilot,
		}, bundle.executor)
		require.NoError(t, err)
		bundle.service.StaggerStartupDisable(true)
		bundle.service.TestSignals.Init(t)
		t.Cleanup(func() {
			cancel()
			bundle.releaseSchedule()
			bundle.service.Stop()
		})
		return bundle, ctx
	}

	runKeepAlive := func(ctx context.Context, t *testing.T, bundle *testBundle) []string {
		t.Helper()

		ctx, cancel := context.WithCancel(ctx)
		var stopped testsignal.TestSignal[struct{}]
		stopped.Init(t)
		require.NoError(t, bundle.service.periodicJobKeepAliveAndReapPeriodically(ctx, true, func() {}, func() { stopped.Signal(struct{}{}) }))
		defer func() { cancel(); stopped.WaitOrTimeout() }()
		return bundle.keepAliveIDs.WaitOrTimeout()
	}

	t.Run("InitialPersistence", func(t *testing.T) {
		t.Parallel()

		for _, outcome := range []string{"BeginFailed", "CommitFailed", "Committed", "UpsertFailed"} {
			t.Run(outcome, func(t *testing.T) {
				t.Parallel()

				bundle, ctx := setup(t)
				switch outcome {
				case "BeginFailed":
					bundle.executor.beginErr = errors.New("begin failed")
				case "CommitFailed":
					bundle.executor.failCommit.Store(true)
				case "UpsertFailed":
					bundle.upsertErr = errors.New("upsert failed")
				}
				require.NoError(t, bundle.service.Start(ctx))
				bundle.service.TestSignals.EnteredLoop.WaitOrTimeout()
				bundle.service.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
				bundle.keepAliveIDs.WaitOrTimeout()

				expected := []string{"fresh"}
				if outcome == "Committed" {
					expected = append(expected, "pending")
				}
				require.ElementsMatch(t, expected, runKeepAlive(ctx, t, bundle))
			})
		}
	})

	t.Run("LaterCommittedOccurrence", func(t *testing.T) {
		t.Parallel()

		bundle, ctx := setup(t)
		bundle.executor.failCommit.Store(true)
		require.NoError(t, bundle.service.Start(ctx))
		bundle.service.TestSignals.EnteredLoop.WaitOrTimeout()
		bundle.service.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
		bundle.keepAliveIDs.WaitOrTimeout()

		bundle.executor.failCommit.Store(false)
		bundle.service.Time.StubNow(bundle.now.Add(time.Hour))
		bundle.service.recalculateNextRun <- struct{}{}
		bundle.service.TestSignals.PeriodicJobUpserted.WaitOrTimeout()

		require.ElementsMatch(t, []string{"fresh", "pending"}, runKeepAlive(ctx, t, bundle))
	})

	t.Run("PendingStartup", func(t *testing.T) {
		t.Parallel()

		bundle, ctx := setup(t)
		bundle.blockSchedule = true
		require.NoError(t, bundle.service.Start(ctx))
		bundle.scheduleEntered.WaitOrTimeout()

		require.ElementsMatch(t, []string{"fresh"}, bundle.keepAliveIDs.WaitOrTimeout())
	})

	t.Run("RemovedAndReadded", func(t *testing.T) {
		t.Parallel()

		bundle, ctx := setup(t)
		// Replace the registration while its original transaction is committing.
		commitBarrier := make(chan struct{})
		var releaseOnce sync.Once
		releaseCommit := func() { releaseOnce.Do(func() { close(commitBarrier) }) }
		t.Cleanup(releaseCommit)
		bundle.executor.commitBarrier = commitBarrier
		require.NoError(t, bundle.service.Start(ctx))
		bundle.executor.testSignals.CommitStarted.WaitOrTimeout()
		bundle.service.mu.RLock()
		pendingJob := bundle.service.periodicJobs[bundle.service.periodicJobIDs["pending"]]
		bundle.service.mu.RUnlock()
		require.True(t, bundle.service.RemoveByID("pending"))
		bundle.blockSchedule = true
		// Reuse the pointer so a late commit could mark the new registration.
		_, err := bundle.service.AddSafely(pendingJob)
		require.NoError(t, err)
		releaseCommit()
		bundle.scheduleEntered.WaitOrTimeout()
		bundle.service.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
		bundle.keepAliveIDs.WaitOrTimeout()

		require.ElementsMatch(t, []string{"fresh"}, runKeepAlive(ctx, t, bundle))
	})

	t.Run("Restarted", func(t *testing.T) {
		t.Parallel()

		bundle, ctx := setup(t)
		require.NoError(t, bundle.service.Start(ctx))
		bundle.service.TestSignals.EnteredLoop.WaitOrTimeout()
		bundle.service.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()
		bundle.keepAliveIDs.WaitOrTimeout()
		bundle.service.Stop()

		bundle.initialRows = nil
		bundle.blockSchedule = true
		bundle.service.TestSignals.Init(t)
		require.NoError(t, bundle.service.Start(ctx))
		bundle.scheduleEntered.WaitOrTimeout()
		bundle.service.TestSignals.PeriodicJobKeepAliveAndReap.WaitOrTimeout()

		require.Empty(t, bundle.keepAliveIDs.WaitOrTimeout())
	})
}
