package maintenance

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestQueueStateCounter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
	}

	setup := func(t *testing.T, hooks []rivertype.Hook, queueNames []string) (*QueueStateCounter, *testBundle) {
		t.Helper()

		var (
			tx   = riverdbtest.TestTxPgx(ctx, t)
			exec = riverpgxv5.New(nil).UnwrapExecutor(tx)
		)

		svc := NewQueueStateCounter(
			riversharedtest.BaseServiceArchetype(t),
			&QueueStateCounterConfig{
				HookLookupGlobal: hooklookup.NewHookLookup(hooks),
				QueueNames:       queueNames,
			},
			exec,
		)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)
		t.Cleanup(svc.Stop)

		return svc, &testBundle{exec: exec}
	}

	t.Run("CountsJobsByQueueAndState", func(t *testing.T) {
		t.Parallel()

		hook := &capturingQueueStateCountHook{}
		svc, bundle := setup(t, []rivertype.Hook{hook}, []string{"queue1", "queue2", "queue_empty"})

		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.CountedOnce.WaitOrTimeout()

		counts := hook.LastParams().ByQueue

		// All three configured queues are present, including queue_empty
		// which has no jobs.
		require.Len(t, counts, 3)

		// All job states are present for each queue, even those with zero jobs.
		for _, queue := range []string{"queue1", "queue2", "queue_empty"} {
			require.Len(t, counts[queue].ByState, len(rivertype.JobStates()), "queue %q should have all states", queue)
		}

		require.Equal(t, 2, counts["queue1"].ByState[rivertype.JobStateAvailable])
		require.Equal(t, 1, counts["queue1"].ByState[rivertype.JobStateRunning])
		require.Equal(t, 0, counts["queue1"].ByState[rivertype.JobStateCompleted])
		require.Equal(t, 3, counts["queue1"].Total)

		require.Equal(t, 1, counts["queue2"].ByState[rivertype.JobStateAvailable])
		require.Equal(t, 2, counts["queue2"].ByState[rivertype.JobStateCompleted])
		require.Equal(t, 1, counts["queue2"].ByState[rivertype.JobStateDiscarded])
		require.Equal(t, 0, counts["queue2"].ByState[rivertype.JobStateRunning])
		require.Equal(t, 4, counts["queue2"].Total)

		// queue_empty has all states present, all zero.
		for _, state := range rivertype.JobStates() {
			require.Equal(t, 0, counts["queue_empty"].ByState[state], "queue_empty[%s] should be 0", state)
		}
		require.Equal(t, 0, counts["queue_empty"].Total)
	})

	t.Run("NoopsWithoutHooks", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t, nil, nil)

		// With no hooks, Start returns immediately without starting the service.
		require.NoError(t, svc.Start(ctx))

		// Service should not have started, so calling Start again should also
		// succeed (StartInit would return false if it were already running).
		require.NoError(t, svc.Start(ctx))
	})

	t.Run("StartStop", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t, []rivertype.Hook{&capturingQueueStateCountHook{}}, nil)

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.CountedOnce.WaitOrTimeout()
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t, []rivertype.Hook{&capturingQueueStateCountHook{}}, nil)
		svc.Logger = riversharedtest.LoggerWarn(t)       // loop started/stop log is very noisy; suppress
		svc.TestSignals = QueueStateCounterTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, svc)
	})

	t.Run("StartStopStressNoHooks", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t, nil, nil)
		svc.Logger = riversharedtest.LoggerWarn(t)       // loop started/stop log is very noisy; suppress
		svc.TestSignals = QueueStateCounterTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, svc)
	})
}

// capturingQueueStateCountHook captures the last params received from the hook
// invocation.
type capturingQueueStateCountHook struct {
	mu         sync.Mutex
	lastParams *rivertype.HookQueueStateCountParams
}

func (h *capturingQueueStateCountHook) IsHook() bool { return true }

func (h *capturingQueueStateCountHook) QueueStateCount(_ context.Context, params *rivertype.HookQueueStateCountParams) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastParams = params
}

func (h *capturingQueueStateCountHook) LastParams() *rivertype.HookQueueStateCountParams {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastParams
}
