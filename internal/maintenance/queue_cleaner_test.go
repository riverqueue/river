package maintenance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func TestQueueCleaner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		deleteHorizon time.Time
		exec          riverdriver.Executor
	}

	setup := func(t *testing.T) (*QueueCleaner, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
		bundle := &testBundle{
			deleteHorizon: time.Now().Add(-QueueRetentionPeriodDefault),
			exec:          riverpgxv5.New(nil).UnwrapExecutor(tx),
		}

		cleaner := NewQueueCleaner(
			riverinternaltest.BaseServiceArchetype(t),
			&QueueCleanerConfig{
				Interval:        queueCleanerIntervalDefault,
				RetentionPeriod: QueueRetentionPeriodDefault,
			},
			bundle.exec)
		cleaner.TestSignals.Init()

		return cleaner, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewQueueCleaner(riverinternaltest.BaseServiceArchetype(t), &QueueCleanerConfig{}, nil)

		require.Equal(t, QueueRetentionPeriodDefault, cleaner.Config.RetentionPeriod)
		require.Equal(t, queueCleanerIntervalDefault, cleaner.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		cleaner.TestSignals = QueueCleanerTestSignals{}  // deinit so channels don't fill

		wrapped := baseservice.Init(riverinternaltest.BaseServiceArchetype(t), &maintenanceServiceWrapper{
			service: cleaner,
		})
		startstoptest.Stress(ctx, t, wrapped)
	})

	t.Run("DeletesExpiredQueues", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)

		now := time.Now()
		// None of these should get removed:
		queue1 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue1"), UpdatedAt: ptrutil.Ptr(now)})
		queue2 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue2"), UpdatedAt: ptrutil.Ptr(now.Add(-23 * time.Hour))})

		// These get deleted:
		queue3 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue3"), UpdatedAt: ptrutil.Ptr(now.Add(-25 * time.Hour))})
		queue4 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue4"), UpdatedAt: ptrutil.Ptr(now.Add(-26 * time.Hour))})
		queue5 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue5"), UpdatedAt: ptrutil.Ptr(now.Add(-48 * time.Hour))})

		runMaintenanceService(ctx, t, cleaner)

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.QueueGet(ctx, queue1.Name)
		require.NoError(t, err) // still there
		_, err = bundle.exec.QueueGet(ctx, queue2.Name)
		require.NoError(t, err) // still there

		_, err = bundle.exec.QueueGet(ctx, queue3.Name)
		require.ErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.QueueGet(ctx, queue4.Name)
		require.ErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.QueueGet(ctx, queue5.Name)
		require.ErrorIs(t, err, rivertype.ErrNotFound) // still there
	})

	t.Run("DeletesInBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numQueues := cleaner.batchSize + 1

		queues := make([]*rivertype.Queue, numQueues)

		for i := 0; i < numQueues; i++ {
			queue := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{
				Name:      ptrutil.Ptr(fmt.Sprintf("queue%d", i)),
				UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-25 * time.Hour)),
			})
			queues[i] = queue
		}

		runMaintenanceService(ctx, t, cleaner)

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		for _, queue := range queues {
			_, err := bundle.exec.QueueGet(ctx, queue.Name)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = 1 * time.Microsecond

		runMaintenanceService(ctx, t, cleaner)

		// This should trigger ~immediately every time:
		for i := 0; i < 5; i++ {
			t.Logf("Iteration %d", i)
			cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		}
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		MaintenanceServiceStopsImmediately(ctx, t, cleaner)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		queue1 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue1"), UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-1 * time.Hour))})

		ctx1, cancel1 := context.WithCancel(ctx)
		stopCh1 := make(chan struct{})
		go func() {
			defer close(stopCh1)
			cleaner.Run(ctx1)
		}()
		t.Cleanup(func() { riverinternaltest.WaitOrTimeout(t, stopCh1) })
		t.Cleanup(cancel1)

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		cancel1()
		riverinternaltest.WaitOrTimeout(t, stopCh1)

		queue2 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue2"), UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-1 * time.Minute))})

		ctx2, cancel2 := context.WithCancel(ctx)
		stopCh2 := make(chan struct{})
		go func() {
			defer close(stopCh2)
			cleaner.Run(ctx2)
		}()
		t.Cleanup(func() { riverinternaltest.WaitOrTimeout(t, stopCh2) })
		t.Cleanup(cancel2)

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.QueueGet(ctx, queue1.Name)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.QueueGet(ctx, queue2.Name)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})
}
