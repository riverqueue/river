package maintenance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
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
			riversharedtest.BaseServiceArchetype(t),
			&QueueCleanerConfig{
				Interval:        queueCleanerIntervalDefault,
				RetentionPeriod: QueueRetentionPeriodDefault,
			},
			bundle.exec)
		cleaner.StaggerStartupDisable(true)
		cleaner.TestSignals.Init()
		t.Cleanup(cleaner.Stop)

		return cleaner, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewQueueCleaner(riversharedtest.BaseServiceArchetype(t), &QueueCleanerConfig{}, nil)

		require.Equal(t, QueueRetentionPeriodDefault, cleaner.Config.RetentionPeriod)
		require.Equal(t, queueCleanerIntervalDefault, cleaner.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riversharedtest.LoggerWarn(t)  // loop started/stop log is very noisy; suppress
		cleaner.TestSignals = QueueCleanerTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, cleaner)
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

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue1.Name,
			Schema: cleaner.Config.Schema,
		})
		require.NoError(t, err) // still there
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue2.Name,
			Schema: cleaner.Config.Schema,
		})
		require.NoError(t, err) // still there

		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue3.Name,
			Schema: cleaner.Config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue4.Name,
			Schema: cleaner.Config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue5.Name,
			Schema: cleaner.Config.Schema,
		})
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

		for i := range numQueues {
			queue := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{
				Name:      ptrutil.Ptr(fmt.Sprintf("queue%d", i)),
				UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-25 * time.Hour)),
			})
			queues[i] = queue
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		for _, queue := range queues {
			_, err := bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name:   queue.Name,
				Schema: cleaner.Config.Schema,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = 1 * time.Microsecond

		require.NoError(t, cleaner.Start(ctx))

		// This should trigger ~immediately every time:
		for i := range 5 {
			t.Logf("Iteration %d", i)
			cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		}
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		require.NoError(t, cleaner.Start(ctx))
		cleaner.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		ctx, cancelFunc := context.WithCancel(ctx)

		require.NoError(t, cleaner.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := cleaner.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		queue1 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue1"), UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-1 * time.Hour))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		cleaner.Stop()

		queue2 := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue2"), UpdatedAt: ptrutil.Ptr(bundle.deleteHorizon.Add(-1 * time.Minute))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue1.Name,
			Schema: cleaner.Config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queue2.Name,
			Schema: cleaner.Config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})
}
