package river

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

func Test_Monitor_Stop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	monitor := newClientMonitor()
	require.NoError(t, monitor.Start(ctx))

	stopDone := make(chan struct{})
	go func() {
		monitor.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout awaiting monitor shutdown")
	}
}

func awaitSnapshot(t *testing.T, snapshotCh <-chan componentstatus.ClientSnapshot) componentstatus.ClientSnapshot {
	t.Helper()
	select {
	case update := <-snapshotCh:
		return update
	case <-time.After(5 * time.Second):
		t.Fatal("timeout awaiting snapshot")
		return componentstatus.ClientSnapshot{}
	}
}

func Test_Monitor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T, initialProducerQueues ...string) (*clientMonitor, <-chan componentstatus.ClientSnapshot) {
		t.Helper()
		monitor := newClientMonitor()
		snapshotCh := monitor.RegisterUpdates()

		for _, queueName := range initialProducerQueues {
			monitor.InitializeProducerStatus(queueName)
		}

		require.NoError(t, monitor.Start(ctx))
		t.Cleanup(monitor.Stop)

		return monitor, snapshotCh
	}

	t.Run("ElectorUpdates", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		monitor, snapshotCh := setup(t)

		monitor.SetElectorStatus(componentstatus.ElectorLeader)
		update := awaitSnapshot(t, snapshotCh)
		require.Empty(update.Producers)
		require.Equal(componentstatus.ElectorLeader, update.Elector)
		require.Equal(componentstatus.Uninitialized, update.Notifier)

		monitor.SetElectorStatus(componentstatus.ElectorResigning)
		update = awaitSnapshot(t, snapshotCh)
		require.Equal(componentstatus.ElectorResigning, update.Elector)
	})

	t.Run("NotifierUpdates", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		monitor, snapshotCh := setup(t)

		monitor.SetNotifierStatus(componentstatus.Initializing)
		update := awaitSnapshot(t, snapshotCh)
		require.Empty(update.Producers)
		require.Equal(componentstatus.ElectorNonLeader, update.Elector)
		require.Equal(componentstatus.Initializing, update.Notifier)

		monitor.SetNotifierStatus(componentstatus.Healthy)
		update = awaitSnapshot(t, snapshotCh)
		require.Equal(componentstatus.Healthy, update.Notifier)

		monitor.SetNotifierStatus(componentstatus.Unhealthy)
		update = awaitSnapshot(t, snapshotCh)
		require.Equal(componentstatus.Unhealthy, update.Notifier)
	})

	t.Run("ProducerUpdates", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		monitor, snapshotCh := setup(t, "queue1", "queue2")

		monitor.SetProducerStatus("queue2", componentstatus.Initializing)
		update := awaitSnapshot(t, snapshotCh)
		require.Len(update.Producers, 2)
		require.Equal(componentstatus.Uninitialized, update.Producers["queue1"])
		require.Equal(componentstatus.Initializing, update.Producers["queue2"])
		require.Equal(componentstatus.ElectorNonLeader, update.Elector)
		require.Equal(componentstatus.Uninitialized, update.Notifier)

		monitor.SetProducerStatus("queue2", componentstatus.Healthy)
		update = awaitSnapshot(t, snapshotCh)
		require.Len(update.Producers, 2)
		require.Equal(componentstatus.Uninitialized, update.Producers["queue1"])
		require.Equal(componentstatus.Healthy, update.Producers["queue2"])
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		monitor, snapshotCh := setup(t)
		t.Cleanup(riverinternaltest.DiscardContinuously(snapshotCh))

		startstoptest.Stress(ctx, t, monitor)
	})
}
