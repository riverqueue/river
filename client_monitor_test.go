package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
)

func Test_Monitor_Shutdown(t *testing.T) {
	t.Parallel()

	monitor := newClientMonitor()
	go monitor.Run()

	shutdownDone := make(chan struct{})
	go func() {
		monitor.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
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

	setup := func(t *testing.T, initialProducerQueues ...string) (*clientMonitor, <-chan componentstatus.ClientSnapshot) {
		t.Helper()
		monitor := newClientMonitor()
		snapshotCh := monitor.RegisterUpdates()

		for _, queueName := range initialProducerQueues {
			monitor.InitializeProducerStatus(queueName)
		}

		go monitor.Run()
		t.Cleanup(monitor.Shutdown)
		return monitor, snapshotCh
	}

	t.Run("ElectorUpdates", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)
		monitor, snapshotCh := setup(t)

		monitor.SetElectorStatus(componentstatus.ElectorLeader)
		update := awaitSnapshot(t, snapshotCh)
		require.Len(update.Producers, 0)
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
		require.Len(update.Producers, 0)
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
}
