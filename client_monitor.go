package river

import (
	"sync"

	"github.com/riverqueue/river/internal/componentstatus"
)

type clientMonitor struct {
	// internal buffer of status updates/snapshots awaiting broadcast
	snapshotBuffer chan snapshotAndSubscribers

	statusSnapshotMu    sync.Mutex
	snapshotSubscribers []chan<- componentstatus.ClientSnapshot
	currentSnapshot     componentstatus.ClientSnapshot

	shutdownOnce *sync.Once
	shutdownCh   chan struct{}
	doneCh       chan struct{}
}

func newClientMonitor() *clientMonitor {
	return &clientMonitor{
		snapshotSubscribers: make([]chan<- componentstatus.ClientSnapshot, 0),
		// This serves as an ordered buffer of status update snapshots. We allow a small buffer
		// so that the senders can avoid blocking and to account for some delivery delay.
		snapshotBuffer:  make(chan snapshotAndSubscribers, 100),
		currentSnapshot: componentstatus.ClientSnapshot{Producers: make(map[string]componentstatus.Status)},

		shutdownOnce: &sync.Once{},
		shutdownCh:   make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

func (m *clientMonitor) Run() {
	for {
		select {
		case <-m.shutdownCh:
			close(m.doneCh)
			return
		case update := <-m.snapshotBuffer:
			m.broadcastOneUpdate(update)
		}
	}
}

// Shutdown initiates a shutdown of the Monitor and blocks until shutdown has completed.
func (m *clientMonitor) Shutdown() {
	m.shutdownOnce.Do(func() {
		close(m.shutdownCh)
	})
	// Wait for done signal:
	<-m.doneCh
}

// InititializeProducerStatus sets the status for a new producer to
// uninitialized.  Unlike SetProducerStatus, it does not broadcast the change
// and is only meant to be used during initial client startup.
func (m *clientMonitor) InitializeProducerStatus(queueName string) {
	m.currentSnapshot.Producers[queueName] = componentstatus.Uninitialized
}

func (m *clientMonitor) SetProducerStatus(queueName string, status componentstatus.Status) {
	m.statusSnapshotMu.Lock()
	defer m.statusSnapshotMu.Unlock()
	m.currentSnapshot.Producers[queueName] = status
	m.bufferStatusUpdate()
}

func (m *clientMonitor) SetElectorStatus(newStatus componentstatus.ElectorStatus) {
	m.statusSnapshotMu.Lock()
	defer m.statusSnapshotMu.Unlock()
	m.currentSnapshot.Elector = newStatus
	m.bufferStatusUpdate()
}

func (m *clientMonitor) SetNotifierStatus(newStatus componentstatus.Status) {
	m.statusSnapshotMu.Lock()
	defer m.statusSnapshotMu.Unlock()
	m.currentSnapshot.Notifier = newStatus
	m.bufferStatusUpdate()
}

func (m *clientMonitor) RegisterUpdates() <-chan componentstatus.ClientSnapshot {
	snapshotCh := make(chan componentstatus.ClientSnapshot, 100)

	m.statusSnapshotMu.Lock()
	defer m.statusSnapshotMu.Unlock()
	m.snapshotSubscribers = append(m.snapshotSubscribers, snapshotCh)
	return snapshotCh
}

// must be run with c.statusUpdateMu already held. Copies the current health snapshot
// and the list of subscribers, then buffers a status snapshot so it can be broadcast to
// subscribers outside of the mutex lock.
func (m *clientMonitor) bufferStatusUpdate() {
	snapshot := m.currentSnapshot.Copy()
	subs := make([]chan<- componentstatus.ClientSnapshot, len(m.snapshotSubscribers))
	copy(subs, m.snapshotSubscribers)
	select {
	case m.snapshotBuffer <- snapshotAndSubscribers{snapshot: snapshot, subscribers: subs}:
	default:
		// TODO: status update buffer full :(
	}
}

func (m *clientMonitor) broadcastOneUpdate(update snapshotAndSubscribers) {
	for i := range update.subscribers {
		select {
		case update.subscribers[i] <- update.snapshot:
		default:
			// dropped update because subscriber's buffer was full
		}
	}
}

type snapshotAndSubscribers struct {
	snapshot    componentstatus.ClientSnapshot
	subscribers []chan<- componentstatus.ClientSnapshot
}
