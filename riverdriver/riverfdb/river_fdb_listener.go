//go:build foundationdb

package riverfdb

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const notificationBatchSize = 256

var _ riverdriver.Listener = (*Listener)(nil)

// Listener receives committed notifications using a schema's notification log
// and a FoundationDB watch. Each listener has its own cursor, so delivery fans
// out to all subscribers. Watches are one shot and only signal that the log may
// have changed; payloads always come from the log.
type Listener struct {
	TestSignals ListenerTestSignals

	driver *Driver
	schema string
	waitMu sync.Mutex

	mu               sync.Mutex
	afterConnectExec string
	generation       uint64
	isConnected      bool
	lastID           int64
	pending          []*notificationRecord
	topics           map[string]int64
	waitCancelFunc   context.CancelFunc
}

func (l *Listener) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.waitCancelFunc != nil {
		l.waitCancelFunc()
	}
	l.isConnected = false
	l.pending = nil
	l.topics = nil
	return nil
}

func (l *Listener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isConnected {
		return errors.New("riverfdb: listener is already connected")
	}
	if l.afterConnectExec != "" {
		return unsupported("listener SQL")
	}
	id, err := l.sequence(ctx)
	if err != nil {
		return err
	}
	l.generation++
	l.isConnected = true
	l.lastID = id
	l.pending = nil
	l.topics = make(map[string]int64)
	return nil
}

func (l *Listener) Listen(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isConnected {
		return errors.New("riverfdb: listener is not connected")
	}
	if _, ok := l.topics[topic]; ok {
		return nil
	}
	id, err := l.sequence(ctx)
	if err != nil {
		return err
	}
	// Only change this topic's cursor, preserving other topics' unread rows.
	l.topics[topic] = id
	return nil
}

func (l *Listener) Ping(ctx context.Context) error {
	l.mu.Lock()
	connected := l.isConnected
	l.mu.Unlock()
	if !connected {
		return errors.New("riverfdb: listener is not connected")
	}
	return l.driver.GetExecutor().Ping(ctx)
}

func (l *Listener) Schema() string { return l.schema }

func (l *Listener) SetAfterConnectExec(sql string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.afterConnectExec = sql
}

func (l *Listener) Unlisten(_ context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isConnected {
		return errors.New("riverfdb: listener is not connected")
	}
	delete(l.topics, topic)
	return nil
}

func (l *Listener) WaitForNotification(ctx context.Context) (*riverdriver.Notification, error) {
	if !l.waitMu.TryLock() {
		return nil, errors.New("riverfdb: listener is already waiting")
	}
	defer l.waitMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l.mu.Lock()
	if !l.isConnected {
		l.mu.Unlock()
		return nil, errors.New("riverfdb: listener is not connected")
	}
	generation := l.generation
	l.waitCancelFunc = cancel
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.waitCancelFunc = nil
		l.mu.Unlock()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		l.mu.Lock()
		if !l.isConnected || generation != l.generation {
			l.mu.Unlock()
			return nil, errors.New("riverfdb: listener is closed")
		}
		for len(l.pending) > 0 {
			notification := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			if startID, ok := l.topics[notification.Topic]; ok && notification.ID > startID {
				l.mu.Unlock()
				return &riverdriver.Notification{Payload: notification.Payload, Topic: notification.Topic}, nil
			}
		}
		after := l.lastID
		l.mu.Unlock()

		pending, watch, err := l.readOrWatch(ctx, after)
		if err != nil {
			return nil, err
		}
		if watch != nil {
			// Cancellation releases the native watch, including when River
			// interrupts a wait to change subscriptions or ping the database.
			stopCancelFunc := context.AfterFunc(ctx, watch.Cancel)
			l.TestSignals.WatchArmed.Signal(struct{}{})
			err := watch.Get()
			stopCancelFunc()
			watch.Cancel()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil {
				return nil, err
			}
			continue
		}
		l.mu.Lock()
		if !l.isConnected || generation != l.generation {
			l.mu.Unlock()
			return nil, errors.New("riverfdb: listener is closed")
		}
		l.pending = pending
		l.lastID = pending[len(pending)-1].ID
		l.mu.Unlock()
	}
}

func (l *Listener) readOrWatch(ctx context.Context, after int64) ([]*notificationRecord, fdb.FutureNil, error) {
	var watch fdb.FutureNil
	pending, err := transact(ctx, &executor{driver: l.driver}, func(tx fdb.Transaction) ([]*notificationRecord, error) {
		// A failed commit invalidates its watch. Discard it before retrying.
		if watch != nil {
			watch.Cancel()
			watch = nil
		}
		entries, err := tx.GetRange(fdb.SelectorRange{
			Begin: fdb.FirstGreaterThan(l.driver.key(l.schema, "notification", after)),
			End:   fdb.FirstGreaterOrEqual(prefixRange(l.driver.key(l.schema, "notification")).End),
		}, fdb.RangeOptions{Limit: notificationBatchSize}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		pending := make([]*notificationRecord, 0, len(entries))
		for _, entry := range entries {
			var notification notificationRecord
			if err := json.Unmarshal(entry.Value, &notification); err != nil {
				return nil, err
			}
			pending = append(pending, &notification)
		}
		if len(pending) == 0 {
			// The range read and watch share a read version. A publisher that
			// commits after this read wakes the watch, even if it commits before
			// the watch is armed. There is no read-to-watch polling gap.
			watch = tx.Watch(l.driver.key(l.schema, "notification_sequence"))
		}
		return pending, nil
	})
	if err != nil && watch != nil {
		watch.Cancel()
		watch = nil
	}
	return pending, watch, err
}

func (l *Listener) sequence(ctx context.Context) (int64, error) {
	return transact(ctx, &executor{driver: l.driver}, func(tx fdb.Transaction) (int64, error) {
		return notificationSequence(tx, l.driver.key(l.schema, "notification_sequence"))
	})
}

// ListenerTestSignals are internal signals used exclusively in tests.
type ListenerTestSignals struct {
	WatchArmed testsignal.TestSignal[struct{}]
}

// Init enables listener test signals.
func (ts *ListenerTestSignals) Init(tb testutil.TestingTB) {
	ts.WatchArmed.Init(tb)
}
