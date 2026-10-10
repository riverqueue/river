package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Notification is one notification River published.
type Notification struct {
	Payload string

	// PayloadType is SQLite's storage type of the payload, and empty on
	// Postgres.
	PayloadType string

	Topic string
}

// Notifications captures the notifications published on River's topics:
// on Postgres by listening on the scenario schema's channels, and on
// SQLite by reading the notification outbox.
type Notifications struct {
	afterID   int64
	db        *Database
	listeners map[string]*pgx.Conn
	marker    int
}

// Topics River publishes notifications on.
var notificationTopics = []string{"river_control", "river_insert", "river_leadership"} //nolint:gochecknoglobals // fixed list

// Listen starts capturing notifications.
func (d *Database) Listen(t *testing.T) *Notifications {
	t.Helper()

	capture := &Notifications{db: d}
	if d.pool == nil {
		_ = capture.Next(t)
		return capture
	}

	capture.listeners = make(map[string]*pgx.Conn)
	for _, topic := range notificationTopics {
		config, err := pgx.ParseConfig(d.baseURL)
		require.NoError(t, err)
		config.RuntimeParams["application_name"] = harnessApplicationName
		conn, err := pgx.ConnectConfig(context.Background(), config)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		_, err = conn.Exec(context.Background(), "LISTEN "+pgx.Identifier{d.Schema + "." + topic}.Sanitize())
		require.NoError(t, err)
		capture.listeners[topic] = conn
	}
	return capture
}

// Next returns the notifications published since the previous call, grouped
// by topic in the order of notificationTopics, each in commit order. On
// Postgres it sends a marker on each channel and returns everything
// delivered before it, which includes every notification committed earlier.
func (n *Notifications) Next(t *testing.T) []Notification {
	t.Helper()

	var notifications []Notification
	if n.listeners == nil {
		rows, err := n.db.sqlite.QueryContext(context.Background(),
			"SELECT id, payload, typeof(payload), topic FROM river_notification WHERE id > ? ORDER BY id", n.afterID)
		require.NoError(t, err)
		defer rows.Close()
		byTopic := make(map[string][]Notification)
		for rows.Next() {
			var notification Notification
			require.NoError(t, rows.Scan(&n.afterID, &notification.Payload, &notification.PayloadType, &notification.Topic))
			byTopic[notification.Topic] = append(byTopic[notification.Topic], notification)
		}
		require.NoError(t, rows.Err())
		for _, topic := range notificationTopics {
			notifications = append(notifications, byTopic[topic]...)
		}
		return notifications
	}

	for _, topic := range notificationTopics {
		n.marker++
		// Valid JSON for a queue nothing works, so River's listeners ignore it.
		marker := fmt.Sprintf(`{"action":"conformance_marker","marker":%d,"queue":"conformance_marker"}`, n.marker)
		channel := n.db.Schema + "." + topic
		_, err := n.db.pool.Exec(context.Background(), "SELECT pg_notify($1, $2)", channel, marker)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		for {
			notification, err := n.listeners[topic].WaitForNotification(ctx)
			require.NoError(t, err, "marker %s wasn't delivered", marker)
			if notification.Payload == marker {
				break
			}
			notifications = append(notifications, Notification{Payload: notification.Payload, Topic: topic})
		}
		cancel()
	}
	return notifications
}

// ConnectionCount counts an adapter's Postgres connections.
func (d *Database) ConnectionCount(t *testing.T, adapter *Adapter) int {
	t.Helper()

	var count int
	d.QueryRow(t, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", []any{adapter.ApplicationName}, &count)
	return count
}

// ListenerCount counts an adapter's Postgres connections that listen for
// notifications.
func (d *Database) ListenerCount(t *testing.T, adapter *Adapter) int {
	t.Helper()

	var count int
	d.QueryRow(t, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND query ILIKE 'listen %'",
		[]any{adapter.ApplicationName}, &count)
	return count
}

// NextTransactionID returns the next transaction ID Postgres will assign.
// Read-only statements don't consume IDs, so the difference between two
// readings counts the write transactions in between.
func (d *Database) NextTransactionID(t *testing.T) int64 {
	t.Helper()

	var next int64
	d.QueryRow(t, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint", nil, &next)
	return next
}

// TerminateConnections terminates an adapter's Postgres connections, only
// its listeners if listenersOnly, and returns how many it terminated.
func (d *Database) TerminateConnections(t *testing.T, adapter *Adapter, listenersOnly bool) int {
	t.Helper()

	query := "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = $1"
	if listenersOnly {
		query += " AND query ILIKE 'listen %'"
	}
	var count int
	d.QueryRow(t, query, []any{adapter.ApplicationName}, &count)
	return count
}

// WaitListening waits until an adapter listens for notifications. On SQLite,
// where notifications are polled, it returns at once.
func (d *Database) WaitListening(t *testing.T, adapter *Adapter) {
	t.Helper()

	if d.pool == nil {
		return
	}
	WaitFor(t, adapter.Label+" listening", 10*time.Second, func() bool { return d.ListenerCount(t, adapter) > 0 })
}

// WaitLockWait waits until a statement of the adapter is blocked on a lock,
// which proves a request is waiting on another transaction rather than
// merely being slow.
func (d *Database) WaitLockWait(t *testing.T, adapter *Adapter) {
	t.Helper()

	WaitFor(t, adapter.Label+" waiting on a lock", 10*time.Second, func() bool { return d.LockWaiters(t, adapter) > 0 })
}

// LockWaiters counts an adapter's statements blocked on a lock.
func (d *Database) LockWaiters(t *testing.T, adapter *Adapter) int {
	t.Helper()

	var count int
	d.QueryRow(t, `SELECT count(*) FROM pg_stat_activity
		WHERE application_name = $1 AND state = 'active' AND wait_event_type = 'Lock'`,
		[]any{adapter.ApplicationName}, &count)
	return count
}
