//go:build riverconformance

package harness_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// harnessApplicationName identifies the harness's own observation
// connections so fault injection that targets adapters never disconnects them.
const harnessApplicationName = "river-conformance-harness"

// postgresObserver makes observations that one adapter cannot make about
// another through the protocol: lock waits, transaction ID consumption, and
// raw notification delivery. It never writes River tables.
type postgresObserver struct {
	databaseURL string
	pool        *pgxpool.Pool
}

func newPostgresObserver(t *testing.T, databaseURL string) *postgresObserver {
	t.Helper()

	config, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["application_name"] = harnessApplicationName
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &postgresObserver{databaseURL: databaseURL, pool: pool}
}

// currentSchema returns the schema River uses when no schema is configured.
// Notification channels are prefixed with it.
func (observer *postgresObserver) currentSchema(t *testing.T) string {
	t.Helper()

	var schema string
	require.NoError(t, observer.pool.QueryRow(context.Background(), "SELECT current_schema()").Scan(&schema))
	return schema
}

// nextTransactionID returns the next transaction ID PostgreSQL will assign.
// Read-only statements do not consume transaction IDs, so the difference
// between two readings counts write transactions in between.
func (observer *postgresObserver) nextTransactionID(t *testing.T) int64 {
	t.Helper()

	var next int64
	require.NoError(t, observer.pool.QueryRow(context.Background(),
		"SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint",
	).Scan(&next))
	return next
}

// waitForLockWait waits until a backend of the given application is blocked
// on a heavyweight lock, which proves a request is waiting for another
// transaction rather than merely being slow.
func (observer *postgresObserver) waitForLockWait(t *testing.T, applicationName string) {
	t.Helper()

	require.NotEmpty(t, applicationName)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		require.NoError(t, observer.pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE application_name = $1
				AND state = 'active'
				AND wait_event_type = 'Lock'`,
			applicationName,
		).Scan(&waiting))
		if waiting > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %s backend blocked on a lock", applicationName)
}

// postgresNotificationListener receives raw notifications for one channel on
// a dedicated harness connection.
type postgresNotificationListener struct {
	channel string
	conn    *pgx.Conn
}

// listen subscribes to a raw notification channel such as
// "public.river_insert".
func (observer *postgresObserver) listen(t *testing.T, channel string) *postgresNotificationListener {
	t.Helper()

	config, err := pgx.ParseConfig(observer.databaseURL)
	require.NoError(t, err)
	config.RuntimeParams["application_name"] = harnessApplicationName
	conn, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	_, err = conn.Exec(context.Background(), "LISTEN "+pgx.Identifier{channel}.Sanitize())
	require.NoError(t, err)
	return &postgresNotificationListener{channel: channel, conn: conn}
}

// receiveUntilMarker sends a marker notification on the listener's channel
// and returns every payload delivered before it. PostgreSQL delivers
// notifications in commit order, so any notification committed before the
// marker is guaranteed to be returned.
func (listener *postgresNotificationListener) receiveUntilMarker(t *testing.T, observer *postgresObserver, marker string) []string {
	t.Helper()

	_, err := observer.pool.Exec(context.Background(), "SELECT pg_notify($1, $2)", listener.channel, marker)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var payloads []string
	for {
		notification, err := listener.conn.WaitForNotification(ctx)
		require.NoError(t, err, "marker notification %q was not delivered", marker)
		if notification.Payload == marker {
			return payloads
		}
		payloads = append(payloads, notification.Payload)
	}
}
