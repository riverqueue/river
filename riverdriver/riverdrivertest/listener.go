package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
)

type testListenerBundle[TTx any] struct {
	driver riverdriver.Driver[TTx]
	exec   riverdriver.Executor
}

func setupListener[TTx any](ctx context.Context, t *testing.T, driverWithPool func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string)) (riverdriver.Listener, *testListenerBundle[TTx]) {
	t.Helper()

	var (
		driver, schema = driverWithPool(ctx, t, nil)
		listener       = driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema})
	)

	return listener, &testListenerBundle[TTx]{
		driver: driver,
		exec:   driver.GetExecutor(),
	}
}

func exerciseListener[TTx any](ctx context.Context, t *testing.T, driverWithPool func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string)) {
	t.Helper()

	connectListener := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		require.NoError(t, listener.Connect(ctx))
		t.Cleanup(func() { require.NoError(t, listener.Close(ctx)) })
	}

	requireNoNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		// Ugh, this is a little sketchy, but hard to test in another way.
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded, "Expected no notification, but got: %+v", notification)
	}

	waitForNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) *riverdriver.Notification {
		t.Helper()

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)

		return notification
	}

	t.Run("Close_NoOpIfNotConnected", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)
		require.NoError(t, listener.Close(ctx))
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Listen(ctx, "topic2"))

		require.NoError(t, listener.Ping(ctx)) // still alive

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_1"}, Schema: listener.Schema()}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_1"}, Schema: listener.Schema()}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_1"}, notification)
			notification = waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic2", Payload: "payload2_1"}, notification)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic2"))

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_2"}, Schema: listener.Schema()}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_2"}, Schema: listener.Schema()}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_2"}, notification)

			requireNoNotification(ctx, t, listener)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
	})

	t.Run("SchemaFromParameter", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: "my_custom_schema"})
		)

		require.Equal(t, "my_custom_schema", listener.Schema())
	})

	t.Run("SchemaFromSearchPath", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: ""})
		)

		listener.SetAfterConnectExec("SET search_path TO 'public'")

		connectListener(ctx, t, listener)
		require.Equal(t, "public", listener.Schema())
	})

	t.Run("EmptySchemaFromSearchPath", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: ""})
		)

		connectListener(ctx, t, listener)
		require.Empty(t, listener.Schema())
	})

	t.Run("TransactionGated", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))

		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)

		require.NoError(t, tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1"}, Schema: listener.Schema()}))

		// No notification because the transaction hasn't committed yet.
		requireNoNotification(ctx, t, listener)

		require.NoError(t, tx.Commit(ctx))

		// Notification received now that transaction has committed.
		notification := waitForNotification(ctx, t, listener)
		require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1"}, notification)
	})

	t.Run("MultipleReuse", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
		require.NoError(t, listener.Connect(ctx))

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))
	})
}
