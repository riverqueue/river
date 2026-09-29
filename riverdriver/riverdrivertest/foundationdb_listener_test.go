//go:build foundationdb

package riverdrivertest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverfdb"
	"github.com/riverqueue/river/rivershared/testsignal"
)

func exerciseFoundationDBWatches(t *testing.T, setupDriverFunc func(*testing.T) *riverfdb.Driver) {
	t.Helper()

	type testBundle struct {
		exec     riverdriver.Executor
		listener *riverfdb.Listener
	}
	type waitResult struct {
		err          error
		notification *riverdriver.Notification
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := setupDriverFunc(t)
		listener, ok := driver.GetListener(&riverdriver.GetListenenerParams{}).(*riverfdb.Listener)
		require.True(t, ok)
		listener.TestSignals.Init(t)
		require.NoError(t, listener.Connect(t.Context()))
		t.Cleanup(func() { require.NoError(t, listener.Close(context.Background())) })
		require.NoError(t, listener.Listen(t.Context(), "topic"))
		return &testBundle{exec: driver.GetExecutor(), listener: listener}
	}
	waitFunc := func(ctx context.Context, t *testing.T, listener *riverfdb.Listener) *testsignal.TestSignal[waitResult] {
		t.Helper()

		var result testsignal.TestSignal[waitResult]
		result.Init(t)
		go func() {
			notification, err := listener.WaitForNotification(ctx)
			result.Signal(waitResult{err: err, notification: notification})
		}()
		return &result
	}

	t.Run("CancelAndReuse", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := waitFunc(ctx, t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		cancel()
		require.ErrorIs(t, result.WaitOrTimeout().err, context.Canceled)
		result = waitFunc(t.Context(), t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		require.NoError(t, bundle.exec.NotifyMany(t.Context(), &riverdriver.NotifyManyParams{Payload: []string{"new"}, Topic: "topic"}))
		received := result.WaitOrTimeout()
		require.NoError(t, received.err)
		require.Equal(t, &riverdriver.Notification{Payload: "new", Topic: "topic"}, received.notification)
	})

	t.Run("CloseInterruptsWait", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		result := waitFunc(t.Context(), t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		require.NoError(t, bundle.listener.Close(t.Context()))
		require.Error(t, result.WaitOrTimeout().err)
		require.NoError(t, bundle.listener.Connect(t.Context()))
		require.NoError(t, bundle.listener.Listen(t.Context(), "topic"))
		result = waitFunc(t.Context(), t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		require.NoError(t, bundle.exec.NotifyMany(t.Context(), &riverdriver.NotifyManyParams{Payload: []string{"reconnected"}, Topic: "topic"}))
		received := result.WaitOrTimeout()
		require.NoError(t, received.err)
		require.Equal(t, "reconnected", received.notification.Payload)
	})

	t.Run("ConcurrentPublishers", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		// Leave signals disabled here because scheduling determines how often
		// the reader runs out of rows between commits.
		bundle.listener.TestSignals = riverfdb.ListenerTestSignals{}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		var publishers sync.WaitGroup
		for publisher := range 8 {
			publishers.Go(func() {
				for i := range 10 {
					if err := bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{
						Payload: []string{fmt.Sprintf("%d/%d", publisher, i)}, Topic: "topic",
					}); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		seen := make(map[string]bool)
		for range 80 {
			notification, err := bundle.listener.WaitForNotification(ctx)
			require.NoError(t, err)
			require.False(t, seen[notification.Payload], "duplicate notification: %s", notification.Payload)
			seen[notification.Payload] = true
		}
		publishers.Wait()
	})

	t.Run("FailedBatchIsAtomic", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(context.Background())) })
		require.Error(t, tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"partial", strings.Repeat("x", 100_000)}, Topic: "topic"}))
		require.Error(t, tx.Commit(ctx))
		result := waitFunc(ctx, t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"whole"}, Topic: "topic"}))
		received := result.WaitOrTimeout()
		require.NoError(t, received.err)
		require.Equal(t, "whole", received.notification.Payload)
	})

	t.Run("NamespaceIsolation", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		result := waitFunc(ctx, t, bundle.listener)
		bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
		require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"other schema"}, Schema: "other", Topic: "topic"}))
		other := setupDriverFunc(t)
		require.NoError(t, other.GetExecutor().NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"other prefix"}, Topic: "topic"}))
		require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"ours"}, Topic: "topic"}))
		received := result.WaitOrTimeout()
		require.NoError(t, received.err)
		require.Equal(t, "ours", received.notification.Payload)
		deleted, err := bundle.exec.NotificationDeleteBefore(ctx, &riverdriver.NotificationDeleteBeforeParams{CreatedAtHorizon: time.Now().Add(time.Hour), Max: 100})
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		deleted, err = bundle.exec.NotificationDeleteBefore(ctx, &riverdriver.NotificationDeleteBeforeParams{CreatedAtHorizon: time.Now().Add(time.Hour), Max: 100, Schema: "other"})
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
	})

	t.Run("PublishersCommitInReverseOrder", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		first, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, first.Rollback(context.Background())) })
		second, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, second.Rollback(context.Background())) })
		require.NoError(t, first.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"first"}, Topic: "topic"}))
		require.NoError(t, second.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"second"}, Topic: "topic"}))
		require.NoError(t, second.Commit(ctx))
		require.Error(t, first.Commit(ctx))
		notification, err := bundle.listener.WaitForNotification(ctx)
		require.NoError(t, err)
		require.Equal(t, "second", notification.Payload)

		// Retrying the first publication must place it after the committed
		// second one, where the listener can still see it.
		require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"first"}, Topic: "topic"}))
		notification, err = bundle.listener.WaitForNotification(ctx)
		require.NoError(t, err)
		require.Equal(t, "first", notification.Payload)
	})

	t.Run("Rearm", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		for range 20 {
			result := waitFunc(t.Context(), t, bundle.listener)
			bundle.listener.TestSignals.WatchArmed.WaitOrTimeout()
			// Identical payloads still change the sequence and wake each watch.
			require.NoError(t, bundle.exec.NotifyMany(t.Context(), &riverdriver.NotifyManyParams{Payload: []string{"same"}, Topic: "topic"}))
			received := result.WaitOrTimeout()
			require.NoError(t, received.err)
			require.Equal(t, "same", received.notification.Payload)
		}
	})
}
