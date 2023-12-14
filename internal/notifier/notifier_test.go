package notifier

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func expectReceiveStatus(t *testing.T, statusCh <-chan componentstatus.Status, expected componentstatus.Status) {
	t.Helper()
	select {
	case status := <-statusCh:
		require.Equal(t, expected, status, "expected status=%s, got=%s", expected, status)
	case <-time.After(5 * time.Second):
		t.Fatalf("expected to receive status update with %s, got none", expected)
	}
}

func TestNotifierReceivesNotification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require := require.New(t)
	dbPool := riverinternaltest.TestDB(ctx, t)
	listener := riverpgxv5.New(dbPool).GetListener()

	statusUpdateCh := make(chan componentstatus.Status, 10)
	statusUpdate := func(status componentstatus.Status) {
		statusUpdateCh <- status
	}

	notifier := New(riverinternaltest.BaseServiceArchetype(t), listener, statusUpdate, riverinternaltest.Logger(t))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	sub1Ch := make(chan string, 1)
	fn1 := func(topic NotificationTopic, payload string) {
		t.Logf("sub1 received topic=%s, payload=%q\n", topic, payload)
		sub1Ch <- payload
	}

	// add a subscription *before* the run loop starts:
	sub1 := notifier.Listen(NotificationTopicInsert, fn1)
	defer sub1.Unlisten()

	go notifier.Run(ctx)

	// Wait for the listener to become active:
	expectReceiveStatus(t, statusUpdateCh, componentstatus.Initializing)
	expectReceiveStatus(t, statusUpdateCh, componentstatus.Healthy)

	sendNotification(t, dbPool, string(NotificationTopicInsert), "a_queue_name")

	select {
	case payload := <-sub1Ch:
		require.Equal("a_queue_name", payload)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for notification")
	}

	// add a subscription *after* the run loop starts:
	sub2Ch := make(chan string, 1)
	fn2 := func(topic NotificationTopic, payload string) {
		t.Logf("sub2 received topic=%s, payload=%q\n", topic, payload)
		sub2Ch <- payload
	}
	sub2 := notifier.Listen(NotificationTopicInsert, fn2)
	defer sub2.Unlisten()

	sendNotification(t, dbPool, string(NotificationTopicInsert), "a_queue_name_b")

	receivedOn1 := false
	receivedOn2 := false

Loop:
	for {
		select {
		case payload := <-sub1Ch:
			require.Equal("a_queue_name_b", payload)
			receivedOn1 = true
			if receivedOn2 {
				break Loop
			}
		case payload := <-sub2Ch:
			require.Equal("a_queue_name_b", payload)
			receivedOn2 = true
			if receivedOn1 {
				break Loop
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for notification, receivedOn1=%v, receivedOn2=%v", receivedOn1, receivedOn2)
		}
	}

	// remove a subscription:
	sub1.Unlisten()
	sendNotification(t, dbPool, string(NotificationTopicInsert), "a_queue_name_b")

	select {
	case payload := <-sub2Ch:
		require.Equal("a_queue_name_b", payload)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for notification on sub2")
	}

	select {
	case payload := <-sub1Ch:
		require.Fail("sub1 should have been removed but received notification %s", payload)
	case <-time.After(20 * time.Millisecond):
	}

	t.Log("Canceling context")
	cancel()

	expectReceiveStatus(t, statusUpdateCh, componentstatus.ShuttingDown)
	expectReceiveStatus(t, statusUpdateCh, componentstatus.Stopped)
}

func sendNotification(t *testing.T, dbPool *pgxpool.Pool, topic string, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := dbPool.Exec(ctx, "SELECT pg_notify($1, $2)", topic, payload)
	require.NoError(t, err)
}
