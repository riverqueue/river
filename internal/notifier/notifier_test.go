package notifier

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
)

func TestNotifier(t *testing.T) {
	t.Parallel()

	const (
		testTopic1 = "test_topic1"
		testTopic2 = "test_topic2"
	)

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		exec   riverdriver.Executor
	}

	setup := func(t *testing.T) (*Notifier, *testBundle) {
		t.Helper()

		var (
			dbPool   = riverinternaltest.TestDB(ctx, t)
			driver   = riverpgxv5.New(dbPool)
			listener = driver.GetListener()
		)

		notifier := New(riversharedtest.BaseServiceArchetype(t), listener)
		notifier.testSignals.Init()

		return notifier, &testBundle{
			dbPool: dbPool,
			exec:   driver.GetExecutor(),
		}
	}

	start := func(t *testing.T, notifier *Notifier) {
		t.Helper()

		require.NoError(t, notifier.Start(ctx))
		t.Cleanup(notifier.Stop)
	}

	t.Run("StartsAndStops", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)
		start(t, notifier)

		notifier.testSignals.ListeningBegin.WaitOrTimeout()

		notifier.Stop()

		notifier.testSignals.ListeningEnd.WaitOrTimeout()
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)
		notifier.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		notifier.testSignals = notifierTestSignals{}    // deinit so channels don't fill

		startstoptest.Stress(ctx, t, notifier)
	})

	t.Run("StartErrorsOnImmediateProblem", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)

		t.Log("Closing database pool")
		bundle.dbPool.Close()

		require.EqualError(t, notifier.Start(ctx), "closed pool")
	})

	t.Run("ListenErrorsOnImmediateProblem", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		// Use a mock to simulate an error for this one because it's really hard
		// to get the timing right otherwise, and hard to avoid races.
		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.listenFunc = func(ctx context.Context, topic string) error {
			return errors.New("error from listener")
		}
		notifier.listener = listenerMock

		start(t, notifier)

		notifier.testSignals.ListeningBegin.WaitOrTimeout()

		_, err := notifier.Listen(ctx, testTopic1, nil)
		require.EqualError(t, err, fmt.Sprintf("error listening on topic %q: error from listener", testTopic1))

		require.Empty(t, notifier.subscriptions)
	})

	// A reasonable amount of time to wait for a notification that we don't
	// expect to come through before timing out and assuming that it won't.
	const notificationWaitLeeway = 50 * time.Millisecond

	requireNoNotification := func(t *testing.T, notifyChan chan TopicAndPayload) {
		t.Helper()

		if len(notifyChan) > 0 {
			notification := <-notifyChan
			require.FailNow(t, "Expected no more notifications", "Expected no more notifications, but got: %+v", notification)
		}
	}

	t.Run("ListensAndUnlistens", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)
		start(t, notifier)

		notifyChan := make(chan TopicAndPayload, 10)

		sub, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan))
		require.NoError(t, err)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1")

		require.Equal(t, TopicAndPayload{testTopic1, "msg1"}, riversharedtest.WaitOrTimeout(t, notifyChan))

		sub.Unlisten(ctx)

		require.Empty(t, notifier.subscriptions)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg2")

		time.Sleep(notificationWaitLeeway)

		requireNoNotification(t, notifyChan)
	})

	t.Run("ListenWithoutStart", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)

		notifyChan := make(chan TopicAndPayload, 10)

		sub, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan))
		require.NoError(t, err)
		t.Cleanup(func() { sub.Unlisten(ctx) })

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1")

		time.Sleep(notificationWaitLeeway)

		// Not received because the notifier was never started and therefore
		// never started processing messages.
		requireNoNotification(t, notifyChan)
	})

	// This next set of tests are largely here to make sure in case of listen
	// problems, the internal subscriptions map is correctly reset back to its
	// expected state by removing the problematic subscription.

	t.Run("ListenWithoutStartConnectError", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.connectFunc = func(ctx context.Context) error {
			return errors.New("error on connect")
		}
		notifier.listener = listenerMock

		_, err := notifier.Listen(ctx, testTopic1, nil)
		require.EqualError(t, err, "error on connect")

		require.Empty(t, notifier.subscriptions)
	})

	t.Run("ListenWithoutStartListenError", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.listenFunc = func(ctx context.Context, topic string) error {
			return errors.New("error on listen")
		}
		notifier.listener = listenerMock

		_, err := notifier.Listen(ctx, testTopic1, nil)
		require.EqualError(t, err, fmt.Sprintf("error listening on topic %q: error on listen", testTopic1))

		require.Empty(t, notifier.subscriptions)
	})

	t.Run("ListenWithoutStartMultipleSubscriptionsError", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.listenFunc = func(ctx context.Context, topic string) error {
			// First is allowed to succeed. Others fail.
			switch topic {
			case testTopic2:
				return errors.New("error on listen")
			default:
				return listenerMock.Listener.Listen(ctx, topic)
			}
		}
		notifier.listener = listenerMock

		sub, err := notifier.Listen(ctx, testTopic1, nil)
		require.NoError(t, err)
		t.Cleanup(func() { sub.Unlisten(ctx) })

		_, err = notifier.Listen(ctx, testTopic2, nil)
		require.EqualError(t, err, fmt.Sprintf("error listening on topic %q: error on listen", testTopic2))

		// Only the successful subscription is left.
		require.Equal(t, []NotificationTopic{testTopic1}, maputil.Keys(notifier.subscriptions))
	})

	t.Run("ListenBeforeStart", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)

		notifyChan := make(chan TopicAndPayload, 10)

		sub, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan))
		require.NoError(t, err)
		t.Cleanup(func() { sub.Unlisten(ctx) })

		start(t, notifier)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1")

		require.Equal(t, TopicAndPayload{testTopic1, "msg1"}, riversharedtest.WaitOrTimeout(t, notifyChan))
	})

	t.Run("SingleTopicMultipleSubscribers", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)
		start(t, notifier)

		notifyChan1 := make(chan TopicAndPayload, 10)
		notifyChan2 := make(chan TopicAndPayload, 10)

		sub1, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan1))
		require.NoError(t, err)
		sub2, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan2))
		require.NoError(t, err)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1")

		require.Equal(t, TopicAndPayload{testTopic1, "msg1"}, riversharedtest.WaitOrTimeout(t, notifyChan1))
		require.Equal(t, TopicAndPayload{testTopic1, "msg1"}, riversharedtest.WaitOrTimeout(t, notifyChan2))

		sub1.Unlisten(ctx)
		sub2.Unlisten(ctx)

		require.Empty(t, notifier.subscriptions)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg2")

		time.Sleep(notificationWaitLeeway)

		requireNoNotification(t, notifyChan1)
		requireNoNotification(t, notifyChan2)
	})

	t.Run("MultipleTopicsLockStep", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)
		start(t, notifier)

		notifyChan1 := make(chan TopicAndPayload, 10)
		notifyChan2 := make(chan TopicAndPayload, 10)

		sub1, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan1))
		require.NoError(t, err)
		sub2, err := notifier.Listen(ctx, testTopic2, topicAndPayloadNotifyFunc(notifyChan2))
		require.NoError(t, err)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg1_2")

		require.Equal(t, TopicAndPayload{testTopic1, "msg1_1"}, riversharedtest.WaitOrTimeout(t, notifyChan1))
		require.Equal(t, TopicAndPayload{testTopic2, "msg1_2"}, riversharedtest.WaitOrTimeout(t, notifyChan2))

		sub1.Unlisten(ctx)
		sub2.Unlisten(ctx)

		require.Empty(t, notifier.subscriptions)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg2_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg2_2")

		time.Sleep(notificationWaitLeeway)

		requireNoNotification(t, notifyChan1)
		requireNoNotification(t, notifyChan2)
	})

	t.Run("MultipleTopicsStaggered", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)
		start(t, notifier)

		notifyChan1 := make(chan TopicAndPayload, 10)
		notifyChan2 := make(chan TopicAndPayload, 10)

		sub1, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan1))
		require.NoError(t, err)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg1_2")

		time.Sleep(notificationWaitLeeway)

		// Only the first channel is subscribed.
		require.Equal(t, TopicAndPayload{testTopic1, "msg1_1"}, riversharedtest.WaitOrTimeout(t, notifyChan1))
		requireNoNotification(t, notifyChan2)

		sub2, err := notifier.Listen(ctx, testTopic2, topicAndPayloadNotifyFunc(notifyChan2))
		require.NoError(t, err)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg2_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg2_2")

		// Now both subscriptions are active.
		require.Equal(t, TopicAndPayload{testTopic1, "msg2_1"}, riversharedtest.WaitOrTimeout(t, notifyChan1))
		require.Equal(t, TopicAndPayload{testTopic2, "msg2_2"}, riversharedtest.WaitOrTimeout(t, notifyChan2))

		sub1.Unlisten(ctx)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg3_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg3_2")

		time.Sleep(notificationWaitLeeway)

		// First channel unsubscribed, but the second remains.
		requireNoNotification(t, notifyChan1)
		require.Equal(t, TopicAndPayload{testTopic2, "msg3_2"}, riversharedtest.WaitOrTimeout(t, notifyChan2))

		sub2.Unlisten(ctx)

		require.Empty(t, notifier.subscriptions)

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg4_1")
		sendNotification(ctx, t, bundle.exec, testTopic2, "msg4_2")

		time.Sleep(notificationWaitLeeway)

		requireNoNotification(t, notifyChan1)
		requireNoNotification(t, notifyChan2)
	})

	// Stress test meant to suss out any races that there might be in the
	// subscribe or interrupt loop code.
	t.Run("MultipleSubscribersStress", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)
		start(t, notifier)

		const (
			numSubscribers         = 10
			numSubscribeIterations = 5
		)

		notifyChans := make([]chan TopicAndPayload, numSubscribers)
		for i := range notifyChans {
			notifyChans[i] = make(chan TopicAndPayload, 1000)
		}

		// Start a goroutine to send messages constantly.
		var (
			sendNotificationsDone     = make(chan struct{})
			sendNotificationsShutdown = make(chan struct{})
		)
		go func() {
			defer close(sendNotificationsDone)

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for messageNum := 0; ; messageNum++ {
				sendNotification(ctx, t, bundle.exec, testTopic1, "msg"+strconv.Itoa(messageNum))

				select {
				case <-ctx.Done():
					return
				case <-sendNotificationsShutdown:
					return
				case <-ticker.C:
					// loop again
				}
			}
		}()

		var wg sync.WaitGroup
		wg.Add(len(notifyChans))
		for i := range notifyChans {
			notifyChan := notifyChans[i]

			go func() {
				defer wg.Done()

				for j := 0; j < numSubscribeIterations; j++ {
					sub, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan))
					require.NoError(t, err)

					// Pause a random brief amount of time.
					serviceutil.CancellableSleep(ctx, randutil.DurationBetween(15*time.Millisecond, 50*time.Millisecond))

					sub.Unlisten(ctx)
				}
			}()
		}

		wg.Wait()                        // wait for subscribe loops to finish all their work
		close(sendNotificationsShutdown) // stop sending notifications
		<-sendNotificationsDone          // wait for notifications goroutine to finish

		for i := range notifyChans {
			t.Logf("Channel %2d contains %3d message(s)", i, len(notifyChans[i]))

			// Don't require a specific number of messages to have been received
			// since it's non-deterministic, but every channel should've gotten
			// at least one message. It my test runs, they receive ~15 each.
			require.NotEmpty(t, notifyChans[i])
		}
	})

	t.Run("WaitErrorAndBackoff", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		notifier.disableSleep = true

		var errorNum int

		// Use a mock to simulate an error for this one because it's really hard
		// to get the timing right otherwise, and hard to avoid races.
		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.waitForNotificationFunc = func(ctx context.Context) (*riverdriver.Notification, error) {
			errorNum++
			return nil, fmt.Errorf("error during wait %d", errorNum)
		}
		notifier.listener = listenerMock

		start(t, notifier)

		// The service normally sleeps with an exponential backoff after an
		// error, but we've disabled sleep above, so we can pull errors out of
		// the test signal as quickly as we want.
		require.EqualError(t, notifier.testSignals.BackoffError.WaitOrTimeout(), "error during wait 1")
		require.EqualError(t, notifier.testSignals.BackoffError.WaitOrTimeout(), "error during wait 2")
		require.EqualError(t, notifier.testSignals.BackoffError.WaitOrTimeout(), "error during wait 3")
	})

	t.Run("BackoffSleepCancelledOnStop", func(t *testing.T) {
		t.Parallel()

		notifier, _ := setup(t)

		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.waitForNotificationFunc = func(ctx context.Context) (*riverdriver.Notification, error) {
			return nil, errors.New("error during wait")
		}
		notifier.listener = listenerMock

		start(t, notifier)

		// The loop goes to sleep as soon as it fires this test signal, but it's
		// cancelled immediately as the test cleanup look issues a Stop.
		require.EqualError(t, notifier.testSignals.BackoffError.WaitOrTimeout(), "error during wait")
	})

	t.Run("StillFunctionalAfterMainLoopFailure", func(t *testing.T) {
		t.Parallel()

		notifier, bundle := setup(t)

		// Disable the backoff sleep that would occur after the first retry.
		notifier.disableSleep = true

		var errorNum int

		listenerMock := NewListenerMock(notifier.listener)
		listenerMock.waitForNotificationFunc = func(ctx context.Context) (*riverdriver.Notification, error) {
			// Returns an error the first time, but then works after.
			errorNum++
			switch errorNum {
			case 1:
				return nil, errors.New("error during wait")
			default:
				return listenerMock.Listener.WaitForNotification(ctx)
			}
		}
		notifier.listener = listenerMock

		notifyChan := make(chan TopicAndPayload, 10)

		start(t, notifier)

		notifier.testSignals.ListeningBegin.WaitOrTimeout()

		sub, err := notifier.Listen(ctx, testTopic1, topicAndPayloadNotifyFunc(notifyChan))
		require.NoError(t, err)
		t.Cleanup(func() { sub.Unlisten(ctx) })

		// First failure, after which the loop will reenter and start producing again.
		require.EqualError(t, notifier.testSignals.BackoffError.WaitOrTimeout(), "error during wait")

		// It is possible for notifications to be missed while the loop is
		// restarting, so make sure we're back in the listening loop before
		// sending the notification below.
		notifier.testSignals.ListeningBegin.WaitOrTimeout()

		sendNotification(ctx, t, bundle.exec, testTopic1, "msg1")

		// Subscription should still work.
		require.Equal(t, TopicAndPayload{testTopic1, "msg1"}, riversharedtest.WaitOrTimeout(t, notifyChan))
	})
}

type ListenerMock struct {
	riverdriver.Listener

	connectFunc             func(ctx context.Context) error
	listenFunc              func(ctx context.Context, topic string) error
	waitForNotificationFunc func(ctx context.Context) (*riverdriver.Notification, error)
}

func NewListenerMock(listener riverdriver.Listener) *ListenerMock {
	return &ListenerMock{
		Listener: listener,

		connectFunc:             listener.Connect,
		listenFunc:              listener.Listen,
		waitForNotificationFunc: listener.WaitForNotification,
	}
}

func (l *ListenerMock) Connect(ctx context.Context) error {
	return l.connectFunc(ctx)
}

func (l *ListenerMock) Listen(ctx context.Context, topic string) error {
	return l.listenFunc(ctx, topic)
}

func (l *ListenerMock) WaitForNotification(ctx context.Context) (*riverdriver.Notification, error) {
	return l.waitForNotificationFunc(ctx)
}

type TopicAndPayload struct {
	topic   NotificationTopic
	payload string
}

func topicAndPayloadNotifyFunc(notifyChan chan TopicAndPayload) NotifyFunc {
	return func(topic NotificationTopic, payload string) {
		notifyChan <- TopicAndPayload{topic, payload}
	}
}

func sendNotification(ctx context.Context, t *testing.T, exec riverdriver.Executor, topic string, payload string) {
	t.Helper()

	t.Logf("Sending notification on %q: %s", topic, payload)
	require.NoError(t, exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{payload}, Topic: topic}))
}
