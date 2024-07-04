package notifier

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/startstop"
	"github.com/riverqueue/river/internal/util/maputil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
)

type NotificationTopic string

const (
	NotificationTopicControl    NotificationTopic = "river_control"
	NotificationTopicInsert     NotificationTopic = "river_insert"
	NotificationTopicLeadership NotificationTopic = "river_leadership"
)

type NotifyFunc func(topic NotificationTopic, payload string)

type Subscription struct {
	notifyFunc   NotifyFunc
	notifier     *Notifier
	topic        NotificationTopic
	unlistenOnce sync.Once
}

func (s *Subscription) Unlisten(ctx context.Context) {
	s.unlistenOnce.Do(func() {
		// Unlisten uses background context in case of cancellation.
		if err := s.notifier.unlisten(context.Background(), s); err != nil { //nolint:contextcheck
			s.notifier.Logger.ErrorContext(ctx, s.notifier.Name+": Error unlistening on topic", "err", err, "topic", s.topic)
		}
	})
}

// Test-only properties.
type notifierTestSignals struct {
	BackoffError   rivercommon.TestSignal[error]    // non-cancellation error received by main run loop
	ListeningBegin rivercommon.TestSignal[struct{}] // notifier has entered a listen loop
	ListeningEnd   rivercommon.TestSignal[struct{}] // notifier has left a listen loop
}

func (ts *notifierTestSignals) Init() {
	ts.BackoffError.Init()
	ts.ListeningBegin.Init()
	ts.ListeningEnd.Init()
}

type Notifier struct {
	baseservice.BaseService
	startstop.BaseStartStop

	disableSleep      bool // for tests only; disable sleep on exponential backoff
	listener          riverdriver.Listener
	notificationBuf   chan *riverdriver.Notification
	statusChangeFunc  func(componentstatus.Status)
	testSignals       notifierTestSignals
	waitInterruptChan chan func()

	mu            sync.RWMutex
	isConnected   bool
	isStarted     bool
	isWaiting     bool
	subscriptions map[NotificationTopic][]*Subscription
	waitCancel    context.CancelFunc
}

func New(archetype *baseservice.Archetype, listener riverdriver.Listener, statusChangeFunc func(componentstatus.Status)) *Notifier {
	notifier := baseservice.Init(archetype, &Notifier{
		listener:          listener,
		notificationBuf:   make(chan *riverdriver.Notification, 1000),
		statusChangeFunc:  statusChangeFunc,
		waitInterruptChan: make(chan func(), 10),

		subscriptions: make(map[NotificationTopic][]*Subscription),
	})
	return notifier
}

func (n *Notifier) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := n.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	// The loop below will connect/close on every iteration, but do one initial
	// connect so the notifier fails fast in case of an obvious problem.
	if err := n.listenerConnect(ctx, false); err != nil {
		stopped()
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	go func() {
		started()
		defer stopped()

		n.Logger.DebugContext(ctx, n.Name+": Run loop started")
		defer n.Logger.DebugContext(ctx, n.Name+": Run loop stopped")

		n.withLock(func() { n.isStarted = true })
		defer n.withLock(func() { n.isStarted = false })

		defer n.listenerClose(ctx, false)

		n.statusChangeFunc(componentstatus.Initializing)
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			n.deliverNotifications(ctx)
		}()

		for attempt := 0; ; attempt++ {
			if err := n.listenAndWait(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}

				sleepDuration := n.ExponentialBackoff(attempt, baseservice.MaxAttemptsBeforeResetDefault)
				n.Logger.ErrorContext(ctx, n.Name+": Error running listener (will attempt reconnect after backoff)",
					"attempt", attempt, "err", err, "sleep_duration", sleepDuration)
				n.testSignals.BackoffError.Signal(err)
				if !n.disableSleep {
					n.CancellableSleep(ctx, sleepDuration)
				}
			}
		}

		n.statusChangeFunc(componentstatus.ShuttingDown)
		wg.Wait()
		n.statusChangeFunc(componentstatus.Stopped)
	}()

	return nil
}

func (n *Notifier) deliverNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case notification := <-n.notificationBuf:
			notifyFuncs := func() []NotifyFunc {
				n.mu.RLock()
				defer n.mu.RUnlock()

				return sliceutil.Map(n.subscriptions[NotificationTopic(notification.Topic)], func(s *Subscription) NotifyFunc { return s.notifyFunc })
			}()

			for _, notifyFunc := range notifyFuncs {
				// TODO: panic recovery on delivery attempts
				notifyFunc(NotificationTopic(notification.Topic), notification.Payload)
			}
		}
	}
}

func (n *Notifier) listenAndWait(ctx context.Context) error {
	if err := n.listenerConnect(ctx, false); err != nil {
		return err
	}
	defer n.listenerClose(ctx, false)

	topics := func() []NotificationTopic {
		n.mu.RLock()
		defer n.mu.RUnlock()

		return maputil.Keys(n.subscriptions)
	}()

	for _, topic := range topics {
		if err := n.listenerListen(ctx, topic); err != nil {
			return err
		}
	}

	n.Logger.DebugContext(ctx, n.Name+": Notifier healthy")
	n.statusChangeFunc(componentstatus.Healthy)

	n.testSignals.ListeningBegin.Signal(struct{}{})
	defer n.testSignals.ListeningEnd.Signal(struct{}{})

	drainInterrupts := func() {
		for {
			select {
			case interruptOperation := <-n.waitInterruptChan:
				interruptOperation()
			default:
				return
			}
		}
	}

	// Drain interrupts one last time before leaving to make sure we're not
	// leaving any goroutines hanging anywhere.
	defer drainInterrupts()

	for {
		// Top level context is done, meaning we're shutting down.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Drain any and all interrupt operations before continuing back into a
		// new wait to give any new subscribers a chance to listen/unlisten.
		drainInterrupts()

		err := n.waitOnce(ctx)
		if err != nil {
			// On cancellation, reenter loop, but the check at the top on
			// `ctx.Err()` will end it if the service is shutting down.
			if errors.Is(err, context.Canceled) {
				continue
			}

			n.Logger.InfoContext(ctx, n.Name+": Notifier unhealthy")
			n.statusChangeFunc(componentstatus.Unhealthy)

			return err
		}
	}
}

func (n *Notifier) listenerClose(ctx context.Context, skipLock bool) {
	if !skipLock {
		n.mu.Lock()
		defer n.mu.Unlock()
	}

	if !n.isConnected {
		return
	}

	n.Logger.InfoContext(ctx, n.Name+": Listener closing")
	if err := n.listener.Close(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			n.Logger.ErrorContext(ctx, n.Name+": Error closing listener", "err", err)
		}
	}

	n.isConnected = false
}

const listenerTimeout = 10 * time.Second

func (n *Notifier) listenerConnect(ctx context.Context, skipLock bool) error {
	if !skipLock {
		n.mu.Lock()
		defer n.mu.Unlock()
	}

	if n.isConnected {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.Logger.InfoContext(ctx, n.Name+": Listener connecting")
	if err := n.listener.Connect(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			n.Logger.ErrorContext(ctx, n.Name+": Error connecting listener", "err", err)
		}

		return err
	}

	n.isConnected = true
	return nil
}

// Listens on a topic with an appropriate logging statement. Should be preferred
// to `listener.Listen` for improved logging/telemetry.
//
// Not protected by mutex because it doesn't modify any notifier state and the
// underlying listener has a mutex around its operations.
func (n *Notifier) listenerListen(ctx context.Context, topic NotificationTopic) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.Logger.InfoContext(ctx, n.Name+": Listening on topic", "topic", topic)
	if err := n.listener.Listen(ctx, string(topic)); err != nil {
		return fmt.Errorf("error listening on topic %q: %w", topic, err)
	}

	return nil
}

// Unlistens on a topic with an appropriate logging statement. Should be
// preferred to `listener.Unlisten` for improved logging/telemetry.
//
// Not protected by mutex because it doesn't modify any notifier state and the
// underlying listener has a mutex around its operations.
func (n *Notifier) listenerUnlisten(ctx context.Context, topic NotificationTopic) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.Logger.InfoContext(ctx, n.Name+": Unlistening on topic", "topic", topic)
	if err := n.listener.Unlisten(ctx, string(topic)); err != nil {
		return fmt.Errorf("error unlistening on topic %q: %w", topic, err)
	}

	return nil
}

// Enters a single blocking wait for notifications on the underlying listener.
// Waiting for a notification locks an underlying connection, so infrastructure
// elsewhere in the notifier must preempt it by sending to `n.waitInterruptChan`
// and invoking `n.waitCancel()`. Cancelling the input context (as occurs during
// shutdown) also unblocks the wait.
func (n *Notifier) waitOnce(ctx context.Context) error {
	n.withLock(func() {
		n.isWaiting = true
		ctx, n.waitCancel = context.WithCancel(ctx)
	})
	defer n.withLock(func() {
		n.isWaiting = false
		n.waitCancel()
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error)

	go func() {
		for {
			notification, err := n.listener.WaitForNotification(ctx)
			if err != nil {
				errChan <- err
				return
			}

			select {
			case n.notificationBuf <- notification:
			default:
				n.Logger.WarnContext(ctx, n.Name+": Dropping notification due to full buffer", "payload", notification.Payload)
			}
		}
	}()

	drainErrChan := func() error {
		cancel()

		// There's a chance we encounter some other error before the context.Canceled comes in:
		err := <-errChan
		if err != nil && !errors.Is(err, context.Canceled) {
			// A non-cancel error means something went wrong with the conn, so we should bail.
			n.Logger.ErrorContext(ctx, n.Name+": Error on draining notification wait", "err", err)
			return err
		}
		// If we got a context cancellation error, it means we successfully
		// interrupted the WaitForNotification so that we could make the
		// subscription change.
		return nil
	}

	needPingCtx, needPingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer needPingCancel()

	// * Wait for notifications
	// * Ping conn if 5 seconds have elapsed between notifications to keep it alive
	// * Manage listens/unlistens on conn (waitInterruptChan)
	// * If any errors are encountered, return them so we can kill the conn and start over
	select {
	case <-ctx.Done():
		return <-errChan

	case <-needPingCtx.Done():
		if err := drainErrChan(); err != nil {
			return err
		}
		// Ping the conn to see if it's still alive
		if err := n.listener.Ping(ctx); err != nil {
			return err
		}

	case err := <-errChan:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			n.Logger.ErrorContext(ctx, n.Name+": Error from notification wait", "err", err)
			return err
		}
	}

	return nil
}

// Sends an interrupt operation to the main loop, waits on the result, and
// returns an error if there was one.
//
// MUST be called with the `n.mu` mutex already locked.
func (n *Notifier) sendInterruptAndReceiveResult(operation func() error) error {
	errChan := make(chan error)
	n.waitInterruptChan <- func() {
		errChan <- operation()
	}

	n.waitCancel()

	// Notably, these unlock then lock again, the reverse of what you'd normally
	// expect in a mutex pattern. This is because this function is only expected
	// to be called with the mutex already locked, but we need to unlock it to
	// give the main loop a chance to run interrupt operations.
	n.mu.Unlock()
	defer n.mu.Lock()

	select {
	case err := <-errChan:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for interrupt operation")
	}
}

func (n *Notifier) Listen(ctx context.Context, topic NotificationTopic, notifyFunc NotifyFunc) (*Subscription, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	sub := &Subscription{
		notifyFunc: notifyFunc,
		topic:      topic,
		notifier:   n,
	}

	existingSubs, existingTopic := n.subscriptions[topic]
	if !existingTopic {
		existingSubs = make([]*Subscription, 0, 10)
	}
	n.subscriptions[topic] = append(existingSubs, sub)

	n.Logger.DebugContext(ctx, n.Name+": Added subscription", "new_num_subscriptions", len(n.subscriptions[topic]), "topic", topic)

	// We add the new subscription to the subscription list optimistically, and
	// it needs to be done this way in case of a restart after an interrupt
	// below has been run, but after a return to this function (say we were to
	// add the new sub at the end of this function, it would not be picked
	// during the restart). But in case of an error subscribing, remove the sub.
	//
	// By the time this function is run (i.e. after an interrupt), a lock on
	// `n.mu` has been reacquired, and modifying subscription state is safe.
	removeSub := func() { n.removeSubscription(ctx, sub) }

	if !existingTopic {
		// If already waiting, send an interrupt to the wait function to run a
		// listen operation. If not, connect and listen directly, returning any
		// errors as feedback to the caller.
		if n.isWaiting {
			if err := n.sendInterruptAndReceiveResult(func() error { return n.listenerListen(ctx, topic) }); err != nil {
				removeSub()
				return nil, err
			}
		} else {
			var justConnected bool

			if !n.isConnected {
				if err := n.listenerConnect(ctx, true); err != nil {
					removeSub()
					return nil, err
				}
				justConnected = true
			}

			if err := n.listenerListen(ctx, topic); err != nil {
				removeSub()

				// If we just connected above and the notifier hasn't started in
				// the interim, also close the connection so we don't leave any
				// resources hanging.
				if justConnected && !n.isStarted {
					n.listenerClose(ctx, true)
				}

				return nil, err
			}
		}
	}

	return sub, nil
}

func (n *Notifier) unlisten(ctx context.Context, sub *Subscription) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	subs := n.subscriptions[sub.topic]

	// If this is the last subscription on the topic, unlisten if we're connected.
	if len(subs) <= 1 {
		// If already waiting, send an interrupt to the wait function to run an
		// unlisten operation. If not, if connected, unlisten directly.
		if n.isWaiting {
			if err := n.sendInterruptAndReceiveResult(func() error { return n.listenerUnlisten(ctx, sub.topic) }); err != nil {
				return err
			}
		} else {
			if n.isConnected {
				if err := n.listenerUnlisten(ctx, sub.topic); err != nil {
					return err
				}

				// If this was the last subscription, we weren't in a wait loop,
				// and the notifier never started, also clean up by closing the
				// listener.
				if !n.isStarted && len(n.subscriptions) <= 1 {
					n.listenerClose(ctx, true)
				}
			}
		}
	}

	n.removeSubscription(ctx, sub)

	return nil
}

// This function requires that the caller already has a lock on `n.mu`.
func (n *Notifier) removeSubscription(ctx context.Context, sub *Subscription) {
	n.subscriptions[sub.topic] = slices.DeleteFunc(n.subscriptions[sub.topic], func(s *Subscription) bool {
		return s == sub
	})

	if len(n.subscriptions[sub.topic]) < 1 {
		delete(n.subscriptions, sub.topic)
	}

	n.Logger.DebugContext(ctx, n.Name+": Removed subscription", "new_num_subscriptions", len(n.subscriptions[sub.topic]), "topic", sub.topic)
}

func (n *Notifier) withLock(lockedFunc func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	lockedFunc()
}
