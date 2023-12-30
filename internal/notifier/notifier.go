package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
)

const statementTimeout = 5 * time.Second

type NotificationTopic string

const (
	NotificationTopicInsert     NotificationTopic = "river_insert"
	NotificationTopicLeadership NotificationTopic = "river_leadership"
	NotificationTopicJobControl NotificationTopic = "river_job_control"
)

type NotifyFunc func(topic NotificationTopic, payload string)

type Subscription struct {
	creationTime time.Time
	topic        NotificationTopic
	notifyFunc   NotifyFunc

	unlistenOnce *sync.Once
	notifier     *Notifier
}

func (s *Subscription) Unlisten() {
	s.unlistenOnce.Do(func() {
		s.notifier.unlisten(s)
	})
}

type subscriptionChange struct {
	isNewTopic bool
	topic      NotificationTopic
}

type Notifier struct {
	baseservice.BaseService

	connConfig       *pgx.ConnConfig
	notificationBuf  chan *pgconn.Notification
	statusChangeFunc func(componentstatus.Status)
	logger           *slog.Logger

	mu           sync.Mutex
	isConnActive bool
	subs         map[NotificationTopic][]*Subscription
	subChangeCh  chan *subscriptionChange
}

func New(archetype *baseservice.Archetype, connConfig *pgx.ConnConfig, statusChangeFunc func(componentstatus.Status), logger *slog.Logger) *Notifier {
	copiedConfig := connConfig.Copy()
	// Rely on an overall statement timeout instead of setting identical context timeouts on every query:
	copiedConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(int(statementTimeout.Milliseconds()))
	notifier := baseservice.Init(archetype, &Notifier{
		connConfig:       copiedConfig,
		notificationBuf:  make(chan *pgconn.Notification, 1000),
		statusChangeFunc: statusChangeFunc,
		logger:           logger.WithGroup("notifier"),

		subs:        make(map[NotificationTopic][]*Subscription),
		subChangeCh: make(chan *subscriptionChange, 1000),
	})
	copiedConfig.OnNotification = notifier.handleNotification
	return notifier
}

func (n *Notifier) Run(ctx context.Context) {
	n.statusChangeFunc(componentstatus.Initializing)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		n.deliverNotifications(ctx)
	}()

	for {
		n.getConnAndRun(ctx)

		select {
		case <-ctx.Done():
			wg.Wait()
			n.statusChangeFunc(componentstatus.Stopped)
			return
		default:
			// TODO: exponential backoff
		}
	}
}

func (n *Notifier) deliverNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notif := <-n.notificationBuf:
			n.deliverNotification(notif)
		}
	}
}

func (n *Notifier) deliverNotification(notif *pgconn.Notification) {
	n.mu.Lock()

	fns := make([]NotifyFunc, len(n.subs[NotificationTopic(notif.Channel)]))
	for i, sub := range n.subs[NotificationTopic(notif.Channel)] {
		fns[i] = sub.notifyFunc
	}
	n.mu.Unlock()

	for _, fn := range fns {
		// TODO: panic recovery on delivery attempts
		fn(NotificationTopic(notif.Channel), notif.Payload)
	}
}

func (n *Notifier) getConnAndRun(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := n.establishConn(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		// Log at a lower verbosity level in case an error is received when the
		// context is already done (probably because the client is stopping).
		// Example tests can finish before the notifier connects and starts
		// listening, and on client stop may produce a connection error that
		// would otherwise pollute output and fail the test.
		select {
		case <-ctx.Done():
			n.logger.Info("error establishing connection from pool", "err", err)
		default:
			n.logger.Error("error establishing connection from pool", "err", err)
		}
		return
	}
	defer func() {
		// use an already-canceled context here so conn.Close() does not block the run loop
		ctx, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()
		conn.Close(ctx)
	}()

	startingTopics := n.setConnActive()
	defer n.setConnInactive()

	// TODO: need to drain subChangeCh before returning (but after setting conn
	// active = false) just to ensure nobody is blocking on sending to it

	for _, topic := range startingTopics {
		if err := n.execListen(ctx, conn, topic); err != nil {
			// TODO: log?
			return
		}
	}

	n.statusChangeFunc(componentstatus.Healthy)
	for {
		// If context is already done, don't bother waiting for notifications:
		select {
		case <-ctx.Done():
			n.statusChangeFunc(componentstatus.ShuttingDown)
			return
		default:
		}

		err := n.runOnce(ctx, conn)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				n.statusChangeFunc(componentstatus.ShuttingDown)
				return
			}
			n.statusChangeFunc(componentstatus.Unhealthy)
			return
		}
	}
}

func (n *Notifier) runOnce(ctx context.Context, conn *pgx.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error)

	go func() {
		err := conn.PgConn().WaitForNotification(ctx)
		errCh <- err
	}()

	drainErrCh := func() error {
		cancel()

		// There's a chance we encounter some other error before the context.Canceled comes in:
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			// A non-cancel error means something went wrong with the conn, so we should bail.
			n.logger.Error("error on draining notification wait", "err", err)
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
	// * Manage listens/unlistens on conn
	// * If any errors are encountered, return them so we can kill the conn and start over
	select {
	case <-ctx.Done():
		return <-errCh
	case <-needPingCtx.Done():
		if err := drainErrCh(); err != nil {
			return err
		}
		// Ping the conn to see if it's still alive
		if err := conn.Ping(ctx); err != nil {
			return err
		}
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			n.logger.Error("error from notification wait", "err", err)
			return err
		}
	case subChange := <-n.subChangeCh:
		if err := drainErrCh(); err != nil {
			return err
		}
		// Apply the subscription change
		if subChange.isNewTopic {
			return n.execListen(ctx, conn, subChange.topic)
		} else {
			return n.execUnlisten(ctx, conn, subChange.topic)
		}
	}
	return nil
}

func (n *Notifier) execListen(ctx context.Context, conn *pgx.Conn, topic NotificationTopic) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", topic))
	return err
}

func (n *Notifier) execUnlisten(ctx context.Context, conn *pgx.Conn, topic NotificationTopic) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := conn.Exec(ctx, fmt.Sprintf("UNLISTEN %s", topic))
	return err
}

func (n *Notifier) setConnActive() []NotificationTopic {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isConnActive = true

	topics := make([]NotificationTopic, 0, len(n.subs))
	for topic := range n.subs {
		topics = append(topics, topic)
	}
	return topics
}

func (n *Notifier) setConnInactive() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isConnActive = false

	// drain any pending changes from subChangeCh, because they'll be reflected
	// automatically when the conn restarts.
	for {
		select {
		case <-n.subChangeCh:
		default:
			return
		}
	}
}

func (n *Notifier) establishConn(ctx context.Context) (*pgx.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return pgx.ConnectConfig(ctx, n.connConfig)
}

func (n *Notifier) handleNotification(conn *pgconn.PgConn, notification *pgconn.Notification) {
	select {
	case n.notificationBuf <- notification:
	default:
		n.logger.Warn("dropping notification due to full buffer", "payload", notification.Payload)
	}
}

func (n *Notifier) Listen(topic NotificationTopic, notifyFunc NotifyFunc) *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	sub := &Subscription{
		creationTime: time.Now(),
		topic:        topic,
		notifyFunc:   notifyFunc,
		unlistenOnce: &sync.Once{},
		notifier:     n,
	}

	isNewTopic := false

	subs := n.subs[topic]
	if subs == nil {
		isNewTopic = true
		subs = make([]*Subscription, 0, 10)
	}
	subs = append(subs, sub)
	n.subs[topic] = subs

	if isNewTopic && n.isConnActive {
		// send to chan
		n.subChangeCh <- &subscriptionChange{
			topic:      topic,
			isNewTopic: isNewTopic,
		}
	}

	return sub
}

func (n *Notifier) unlisten(sub *Subscription) {
	success := n.tryUnlisten(sub)
	if !success {
		panic("BUG: tried to unlisten for subscription not in list")
	}
}

// needs to be in a separate method so the defer will cleanly unlock the mutex,
// even if we panic.
func (n *Notifier) tryUnlisten(sub *Subscription) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	sl := n.subs[sub.topic]
	for i, elem := range sl {
		if elem == sub {
			sl = append(sl[:i], sl[i+1:]...)
			n.subs[sub.topic] = sl
			return true
		}
	}
	return false
}
