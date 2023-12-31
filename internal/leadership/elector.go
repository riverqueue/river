package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/notifier"
)

type pgNotification struct {
	Name     string `json:"name"`
	LeaderID string `json:"leader_id"`
	Action   string `json:"action"`
}

type Notification struct {
	IsLeader  bool
	Timestamp time.Time
}

type Subscription struct {
	creationTime time.Time
	ch           chan *Notification

	unlistenOnce *sync.Once
	e            *Elector
}

func (s *Subscription) C() <-chan *Notification {
	return s.ch
}

func (s *Subscription) Unlisten() {
	s.unlistenOnce.Do(func() {
		s.e.unlisten(s)
	})
}

type Elector struct {
	adapter  dbadapter.Adapter
	id       string
	interval time.Duration
	name     string
	notifier *notifier.Notifier
	logger   *slog.Logger

	mu            sync.Mutex
	isLeader      bool
	subscriptions []*Subscription
}

// NewElector returns an Elector using the given adapter. The name should correspond
// to the name of the database + schema combo and should be shared across all Clients
// running with that combination. The id should be unique to the Client.
func NewElector(adapter dbadapter.Adapter, notifier *notifier.Notifier, name, id string, interval time.Duration, logger *slog.Logger) (*Elector, error) {
	// TODO: validate name + id length/format, interval, etc
	return &Elector{
		adapter:  adapter,
		id:       id,
		interval: interval,
		name:     name,
		notifier: notifier,
		logger:   logger.WithGroup("elector"),
	}, nil
}

func (e *Elector) Run(ctx context.Context) {
	// Before the elector returns, run a delete with NOTIFY to give up any
	// leadership that we have. If we do that here, we guarantee that any locks we
	// have will be released (even if they were acquired in gainLeadership but we
	// didn't wait for the response)
	//
	// This doesn't use ctx because it runs *after* the ctx is done.
	defer e.giveUpLeadership() //nolint:contextcheck

	// We'll send to this channel anytime a leader resigns on the key with `name`
	leadershipNotificationChan := make(chan struct{})

	handleNotification := func(topic notifier.NotificationTopic, payload string) {
		if topic != notifier.NotificationTopicLeadership {
			// This should not happen unless the notifier is broken.
			e.logger.Error("received unexpected notification", "topic", topic, "payload", payload)
			return
		}
		notification := pgNotification{}
		if err := json.Unmarshal([]byte(payload), &notification); err != nil {
			e.logger.Error("unable to unmarshal leadership notification", "err", err)
			return
		}

		if notification.Action != "resigned" || notification.Name != e.name {
			// We only care about resignations on because we use them to preempt the
			// election attempt backoff. And we only care about our own key name.
			return
		}

		select {
		case <-ctx.Done():
			return
		case leadershipNotificationChan <- struct{}{}:
		}
	}

	subscription := e.notifier.Listen(notifier.NotificationTopicLeadership, handleNotification)
	defer subscription.Unlisten()

	for {
		if success := e.gainLeadership(ctx, leadershipNotificationChan); !success {
			select {
			case <-ctx.Done():
				return
			default:
				// TODO: proper backoff
				e.logger.Error("gainLeadership returned unexpectedly, waiting to try again")
				time.Sleep(time.Second)
				continue
			}
		}

		// notify all subscribers that we're the leader
		e.notifySubscribers(true)

		err := e.keepLeadership(ctx, leadershipNotificationChan)
		e.notifySubscribers(false)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				// TODO: backoff
				e.logger.Error("error keeping leadership", "err", err)
				continue
			}
		}
	}
}

func (e *Elector) gainLeadership(ctx context.Context, leadershipNotificationChan <-chan struct{}) bool {
	for {
		success, err := e.attemptElect(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.Error("error attempting to elect", "err", err)
		}
		if success {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.interval):
			// TODO: This could potentially leak memory / timers if we're seeing a ton
			// of resignations. May want to make this reusable & cancel it when retrying?
		case <-leadershipNotificationChan:
			// Somebody just resigned, try to win the next election immediately.
		}
	}
}

func (e *Elector) attemptElect(ctx context.Context) (bool, error) {
	elected, err := e.adapter.LeadershipAttemptElect(ctx, false, e.name, e.id, e.interval)
	if err != nil {
		return false, err
	}

	select {
	case <-ctx.Done():
		// Whether or not we won an election here, it will be given up momentarily
		// when the parent loop exits.
		return elected, ctx.Err()
	default:
	}

	return elected, nil
}

func (e *Elector) keepLeadership(ctx context.Context, leadershipNotificationChan <-chan struct{}) error {
	reelectionErrCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-leadershipNotificationChan:
			// We don't care about notifications when we know we're the leader, do we?
		case <-time.After(e.interval):
			// TODO: this leaks timers if we're receiving notifications
			reelected, err := e.adapter.LeadershipAttemptElect(ctx, true, e.name, e.id, e.interval)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				reelectionErrCount += 1
				if reelectionErrCount > 5 {
					return err
				}
				e.logger.Error("error attempting reelection", "err", err)
				continue
			}
			if !reelected {
				return errors.New("lost leadership with no error")
			}
			reelectionErrCount = 0
		}
	}
}

// try up to 10 times to give up any currently held leadership.
func (e *Elector) giveUpLeadership() {
	for i := 0; i < 10; i++ {
		if err := e.attemptResign(i); err != nil {
			e.logger.Error("error attempting to resign", "err", err)
			// TODO: exponential backoff? wait longer than ~1s total?
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

func (e *Elector) attemptResign(attempt int) error {
	// Wait one second longer each time we try to resign:
	timeout := time.Duration(attempt+1) * time.Second
	// This does not inherit the parent context because we want to give up leadership
	// even during a shutdown. There is no way to short-circuit this.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return e.adapter.LeadershipResign(ctx, e.name, e.id)
}

func (e *Elector) Listen() *Subscription {
	subscription := &Subscription{
		creationTime: time.Now().UTC(),
		ch:           make(chan *Notification, 1),
		e:            e,
		unlistenOnce: &sync.Once{},
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	initialNotification := &Notification{
		IsLeader:  e.isLeader,
		Timestamp: subscription.creationTime,
	}
	subscription.ch <- initialNotification

	e.subscriptions = append(e.subscriptions, subscription)
	return subscription
}

func (e *Elector) unlisten(sub *Subscription) {
	success := e.tryUnlisten(sub)
	if !success {
		panic("BUG: tried to unlisten for subscription not in list")
	}
}

// needs to be in a separate method so the defer will cleanly unlock the mutex,
// even if we panic.
func (e *Elector) tryUnlisten(sub *Subscription) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, s := range e.subscriptions {
		if s.creationTime.Equal(sub.creationTime) {
			e.subscriptions = append(e.subscriptions[:i], e.subscriptions[i+1:]...)
			return true
		}
	}
	return false
}

func (e *Elector) notifySubscribers(isLeader bool) {
	notifyTime := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()

	e.isLeader = isLeader

	for _, s := range e.subscriptions {
		s.ch <- &Notification{
			IsLeader:  isLeader,
			Timestamp: notifyTime,
		}
	}
}
