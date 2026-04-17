package leadership

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	electIntervalDefault            = 5 * time.Second
	electIntervalJitterDefault      = 1 * time.Second
	electIntervalTTLPaddingDefault  = 10 * time.Second
	leaderLocalDeadlineSafetyMargin = 1 * time.Second
)

type DBNotification struct {
	Action   DBNotificationKind `json:"action"`
	LeaderID string             `json:"leader_id"`
}

type DBNotificationKind string

const (
	DBNotificationKindRequestResign DBNotificationKind = "request_resign"
	DBNotificationKindResigned      DBNotificationKind = "resigned"
)

type Notification struct {
	IsLeader  bool
	Timestamp time.Time
}

// Subscription is a client-facing stream of leadership transitions.
//
// The elector publishes every transition (`false -> true -> false -> ...`) to
// each subscription. Delivery is delegated to a subscriptionRelay so the
// elector never blocks on a slow listener.
type Subscription struct {
	creationTime time.Time
	relay        *subscriptionRelay

	unlistenOnce *sync.Once
	e            *Elector
}

func (s *Subscription) C() <-chan *Notification {
	return s.relay.C()
}

func (s *Subscription) enqueue(notification *Notification) {
	s.relay.enqueue(notification)
}

func (s *Subscription) stop() {
	s.relay.stop()
}

func (s *Subscription) Unlisten() {
	s.unlistenOnce.Do(func() {
		s.e.unlisten(s)
	})
}

// subscriptionRelay decouples elector publication from subscriber consumption.
//
// The elector may need to publish `true` and `false` transitions promptly while
// maintenance components are still busy reacting to the previous one. A plain
// buffered channel would either block the elector or force us to drop
// transitions when the buffer filled. The relay solves that by:
//   - appending every Notification to an in-memory FIFO queue
//   - waking a dedicated goroutine via `pendingChan`
//   - letting that goroutine drain queued notifications into the subscriber's
//     public channel `ch`
//
// `pendingChan` is only a wakeup signal. It does not carry the notifications
// themselves, so multiple sends may coalesce while the goroutine is already
// awake. That is safe because the authoritative queue is pendingNotifications.
type subscriptionRelay struct {
	ch          chan *Notification // public per-subscription delivery channel
	done        chan struct{}      // closes the relay goroutine during Unlisten/stop
	pendingChan chan struct{}      // coalesced wakeup signal that queued work exists

	// pendingNotifications preserves every leadership transition in order.
	// A dedicated goroutine drains it into `ch` so slow subscribers cannot
	// block the elector, but consumers like QueueMaintainerLeader still see
	// each `false` transition instead of only the latest state.
	pendingMu            sync.Mutex
	pendingNotifications []*Notification
}

func newSubscriptionRelay() *subscriptionRelay {
	relay := &subscriptionRelay{
		ch:          make(chan *Notification, 1),
		done:        make(chan struct{}),
		pendingChan: make(chan struct{}, 1),
	}

	go relay.run()

	return relay
}

func (r *subscriptionRelay) C() <-chan *Notification {
	return r.ch
}

// enqueue appends a transition to the pending FIFO, then nudges the relay
// goroutine. The notification argument is the exact transition to preserve in
// order; unlike the notifier wakeup path elsewhere in the elector, these items
// must not be coalesced or replaced.
func (r *subscriptionRelay) enqueue(notification *Notification) {
	r.pendingMu.Lock()
	r.pendingNotifications = append(r.pendingNotifications, notification)
	r.pendingMu.Unlock()

	select {
	case r.pendingChan <- struct{}{}:
	default:
	}
}

// nextPending pops the next queued notification for the relay goroutine.
func (r *subscriptionRelay) nextPending() (*Notification, bool) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	if len(r.pendingNotifications) == 0 {
		return nil, false
	}

	notification := r.pendingNotifications[0]
	r.pendingNotifications = r.pendingNotifications[1:]
	return notification, true
}

// run waits until queued work exists, then drains as many pending
// notifications as possible into the subscriber channel before sleeping again.
// It exits promptly when stop closes done.
func (r *subscriptionRelay) run() {
	for {
		select {
		case <-r.done:
			return

		case <-r.pendingChan:
		}

		for {
			notification, ok := r.nextPending()
			if !ok {
				break
			}

			select {
			case <-r.done:
				return
			case r.ch <- notification:
			}
		}
	}
}

// stop terminates the relay goroutine. Callers must ensure they stop enqueueing
// through the owning Subscription afterwards.
func (r *subscriptionRelay) stop() {
	close(r.done)
}

// Test-only properties.
type electorTestSignals struct {
	DeniedLeadership     testsignal.TestSignal[struct{}] // notifies when elector fails to gain leadership
	GainedLeadership     testsignal.TestSignal[struct{}] // notifies when elector gains leadership
	LostLeadership       testsignal.TestSignal[struct{}] // notifies when an elected leader loses leadership
	MaintainedLeadership testsignal.TestSignal[struct{}] // notifies when elector maintains leadership
	ResignedLeadership   testsignal.TestSignal[struct{}] // notifies when elector resigns leadership
}

func (ts *electorTestSignals) Init(tb testutil.TestingTB) {
	ts.DeniedLeadership.Init(tb)
	ts.GainedLeadership.Init(tb)
	ts.LostLeadership.Init(tb)
	ts.MaintainedLeadership.Init(tb)
	ts.ResignedLeadership.Init(tb)
}

type Config struct {
	ClientID            string
	ElectInterval       time.Duration // period on which each elector attempts elect even without having received a resignation notification
	ElectIntervalJitter time.Duration
	Schema              string
}

func (c *Config) mustValidate() *Config {
	if c.ClientID == "" {
		panic("Config.ClientID must be non-empty")
	}
	if c.ElectInterval <= 0 {
		panic("Config.ElectInterval must be above zero")
	}

	return c
}

type Elector struct {
	baseservice.BaseService
	startstop.BaseStartStop

	config      *Config
	exec        riverdriver.Executor
	notifier    *notifier.Notifier
	testSignals electorTestSignals
	wakeupChan  chan struct{}

	mu                   sync.Mutex
	isLeader             bool
	pendingRequestResign bool
	subscriptions        []*Subscription
}

type leadershipTerm struct {
	clientID     string
	electedAt    time.Time
	trustedUntil time.Time
}

func newLeadershipTerm(clientID string, electedAt, attemptStarted time.Time, ttl time.Duration) leadershipTerm {
	term := leadershipTerm{
		clientID:  clientID,
		electedAt: electedAt,
	}

	trustDuration := ttl - leaderLocalDeadlineSafetyMargin
	if trustDuration <= 0 {
		term.trustedUntil = attemptStarted
		return term
	}

	term.trustedUntil = attemptStarted.Add(trustDuration)
	return term
}

func (t leadershipTerm) remaining(now time.Time) time.Duration {
	if !t.trustedUntil.After(now) {
		return 0
	}

	return t.trustedUntil.Sub(now)
}

func (t leadershipTerm) reelectAttemptTimeout(now time.Time) time.Duration {
	remainingDuration := t.remaining(now)
	if remainingDuration <= 0 {
		return 0
	}
	if remainingDuration < deadlineTimeout {
		return remainingDuration
	}

	return deadlineTimeout
}

// NewElector returns an Elector using the given adapter. The name should correspond
// to the name of the database + schema combo and should be shared across all Clients
// running with that combination. The id should be unique to the Client.
func NewElector(archetype *baseservice.Archetype, exec riverdriver.Executor, notifier *notifier.Notifier, config *Config) *Elector {
	return baseservice.Init(archetype, &Elector{
		config: (&Config{
			ClientID:            config.ClientID,
			ElectInterval:       cmp.Or(config.ElectInterval, electIntervalDefault),
			ElectIntervalJitter: cmp.Or(config.ElectIntervalJitter, electIntervalJitterDefault),
			Schema:              config.Schema,
		}).mustValidate(),
		exec:     exec,
		notifier: notifier,
	})
}

func trySendWakeup(ctx context.Context, wakeupChan chan struct{}) {
	if ctx.Err() != nil {
		return
	}

	select {
	case <-ctx.Done():
	case wakeupChan <- struct{}{}:
	default:
	}
}

func (e *Elector) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := e.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	// Buffered to 1 so notifications coalesce instead of blocking the elector.
	e.wakeupChan = make(chan struct{}, 1)

	var sub *notifier.Subscription
	if e.notifier == nil {
		e.Logger.DebugContext(ctx, e.Name+": No notifier configured; starting in poll mode", "client_id", e.config.ClientID)
	} else {
		e.Logger.DebugContext(ctx, e.Name+": Listening for leadership changes", "client_id", e.config.ClientID, "topic", notifier.NotificationTopicLeadership)
		var err error
		sub, err = e.notifier.Listen(ctx, notifier.NotificationTopicLeadership, func(topic notifier.NotificationTopic, payload string) {
			e.handleLeadershipNotification(ctx, topic, payload)
		})
		if err != nil {
			stopped()
			if strings.HasSuffix(err.Error(), "conn closed") || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		e.Logger.DebugContext(ctx, e.Name+": Run loop started")
		defer e.Logger.DebugContext(ctx, e.Name+": Run loop stopped")

		if sub != nil {
			defer sub.Unlisten(ctx)
		}

		for {
			term, err := e.runFollowerState(ctx)
			if err != nil {
				// Function above only returns an error if context was cancelled
				// or overall context is done.
				if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
					panic(err)
				}
				return
			}

			e.publishLeadershipState(true)
			e.Logger.DebugContext(ctx, e.Name+": Gained leadership", "client_id", e.config.ClientID)
			e.testSignals.GainedLeadership.Signal(struct{}{})

			err = e.runLeaderState(ctx, term)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				e.Logger.ErrorContext(ctx, e.Name+": Error keeping leadership", "client_id", e.config.ClientID, "err", err)
			}
		}
	}()

	return nil
}

// runFollowerState is the follower side of the elector state machine. It keeps
// attempting election until this client becomes leader or the elector stops.
func (e *Elector) runFollowerState(ctx context.Context) (leadershipTerm, error) {
	var attempt int
	for {
		attempt++
		e.Logger.DebugContext(ctx, e.Name+": Attempting to gain leadership", "client_id", e.config.ClientID)
		// Use the local monotonic-bearing clock for the trust window. The
		// DB-facing timestamp path stays on NowUTCOrNil below.
		attemptStarted := e.Time.Now()

		leader, err := attemptElect(ctx, e.exec, &riverdriver.LeaderElectParams{
			LeaderID: e.config.ClientID,
			Now:      e.Time.NowOrNil(),
			Schema:   e.config.Schema,
			TTL:      e.leaderTTL(),
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return leadershipTerm{}, err
			}
			if !errors.Is(err, rivertype.ErrNotFound) {
				sleepDuration := serviceutil.ExponentialBackoff(attempt, serviceutil.MaxAttemptsBeforeResetDefault)
				e.Logger.ErrorContext(ctx, e.Name+": Error attempting to elect", e.errorSlogArgs(err, attempt, sleepDuration)...)
				serviceutil.CancellableSleep(ctx, sleepDuration)
				continue
			}
		}

		if leader != nil {
			return newLeadershipTerm(leader.LeaderID, leader.ElectedAt, attemptStarted, e.leaderTTL()), nil
		}

		attempt = 0

		e.Logger.DebugContext(ctx, e.Name+": Leadership bid was unsuccessful (not an error)", "client_id", e.config.ClientID)
		e.testSignals.DeniedLeadership.Signal(struct{}{})

		select {
		case <-serviceutil.CancellableSleepC(ctx, randutil.DurationBetween(e.config.ElectInterval, e.config.ElectInterval+e.config.ElectIntervalJitter)):
			if ctx.Err() != nil { // context done
				return leadershipTerm{}, ctx.Err()
			}

		case <-e.wakeupChan:
			// Somebody just resigned, try to win the next election after a very
			// short random interval (to prevent all clients from bidding at once).
			serviceutil.CancellableSleep(ctx, randutil.DurationBetween(0, 50*time.Millisecond))
		}
	}
}

// Handles a leadership notification from the notifier.
func (e *Elector) handleLeadershipNotification(ctx context.Context, topic notifier.NotificationTopic, payload string) {
	if topic != notifier.NotificationTopicLeadership {
		// This should not happen unless the notifier is broken.
		e.Logger.ErrorContext(ctx, e.Name+": Received unexpected notification", "client_id", e.config.ClientID, "topic", topic, "payload", payload)
		return
	}

	notification := DBNotification{}
	if err := json.Unmarshal([]byte(payload), &notification); err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Unable to unmarshal leadership notification", "client_id", e.config.ClientID, "err", err)
		return
	}

	e.Logger.DebugContext(ctx, e.Name+": Received notification from notifier", "action", notification.Action, "client_id", e.config.ClientID)

	// Do an initial context check so in case context is done, it always takes
	// precedence over sending a leadership notification.
	if ctx.Err() != nil {
		return
	}

	switch notification.Action {
	case DBNotificationKindRequestResign:
		if !e.markPendingRequestResign() {
			return
		}

		trySendWakeup(ctx, e.wakeupChan)
	case DBNotificationKindResigned:
		// If this a resignation from _this_ client, ignore the change.
		if notification.LeaderID == e.config.ClientID {
			return
		}

		trySendWakeup(ctx, e.wakeupChan)
	}
}

// runLeaderState is the leader side of the elector state machine. It waits for
// either a reelection interval, a forced resignation, or shutdown.
func (e *Elector) runLeaderState(ctx context.Context, term leadershipTerm) error {
	defer e.clearPendingRequestResign()
	defer e.publishLeadershipState(false)

	shouldResign := true

	// Before the elector returns, run a delete with NOTIFY to give up any
	// leadership that we have. If we do that here, we guarantee that any locks
	// we have will be released (even if they were acquired in
	// attemptGainLeadership but we didn't wait for the response)
	//
	// This doesn't use ctx because it runs *after* the ctx is done.
	defer func() {
		if shouldResign {
			e.attemptResignLoop(ctx, term) // will resign using WithoutCancel context, but ctx sent for logging
		}
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()

	numErrors := 0
	waitDuration := e.config.ElectInterval

	for {
		resetTimer(timer, waitDuration)

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-e.wakeupChan:
			if !e.takePendingRequestResign() {
				continue
			}

			e.Logger.InfoContext(ctx, e.Name+": Current leader received forced resignation", "client_id", e.config.ClientID)

			// This client may win leadership again, but drop out of this
			// function and make it start all over.
			return nil

		case <-timer.C:
			// Reelect timer expired; attempt reelection below.
		}

		e.Logger.DebugContext(ctx, e.Name+": Current leader attempting reelect", "client_id", e.config.ClientID)

		// Use the local monotonic-bearing clock for the trust window. The
		// DB-facing timestamp path stays on NowOrNil below.
		attemptStarted := e.Time.Now()
		attemptTimeout := term.reelectAttemptTimeout(attemptStarted)
		if attemptTimeout <= 0 {
			e.Logger.WarnContext(ctx, e.Name+": Current leader stepping down because the reelection deadline elapsed", "client_id", e.config.ClientID)
			e.testSignals.LostLeadership.Signal(struct{}{})
			return nil
		}

		leader, err := attemptReelectWithTimeout(ctx, e.exec, &riverdriver.LeaderReelectParams{
			ElectedAt: term.electedAt,
			LeaderID:  term.clientID,
			Now:       e.Time.NowOrNil(),
			Schema:    e.config.Schema,
			TTL:       e.leaderTTL(),
		}, attemptTimeout)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			if errors.Is(err, rivertype.ErrNotFound) {
				shouldResign = false
				e.testSignals.LostLeadership.Signal(struct{}{})
				return nil
			}

			numErrors++
			sleepDuration := serviceutil.ExponentialBackoff(numErrors, 3)
			remainingDuration := term.remaining(e.Time.Now())
			if remainingDuration <= 0 {
				e.Logger.WarnContext(ctx, e.Name+": Current leader stepping down because the reelection deadline elapsed after an error", "client_id", e.config.ClientID)
				e.testSignals.LostLeadership.Signal(struct{}{})
				return nil
			}

			e.Logger.ErrorContext(ctx, e.Name+": Error attempting reelection", e.errorSlogArgs(err, numErrors, sleepDuration)...)
			if remainingDuration < sleepDuration {
				sleepDuration = remainingDuration
			}
			serviceutil.CancellableSleep(ctx, sleepDuration)
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Retry immediately after the backoff because the time budget for this
			// lease has already been reduced by the failed attempt above.
			waitDuration = 0
			continue
		}

		numErrors = 0
		term = newLeadershipTerm(leader.LeaderID, leader.ElectedAt, attemptStarted, e.leaderTTL())
		e.testSignals.MaintainedLeadership.Signal(struct{}{})
		waitDuration = e.config.ElectInterval
	}
}

// Try up to 3 times to give up any currently held leadership.
//
// The context received is used for logging purposes, but the function actually
// makes use of a background context to try and guarantee that leadership is
// always surrendered in a timely manner so it can be picked up quickly by
// another client, even in the event of a cancellation.
func (e *Elector) attemptResignLoop(ctx context.Context, term leadershipTerm) {
	e.Logger.DebugContext(ctx, e.Name+": Attempting to resign leadership", "client_id", e.config.ClientID)

	// Make a good faith attempt to resign, even in the presence of errors, but
	// don't keep hammering if it doesn't work. In case a resignation failure,
	// leader TTLs will act as an additional hedge to ensure a new leader can
	// still be elected.
	const maxNumErrors = 3

	// This does not inherit the parent context's cancellation because we want to
	// give up leadership even during a shutdown. There is no way to short-circuit
	// this, though there are timeouts per call within attemptResign.
	ctx = context.WithoutCancel(ctx)

	for attempt := 1; attempt <= maxNumErrors; attempt++ {
		if err := e.attemptResign(ctx, attempt, term); err != nil {
			sleepDuration := serviceutil.ExponentialBackoff(attempt, maxNumErrors)
			e.Logger.ErrorContext(ctx, e.Name+": Error attempting to resign", e.errorSlogArgs(err, attempt, sleepDuration)...)
			serviceutil.CancellableSleep(ctx, sleepDuration)

			continue
		}

		return
	}
}

// attemptResign attempts to resign any currently held leaderships for the
// elector's name and leader ID.
func (e *Elector) attemptResign(ctx context.Context, attempt int, term leadershipTerm) error {
	// Wait one second longer each time we try to resign:
	timeout := time.Duration(attempt) * time.Second

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resigned, err := e.exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
		ElectedAt:       term.electedAt,
		LeaderID:        term.clientID,
		LeadershipTopic: string(notifier.NotificationTopicLeadership),
		Schema:          e.config.Schema,
	})
	if err != nil {
		return err
	}

	if resigned {
		e.Logger.DebugContext(ctx, e.Name+": Resigned leadership successfully", "client_id", e.config.ClientID)
		e.testSignals.ResignedLeadership.Signal(struct{}{})
	}

	return nil
}

// Produces a common set of key/value pairs for logging when an error occurs.
//
// Refactored out because we had three repeats of identical information in this
// file, but if it causes things to get messy, may want to refactor again.
func (e *Elector) errorSlogArgs(err error, attempt int, sleepDuration time.Duration) []any {
	return []any{
		slog.Int("attempt", attempt),
		slog.String("client_id", e.config.ClientID),
		slog.String("err", err.Error()),
		slog.String("sleep_duration", sleepDuration.String()),
	}
}

func (e *Elector) Listen() *Subscription {
	sub := &Subscription{
		creationTime: time.Now().UTC(),
		e:            e,
		relay:        newSubscriptionRelay(),
		unlistenOnce: &sync.Once{},
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	initialNotification := &Notification{
		IsLeader:  e.isLeader,
		Timestamp: sub.creationTime,
	}
	sub.enqueue(initialNotification)

	e.subscriptions = append(e.subscriptions, sub)
	return sub
}

func (e *Elector) unlisten(sub *Subscription) {
	success := e.tryUnlisten(sub)
	if !success {
		panic("BUG: tried to unlisten for subscription not in list")
	}

	sub.stop()
}

// needs to be in a separate method so the defer will cleanly unlock the mutex,
// even if we panic.
func (e *Elector) tryUnlisten(sub *Subscription) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, s := range e.subscriptions {
		if s == sub {
			e.subscriptions = append(e.subscriptions[:i], e.subscriptions[i+1:]...)
			return true
		}
	}
	return false
}

// leaderTTL is at least the reelect run interval used by clients to try and gain
// leadership or reelect themselves as leader, plus a little padding to account
// to give the leader a little breathing room in its reelection loop.
func (e *Elector) leaderTTL() time.Duration {
	return e.config.ElectInterval + electIntervalTTLPaddingDefault
}

func (e *Elector) markPendingRequestResign() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isLeader {
		return false
	}

	e.pendingRequestResign = true
	return true
}

func (e *Elector) publishLeadershipState(isLeader bool) {
	notifyTime := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()

	e.isLeader = isLeader
	if !isLeader {
		e.pendingRequestResign = false
	}

	notification := &Notification{
		IsLeader:  isLeader,
		Timestamp: notifyTime,
	}

	for _, s := range e.subscriptions {
		s.enqueue(notification)
	}
}

func (e *Elector) clearPendingRequestResign() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pendingRequestResign = false
}

func (e *Elector) takePendingRequestResign() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.pendingRequestResign {
		return false
	}

	e.pendingRequestResign = false
	return true
}

const deadlineTimeout = 5 * time.Second

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	timer.Reset(duration)
}

// attemptElect attempts to elect a leader for the given name. If there is no
// current leader or the previous leader expired, the provided leader ID is set
// as the new leader with a TTL of `params.TTL`.
func attemptElect(ctx context.Context, exec riverdriver.Executor, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
	return attemptElectWithTimeout(ctx, exec, params, deadlineTimeout)
}

func attemptElectWithTimeout(ctx context.Context, exec riverdriver.Executor, params *riverdriver.LeaderElectParams, timeout time.Duration) (*riverdriver.Leader, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	execTx, err := exec.Begin(ctx)
	if err != nil {
		var additionalDetail string
		if errors.Is(err, context.DeadlineExceeded) {
			additionalDetail = " (a common cause of this is a database pool that's at its connection limit; you may need to increase maximum connections)"
		}

		return nil, fmt.Errorf("error beginning transaction: %w%s", err, additionalDetail)
	}
	defer dbutil.RollbackWithoutCancel(ctx, execTx)

	if _, err := execTx.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{
		Now:    params.Now,
		Schema: params.Schema,
	}); err != nil {
		return nil, err
	}

	leader, err := execTx.LeaderAttemptElect(ctx, params)
	if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
		return nil, err
	}
	if err := execTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}
	if err != nil {
		return nil, err
	}

	return leader, nil
}

func attemptReelectWithTimeout(ctx context.Context, exec riverdriver.Executor, params *riverdriver.LeaderReelectParams, timeout time.Duration) (*riverdriver.Leader, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return exec.LeaderAttemptReelect(ctx, params)
}
