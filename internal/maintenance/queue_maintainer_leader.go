package maintenance

import (
	"context"
	"log/slog"
	"sync"

	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const queueMaintainerMaxStartAttempts = 3

// QueueMaintainerLeaderTestSignals are internal signals used exclusively in tests.
type QueueMaintainerLeaderTestSignals struct {
	ElectedLeader         testsignal.TestSignal[struct{}] // notifies when elected leader
	StartError            testsignal.TestSignal[error]    // notifies on each failed queue maintainer start attempt
	StartRetriesExhausted testsignal.TestSignal[struct{}] // notifies when all start retries have been exhausted
}

func (ts *QueueMaintainerLeaderTestSignals) Init(tb testutil.TestingTB) {
	ts.ElectedLeader.Init(tb)
	ts.StartError.Init(tb)
	ts.StartRetriesExhausted.Init(tb)
}

// QueueMaintainerLeaderConfig is the configuration for QueueMaintainerLeader.
type QueueMaintainerLeaderConfig struct {
	// ClientID is used for logging on leadership changes.
	ClientID string

	// Elector provides leadership change notifications.
	Elector *leadership.Elector

	// QueueMaintainer is the underlying maintainer to start/stop on leadership
	// changes.
	QueueMaintainer *QueueMaintainer

	// RequestResignFunc sends a notification requesting leader resignation.
	// It's injected from the client because the notification mechanism depends
	// on the driver, which the maintenance package doesn't know about.
	RequestResignFunc func(ctx context.Context) error
}

// QueueMaintainerLeader listens for leadership changes and starts/stops the
// queue maintainer accordingly. It handles retries with exponential backoff on
// start failures, and requests leader resignation when all retries are
// exhausted. This is extracted to a separate struct because to get all the edge
// cases right, it ends up being a fair bit of code that would otherwise make
// Client fairly heavy.
type QueueMaintainerLeader struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	TestSignals QueueMaintainerLeaderTestSignals

	config *QueueMaintainerLeaderConfig

	// epoch is incremented each time leadership is gained, giving each start
	// goroutine a term number. mu serializes epoch checks with Stop calls so
	// a stale goroutine cannot tear down a newer term's maintainer.
	epoch int64
	mu    sync.Mutex
}

func NewQueueMaintainerLeader(archetype *baseservice.Archetype, config *QueueMaintainerLeaderConfig) *QueueMaintainerLeader {
	return baseservice.Init(archetype, &QueueMaintainerLeader{
		config: config,
	})
}

func (s *QueueMaintainerLeader) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		sub := s.config.Elector.Listen()
		defer sub.Unlisten()

		// Cancel function for an in-progress start attempt. If leadership is
		// lost while the start process is still retrying, used to abort it
		// promptly instead of waiting for retries to finish.
		var cancelStart context.CancelCauseFunc = func(_ error) {}

		// Tracks in-flight tryStart goroutines so we can wait for them to
		// finish before returning, preventing logging on a dead test.
		var startWg sync.WaitGroup
		defer startWg.Wait()

		for {
			select {
			case <-ctx.Done():
				cancelStart(context.Cause(ctx))
				return

			case notification := <-sub.C():
				s.Logger.DebugContext(ctx, s.Name+": Election change received",
					slog.String("client_id", s.config.ClientID), slog.Bool("is_leader", notification.IsLeader))

				switch {
				case notification.IsLeader:
					s.TestSignals.ElectedLeader.Signal(struct{}{})

					// Start with retries in a separate goroutine so the
					// leadership change loop remains responsive.
					var startCtx context.Context
					startCtx, cancelStart = context.WithCancelCause(ctx)

					s.mu.Lock()
					s.epoch++
					epoch := s.epoch
					s.mu.Unlock()

					startWg.Go(func() {
						s.tryStart(startCtx, epoch)
					})

				default:
					// Cancel any in-progress start attempts before stopping.
					// Send ErrStop so services like Reindexer run cleanup.
					cancelStart(startstop.ErrStop)
					cancelStart = func(_ error) {}

					s.config.QueueMaintainer.Stop()
				}
			}
		}
	}()

	return nil
}

func (s *QueueMaintainerLeader) tryStart(ctx context.Context, epoch int64) {
	var lastErr error
	for attempt := 1; attempt <= queueMaintainerMaxStartAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}

		if lastErr = s.config.QueueMaintainer.Start(ctx); lastErr == nil {
			return
		}

		s.Logger.ErrorContext(ctx, s.Name+": Error starting queue maintainer",
			slog.String("err", lastErr.Error()), slog.Int("attempt", attempt))

		s.TestSignals.StartError.Signal(lastErr)

		// Stop to fully reset state before retrying. The mutex serializes
		// the epoch check with the increment in Start so a stale goroutine
		// cannot tear down a newer term's maintainer.
		s.mu.Lock()
		stale := s.epoch != epoch
		if !stale {
			s.config.QueueMaintainer.Stop()
		}
		s.mu.Unlock()
		if stale {
			return
		}

		if attempt < queueMaintainerMaxStartAttempts {
			serviceutil.CancellableSleep(ctx, serviceutil.ExponentialBackoff(attempt, serviceutil.MaxAttemptsBeforeResetDefault))
		}
	}

	if ctx.Err() != nil {
		return
	}

	s.Logger.ErrorContext(ctx, s.Name+": Queue maintainer failed to start after all attempts, requesting leader resignation",
		slog.String("err", lastErr.Error()))

	s.TestSignals.StartRetriesExhausted.Signal(struct{}{})

	if err := s.config.RequestResignFunc(ctx); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error requesting leader resignation", slog.String("err", err.Error()))
	}
}
