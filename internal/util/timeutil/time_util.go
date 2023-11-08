package timeutil

import (
	"context"
	"time"
)

// SecondsAsDuration is a simple shortcut for converting seconds represented as
// a float to a `time.Duration`.
func SecondsAsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// TickerWithInitialTick is similar to `time.Ticker`, except that it fires once
// immediately upon initialization. It also respects context cancellation and
// prefers to be stopped that way rather than an explicit `Stop` function.
type TickerWithInitialTick struct {
	// C fires once on initial startup, then after each interval has passed.
	C <-chan time.Time

	interval time.Duration
	tickChan chan time.Time
}

// NewTickerWithInitialTick creates a new ticker similar to `time.Ticker`,
// except that it fires once immediately upon initialization. It also respects
// context cancellation and prefers to be stopped that way rather than an
// explicit `Stop` function.
func NewTickerWithInitialTick(ctx context.Context, interval time.Duration) *TickerWithInitialTick {
	// Channel of size one combined with non-blocking send are modeled on how
	// Go's internal ticker works. Ticks may be dropped if the caller falls behind.
	tickChan := make(chan time.Time, 1)

	timer := &TickerWithInitialTick{
		C:        tickChan,
		interval: interval,
		tickChan: tickChan,
	}
	go timer.runLoop(ctx)
	return timer
}

// Sends a non-blocking tick into the ticker's channel. Ticks may be dropped if
// the caller falls behind.
func (t *TickerWithInitialTick) nonBlockingTick(tm time.Time) {
	select {
	case t.tickChan <- tm:
	default:
	}
}

func (t *TickerWithInitialTick) runLoop(ctx context.Context) {
	// Return immediately if context is done.
	select {
	case <-ctx.Done():
		return
	default:
	}

	t.nonBlockingTick(time.Now())

	ticker := time.NewTicker(t.interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return

		case tm := <-ticker.C:
			t.nonBlockingTick(tm)
		}
	}
}
