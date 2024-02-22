package chanutil

import (
	"context"
	"sync"
	"time"
)

// DebouncedChan is a function that will only be called once per cooldown
// period, at the leading edge. If it is called again during the cooldown, the
// subsequent calls are delayed until the cooldown period has elapsed and are
// also coalesced into a single call.
type DebouncedChan struct {
	c        chan struct{}
	cooldown time.Duration
	ctxDone  <-chan struct{}

	// mu protects variables in group below
	mu                 sync.Mutex
	sendOnTimerExpired bool
	timer              *time.Timer
	timerDone          bool
}

// NewDebouncedChan returns a new DebouncedChan which sends on the channel no
// more often than the cooldown period.
func NewDebouncedChan(ctx context.Context, cooldown time.Duration) *DebouncedChan {
	return &DebouncedChan{
		ctxDone:  ctx.Done(),
		c:        make(chan struct{}, 1),
		cooldown: cooldown,
	}
}

// C is the debounced channel. Multiple invocations to Call during the cooldown
// period will deduplicate to a single emission on this channel on the period's
// leading edge, and one more on the trailing edge.
func (d *DebouncedChan) C() <-chan struct{} {
	return d.c
}

// Call invokes the debounced channel, and is the call which will be debounced.
// If multiple invocations of this function are made during the cooldown period,
// they'll be debounced to a single emission on C on the period's leading edge,
// and one more on the trailing edge.
func (d *DebouncedChan) Call() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// A timer has already been initialized and hasn't already expired. (If it
	// has expired, we'll reset it below.) Set to signal when it does expire.
	if d.timer != nil && !d.timerDone {
		d.sendOnTimerExpired = true
		return
	}

	// No timer had been started yet, or the last one running was expired and
	// will be reset. Send immediately. (i.e. On the leading edge of the
	// debounce period.)
	d.nonBlockingSendOnC()

	// Next, start the timer, during which we'll monitor for additional calls,
	// and send at the end of the period if any came in. Create a new timer if
	// this is the first run. Otherwise, reset an existing one.
	if d.timer == nil {
		d.timer = time.NewTimer(d.cooldown)
	} else {
		d.timer.Reset(d.cooldown)
	}
	d.timerDone = false

	go d.waitForTimer()
}

func (d *DebouncedChan) nonBlockingSendOnC() {
	select {
	case d.c <- struct{}{}:
	default:
	}
}

func (d *DebouncedChan) waitForTimer() {
	select {
	case <-d.ctxDone:
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.timer != nil {
			if !d.timer.Stop() {
				<-d.timer.C
			}
			d.timerDone = true
		}

	case <-d.timer.C:
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.sendOnTimerExpired {
			d.nonBlockingSendOnC()
		}
		d.timerDone = true
		d.sendOnTimerExpired = false
	}
}
