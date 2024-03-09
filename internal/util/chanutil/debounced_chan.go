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
	c           chan struct{}
	cooldown    time.Duration
	ctxDone     <-chan struct{}
	sendLeading bool

	// mu protects variables in group below
	mu                 sync.Mutex
	sendOnTimerExpired bool
	timer              *time.Timer
	timerDone          bool
}

// NewDebouncedChan returns a new DebouncedChan which sends on the channel no
// more often than the cooldown period.
//
// If sendLeading is true, the channel will signal once on C the first time it
// receives a signal, then again once per cooldown period. If sendLeading is
// false, the initial signal isn't sent.
func NewDebouncedChan(ctx context.Context, cooldown time.Duration, sendLeading bool) *DebouncedChan {
	return &DebouncedChan{
		ctxDone:     ctx.Done(),
		c:           make(chan struct{}, 1),
		cooldown:    cooldown,
		sendLeading: sendLeading,
	}
}

// C is the debounced channel. Multiple invocations to Call during the cooldown
// period will deduplicate to a single emission on this channel on the period's
// leading edge (if sendLeading was enabled), and one more on the trailing edge
// for as many periods as invocations continue to come in.
func (d *DebouncedChan) C() <-chan struct{} {
	return d.c
}

// Call invokes the debounced channel, and is the call which will be debounced.
// If multiple invocations of this function are made during the cooldown period,
// they'll be debounced to a single emission on C on the period's leading edge
// (if sendLeading is enabled), and then one fire on the trailing edge of each
// period for as long as Call continues to be invoked. If a timer period elapses
// without an invocation on Call, the timer is stopped and behavior resets the
// next time Call is invoked again.
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
	// will be reset. Send immediately (i.e. n the leading edge of the
	// debounce period), if sendLeading is enabled.
	if d.sendLeading {
		d.nonBlockingSendOnC()
	} else {
		d.sendOnTimerExpired = true
	}

	// Next, start the timer, during which we'll monitor for additional calls,
	// and send at the end of the period if any came in. Create a new timer if
	// this is the first run. Otherwise, reset an existing one.
	if d.timer == nil {
		d.timer = time.NewTimer(d.cooldown)
	} else {
		d.timer.Reset(d.cooldown)
	}
	d.timerDone = false

	go d.waitForTimerLoop()
}

func (d *DebouncedChan) nonBlockingSendOnC() {
	select {
	case d.c <- struct{}{}:
	default:
	}
}

// Waits for the timer to be fired, and loops as long as Call invocations come
// in. If a period elapses without a new Call coming in, the loop returns, and
// DebouncedChan returns to its initial state, waiting for a new Call.
//
// The loop also stops if context becomes done.
func (d *DebouncedChan) waitForTimerLoop() {
	for {
		if stopLoop := d.waitForTimerOnce(); stopLoop {
			break
		}
	}
}

// Waits for the timer to fire once or context becomes done. Returns true if the
// caller should stop looping (i.e. don't wait on the timer again), and false
// otherwise.
func (d *DebouncedChan) waitForTimerOnce() bool {
	select {
	case <-d.ctxDone:
		d.mu.Lock()
		defer d.mu.Unlock()

		if d.timer != nil {
			if !d.timer.Stop() {
				<-d.timer.C
			}
		}

		d.timerDone = true

	case <-d.timer.C:
		d.mu.Lock()
		defer d.mu.Unlock()

		if d.sendOnTimerExpired {
			d.sendOnTimerExpired = false
			d.nonBlockingSendOnC()

			// Wait for another timer expiry, which will fire again if another
			// Call comes in during that time. If no Call comes in, the timer
			// will stop on the next cycle and we return to initial state.
			d.timer.Reset(d.cooldown)
			return false // do _not_ stop looping
		}

		d.timerDone = true
	}

	return true // stop looping
}
