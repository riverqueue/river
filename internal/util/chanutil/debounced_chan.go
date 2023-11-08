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
	done     <-chan struct{}
	c        chan struct{}
	cooldown time.Duration

	mu                   sync.Mutex
	timer                *time.Timer
	sendWhenTimerExpired bool
}

// NewDebouncedChan returns a new DebouncedChan which sends on the channel no
// more often than the cooldown period.
func NewDebouncedChan(ctx context.Context, cooldown time.Duration) *DebouncedChan {
	return &DebouncedChan{
		done:     ctx.Done(),
		c:        make(chan struct{}, 1),
		cooldown: cooldown,
	}
}

func (d *DebouncedChan) C() <-chan struct{} {
	return d.c
}

func (d *DebouncedChan) Call() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.sendWhenTimerExpired = true
		return
	}

	// TODO: The design of this can probably be simplified to not rely on creating
	// a new timer every time.
	d.timer = time.NewTimer(d.cooldown)
	go d.waitForTimer()
	d.sendOnChan()
}

func (d *DebouncedChan) sendOnChan() {
	select {
	case d.c <- struct{}{}:
	default:
	}
}

func (d *DebouncedChan) waitForTimer() {
	select {
	case <-d.done:
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.timer != nil {
			if !d.timer.Stop() {
				<-d.timer.C
			}
		}
	case <-d.timer.C:
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.sendWhenTimerExpired {
			d.sendOnChan()
		}
		d.timer = nil
		d.sendWhenTimerExpired = false
	}
}
