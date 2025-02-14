package rivertest

import (
	"sync"
	"time"
)

// TimeStub implements rivertype.TimeGenerator to allow time to be stubbed in
// tests. It is implemented in a thread-safe manner with a mutex, allowing the
// current time to be stubbed at any time with StubNowUTC.
type TimeStub struct {
	mu     sync.RWMutex
	nowUTC *time.Time
}

// NowUTC returns the current time. This may be a stubbed time if the time has
// been actively stubbed in a test.
func (t *TimeStub) NowUTC() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.nowUTC == nil {
		return time.Now().UTC()
	}

	return *t.nowUTC
}

// NowUTCOrNil returns if the currently stubbed time _if_ the current time
// is stubbed, and returns nil otherwise. This is generally useful in cases
// where a component may want to use a stubbed time if the time is stubbed,
// but to fall back to a database time default otherwise.
func (t *TimeStub) NowUTCOrNil() *time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.nowUTC
}

// StubNowUTC stubs the current time. It will panic if invoked outside of tests.
// Returns the same time passed as parameter for convenience.
func (t *TimeStub) StubNowUTC(nowUTC time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nowUTC = &nowUTC
	return nowUTC
}
