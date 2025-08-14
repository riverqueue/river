package rivertype

import "time"

// TimeGenerator generates a current time in UTC. In test environments it's
// implemented by riversharedtest.TimeStub which lets the current time be
// stubbed. Otherwise, it's implemented as UnStubbableTimeGenerator which
// doesn't allow stubbing.
type TimeGenerator interface {
	// NowUTC returns the current time. This may be a stubbed time if the time
	// has been actively stubbed in a test.
	NowUTC() time.Time

	// NowUTCOrNil returns if the currently stubbed time _if_ the current time
	// is stubbed, and returns nil otherwise. This is generally useful in cases
	// where a component may want to use a stubbed time if the time is stubbed,
	// but to fall back to a database time default otherwise.
	NowUTCOrNil() *time.Time
}
