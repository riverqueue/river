package rivertype

import "time"

// TimeGenerator generates current time values for in-process timing math and
// optional stubbed wall-clock timestamps in tests. In test environments it's
// implemented by riversharedtest.TimeStub which lets the current time be
// stubbed. Otherwise, it's implemented as UnStubbableTimeGenerator which
// doesn't allow stubbing.
type TimeGenerator interface {
	// Now returns the current time. This may be a stubbed time if the time has
	// been actively stubbed in a test.
	//
	// Production implementations should preserve Go's monotonic clock reading
	// from time.Now for in-process duration and deadline math. Do not normalize
	// through `t.UTC()` here: per the time package's monotonic clock semantics,
	// changing location strips the monotonic reading. Normalize at database or
	// serialization boundaries instead.
	Now() time.Time

	// NowOrNil returns the currently stubbed time if the current time is
	// stubbed, and nil otherwise.
	//
	// This is mainly for database-facing test paths that want to inject a
	// deterministic wall-clock timestamp when time is stubbed, but to fall back
	// to a database-side time default in production.
	NowOrNil() *time.Time
}
