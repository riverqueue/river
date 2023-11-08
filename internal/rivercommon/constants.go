package rivercommon

// These constants are made available in rivercommon so that they're accessible
// by internal packages, but the top-level river package re-exports them, and
// all user code must use that set instead.
const (
	DefaultMaxAttempts = 25
	DefaultPriority    = 1
	DefaultQueue       = "default"
)
