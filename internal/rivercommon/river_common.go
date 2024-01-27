package rivercommon

import "errors"

// These constants are made available in rivercommon so that they're accessible
// by internal packages, but the top-level river package re-exports them, and
// all user code must use that set instead.
const (
	MaxAttemptsDefault = 25
	PriorityDefault    = 1
	QueueDefault       = "default"
)

// ErrShutdown is a special error injected by the client into its fetch and work
// CancelCauseFuncs when it's stopping. It may be used by components for such
// cases like avoiding logging an error during a normal shutdown procedure. This
// is internal for the time being, but we could also consider exposing it.
var ErrShutdown = errors.New("shutdown initiated")
