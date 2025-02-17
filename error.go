package river

import (
	"time"

	"github.com/riverqueue/river/rivertype"
)

// ErrJobCancelledRemotely is a sentinel error indicating that the job was cancelled remotely.
var ErrJobCancelledRemotely = rivertype.ErrJobCancelledRemotely

// JobCancelError is the error type returned by JobCancel. It should not be
// initialized directly, but is returned from the [JobCancel] function and can
// be used for test assertions.
type JobCancelError = rivertype.JobCancelError

// JobCancel wraps err and can be returned from a Worker's Work method to cancel
// the job at the end of execution. Regardless of whether or not the job has any
// remaining attempts, this will ensure the job does not execute again.
func JobCancel(err error) error {
	return rivertype.JobCancel(err)
}

// JobSnoozeError is the error type returned by JobSnooze. It should not be
// initialized directly, but is returned from the [JobSnooze] function and can
// be used for test assertions.
type JobSnoozeError = rivertype.JobSnoozeError

// JobSnooze can be returned from a Worker's Work method to cause the job to be
// tried again after the specified duration. This also has the effect of
// incrementing the job's MaxAttempts by 1, meaning that jobs can be repeatedly
// snoozed without ever being discarded.
//
// Panics if duration is < 0.
func JobSnooze(duration time.Duration) error {
	return &rivertype.JobSnoozeError{Duration: duration}
}

// UnknownJobKindError is returned when a Client fetches and attempts to
// work a job that has not been registered on the Client's Workers bundle (using AddWorker).
type UnknownJobKindError = rivertype.UnknownJobKindError
