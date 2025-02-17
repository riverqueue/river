package rivertype

import (
	"errors"
	"fmt"
	"time"
)

var ErrJobCancelledRemotely = JobCancel(errors.New("job cancelled remotely"))

// JobCancel wraps err and can be returned from a Worker's Work method to cancel
// the job at the end of execution. Regardless of whether or not the job has any
// remaining attempts, this will ensure the job does not execute again.
//
// This function primarily exists for cross module compatibility. Users should
// use river.JobCancel instead.
func JobCancel(err error) error {
	return &JobCancelError{err: err}
}

// JobCancelError is the error type returned by JobCancel. It should not be
// initialized directly, but is returned from the [JobCancel] function and can
// be used for test assertions.
type JobCancelError struct {
	err error
}

func (e *JobCancelError) Error() string {
	if e.err == nil {
		return "JobCancelError: <nil>"
	}
	// should not ever be called, but add a prefix just in case:
	return "JobCancelError: " + e.err.Error()
}

func (e *JobCancelError) Is(target error) bool {
	_, ok := target.(*JobCancelError)
	return ok
}

func (e *JobCancelError) Unwrap() error { return e.err }

// JobSnoozeError is the error type returned by JobSnooze. It should not be
// initialized directly, but is returned from the [JobSnooze] function and can
// be used for test assertions.
type JobSnoozeError struct {
	Duration time.Duration
}

func (e *JobSnoozeError) Error() string {
	// should not ever be called, but add a prefix just in case:
	return fmt.Sprintf("JobSnoozeError: %s", e.Duration)
}

func (e *JobSnoozeError) Is(target error) bool {
	_, ok := target.(*JobSnoozeError)
	return ok
}

// UnknownJobKindError is returned when a Client fetches and attempts to
// work a job that has not been registered on the Client's Workers bundle (using
// AddWorker).
type UnknownJobKindError struct {
	// Kind is the string that was returned by the JobArgs Kind method.
	Kind string
}

// Error returns the error string.
func (e *UnknownJobKindError) Error() string {
	return "job kind is not registered in the client's Workers bundle: " + e.Kind
}

// Is implements the interface used by errors.Is to determine if errors are
// equivalent. It returns true for any other UnknownJobKindError without
// regard to the Kind string so it is possible to detect this type of error
// with:
//
//	errors.Is(err, &UnknownJobKindError{})
func (e *UnknownJobKindError) Is(target error) bool {
	_, ok := target.(*UnknownJobKindError)
	return ok
}
