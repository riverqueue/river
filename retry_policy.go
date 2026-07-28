package river

import (
	"time"

	"github.com/riverqueue/river/internal/retrypolicy"
	"github.com/riverqueue/river/rivertype"
)

// ClientRetryPolicy is an interface that can be implemented to provide a retry
// policy for how River deals with failed jobs at the client level (when a
// worker does not define an override for `NextRetry`). Jobs are scheduled to be
// retried in the future up until they've reached the job's max attempts, at
// which pointed they're set as discarded.
//
// The ClientRetryPolicy does not have access to generics and operates on the
// raw JobRow struct with encoded args.
type ClientRetryPolicy interface {
	// NextRetry calculates when the next retry for a failed job should take place
	// given when it was last attempted and its number of attempts, or any other
	// of the job's properties a user-configured retry policy might want to
	// consider.
	NextRetry(job *rivertype.JobRow) time.Time
}

// DefaultClientRetryPolicy is River's default retry policy.
type DefaultClientRetryPolicy struct {
	timeNowFunc func() time.Time
}

// NextRetry gets the next retry given for the given job, accounting for when it
// was last attempted and what attempt number that was. Reschedules using a
// basic exponential backoff of `ATTEMPT^4`, so after the first failure a new
// try will be scheduled in 1 seconds, 16 seconds after the second, 1 minute and
// 21 seconds after the third, etc.
//
// Snoozes do not count as attempts and do not influence retry behavior.
// Earlier versions of River would allow the attempt to increment each time a
// job was snoozed. Although this has been changed and snoozes now decrement the
// attempt count, we can maintain the same retry schedule even for pre-existing
// jobs by using the number of errors instead of the attempt count. This ensures
// consistent behavior across River versions.
//
// At degenerately high retry counts (>= 310) the policy starts adding the
// equivalent of the maximum of time.Duration to each retry, about 292 years.
// The schedule is no longer exponential past this point.
func (p *DefaultClientRetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	return retrypolicy.NextRetryAt(p.timeNowUTC(), job)
}

func (p *DefaultClientRetryPolicy) timeNowUTC() time.Time {
	if p.timeNowFunc != nil {
		return p.timeNowFunc()
	}

	return time.Now().UTC()
}
