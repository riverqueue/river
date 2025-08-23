package river

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/riverqueue/river/rivershared/util/timeutil"
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

// River's default retry policy.
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
	// In modern versions of River `len(job.Errors)` is the same number as
	// `attempt`.  However, in older version snoozing a job wouldn't restore its
	// attempt count to the pre-fetch value, and that would lead to incorrect
	// retry durations when jobs are first snoozed, then retried. To avoid this
	// and keep backward compatibility, the number of errors are used instead.
	errorCount := len(job.Errors) + 1

	return p.timeNowUTC().Add(timeutil.SecondsAsDuration(p.retrySeconds(errorCount)))
}

func (p *DefaultClientRetryPolicy) timeNowUTC() time.Time {
	if p.timeNowFunc != nil {
		return p.timeNowFunc()
	}

	return time.Now().UTC()
}

// The maximum value of a duration before it overflows. About 292 years.
const maxDuration time.Duration = 1<<63 - 1

// Same as the above, but changed to a float represented in seconds.
var maxDurationSeconds = maxDuration.Seconds() //nolint:gochecknoglobals

// Gets a number of retry seconds for the given attempt, random jitter included.
func (p *DefaultClientRetryPolicy) retrySeconds(attempt int) float64 {
	retrySeconds := p.retrySecondsWithoutJitter(attempt)

	// After hitting maximum retry durations jitter is no longer applied because
	// it might overflow time.Duration. That's okay though because so much
	// jitter will already have been applied up to this point (jitter measured
	// in decades) that jobs will no longer run anywhere near contemporaneously
	// unless there's been considerable manual intervention.
	if retrySeconds == maxDurationSeconds {
		return maxDurationSeconds
	}

	// Jitter number of seconds +/- 10%.
	retrySeconds += retrySeconds * (rand.Float64()*0.2 - 0.1)

	// Cap retrySeconds once more in case adding random jitter pushed it over
	// maxDurationSeconds. (This should never realistically happen, but protect
	// against it just in case.)
	return min(retrySeconds, maxDurationSeconds)
}

// Gets a base number of retry seconds for the given attempt, jitter excluded.
// If the number of seconds returned would overflow time.Duration if it were to
// be made one, returns the maximum number of seconds that can fit in a
// time.Duration instead, approximately 292 years.
func (p *DefaultClientRetryPolicy) retrySecondsWithoutJitter(attempt int) float64 {
	retrySeconds := math.Pow(float64(attempt), 4)
	return min(retrySeconds, maxDurationSeconds)
}
