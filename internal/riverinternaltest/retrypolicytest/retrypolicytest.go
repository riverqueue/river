package retrypolicytest

import (
	"fmt"
	"math"
	"time"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

// RetryPolicyCustom is a retry policy demonstrating trivial customization.
type RetryPolicyCustom struct{}

func (p *RetryPolicyCustom) NextRetry(job *rivertype.JobRow) time.Time {
	var backoffDuration time.Duration
	switch job.Attempt {
	case 1:
		backoffDuration = 10 * time.Second
	case 2:
		backoffDuration = 20 * time.Second
	case 3:
		backoffDuration = 30 * time.Second
	default:
		panic(fmt.Sprintf("next retry should not have been called for attempt %d", job.Attempt))
	}

	return job.AttemptedAt.Add(backoffDuration)
}

// RetryPolicyInvalid is a retry policy that returns invalid timestamps.
type RetryPolicyInvalid struct{}

func (p *RetryPolicyInvalid) NextRetry(job *rivertype.JobRow) time.Time { return time.Time{} }

// The maximum value of a duration before it overflows. About 292 years.
const maxDuration time.Duration = 1<<63 - 1

// Same as the above, but changed to a float represented in seconds.
var maxDurationSeconds = maxDuration.Seconds() //nolint:gochecknoglobals

// RetryPolicyNoJitter is identical to default retry policy except that it
// leaves off the jitter to make checking against it more convenient.
type RetryPolicyNoJitter struct {
	baseservice.BaseService
}

func (p *RetryPolicyNoJitter) NextRetry(job *rivertype.JobRow) time.Time {
	return job.AttemptedAt.Add(timeutil.SecondsAsDuration(p.retrySecondsWithoutJitter(job.Attempt)))
}

// Gets a base number of retry seconds for the given attempt, jitter excluded.
// If the number of seconds returned would overflow time.Duration if it were to
// be made one, returns the maximum number of seconds that can fit in a
// time.Duration instead, approximately 292 years.
func (p *RetryPolicyNoJitter) retrySecondsWithoutJitter(attempt int) float64 {
	retrySeconds := math.Pow(float64(attempt), 4)
	return min(retrySeconds, maxDurationSeconds)
}

// Choose a number here that's larger than the job rescuer's rescue interval of
// one hour so that can easily use it to test the job rescuer (in addition to
// other things).
const retryPolicySlowInterval = 2 * time.Hour

// RetryPolicySlow is a retry policy that has a very slow retry interval. This
// is used in tests that check retries to make sure that in slower environments
// (like GitHub Actions), jobs aren't accidentally set back to available or
// running before a paused test case can check to make sure it's in a retryable
// state.
type RetryPolicySlow struct{}

// Interval is the slow retry interval exposed for use in test case assertions.
// Unlike the standard retry policy, RetryPolicySlow's interval is constant
// regardless of which attempt number the job was on.
func (p *RetryPolicySlow) Interval() time.Duration {
	return retryPolicySlowInterval
}

func (p *RetryPolicySlow) NextRetry(job *rivertype.JobRow) time.Time {
	return job.AttemptedAt.Add(retryPolicySlowInterval)
}
