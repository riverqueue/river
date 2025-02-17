package retrypolicytest

import (
	"fmt"
	"math"
	"time"

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
type RetryPolicyNoJitter struct{}

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
