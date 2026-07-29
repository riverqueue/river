// Package retrypolicy contains River's internal retry policy implementations.
package retrypolicy

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

// Default is River's clock-aware default retry policy for internal use.
type Default struct {
	timeGenerator rivertype.TimeGenerator
}

// NewDefault returns a default retry policy that derives retries from the
// given time generator.
func NewDefault(timeGenerator rivertype.TimeGenerator) *Default {
	return &Default{timeGenerator: timeGenerator}
}

// NextRetry calculates when the next retry for a failed job should take place.
func (p *Default) NextRetry(job *rivertype.JobRow) time.Time {
	return NextRetryAt(p.timeGenerator.Now().UTC(), job)
}

// NextRetryAt calculates when the next retry for a failed job should take
// place relative to now.
func NextRetryAt(now time.Time, job *rivertype.JobRow) time.Time {
	// In modern versions of River `len(job.Errors)` is the same number as
	// `attempt`. However, in older versions snoozing a job wouldn't restore its
	// attempt count to the pre-fetch value, and that would lead to incorrect
	// retry durations when jobs are first snoozed, then retried. To avoid this
	// and keep backward compatibility, the number of errors are used instead.
	errorCount := len(job.Errors) + 1

	return now.Add(timeutil.SecondsAsDuration(retrySeconds(errorCount)))
}

// The maximum value of a duration before it overflows. About 292 years.
const maxDuration time.Duration = 1<<63 - 1

// Same as the above, but changed to a float represented in seconds.
var maxDurationSeconds = maxDuration.Seconds() //nolint:gochecknoglobals

// Gets a number of retry seconds for the given attempt, random jitter included.
func retrySeconds(attempt int) float64 {
	retrySeconds := retrySecondsWithoutJitter(attempt)

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
func retrySecondsWithoutJitter(attempt int) float64 {
	retrySeconds := math.Pow(float64(attempt), 4)
	return min(retrySeconds, maxDurationSeconds)
}
