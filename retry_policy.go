package river

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/util/randutil"
	"github.com/riverqueue/river/internal/util/timeutil"
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
	rand        *rand.Rand
	randMu      sync.RWMutex
	timeNowFunc func() time.Time
}

// NextRetry gets the next retry given for the given job, accounting for when it
// was last attempted and what attempt number that was. Reschedules using a
// basic exponential backoff of `ATTEMPT^4`, so after the first failure a new
// try will be scheduled in 1 seconds, 16 seconds after the second, 1 minute and
// 21 seconds after the third, etc.
//
// In order to avoid penalizing jobs that are snoozed, the number of errors is
// used instead of the attempt count. This means that snoozing a job (even
// repeatedly) will not lead to a future error having a longer than expected
// retry delay.
func (p *DefaultClientRetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	// For the purposes of calculating the backoff, we can look solely at the
	// number of errors. If we were to use the raw attempt count, this would be
	// incemented and influenced by snoozes. However the use case for snoozing is
	// to try again later *without* counting as an error.
	//
	// Note that we explicitly add 1 here, because the error hasn't been appended
	// yet at the time this is called (that happens in the completer).
	errorCount := len(job.Errors) + 1
	return p.timeNowUTC().Add(timeutil.SecondsAsDuration(p.retrySeconds(errorCount)))
}

func (p *DefaultClientRetryPolicy) timeNowUTC() time.Time {
	if p.timeNowFunc != nil {
		return p.timeNowFunc()
	}

	return time.Now().UTC()
}

// Lazily marshals a random source. Written this way instead of using a
// constructor so that DefaultRetryPolicy can easily be embedded in other
// structs without special initialization.
func (p *DefaultClientRetryPolicy) lazilyMarshalRand() {
	{
		p.randMu.RLock()
		pRand := p.rand
		p.randMu.RUnlock()

		if pRand != nil {
			return
		}
	}

	p.randMu.Lock()
	defer p.randMu.Unlock()

	// One additional nil check in case multiple goroutines raced to this point.
	if p.rand != nil {
		return
	}

	p.rand = randutil.NewCryptoSeededConcurrentSafeRand()
}

// Gets a number of retry seconds for the given attempt, random jitter included.
func (p *DefaultClientRetryPolicy) retrySeconds(attempt int) float64 {
	p.lazilyMarshalRand()

	retrySeconds := p.retrySecondsWithoutJitter(attempt)

	// Jitter number of seconds +/- 10%.
	retrySeconds += retrySeconds * (p.rand.Float64()*0.2 - 0.1)

	return retrySeconds
}

// Gets a base number of retry seconds for the given attempt, jitter excluded.
func (p *DefaultClientRetryPolicy) retrySecondsWithoutJitter(attempt int) float64 {
	return math.Pow(float64(attempt), 4)
}
