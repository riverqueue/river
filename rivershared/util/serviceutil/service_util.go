package serviceutil

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/riverqueue/river/rivershared/util/timeutil"
)

// CancellableSleep sleeps for the given duration, but returns early if context
// has been cancelled.
func CancellableSleep(ctx context.Context, sleepDuration time.Duration) {
	timer := time.NewTimer(sleepDuration)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
	}
}

// CancellableSleep sleeps for the given duration, but returns early if context
// has been cancelled.
//
// This variant returns a channel that should be waited on and which will be
// closed when either the sleep or context is done.
func CancellableSleepC(ctx context.Context, sleepDuration time.Duration) <-chan struct{} {
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)
		CancellableSleep(ctx, sleepDuration)
	}()

	return doneChan
}

// MaxAttemptsBeforeResetDefault is the number of attempts during exponential
// backoff after which attempts is reset so that sleep durations aren't flung
// into a ridiculously distant future. This constant is typically injected into
// the CancellableSleepExponentialBackoff function. It could technically take
// another value instead, but shouldn't unless there's a good reason to do so.
const MaxAttemptsBeforeResetDefault = 10

// ExponentialBackoff returns a duration for a reasonable exponential backoff
// interval for a service based on the given attempt number, which can then be
// fed into CancellableSleep to perform the sleep. Uses a 2**N second algorithm,
// +/- 10% random jitter. Sleep is cancelled if the given context is cancelled.
//
// Attempt should start at one for the first backoff/failure.
func ExponentialBackoff(attempt, maxAttemptsBeforeReset int) time.Duration {
	retrySeconds := exponentialBackoffSecondsWithoutJitter(attempt, maxAttemptsBeforeReset)

	// Jitter number of seconds +/- 10%.
	retrySeconds += retrySeconds * (rand.Float64()*0.2 - 0.1)

	return timeutil.SecondsAsDuration(retrySeconds)
}

func exponentialBackoffSecondsWithoutJitter(attempt, maxAttemptsBeforeReset int) float64 {
	// It's easier for callers and more intuitive if attempt starts at one, but
	// subtract one before sending it the exponent so we start at only one
	// second of sleep instead of two.
	attempt--

	// We use a different exponential backoff algorithm here compared to the
	// default retry policy (2**N versus N**4) because it results in more
	// retries sooner. When it comes to exponential backoffs in services we
	// never want to sleep for hours/days, unlike with failed jobs.
	return math.Pow(2, float64(attempt%maxAttemptsBeforeReset))
}
