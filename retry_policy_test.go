package river

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

// Just proves that DefaultRetryPolicy implements the RetryPolicy interface.
var _ ClientRetryPolicy = &DefaultClientRetryPolicy{}

func TestDefaultClientRetryPolicy_NextRetry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	retryPolicy := &DefaultClientRetryPolicy{}

	for attempt := 1; attempt < 10; attempt++ {
		retrySecondsWithoutJitter := retryPolicy.retrySecondsWithoutJitter(attempt)
		allowedDelta := timeutil.SecondsAsDuration(retrySecondsWithoutJitter * 0.2)

		nextRetryAt := retryPolicy.NextRetry(&rivertype.JobRow{
			Attempt:     attempt,
			AttemptedAt: &now,
			Errors:      make([]rivertype.AttemptError, attempt-1),
		})
		require.WithinDuration(t, now.Add(timeutil.SecondsAsDuration(retrySecondsWithoutJitter)), nextRetryAt, allowedDelta)
	}
}

func TestDefaultRetryPolicy_retrySeconds(t *testing.T) {
	t.Parallel()

	retryPolicy := &DefaultClientRetryPolicy{}

	for attempt := 1; attempt < rivercommon.MaxAttemptsDefault; attempt++ {
		retrySecondsWithoutJitter := retryPolicy.retrySecondsWithoutJitter(attempt)

		// Jitter is number of seconds +/- 10%.
		retrySecondsMin := timeutil.SecondsAsDuration(retrySecondsWithoutJitter - retrySecondsWithoutJitter*0.1)
		retrySecondsMax := timeutil.SecondsAsDuration(retrySecondsWithoutJitter + retrySecondsWithoutJitter*0.1)

		// Run a number of times just to make sure we never generate a number
		// outside of the expected bounds.
		for i := 0; i < 10; i++ {
			retryDuration := timeutil.SecondsAsDuration(retryPolicy.retrySeconds(attempt))

			require.GreaterOrEqual(t, retryDuration, retrySecondsMin)
			require.Less(t, retryDuration, retrySecondsMax)
		}
	}
}

// This is mostly to give a feeling for what the retry schedule looks like with
// real values.
func TestDefaultRetryPolicy_retrySecondsWithoutJitter(t *testing.T) {
	t.Parallel()

	retryPolicy := &DefaultClientRetryPolicy{}

	day := 24 * time.Hour

	testCases := []struct {
		attempt       int
		expectedRetry time.Duration
	}{
		{attempt: 1, expectedRetry: 1 * time.Second},
		{attempt: 2, expectedRetry: 16 * time.Second},
		{attempt: 3, expectedRetry: 1*time.Minute + 21*time.Second},
		{attempt: 4, expectedRetry: 4*time.Minute + 16*time.Second},
		{attempt: 5, expectedRetry: 10*time.Minute + 25*time.Second},
		{attempt: 6, expectedRetry: 21*time.Minute + 36*time.Second},
		{attempt: 7, expectedRetry: 40*time.Minute + 1*time.Second},
		{attempt: 8, expectedRetry: 1*time.Hour + 8*time.Minute + 16*time.Second},
		{attempt: 9, expectedRetry: 1*time.Hour + 49*time.Minute + 21*time.Second},
		{attempt: 10, expectedRetry: 2*time.Hour + 46*time.Minute + 40*time.Second},
		{attempt: 11, expectedRetry: 4*time.Hour + 4*time.Minute + 1*time.Second},
		{attempt: 12, expectedRetry: 5*time.Hour + 45*time.Minute + 36*time.Second},
		{attempt: 13, expectedRetry: 7*time.Hour + 56*time.Minute + 1*time.Second},
		{attempt: 14, expectedRetry: 10*time.Hour + 40*time.Minute + 16*time.Second},
		{attempt: 15, expectedRetry: 14*time.Hour + 3*time.Minute + 45*time.Second},
		{attempt: 16, expectedRetry: 18*time.Hour + 12*time.Minute + 16*time.Second},
		{attempt: 17, expectedRetry: 23*time.Hour + 12*time.Minute + 1*time.Second},
		{attempt: 18, expectedRetry: 1*day + 5*time.Hour + 9*time.Minute + 36*time.Second},
		{attempt: 19, expectedRetry: 1*day + 12*time.Hour + 12*time.Minute + 1*time.Second},
		{attempt: 20, expectedRetry: 1*day + 20*time.Hour + 26*time.Minute + 40*time.Second},
		{attempt: 21, expectedRetry: 2*day + 6*time.Hour + 1*time.Minute + 21*time.Second},
		{attempt: 22, expectedRetry: 2*day + 17*time.Hour + 4*time.Minute + 16*time.Second},
		{attempt: 23, expectedRetry: 3*day + 5*time.Hour + 44*time.Minute + 1*time.Second},
		{attempt: 24, expectedRetry: 3*day + 20*time.Hour + 9*time.Minute + 36*time.Second},
	}
	for _, tt := range testCases {
		tt := tt
		t.Run(fmt.Sprintf("Attempt_%02d", tt.attempt), func(t *testing.T) {
			t.Parallel()

			require.Equal(t,
				tt.expectedRetry,
				time.Duration(retryPolicy.retrySecondsWithoutJitter(tt.attempt))*time.Second)
		})
	}
}

func TestDefaultRetryPolicy_stress(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	retryPolicy := &DefaultClientRetryPolicy{}

	// Hit the source with a bunch of goroutines to help suss out any problems
	// with concurrent safety (when combined with `-race`).
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				_ = retryPolicy.retrySeconds(7)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
