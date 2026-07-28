package retrypolicy

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

func TestDefault_NextRetry(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		now         time.Time
		retryPolicy *Default
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		now := time.Now().UTC()
		timeStub := &riversharedtest.TimeStub{}
		timeStub.StubNow(now)

		return &testBundle{
			now:         now,
			retryPolicy: NewDefault(timeStub),
		}
	}

	t.Run("MaxRetryDuration", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		maxRetryDuration := timeutil.SecondsAsDuration(maxDurationSeconds)

		// First time the maximum will be hit.
		require.Equal(t,
			bundle.now.Add(maxRetryDuration),
			bundle.retryPolicy.NextRetry(&rivertype.JobRow{
				Attempt:     310,
				AttemptedAt: &bundle.now,
				Errors:      make([]rivertype.AttemptError, 310-1),
			}),
		)

		// And will be hit on all subsequent attempts as well.
		require.Equal(t,
			bundle.now.Add(maxRetryDuration),
			bundle.retryPolicy.NextRetry(&rivertype.JobRow{
				Attempt:     1_000,
				AttemptedAt: &bundle.now,
				Errors:      make([]rivertype.AttemptError, 1_000-1),
			}),
		)
	})

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		for attempt := 1; attempt < 10; attempt++ {
			retrySecondsWithoutJitter := retrySecondsWithoutJitter(attempt)
			allowedDelta := timeutil.SecondsAsDuration(retrySecondsWithoutJitter * 0.2)

			nextRetryAt := bundle.retryPolicy.NextRetry(&rivertype.JobRow{
				Attempt:     attempt,
				AttemptedAt: &bundle.now,
				Errors:      make([]rivertype.AttemptError, attempt-1),
			})
			require.WithinDuration(t, bundle.now.Add(timeutil.SecondsAsDuration(retrySecondsWithoutJitter)), nextRetryAt, allowedDelta)
		}
	})
}

func TestRetrySeconds(t *testing.T) {
	t.Parallel()

	for attempt := 1; attempt < rivercommon.MaxAttemptsDefault; attempt++ {
		retrySecondsWithoutJitter := retrySecondsWithoutJitter(attempt)

		// Jitter is number of seconds +/- 10%.
		retrySecondsMin := timeutil.SecondsAsDuration(retrySecondsWithoutJitter - retrySecondsWithoutJitter*0.1)
		retrySecondsMax := timeutil.SecondsAsDuration(retrySecondsWithoutJitter + retrySecondsWithoutJitter*0.1)

		// Run a number of times just to make sure we never generate a number
		// outside of the expected bounds.
		for range 10 {
			retryDuration := timeutil.SecondsAsDuration(retrySeconds(attempt))

			require.GreaterOrEqual(t, retryDuration, retrySecondsMin)
			require.Less(t, retryDuration, retrySecondsMax)
		}
	}
}

// This is mostly to give a feeling for what the retry schedule looks like with
// real values.
func TestRetrySecondsWithoutJitter(t *testing.T) {
	t.Parallel()

	t.Run("MaxDurationSeconds", func(t *testing.T) {
		t.Parallel()

		require.NotEqual(t, time.Duration(maxDurationSeconds)*time.Second, time.Duration(retrySecondsWithoutJitter(309))*time.Second)

		// Attempt number 310 hits the ceiling, and we'll always hit it from thence on.
		require.Equal(t, time.Duration(maxDuration.Seconds())*time.Second, time.Duration(retrySecondsWithoutJitter(310))*time.Second)
		require.Equal(t, time.Duration(maxDuration.Seconds())*time.Second, time.Duration(retrySecondsWithoutJitter(311))*time.Second)
		require.Equal(t, time.Duration(maxDuration.Seconds())*time.Second, time.Duration(retrySecondsWithoutJitter(1_000))*time.Second)
		require.Equal(t, time.Duration(maxDuration.Seconds())*time.Second, time.Duration(retrySecondsWithoutJitter(1_000_000))*time.Second)
	})

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

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
			t.Run(fmt.Sprintf("Attempt_%02d", tt.attempt), func(t *testing.T) {
				t.Parallel()

				require.Equal(t,
					tt.expectedRetry,
					time.Duration(retrySecondsWithoutJitter(tt.attempt))*time.Second)
			})
		}
	})
}

func TestRetrySeconds_stress(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	// Hit the source with a bunch of goroutines to help suss out any problems
	// with concurrent safety (when combined with `-race`).
	for range 10 {
		wg.Go(func() {
			for range 100 {
				_ = retrySeconds(7)
			}
		})
	}

	wg.Wait()
}
