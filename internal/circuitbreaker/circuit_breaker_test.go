package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()

	const (
		limit  = 5
		window = 1 * time.Minute // use a wide window to avoid timing problems
	)

	setup := func() *CircuitBreaker {
		breaker := NewCircuitBreaker(&CircuitBreakerOptions{
			Limit:  limit,
			Window: window,
		})
		return breaker
	}

	stubTime := func(breaker *CircuitBreaker) *riversharedtest.TimeStub {
		timeStub := &riversharedtest.TimeStub{}
		breaker.timeGenerator = timeStub
		return timeStub
	}

	t.Run("Configured", func(t *testing.T) {
		t.Parallel()

		breaker := setup()

		require.Equal(t, limit, breaker.Limit())
	})

	t.Run("BreakerOpens", func(t *testing.T) {
		t.Parallel()

		breaker := setup()

		for range limit - 1 {
			require.False(t, breaker.Trip())
			require.False(t, breaker.Open())
		}

		require.True(t, breaker.Trip())
		require.True(t, breaker.Open())

		require.True(t, breaker.Trip())
		require.True(t, breaker.Open())
	})

	t.Run("WindowEdge", func(t *testing.T) {
		t.Parallel()

		var (
			breaker  = setup()
			timeStub = stubTime(breaker)
			now      = time.Now()
		)

		timeStub.StubNowUTC(now)
		for range limit - 2 {
			require.False(t, breaker.Trip())
		}

		timeStub.StubNowUTC(now.Add(window - 1*time.Second))
		require.False(t, breaker.Trip())

		timeStub.StubNowUTC(now.Add(window))
		require.True(t, breaker.Trip())
	})

	t.Run("TripsFallOutOfWindow", func(t *testing.T) {
		t.Parallel()

		var (
			breaker  = setup()
			timeStub = stubTime(breaker)
			now      = time.Now()
		)

		timeStub.StubNowUTC(now)
		require.False(t, breaker.Trip())

		timeStub.StubNowUTC(now.Add(window - 1*time.Second))
		for range limit - 2 {
			require.False(t, breaker.Trip())
		}

		// Does *not* trip because the first trip is reaped as it's fallen out
		// of the window.
		timeStub.StubNowUTC(now.Add(window + 1*time.Second))
		require.False(t, breaker.Trip())
	})

	t.Run("MultipleFallOut", func(t *testing.T) {
		t.Parallel()

		var (
			breaker  = setup()
			timeStub = stubTime(breaker)
			now      = time.Now()
		)

		timeStub.StubNowUTC(now)
		for range limit - 1 {
			require.False(t, breaker.Trip())
		}

		// Similar to the above, but multiple trips fall out of the window at once.
		timeStub.StubNowUTC(now.Add(window + 1*time.Second))
		require.False(t, breaker.Trip())
	})

	t.Run("ResetIfNotOpen", func(t *testing.T) {
		t.Parallel()

		var (
			breaker  = setup()
			timeStub = stubTime(breaker)
			now      = time.Now()
		)

		timeStub.StubNowUTC(now)
		for range limit - 1 {
			require.False(t, breaker.Trip())
		}

		require.True(t, breaker.ResetIfNotOpen())

		// We're allowed to go right up the limit again because the call to
		// ResetIfNotOpen reset everything.
		timeStub.StubNowUTC(now)
		for range limit - 1 {
			require.False(t, breaker.Trip())
		}

		// One more trip breaks the circuit.
		require.True(t, breaker.Trip())

		// ResetIfNotOpen has no effect anymore because the circuit is open.
		require.False(t, breaker.ResetIfNotOpen())

		// Circuit still open.
		require.True(t, breaker.Trip())
	})
}
