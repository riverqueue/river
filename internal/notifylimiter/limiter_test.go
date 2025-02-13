package notifylimiter

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestLimiter(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func() (*Limiter, *testBundle) {
		bundle := &testBundle{}

		archetype := riversharedtest.BaseServiceArchetype(t)
		limiter := NewLimiter(archetype, 10*time.Millisecond)

		return limiter, bundle
	}

	t.Run("OnlySendsOncePerWaitDuration", func(t *testing.T) {
		t.Parallel()

		limiter, _ := setup()
		now := time.Now()
		limiter.Time.StubNowUTC(now)

		require.True(t, limiter.ShouldTrigger("a"))
		for range 10 {
			require.False(t, limiter.ShouldTrigger("a"))
		}
		// Move the time forward, by just less than waitDuration:
		limiter.Time.StubNowUTC(now.Add(9 * time.Millisecond))
		require.False(t, limiter.ShouldTrigger("a"))

		require.True(t, limiter.ShouldTrigger("b")) // First time being triggered on "b"

		// Move the time forward to just past the waitDuration:
		limiter.Time.StubNowUTC(now.Add(11 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		for range 10 {
			require.False(t, limiter.ShouldTrigger("a"))
		}

		require.False(t, limiter.ShouldTrigger("b")) // has only been 2ms since last trigger of "b"

		// Move forward by another waitDuration (plus padding):
		limiter.Time.StubNowUTC(now.Add(22 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		require.True(t, limiter.ShouldTrigger("b"))
		require.False(t, limiter.ShouldTrigger("b"))
	})

	t.Run("ConcurrentAccessStressTest", func(t *testing.T) {
		t.Parallel()

		doneCh := make(chan struct{})
		t.Cleanup(func() { close(doneCh) })

		limiter, _ := setup()
		now := time.Now()
		limiter.Time.StubNowUTC(now)

		counters := make(map[string]*atomic.Int64)
		for _, topic := range []string{"a", "b", "c"} {
			counters[topic] = &atomic.Int64{}
		}

		signalContinuously := func(topic string) {
			for {
				select {
				case <-doneCh:
					return
				default:
					shouldTrigger := limiter.ShouldTrigger(topic)
					if shouldTrigger {
						counters[topic].Add(1)
					}
				}
			}
		}
		go signalContinuously("a")
		go signalContinuously("b")
		go signalContinuously("c")

		// Duration doesn't really matter here, just need time for these all to fire
		// a bit:
		<-time.After(100 * time.Millisecond)

		require.Equal(t, int64(1), counters["a"].Load())
		require.Equal(t, int64(1), counters["b"].Load())
		require.Equal(t, int64(1), counters["c"].Load())

		limiter.Time.StubNowUTC(now.Add(11 * time.Millisecond))

		<-time.After(100 * time.Millisecond)

		require.Equal(t, int64(2), counters["a"].Load())
		require.Equal(t, int64(2), counters["b"].Load())
		require.Equal(t, int64(2), counters["c"].Load())
	})
}
