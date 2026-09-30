package notifylimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestLimiter(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *Limiter {
		t.Helper()

		return NewLimiter(riversharedtest.BaseServiceArchetype(t), 10*time.Millisecond)
	}

	t.Run("OnlySendsOncePerWaitDuration", func(t *testing.T) {
		t.Parallel()

		limiter := setup(t)
		now := time.Now()
		limiter.Time.StubNow(now)

		require.True(t, limiter.ShouldTrigger("a"))
		for range 10 {
			require.False(t, limiter.ShouldTrigger("a"))
		}
		// Move the time forward, by just less than waitDuration:
		limiter.Time.StubNow(now.Add(9 * time.Millisecond))
		require.False(t, limiter.ShouldTrigger("a"))

		require.True(t, limiter.ShouldTrigger("b")) // First time being triggered on "b"

		// Move the time forward to just past the waitDuration:
		limiter.Time.StubNow(now.Add(11 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		for range 10 {
			require.False(t, limiter.ShouldTrigger("a"))
		}

		require.False(t, limiter.ShouldTrigger("b")) // has only been 2ms since last trigger of "b"

		// Move forward by another waitDuration (plus padding):
		limiter.Time.StubNow(now.Add(22 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		require.True(t, limiter.ShouldTrigger("b"))
		require.False(t, limiter.ShouldTrigger("b"))
	})

	t.Run("ConcurrentAccessStressTest", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			limiter := setup(t)

			counters := make(map[string]*atomic.Int64)
			for _, topic := range []string{"a", "b", "c"} {
				counters[topic] = &atomic.Int64{}
			}

			// Bounded rounds exercise concurrent access without busy loops that
			// would prevent the synctest clock from advancing.
			signalConcurrentlyFunc := func() {
				var wg sync.WaitGroup
				for topic := range counters {
					for range 10 {
						wg.Go(func() {
							for range 100 {
								if limiter.ShouldTrigger(topic) {
									counters[topic].Add(1)
								}
							}
						})
					}
				}
				wg.Wait()
			}

			signalConcurrentlyFunc()
			for _, counter := range counters {
				require.Equal(t, int64(1), counter.Load())
			}

			time.Sleep(11 * time.Millisecond)
			signalConcurrentlyFunc()
			for _, counter := range counters {
				require.Equal(t, int64(2), counter.Load())
			}
		})
	})
}
