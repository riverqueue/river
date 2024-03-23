package notifylimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/stretchr/testify/require"
)

type mockableTime struct {
	mu  sync.Mutex // protects now
	now *time.Time
}

func (m *mockableTime) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.now != nil {
		return *m.now
	}
	return time.Now()
}

func (m *mockableTime) SetNow(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.now = &now
}

func TestLimiter(t *testing.T) {
	type testBundle struct {
		mockTime *mockableTime
	}

	setup := func() (*Limiter, *testBundle) {
		bundle := &testBundle{
			mockTime: &mockableTime{},
		}

		archetype := riverinternaltest.BaseServiceArchetype(t)
		limiter := NewLimiter(archetype, 10*time.Millisecond)
		limiter.TimeNowUTC = bundle.mockTime.Now

		return limiter, bundle
	}

	t.Run("OnlySendsOncePerWaitDuration", func(t *testing.T) {
		limiter, bundle := setup()
		now := time.Now()
		bundle.mockTime.SetNow(now)

		require.True(t, limiter.ShouldTrigger("a"))
		for i := 0; i < 10; i++ {
			require.False(t, limiter.ShouldTrigger("a"))
		}
		// Move the time forward, by just less than waitDuration:
		bundle.mockTime.SetNow(now.Add(9 * time.Millisecond))
		require.False(t, limiter.ShouldTrigger("a"))

		require.True(t, limiter.ShouldTrigger("b")) // First time being triggered on "b"

		// Move the time forward to just past the waitDuration:
		bundle.mockTime.SetNow(now.Add(11 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		for i := 0; i < 10; i++ {
			require.False(t, limiter.ShouldTrigger("a"))
		}

		require.False(t, limiter.ShouldTrigger("b")) // has only been 2ms since last trigger of "b"

		// Move forward by another waitDuration (plus padding):
		bundle.mockTime.SetNow(now.Add(22 * time.Millisecond))
		require.True(t, limiter.ShouldTrigger("a"))
		require.True(t, limiter.ShouldTrigger("b"))
		require.False(t, limiter.ShouldTrigger("b"))
	})

	t.Run("ConcurrentAccessStressTest", func(t *testing.T) {
		doneCh := make(chan struct{})
		t.Cleanup(func() { close(doneCh) })

		limiter, bundle := setup()
		now := time.Now()
		bundle.mockTime.SetNow(now)

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

		bundle.mockTime.SetNow(now.Add(11 * time.Millisecond))

		<-time.After(100 * time.Millisecond)

		require.Equal(t, int64(2), counters["a"].Load())
		require.Equal(t, int64(2), counters["b"].Load())
		require.Equal(t, int64(2), counters["c"].Load())
	})
}
