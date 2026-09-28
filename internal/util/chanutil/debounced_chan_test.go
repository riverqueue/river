package chanutil

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDebouncedChan_TriggersImmediately(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const cooldown = 200 * time.Millisecond
		debouncedChan := NewDebouncedChan(ctx, cooldown, true)
		go debouncedChan.Call()
		synctest.Wait()

		require.Len(t, debouncedChan.C(), 1)
		<-debouncedChan.C()

		// Concurrent calls during the cooldown coalesce into one trailing event.
		var wg sync.WaitGroup
		for range 5 {
			wg.Go(debouncedChan.Call)
		}
		wg.Wait()
		synctest.Wait()
		require.Empty(t, debouncedChan.C())

		time.Sleep(cooldown - time.Nanosecond)
		synctest.Wait()
		require.Empty(t, debouncedChan.C())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Len(t, debouncedChan.C(), 1)
		<-debouncedChan.C()

		// No further calls means no additional trailing event.
		time.Sleep(cooldown)
		synctest.Wait()
		require.Empty(t, debouncedChan.C())
	})
}

func TestDebouncedChan_OnlyBuffersOneEvent(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const cooldown = 100 * time.Millisecond
		debouncedChan := NewDebouncedChan(ctx, cooldown, true)
		debouncedChan.Call()
		time.Sleep(cooldown)
		synctest.Wait()
		debouncedChan.Call()
		synctest.Wait()

		require.Len(t, debouncedChan.C(), 1)
		<-debouncedChan.C()

		time.Sleep(cooldown)
		synctest.Wait()
		require.Empty(t, debouncedChan.C())
	})
}

func TestDebouncedChan_SendLeadingDisabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const cooldown = 100 * time.Millisecond
		debouncedChan := NewDebouncedChan(ctx, cooldown, false)
		debouncedChan.Call()
		synctest.Wait()
		require.Empty(t, debouncedChan.C())

		time.Sleep(cooldown - time.Nanosecond)
		synctest.Wait()
		require.Empty(t, debouncedChan.C())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Len(t, debouncedChan.C(), 1)
		<-debouncedChan.C()
	})
}

func TestDebouncedChan_ContinuousOperation(t *testing.T) {
	t.Parallel()

	// Run in a synctest bubble so sleeps/timers use deterministic fake time.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const (
			cooldown        = 17 * time.Millisecond
			increment       = 1 * time.Millisecond
			cooldownPeriods = 9
		)

		var (
			debouncedChan = NewDebouncedChan(ctx, cooldown, true)
			goroutineDone = make(chan struct{})
			numSignals    int
		)

		go func() {
			defer close(goroutineDone)
			for {
				select {
				case <-ctx.Done():
					return
				case <-debouncedChan.C():
					numSignals++
				}
			}
		}()
		// Ensure the receiver goroutine is blocked on the debounced channel
		// before we start advancing fake time.
		synctest.Wait()

		testTime := cooldown * cooldownPeriods
		// Call more often than the cooldown so the debouncer should emit once
		// on the leading edge plus once per cooldown period.
		for tm := time.Duration(0); tm < testTime; tm += increment {
			time.Sleep(increment)
			debouncedChan.Call()
		}

		// Allow one final trailing-edge signal for the last burst of calls.
		time.Sleep(cooldown)

		cancel()
		<-goroutineDone
		// Wait for any internal timer goroutine to observe cancellation.
		synctest.Wait()

		expectedNumSignals := cooldownPeriods + 1
		require.Equal(t, expectedNumSignals, numSignals)
	})
}
