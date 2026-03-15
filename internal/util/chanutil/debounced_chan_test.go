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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	debouncedChan := NewDebouncedChan(ctx, 200*time.Millisecond, true)
	go debouncedChan.Call()

	select {
	case <-debouncedChan.C():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for debounced chan to trigger")
	}

	// shouldn't trigger immediately again
	go debouncedChan.Call()
	select {
	case <-debouncedChan.C():
		t.Fatal("received from debounced chan unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for range 5 {
		go func() {
			debouncedChan.Call()
			wg.Done()
		}()
	}
	wg.Wait()

	// should trigger again after debounce period
	select {
	case <-debouncedChan.C():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for debounced chan to trigger")
	}

	// shouldn't trigger immediately again
	select {
	case <-debouncedChan.C():
		t.Fatal("received from debounced chan unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDebouncedChan_OnlyBuffersOneEvent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	debouncedChan := NewDebouncedChan(ctx, 100*time.Millisecond, true)
	debouncedChan.Call()
	time.Sleep(150 * time.Millisecond)
	debouncedChan.Call()

	select {
	case <-debouncedChan.C():
	case <-time.After(20 * time.Millisecond):
		t.Fatal("timed out waiting for debounced chan to trigger")
	}

	// shouldn't trigger immediately again
	select {
	case <-debouncedChan.C():
		t.Fatal("received from debounced chan unexpectedly")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestDebouncedChan_SendLeadingDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	debouncedChan := NewDebouncedChan(ctx, 100*time.Millisecond, false)
	debouncedChan.Call()

	// Expect nothing right away because sendLeading is disabled.
	select {
	case <-debouncedChan.C():
		t.Fatal("received from debounced chan unexpectedly")
	case <-time.After(20 * time.Millisecond):
	}

	time.Sleep(100 * time.Millisecond)

	select {
	case <-debouncedChan.C():
	case <-time.After(20 * time.Millisecond):
		t.Fatal("timed out waiting for debounced chan to trigger")
	}
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
