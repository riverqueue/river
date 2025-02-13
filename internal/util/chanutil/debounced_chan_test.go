package chanutil

import (
	"context"
	"math"
	"sync"
	"testing"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const (
		cooldown  = 17 * time.Millisecond
		increment = 1 * time.Millisecond
		testTime  = 150 * time.Millisecond
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

	for tm := increment; tm <= testTime; tm += increment {
		time.Sleep(increment)
		debouncedChan.Call()
	}

	cancel()

	select {
	case <-goroutineDone:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Timed out waiting for goroutine to finish")
	}

	// Expect number of signals equal to number of cooldown periods that fit
	// into our total test time, and +1 for an initial fire.
	//
	// This almost always lands right on the expected number, but allow a delta
	// of +/-2 to allow the channel to be off by two cycles in either direction.
	// By running at `-count 1000` I can usually reproduce an off-by-one-or-two
	// cycle.
	expectedNumSignal := int(math.Round(float64(testTime)/float64(cooldown))) + 1
	t.Logf("Expected: %d, actual: %d", expectedNumSignal, numSignals)
	require.InDelta(t, expectedNumSignal, numSignals, 2)
}
