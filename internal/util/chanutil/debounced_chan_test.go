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

	debouncedChan := NewDebouncedChan(ctx, 200*time.Millisecond)
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
	for i := 0; i < 5; i++ {
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

	debouncedChan := NewDebouncedChan(ctx, 100*time.Millisecond)
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
		debouncedChan = NewDebouncedChan(ctx, cooldown)
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
	// into our total test time, multiplied by two, because the debounced chan
	// fires at the beginning and end of a bounce period. +1 for the last period
	// that fires on the leading edge, but which we don't give time for the
	// timer to fully expire. (We've chosen numbers so that test time doesn't
	// divide by cooldown perfectly.)
	//
	// This usually lands right on the expected number, but allow a delta of
	// +/-4 to allow the channel to be off by two cycles (again, one cycle
	// signals once at leading edge of the period and once at trailing, so 2
	// cycles * 2 signals = 4) in either direction. By running at `-count 1000`
	// or so I can usually reproduce an off-by-one-or-two cycle.
	expectedNumSignal := int(math.Round(float64(testTime)/float64(cooldown)))*2 + 1
	t.Logf("Expected: %d, actual: %d", expectedNumSignal, numSignals)
	require.InDelta(t, expectedNumSignal, numSignals, 4)
}
