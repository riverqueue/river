package chanutil

import (
	"context"
	"sync"
	"testing"
	"time"
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
