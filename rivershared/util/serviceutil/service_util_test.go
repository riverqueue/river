package serviceutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/util/randutil"
)

func TestCancellableSleep(t *testing.T) {
	t.Parallel()

	testCancellableSleep := func(t *testing.T, startSleepFunc func(ctx context.Context) <-chan struct{}) {
		t.Helper()

		ctx := context.Background()

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		sleepDone := startSleepFunc(ctx)

		// Wait a very nominal amount of time just to make sure that some sleep is
		// actually happening.
		select {
		case <-sleepDone:
			require.FailNow(t, "Sleep returned sooner than expected")
		case <-time.After(50 * time.Millisecond):
		}

		cancel()

		select {
		case <-sleepDone:
		case <-time.After(50 * time.Millisecond):
			require.FailNow(t, "Timed out waiting for sleep to finish after cancel")
		}
	}

	// Starts sleep for sleep functions that don't return a channel, returning a
	// channel that's closed when sleep finishes.
	startSleep := func(sleepFunc func()) <-chan struct{} {
		sleepDone := make(chan struct{})
		go func() {
			defer close(sleepDone)
			sleepFunc()
		}()
		return sleepDone
	}

	t.Run("CancellableSleep", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context) <-chan struct{} {
			return startSleep(func() {
				CancellableSleep(ctx, 5*time.Second)
			})
		})
	})

	t.Run("CancellableSleepC", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context) <-chan struct{} {
			return CancellableSleepC(ctx, 5*time.Second)
		})
	})
}

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()

	rand := randutil.NewCryptoSeededConcurrentSafeRand()

	require.InDelta(t, 1.0, ExponentialBackoff(rand, 1, MaxAttemptsBeforeResetDefault).Seconds(), 1.0*0.1)
	require.InDelta(t, 2.0, ExponentialBackoff(rand, 2, MaxAttemptsBeforeResetDefault).Seconds(), 2.0*0.1)
	require.InDelta(t, 4.0, ExponentialBackoff(rand, 3, MaxAttemptsBeforeResetDefault).Seconds(), 4.0*0.1)
	require.InDelta(t, 8.0, ExponentialBackoff(rand, 4, MaxAttemptsBeforeResetDefault).Seconds(), 8.0*0.1)
	require.InDelta(t, 16.0, ExponentialBackoff(rand, 5, MaxAttemptsBeforeResetDefault).Seconds(), 16.0*0.1)
	require.InDelta(t, 32.0, ExponentialBackoff(rand, 6, MaxAttemptsBeforeResetDefault).Seconds(), 32.0*0.1)
}

func TestExponentialBackoffSecondsWithoutJitter(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1, int(exponentialBackoffSecondsWithoutJitter(1, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 2, int(exponentialBackoffSecondsWithoutJitter(2, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 4, int(exponentialBackoffSecondsWithoutJitter(3, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 8, int(exponentialBackoffSecondsWithoutJitter(4, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 16, int(exponentialBackoffSecondsWithoutJitter(5, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 32, int(exponentialBackoffSecondsWithoutJitter(6, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 64, int(exponentialBackoffSecondsWithoutJitter(7, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 128, int(exponentialBackoffSecondsWithoutJitter(8, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 256, int(exponentialBackoffSecondsWithoutJitter(9, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 512, int(exponentialBackoffSecondsWithoutJitter(10, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 1, int(exponentialBackoffSecondsWithoutJitter(11, MaxAttemptsBeforeResetDefault))) // resets
}
