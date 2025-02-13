package testsignal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestTestSignal(t *testing.T) {
	t.Parallel()

	t.Run("Uninitialized", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}

		// Signal can safely be invoked any number of times.
		for range testSignalInternalChanSize + 1 {
			signal.Signal(struct{}{})
		}
	})
	t.Run("Initialized", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}
		signal.Init()

		// Signal can be invoked many times, but not infinitely
		for range testSignalInternalChanSize {
			signal.Signal(struct{}{})
		}

		// Another signal will panic because the internal channel is full.
		require.PanicsWithValue(t, "test only signal channel is full", func() {
			signal.Signal(struct{}{})
		})

		// And we can now wait on all the emitted signals.
		for range testSignalInternalChanSize {
			signal.WaitOrTimeout()
		}
	})

	t.Run("WaitC", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}
		signal.Init()

		select {
		case <-signal.WaitC():
			require.FailNow(t, "Test signal should not have fired")
		default:
		}

		signal.Signal(struct{}{})

		select {
		case <-signal.WaitC():
		default:
			require.FailNow(t, "Test signal should have fired")
		}
	})

	t.Run("WaitOrTimeout", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}
		signal.Init()

		signal.Signal(struct{}{})

		signal.WaitOrTimeout()
	})
}

// Marked as non-parallel because `t.Setenv` is not compatible with `t.Parallel`.
func TestWaitTimeout(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	require.Equal(t, 3*time.Second, riversharedtest.WaitTimeout())

	t.Setenv("GITHUB_ACTIONS", "true")
	require.Equal(t, 10*time.Second, riversharedtest.WaitTimeout())
}
