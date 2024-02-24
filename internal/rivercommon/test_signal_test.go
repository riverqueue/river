package rivercommon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTestSignal(t *testing.T) {
	t.Parallel()

	t.Run("Uninitialized", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}

		// Signal can safely be invoked any number of times.
		for i := 0; i < testSignalInternalChanSize+1; i++ {
			signal.Signal(struct{}{})
		}
	})
	t.Run("Initialized", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}
		signal.Init()

		// Signal can invoked many times, but not infinitely
		for i := 0; i < testSignalInternalChanSize; i++ {
			signal.Signal(struct{}{})
		}

		// Another signal will panic because the internal channel is full.
		require.PanicsWithValue(t, "test only signal channel is full", func() {
			signal.Signal(struct{}{})
		})

		// And we can now wait on all the emitted signals.
		for i := 0; i < testSignalInternalChanSize; i++ {
			signal.WaitOrTimeout()
		}
	})
}

// Marked as non-parallel because `t.Setenv` is not compatible with `t.Parallel`.
func TestWaitTimeout(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	require.Equal(t, 3*time.Second, WaitTimeout())

	t.Setenv("GITHUB_ACTIONS", "true")
	require.Equal(t, 10*time.Second, WaitTimeout())
}
