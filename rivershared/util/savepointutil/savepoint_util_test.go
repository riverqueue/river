package savepointutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBeginOnlyOnce(t *testing.T) {
	t.Parallel()

	beginOnce := NewBeginOnlyOnce(nil)

	require.NoError(t, beginOnce.Begin())
	require.False(t, beginOnce.IsDone())

	// Trying to begin again is an error because there's already a
	// subtransaction in progress.
	require.EqualError(t, beginOnce.Begin(), "subtransaction already in progress")

	childBeginOnce := NewBeginOnlyOnce(beginOnce)

	childBeginOnce.Done()
	require.False(t, beginOnce.IsDone())
	require.True(t, childBeginOnce.IsDone())

	// Now that a subtransaction has been finished, Begin can be called again.
	require.NoError(t, beginOnce.Begin())
}
