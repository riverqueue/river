package testutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var _ TestingTB = &panicTB{}

// Marked as non-parallel because `t.Setenv` is not compatible with `t.Parallel`.
func TestWaitTimeout(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	require.Equal(t, 3*time.Second, WaitTimeout())

	t.Setenv("GITHUB_ACTIONS", "true")
	require.Equal(t, 10*time.Second, WaitTimeout())
}
