package valutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValOrDefault(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1, ValOrDefault(0, 1))
	require.Equal(t, 5, ValOrDefault(5, 1))

	require.Equal(t, "default", ValOrDefault("", "default"))
	require.Equal(t, "hello", ValOrDefault("hello", "default"))
}
