package ptrutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPtr(t *testing.T) {
	t.Parallel()

	{
		v := "s"
		require.Equal(t, &v, Ptr("s"))
	}

	{
		v := 7
		require.Equal(t, &v, Ptr(7))
	}
}

func TestValOrDefault(t *testing.T) {
	t.Parallel()

	val := "val"
	require.Equal(t, val, ValOrDefault(&val, "default"))
	require.Equal(t, "default", ValOrDefault((*string)(nil), "default"))
}

func TestValOrDefaultFunc(t *testing.T) {
	t.Parallel()

	val := "val"
	require.Equal(t, val, ValOrDefaultFunc(&val, func() string { return "default" }))
	require.Equal(t, "default", ValOrDefaultFunc((*string)(nil), func() string { return "default" }))
}
