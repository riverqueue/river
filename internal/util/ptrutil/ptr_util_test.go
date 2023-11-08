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
