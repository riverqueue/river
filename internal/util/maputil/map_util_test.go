package maputil

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	t.Parallel()

	is := require.New(t)

	r1 := Keys(map[string]int{"foo": 1, "bar": 2})
	sort.Strings(r1)

	is.Equal([]string{"bar", "foo"}, r1)
}

func TestValues(t *testing.T) {
	t.Parallel()

	is := require.New(t)

	r1 := Values(map[string]int{"foo": 1, "bar": 2})
	sort.Ints(r1)

	is.Equal([]int{1, 2}, r1)
}
