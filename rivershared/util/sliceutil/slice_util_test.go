package sliceutil

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultIfEmpty(t *testing.T) {
	t.Parallel()

	result1 := FirstNonEmpty([]int{1, 2, 3}, []int{4, 5, 6})
	result2 := FirstNonEmpty([]int{}, []int{4, 5, 6})
	result3 := FirstNonEmpty(nil, []int{4, 5, 6})

	require.Len(t, result1, 3)
	require.Len(t, result2, 3)
	require.Len(t, result3, 3)
	require.Equal(t, []int{1, 2, 3}, result1)
	require.Equal(t, []int{4, 5, 6}, result2)
	require.Equal(t, []int{4, 5, 6}, result3)
}

func TestGroupBy(t *testing.T) {
	t.Parallel()

	result1 := GroupBy([]int{0, 1, 2, 3, 4, 5}, func(i int) int {
		return i % 3
	})

	require.Len(t, result1, 3)
	require.Equal(t, map[int][]int{
		0: {0, 3},
		1: {1, 4},
		2: {2, 5},
	}, result1)
}

func TestKeyBy(t *testing.T) {
	t.Parallel()

	type foo struct {
		baz string
		bar int
	}
	transform := func(f *foo) (string, int) {
		return f.baz, f.bar
	}
	testCases := []struct {
		in     []*foo
		expect map[string]int
	}{
		{
			in:     []*foo{{baz: "apple", bar: 1}},
			expect: map[string]int{"apple": 1},
		},
		{
			in:     []*foo{{baz: "apple", bar: 1}, {baz: "banana", bar: 2}},
			expect: map[string]int{"apple": 1, "banana": 2},
		},
		{
			in:     []*foo{{baz: "apple", bar: 1}, {baz: "apple", bar: 2}},
			expect: map[string]int{"apple": 2},
		},
	}
	for i, tt := range testCases {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			t.Parallel()

			require.Equal(t, KeyBy(tt.in, transform), tt.expect)
		})
	}
}

func TestMap(t *testing.T) {
	t.Parallel()

	result1 := Map([]int{1, 2, 3, 4}, func(x int) string {
		return "Hello"
	})
	result2 := Map([]int64{1, 2, 3, 4}, func(x int64) string {
		return strconv.FormatInt(x, 10)
	})

	require.Len(t, result1, 4)
	require.Len(t, result2, 4)
	require.Equal(t, []string{"Hello", "Hello", "Hello", "Hello"}, result1)
	require.Equal(t, []string{"1", "2", "3", "4"}, result2)
}
