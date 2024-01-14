package valutil

import "golang.org/x/exp/constraints"

// ValOrDefault returns the given value if it's non-zero, and otherwise returns
// the default.
func ValOrDefault[T constraints.Integer | string](val, defaultVal T) T {
	var zero T
	if val != zero {
		return val
	}
	return defaultVal
}

// FirstNonZero returns the first argument that is non-zero, or the zero value if
// all are zero.
func FirstNonZero[T constraints.Integer | ~string](values ...T) T {
	var zero T
	for _, val := range values {
		if val != zero {
			return val
		}
	}
	return zero
}
