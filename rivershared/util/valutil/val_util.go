package valutil

// ValOrDefault returns the given value if it's non-zero, and otherwise returns
// the default.
func ValOrDefault[T comparable](val, defaultVal T) T {
	var zero T
	if val != zero {
		return val
	}
	return defaultVal
}

// ValOrDefault returns the given value if it's non-zero, and otherwise invokes
// defaultFunc to produce a default value.
func ValOrDefaultFunc[T comparable](val T, defaultFunc func() T) T {
	var zero T
	if val != zero {
		return val
	}
	return defaultFunc()
}
