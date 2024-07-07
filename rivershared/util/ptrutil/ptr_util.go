package ptrutil

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}

// ValOrDefault returns the value of the given pointer as long as it's non-nil,
// and the specified default value otherwise.
func ValOrDefault[T any](ptr *T, defaultVal T) T {
	if ptr != nil {
		return *ptr
	}
	return defaultVal
}

// ValOrDefaultFunc returns the value of the given pointer as long as it's
// non-nil, or invokes the given function to produce a default value otherwise.
func ValOrDefaultFunc[T any](ptr *T, defaultFunc func() T) T {
	if ptr != nil {
		return *ptr
	}
	return defaultFunc()
}
