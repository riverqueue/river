package ptrutil

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}
