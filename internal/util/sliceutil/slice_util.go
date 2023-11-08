// Package sliceutil contains helpers related to slices, usually ones that are
// generic-related, and are broadly useful, but which the Go core team, in its
// infinite wisdom, has decided are too much power for the  unwashed mashes, and
// therefore omitted from the utilities in `slices`.
package sliceutil

// GroupBy returns an object composed of keys generated from the results of
// running each element of collection through keyFunc.
func GroupBy[T any, U comparable](collection []T, keyFunc func(T) U) map[U][]T {
	result := map[U][]T{}

	for _, item := range collection {
		key := keyFunc(item)

		result[key] = append(result[key], item)
	}

	return result
}

// KeyBy converts a slice into a map using the key/value tuples returned by
// tupleFunc. If any two pairs would have the same key, the last one wins. Go
// maps are unordered and the order of the new map isn't guaranteed to the same
// as the original slice.
func KeyBy[T any, K comparable, V any](collection []T, tupleFunc func(item T) (K, V)) map[K]V {
	result := make(map[K]V, len(collection))

	for _, t := range collection {
		k, v := tupleFunc(t)
		result[k] = v
	}

	return result
}

// Map manipulates a slice and transforms it to a slice of another type.
func Map[T any, R any](collection []T, mapFunc func(T) R) []R {
	result := make([]R, len(collection))

	for i, item := range collection {
		result[i] = mapFunc(item)
	}

	return result
}
