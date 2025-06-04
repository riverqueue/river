// Package sliceutil contains helpers related to slices, usually ones that are
// generic-related, and are broadly useful, but which the Go core team, in its
// infinite wisdom, has decided are too much power for the  unwashed mashes, and
// therefore omitted from the utilities in `slices`.
package sliceutil

// FirstNonEmpty returns the first non-empty slice from the input, or nil if
// all input slices are empty.
func FirstNonEmpty[T any](inputs ...[]T) []T {
	for _, input := range inputs {
		if len(input) > 0 {
			return input
		}
	}
	return nil
}

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

// MapError manipulates a slice and transforms it to a slice of another type,
// returning the first error that occurred invoking the map function, if there
// was one.
func MapError[T any, R any](collection []T, mapFunc func(T) (R, error)) ([]R, error) {
	result := make([]R, len(collection))

	for i, item := range collection {
		var err error
		result[i], err = mapFunc(item)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Uniq returns a duplicate-free version of an array, in which only the first occurrence of each element is kept.
// The order of result values is determined by the order they occur in the array.
func Uniq[T comparable](collection []T) []T {
	result := make([]T, 0, len(collection))
	seen := make(map[T]struct{}, len(collection))

	for _, item := range collection {
		if _, ok := seen[item]; ok {
			continue
		}

		seen[item] = struct{}{}
		result = append(result, item)
	}

	return result
}
