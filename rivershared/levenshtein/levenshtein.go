// Package levenshtein is a Go implementation to calculate Levenshtein Distance.
//
// Vendored from this repository:
// https://github.com/agnivade/levenshtein
//
// Implementation taken from
// https://gist.github.com/andrei-m/982927#gistcomment-1931258
package levenshtein

import "unicode/utf8"

// minLengthThreshold is the length of the string beyond which
// an allocation will be made. Strings smaller than this will be
// zero alloc.
const minLengthThreshold = 32

// ComputeDistance computes the levenshtein distance between the two
// strings passed as an argument. The return value is the levenshtein distance
//
// Works on runes (Unicode code points) but does not normalize
// the input strings. See https://blog.golang.org/normalization
// and the golang.org/x/text/unicode/norm package.
func ComputeDistance(str1, str2 string) int {
	if len(str1) == 0 {
		return utf8.RuneCountInString(str2)
	}

	if len(str2) == 0 {
		return utf8.RuneCountInString(str1)
	}

	if str1 == str2 {
		return 0
	}

	// We need to convert to []rune if the strings are non-ASCII.
	// This could be avoided by using utf8.RuneCountInString
	// and then doing some juggling with rune indices,
	// but leads to far more bounds checks. It is a reasonable trade-off.
	runeSlice1 := []rune(str1)
	runeSlice2 := []rune(str2)

	// swap to save some memory O(min(a,b)) instead of O(a)
	if len(runeSlice1) > len(runeSlice2) {
		runeSlice1, runeSlice2 = runeSlice2, runeSlice1
	}
	lenRuneSlice1 := len(runeSlice1)
	lenRuneSlice2 := len(runeSlice2)

	// Init the row.
	var distances []uint16
	if lenRuneSlice1+1 > minLengthThreshold {
		distances = make([]uint16, lenRuneSlice1+1)
	} else {
		// We make a small optimization here for small strings. Because a slice
		// of constant length is effectively an array, it does not allocate. So
		// we can re-slice it to the right length as long as it is below a
		// desired threshold.
		distances = make([]uint16, minLengthThreshold)
		distances = distances[:lenRuneSlice1+1]
	}

	// we start from 1 because index 0 is already 0.
	for i := 1; i < len(distances); i++ {
		distances[i] = uint16(i) //nolint:gosec
	}

	// Make a dummy bounds check to prevent the 2 bounds check down below. The
	// one inside the loop is particularly costly.
	_ = distances[lenRuneSlice1]

	// fill in the rest
	for i := 1; i <= lenRuneSlice2; i++ {
		prev := uint16(i) //nolint:gosec
		for j := 1; j <= lenRuneSlice1; j++ {
			current := distances[j-1] // match
			if runeSlice2[i-1] != runeSlice1[j-1] {
				current = min(min(distances[j-1]+1, prev+1), distances[j]+1)
			}
			distances[j-1] = prev
			prev = current
		}
		distances[lenRuneSlice1] = prev
	}
	return int(distances[lenRuneSlice1])
}
