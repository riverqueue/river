package levenshtein_test

import (
	"testing"

	"github.com/riverqueue/river/rivershared/levenshtein"
)

func TestSanity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		str1, str2 string
		want       int
	}{
		{"", "hello", 5},
		{"hello", "", 5},
		{"hello", "hello", 0},
		{"ab", "aa", 1},
		{"ab", "ba", 2},
		{"ab", "aaa", 2},
		{"bbb", "a", 3},
		{"kitten", "sitting", 3},
		{"distance", "difference", 5},
		{"levenshtein", "frankenstein", 6},
		{"resume and cafe", "resumes and cafes", 2},
		{"a very long string that is meant to exceed", "another very long string that is meant to exceed", 6},
	}
	for i, d := range tests {
		n := levenshtein.ComputeDistance(d.str1, d.str2)
		if n != d.want {
			t.Errorf("Test[%d]: ComputeDistance(%q,%q) returned %v, want %v",
				i, d.str1, d.str2, n, d.want)
		}
	}
}

func TestUnicode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		str1, str2 string
		want       int
	}{
		// Testing acutes and umlauts
		{"resumé and café", "resumés and cafés", 2},
		{"resume and cafe", "resumé and café", 2},
		{"Hafþór Júlíus Björnsson", "Hafþor Julius Bjornsson", 4},
		// Only 2 characters are less in the 2nd string
		{"།་གམ་འས་པ་་མ།", "།་གམའས་པ་་མ", 2},
	}
	for i, d := range tests {
		n := levenshtein.ComputeDistance(d.str1, d.str2)
		if n != d.want {
			t.Errorf("Test[%d]: ComputeDistance(%q,%q) returned %v, want %v",
				i, d.str1, d.str2, n, d.want)
		}
	}
}

// Benchmarks
// ----------------------------------------------.
var sink int //nolint:gochecknoglobals

func BenchmarkSimple(b *testing.B) {
	tests := []struct {
		a, b string
		name string
	}{
		// ASCII
		{"levenshtein", "frankenstein", "ASCII"},
		// Testing acutes and umlauts
		{"resumé and café", "resumés and cafés", "French"},
		{"Hafþór Júlíus Björnsson", "Hafþor Julius Bjornsson", "Nordic"},
		{"a very long string that is meant to exceed", "another very long string that is meant to exceed", "long string"},
		// Only 2 characters are less in the 2nd string
		{"།་གམ་འས་པ་་མ།", "།་གམའས་པ་་མ", "Tibetan"},
	}
	tmp := 0
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				tmp = levenshtein.ComputeDistance(test.a, test.b)
			}
		})
	}
	sink = tmp
}
