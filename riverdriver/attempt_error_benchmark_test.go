package riverdriver

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"
)

func BenchmarkUnmarshalAttemptError(b *testing.B) {
	for _, testCase := range []struct {
		data string
		name string
	}{
		{data: `{"at":"not a timestamp","attempt":3,"error":"connection reset by peer","trace":""}`, name: "InvalidTimestamp"},
		{data: `{"at":"2026-09-29T15:04:05.123456Z","attempt":3,"error":"connection reset by peer","trace":""}`, name: "Normal"},
	} {
		data := []byte(testCase.data)
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var attemptError rivertype.AttemptError
				if err := UnmarshalAttemptError(data, &attemptError); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalAttemptErrors(b *testing.B) {
	for _, count := range []int{0, 1, 10, 25} {
		attemptErrors := make([]rivertype.AttemptError, count)
		for i := range attemptErrors {
			attemptErrors[i] = rivertype.AttemptError{
				At:      time.Date(2026, 9, 29, 15, 4, 5, 123456000, time.UTC),
				Attempt: i + 1,
				Error:   "connection reset by peer",
			}
		}
		data, err := json.Marshal(attemptErrors)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("Count%d", count), func(b *testing.B) {
			b.Run("Driver", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var decoded []rivertype.AttemptError
					if err := UnmarshalAttemptErrors(data, &decoded); err != nil {
						b.Fatal(err)
					}
				}
			})

			// Ordinary JSON decoding is the baseline for well-formed histories.
			b.Run("JSON", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var decoded []rivertype.AttemptError
					if err := json.Unmarshal(data, &decoded); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
