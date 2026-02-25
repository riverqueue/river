package jobexecutor

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func BenchmarkMetadataUpdatesBytesForSnooze(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	testCases := []struct {
		name        string
		logSizeByte int
	}{
		{name: "Log256KB", logSizeByte: 256 * 1024},
		{name: "Log2MB", logSizeByte: 2 * 1024 * 1024},
		{name: "Log8MB", logSizeByte: 8 * 1024 * 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			metadataUpdates := map[string]any{
				"river:log": json.RawMessage(makeRiverLogArrayWithApproxSize(tc.logSizeByte)),
			}

			jobMetadata := []byte(`{"snoozes":41}`)
			snoozesValue := gjson.GetBytes(jobMetadata, "snoozes").Int()

			b.Run("MarshalOnly", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					marshaled, err := json.Marshal(metadataUpdates)
					if err != nil {
						b.Fatal(err)
					}
					if len(marshaled) == 0 {
						b.Fatal("expected non-empty metadata updates payload")
					}
				}
			})

			b.Run("MarshalPlusSetSnoozes", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					marshaled, err := json.Marshal(metadataUpdates)
					if err != nil {
						b.Fatal(err)
					}

					// Matches snooze result handling in JobExecutor.reportResult.
					marshaled, err = sjson.SetBytes(marshaled, "snoozes", snoozesValue+1)
					if err != nil {
						b.Fatal(err)
					}
					if len(marshaled) == 0 {
						b.Fatal("expected non-empty metadata updates payload")
					}
				}
			})

			b.Run("SetSnoozesInMapThenMarshal", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					// Matches the optimized reportResult path where snoozes is
					// added to the map before a single marshal.
					metadataUpdates["snoozes"] = snoozesValue + 1

					marshaled, err := json.Marshal(metadataUpdates)
					if err != nil {
						b.Fatal(err)
					}
					if len(marshaled) == 0 {
						b.Fatal("expected non-empty metadata updates payload")
					}
				}
			})
		})
	}
}

type benchmarkLogAttempt struct {
	Attempt int    `json:"attempt"`
	Log     string `json:"log"`
}

func makeRiverLogArrayWithApproxSize(targetBytes int) []byte {
	if targetBytes <= 0 {
		return []byte("[]")
	}

	const perEntryLogSize = 1024
	payload := strings.Repeat("x", perEntryLogSize)
	numEntries := max(1, targetBytes/perEntryLogSize)

	logs := make([]benchmarkLogAttempt, numEntries)
	for i := range numEntries {
		logs[i] = benchmarkLogAttempt{
			Attempt: i + 1,
			Log:     payload,
		}
	}

	arrayBytes, err := json.Marshal(logs)
	if err != nil {
		panic(err)
	}

	return arrayBytes
}
