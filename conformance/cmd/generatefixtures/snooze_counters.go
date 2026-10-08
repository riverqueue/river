package main

import (
	"encoding/json"

	"github.com/riverqueue/river/internal/jobexecutor"
)

type snoozeCounters struct {
	Comment        string              `json:"$comment"`
	SnoozeCounters []snoozeCounterCase `json:"snooze_counters"`
}

type snoozeCounterCase struct {
	ExpectedSnoozes int64           `json:"expected_snoozes"`
	Metadata        json.RawMessage `json:"metadata"`
	Name            string          `json:"name"`
}

// makeSnoozeCounters records increments of canonical non-negative integer
// counters and initialization when the counter is absent. Recovery from invalid
// counter values is implementation-specific, not part of the shared protocol.
func makeSnoozeCounters() snoozeCounters {
	fixture := snoozeCounters{
		Comment: generatedComment("jobexecutor.NextSnoozeCount"),
	}

	for _, testCase := range []struct {
		metadata string
		name     string
	}{
		{metadata: `{}`, name: "absent"},
		{metadata: `{"snoozes":9007199254740993}`, name: "beyond_float_precision"},
		{metadata: `{"snoozes":2}`, name: "integer"},
		{metadata: `{"snoozes":9223372036854775806}`, name: "largest_increment"},
		{metadata: `{"snoozes":0}`, name: "zero"},
	} {
		fixture.SnoozeCounters = append(fixture.SnoozeCounters, snoozeCounterCase{
			ExpectedSnoozes: jobexecutor.NextSnoozeCount([]byte(testCase.metadata)),
			Metadata:        json.RawMessage(testCase.metadata),
			Name:            testCase.name,
		})
	}

	return fixture
}
