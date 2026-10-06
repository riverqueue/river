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

// makeSnoozeCounters records the `snoozes` count the job executor writes when
// a job with the given metadata snoozes, including how it coerces values that
// aren't integers.
func makeSnoozeCounters() snoozeCounters {
	fixture := snoozeCounters{
		Comment: generatedComment("jobexecutor.NextSnoozeCount"),
	}

	for _, testCase := range []struct {
		metadata string
		name     string
	}{
		{metadata: `{}`, name: "absent"},
		{metadata: `{"snoozes":2}`, name: "integer"},
		{metadata: `{"snoozes":2.9}`, name: "fraction_truncates"},
		{metadata: `{"snoozes":-2.5}`, name: "negative_fraction_truncates_toward_zero"},
		{metadata: `{"snoozes":1e3}`, name: "exponent"},
		{metadata: `{"snoozes":9007199254740993}`, name: "beyond_float_precision"},
		{metadata: `{"snoozes":"4"}`, name: "numeric_string"},
		{metadata: `{"snoozes":"-7"}`, name: "negative_numeric_string"},
		{metadata: `{"snoozes":"4.5"}`, name: "fractional_string_is_zero"},
		{metadata: `{"snoozes":" 5"}`, name: "padded_string_is_zero"},
		{metadata: `{"snoozes":"abc"}`, name: "non_numeric_string_is_zero"},
		{metadata: `{"snoozes":true}`, name: "true_is_one"},
		{metadata: `{"snoozes":false}`, name: "false_is_zero"},
		{metadata: `{"snoozes":null}`, name: "null_is_zero"},
		{metadata: `{"snoozes":[3]}`, name: "array_is_zero"},
		{metadata: `{"snoozes":{"count":3}}`, name: "object_is_zero"},
	} {
		fixture.SnoozeCounters = append(fixture.SnoozeCounters, snoozeCounterCase{
			ExpectedSnoozes: jobexecutor.NextSnoozeCount([]byte(testCase.metadata)),
			Metadata:        json.RawMessage(testCase.metadata),
			Name:            testCase.name,
		})
	}

	return fixture
}
