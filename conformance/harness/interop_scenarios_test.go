//go:build riverconformance

package harness_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// uniqueColumnCases are unique options whose stored key and state mask every
// implementation must write identically. The period case schedules the job
// at a fixed time, so its period, derived from the scheduled time, doesn't
// depend on when the scenario runs.
func uniqueColumnCases() []struct {
	name string
	opts map[string]any
} {
	allStates := []string{"available", "cancelled", "completed", "discarded", "pending", "retryable", "running", "scheduled"}
	return []struct {
		name string
		opts map[string]any
	}{
		{name: "by_args", opts: map[string]any{"unique": map[string]any{"by_args": true}}},
		{name: "by_args_exclude_kind", opts: map[string]any{"unique": map[string]any{"by_args": true, "exclude_kind": true}}},
		{name: "by_period", opts: map[string]any{
			"scheduled_at": "2031-02-03T04:05:06.789Z",
			"unique":       map[string]any{"by_period_ms": 3_600_000},
		}},
		{name: "by_queue", opts: map[string]any{"queue": "unique_queue", "unique": map[string]any{"by_queue": true}}},
		{name: "by_state", opts: map[string]any{"unique": map[string]any{"by_state": []string{"available", "pending", "running", "scheduled"}}}},
		{name: "combined", opts: map[string]any{
			"queue":        "unique_queue",
			"scheduled_at": "2031-02-03T04:05:06.789Z",
			"unique": map[string]any{
				"by_args": true, "by_period_ms": 86_400_000, "by_queue": true, "by_state": allStates,
			},
		}},
	}
}

// uniqueColumns is the part of raw_job_row that stores a job's uniqueness.
type uniqueColumns struct {
	Key        *string
	KeyType    *string
	States     *string
	StatesType *string
}

func readUniqueColumns(t *testing.T, reader *adapter, id int64) uniqueColumns {
	t.Helper()

	var row rawJobRow
	reader.call(t, "raw_job_row", map[string]any{"id": id}, &row)
	return uniqueColumns{Key: row.UniqueKey, KeyType: row.UniqueKeyType, States: row.UniqueStates, StatesType: row.UniqueStatesType}
}

// verifyUniqueColumnBytes has each implementation insert the same unique jobs
// and requires the stored `unique_key` and `unique_states` to be identical
// byte for byte, including their SQLite storage types, as read by both
// implementations. A job without unique options stores neither.
func verifyUniqueColumnBytes(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	write := func(writer *adapter, params map[string]any) uniqueColumns {
		t.Helper()

		goAdapter.call(t, "reset", map[string]any{}, nil)
		var inserted normalizedJob
		writer.call(t, "insert", params, &inserted)
		columns := readUniqueColumns(t, goAdapter, inserted.ID)
		require.Equal(t, columns, readUniqueColumns(t, candidateAdapter, inserted.ID),
			"%s and %s render the unique columns %s wrote differently", goAdapter.name, candidateAdapter.name, writer.name)
		return columns
	}

	for _, testCase := range uniqueColumnCases() {
		params := map[string]any{"message": "unique columns " + testCase.name, "opts": testCase.opts}
		reference := write(goAdapter, params)
		require.NotNil(t, reference.Key, "%s: Go stored no unique key", testCase.name)
		require.NotNil(t, reference.States, "%s: Go stored no unique states", testCase.name)
		require.Equal(t, reference, write(candidateAdapter, params),
			"%s: %s and %s stored different unique columns", testCase.name, goAdapter.name, candidateAdapter.name)
	}

	params := map[string]any{"message": "not unique"}
	require.Equal(t, uniqueColumns{}, write(goAdapter, params))
	require.Equal(t, uniqueColumns{}, write(candidateAdapter, params))
}
