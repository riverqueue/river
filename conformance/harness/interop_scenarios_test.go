//go:build riverconformance

package harness_test

import (
	"testing"
	"time"

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

// verifyUniquePeriodicJob has one implementation's leader insert a unique
// run-on-start periodic job and then requires a later leader of the other
// implementation to skip its own run-on-start insertion as a duplicate, in
// both directions. It only skips when both compute the same unique key and
// states for the periodic job.
func verifyUniquePeriodicJob(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		first, second *adapter
	}{
		{first: goAdapter, second: candidateAdapter},
		{first: candidateAdapter, second: goAdapter},
	} {
		pair.first.call(t, "reset", map[string]any{}, nil)
		start := func(leader *adapter) {
			t.Helper()

			clientID := leader.name + "-periodic-unique"
			leader.call(t, "start", map[string]any{
				"client_id": clientID, "instrumented": true, "max_workers": 1,
				"periodic_run_on_start": true, "periodic_unique": true,
			}, nil)
			require.Equal(t, clientID, waitForLeader(t, leader, ""))
			_ = waitForRuntimeStats(t, leader, func(stats runtimeStats) bool { return stats.PeriodicStarts == 1 })
		}

		start(pair.first)
		periodic := waitForPeriodicJob(t, pair.first, "conformance-periodic")
		pair.first.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
		require.Equal(t, "completed", periodic.State)
		pair.first.call(t, "stop", map[string]any{}, nil)

		start(pair.second)
		// Each leader inserts a non-unique marker job after the unique
		// job, so once the second leader's marker exists, its attempt to
		// insert the unique job has been made.
		var periodicJobs []normalizedJob
		deadline := time.Now().Add(10 * time.Second)
		for {
			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			pair.second.call(t, "list", map[string]any{}, &listed)
			markers := 0
			periodicJobs = periodicJobs[:0]
			for _, job := range listed.Jobs {
				switch job.Metadata["river:periodic_job_id"] {
				case "conformance-periodic-marker":
					markers++
				case "conformance-periodic":
					periodicJobs = append(periodicJobs, job)
				}
			}
			if markers == 2 {
				break
			}
			require.True(t, time.Now().Before(deadline), "%s inserted no periodic marker job", pair.second.name)
			time.Sleep(10 * time.Millisecond)
		}
		require.Len(t, periodicJobs, 1, "%s inserted a unique periodic job %s already inserted", pair.second.name, pair.first.name)
		require.Equal(t, periodic.ID, periodicJobs[0].ID)
		pair.second.call(t, "stop", map[string]any{}, nil)
	}
}

// waitForPeriodicJob waits for a job inserted by the periodic job with the
// given ID and returns it.
func waitForPeriodicJob(t *testing.T, observer *adapter, periodicJobID string) normalizedJob {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{}, &listed)
		for _, job := range listed.Jobs {
			if job.Metadata["river:periodic_job_id"] == periodicJobID {
				return job
			}
		}
		require.True(t, time.Now().Before(deadline), "no job from periodic job %s", periodicJobID)
		time.Sleep(10 * time.Millisecond)
	}
}
