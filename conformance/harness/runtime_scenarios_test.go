//go:build riverconformance

package harness_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// verifyWorkerOutcomes checks the persisted row for each terminal worker
// outcome in one implementation.
func verifyWorkerOutcomes(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-outcomes", "max_workers": 2,
	}, nil)
	for _, testCase := range []struct {
		behavior    string
		errorText   string
		maxAttempts int
		state       string
	}{
		{behavior: "cancel", state: "cancelled"},
		{behavior: "discard", maxAttempts: 1, state: "discarded"},
		{behavior: "error", errorText: "conformance retryable error", maxAttempts: 1, state: "discarded"},
	} {
		params := map[string]any{"behavior": testCase.behavior, "message": testCase.behavior}
		if testCase.maxAttempts > 0 {
			params["opts"] = map[string]any{"max_attempts": testCase.maxAttempts}
		}
		var inserted, worked normalizedJob
		current.call(t, "insert", params, &inserted)
		current.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
		require.Equal(t, testCase.state, worked.State, "%s behavior", testCase.behavior)
		require.Equal(t, 1, worked.Attempt, "%s behavior", testCase.behavior)
		require.NotNil(t, worked.FinalizedAt, "%s behavior", testCase.behavior)
		require.Len(t, worked.Errors, 1, "%s behavior", testCase.behavior)
		require.Equal(t, 1, worked.Errors[0].Attempt, "%s behavior", testCase.behavior)
		if testCase.errorText != "" {
			require.Equal(t, testCase.errorText, worked.Errors[0].Error)
		}
	}

	var outputInserted, outputWorked normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "output", "message": "runtime output",
	}, &outputInserted)
	current.call(t, "wait", map[string]any{"id": outputInserted.ID}, &outputWorked)
	require.Equal(t, "completed", outputWorked.State)
	require.Empty(t, outputWorked.Errors)
	require.Equal(t, map[string]any{"message": "runtime output"}, outputWorked.Metadata["output"])
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyCompletionBatching completes many jobs at once and requires the
// completions to share write transactions. PostgreSQL assigns one
// transaction ID per writing transaction, so completing N jobs one at a time
// would consume at least N IDs.
func verifyCompletionBatching(t *testing.T, observer *postgresObserver, current *adapter) {
	t.Helper()

	const jobCount = 1_000
	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-completion-batching", "fetch_poll_interval_ms": 1_000,
		"max_workers": jobCount,
	}, nil)
	current.call(t, "barrier_create", map[string]any{"name": "completion-batching"}, nil)
	jobs := make([]map[string]any, jobCount)
	for index := range jobs {
		jobs[index] = map[string]any{"behavior": "barrier_wait", "message": "completion-batching"}
	}
	var inserted struct {
		Results []normalizedInsertResult `json:"results"`
	}
	current.call(t, "insert_many", map[string]any{"jobs": jobs}, &inserted)
	require.Len(t, inserted.Results, jobCount)
	waitForListedJobCountWithin(t, current, map[string]any{
		"limit": jobCount, "states": []string{"running"},
	}, jobCount, 20*time.Second)

	before := observer.nextTransactionID(t)
	current.call(t, "barrier_release", map[string]any{"name": "completion-batching"}, nil)
	completed := waitForListedJobCountWithin(t, current, map[string]any{
		"limit": jobCount, "states": []string{"completed"},
	}, jobCount, 20*time.Second)
	writes := observer.nextTransactionID(t) - before
	for _, job := range completed {
		require.Equal(t, 1, job.Attempt)
		require.Empty(t, job.Errors)
	}
	require.Less(t, writes, int64(jobCount/4),
		"%s used %d write transactions to complete %d jobs; completions are not batched", current.name, writes, jobCount)
	t.Logf("%s completed %d jobs in %d write transactions", current.name, jobCount, writes)
	current.call(t, "stop", map[string]any{}, nil)
}

func parseTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339Nano, value)
	require.NoError(t, err)
	return parsed
}
