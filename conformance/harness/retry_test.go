//go:build riverconformance

package harness_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// retriedJob is a retried job's row without the values that differ between
// runs.
type retriedJob struct {
	Attempt     int
	Errors      int
	Finalized   bool
	MaxAttempts int
	State       string
}

// verifyExhaustedJobRetry checks retrying finalized jobs from the other
// implementation. Go's retry makes a finalized job available again, and when
// the job has used every attempt it raises max_attempts by one so the job
// gets another one. One implementation works a job that fails on its only
// attempt and is discarded, and one that cancels itself with attempts left,
// and the other retries both, both ways round.
func verifyExhaustedJobRetry(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	retried := make(map[string]map[string]retriedJob)
	for _, pair := range []struct {
		finisher *adapter
		retrier  *adapter
	}{
		{finisher: goAdapter, retrier: candidateAdapter},
		{finisher: candidateAdapter, retrier: goAdapter},
	} {
		pair.finisher.call(t, "reset", map[string]any{}, nil)
		retried[pair.retrier.name] = make(map[string]retriedJob)
		for label, testCase := range map[string]struct {
			finalState string
			params     map[string]any
		}{
			"exhausted": {
				finalState: "discarded",
				params:     map[string]any{"behavior": "error", "message": "exhausted retry", "opts": map[string]any{"max_attempts": 1}},
			},
			"attempts left": {
				finalState: "cancelled",
				params:     map[string]any{"behavior": "cancel", "message": "cancelled retry", "opts": map[string]any{"max_attempts": 3}},
			},
		} {
			var job normalizedJob
			pair.finisher.call(t, "insert", testCase.params, &job)
			pair.finisher.call(t, "work", map[string]any{"id": job.ID}, &job)
			require.Equal(t, testCase.finalState, job.State, "%s %s job", pair.finisher.name, label)
			require.Equal(t, 1, job.Attempt, "%s %s job", pair.finisher.name, label)

			var retriedRow, observed normalizedJob
			pair.retrier.call(t, "retry", map[string]any{"id": job.ID}, &retriedRow)
			pair.finisher.call(t, "get", map[string]any{"id": job.ID}, &observed)
			require.Equal(t, retriedRow, observed, "%s %s job", pair.retrier.name, label)
			require.True(t, parseTime(t, observed.ScheduledAt).After(parseTime(t, *job.FinalizedAt)),
				"%s retried the %s job without rescheduling it", pair.retrier.name, label)
			retried[pair.retrier.name][label] = retriedJob{
				Attempt:     observed.Attempt,
				Errors:      len(observed.Errors),
				Finalized:   observed.FinalizedAt != nil,
				MaxAttempts: observed.MaxAttempts,
				State:       observed.State,
			}
		}
	}

	require.Equal(t, map[string]retriedJob{
		"attempts left": {Attempt: 1, Errors: 1, MaxAttempts: 3, State: "available"},
		"exhausted":     {Attempt: 1, Errors: 1, MaxAttempts: 2, State: "available"},
	}, retried[goAdapter.name])
	require.Equal(t, retried[goAdapter.name], retried[candidateAdapter.name],
		"%s and Go retried finalized jobs differently", candidateAdapter.name)
}
