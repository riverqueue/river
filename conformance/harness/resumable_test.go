//go:build riverconformance

package harness_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Run each attempt in a different implementation. A long retry delay prevents
// the first engine from reclaiming the next attempt before shutdown.
func verifyResumableInteroperability(t *testing.T, first, second *adapter) {
	t.Helper()

	for _, direction := range [][2]*adapter{{first, second}, {second, first}} {
		producer, consumer := direction[0], direction[1]
		producer.call(t, "reset", map[string]any{}, nil)
		var job normalizedJob
		producer.call(t, "insert", map[string]any{
			"behavior": "resumable_cursor", "message": "cross-engine cursor",
			"opts": map[string]any{"max_attempts": 3, "metadata": map[string]any{"application": "retained"}},
		}, &job)
		for index, worker := range []*adapter{producer, consumer, producer} {
			worker.call(t, "start", map[string]any{
				"client_id": worker.name + "-resumable", "max_workers": 1,
				"retry_delay_ms": 60_000,
			}, nil)
			state := "retryable"
			if index == 2 {
				state = "completed"
			}
			worker.call(t, "wait", map[string]any{"id": job.ID, "states": []string{state}}, &job)
			worker.call(t, "stop", map[string]any{}, nil)
			require.Equal(t, index+1, job.Attempt)
			require.Equal(t, "retained", job.Metadata["application"])
			require.EqualValues(t, 1, job.Metadata["first_attempt"], "completed first step must never run again")
			if index == 0 {
				require.Equal(t, "first", job.Metadata["river:resumable_step"])
				cursors, ok := job.Metadata["river:resumable_cursor"].(map[string]any)
				require.True(t, ok, "cursor metadata must be an object")
				require.EqualValues(t, 7, cursors["second"])
			} else {
				require.Equal(t, "second", job.Metadata["river:resumable_step"])
				require.Nil(t, job.Metadata["river:resumable_cursor"], "consumed cursor must be cleared: worker=%s attempt=%d metadata=%v errors=%v", worker.name, job.Attempt, job.Metadata, job.Errors)
				require.EqualValues(t, 7, job.Metadata["cursor_observed"])
			}
			if index < 2 {
				consumer.call(t, "retry", map[string]any{"id": job.ID}, &job)
			}
		}
		require.Len(t, job.Errors, 2)
	}
}

func verifyResumableValidation(t *testing.T, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-resumable-validation", "max_workers": 1,
	}, nil)
	for _, testCase := range []struct {
		behavior string
		metadata map[string]any
		step     string
	}{
		{behavior: "resumable_duplicate", metadata: map[string]any{}, step: "first"},
		{behavior: "resumable_duplicate", metadata: map[string]any{"river:resumable_step": "later"}, step: "later"},
		{behavior: "resumable", metadata: map[string]any{"river:resumable_step": ""}, step: "first"},
		{behavior: "output", metadata: map[string]any{"river:resumable_cursor": []any{}}},
	} {
		var job normalizedJob
		worker.call(t, "insert", map[string]any{
			"behavior": testCase.behavior, "message": "resumable validation",
			"opts": map[string]any{"max_attempts": 1, "metadata": testCase.metadata},
		}, &job)
		worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "discarded", job.State)
		require.Len(t, job.Errors, 1)
		if testCase.step != "" {
			require.Equal(t, testCase.step, job.Metadata["river:resumable_step"])
		} else {
			require.NotContains(t, job.Metadata, "output", "invalid cursors must fail before user work")
		}
		if testCase.behavior == "resumable_duplicate" {
			require.Contains(t, job.Errors[0].Error, "duplicate resumable step")
		}
	}
	worker.call(t, "stop", map[string]any{}, nil)
}
