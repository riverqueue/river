package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeResumableCases(t *testing.T) {
	t.Parallel()

	fixture, err := makeResumableCases()
	require.NoError(t, err)
	want := map[string]string{
		"checkpoint_commit_error":      `{"river:resumable_step":"loop","river:resumable_cursor":{"loop":1}}`,
		"checkpoint_then_cursor_error": `{"river:resumable_step":"loop","river:resumable_cursor":{"loop":2}}`,
		"checkpoint_rollback_error":    `{}`,
		"checkpoint_write_failure":     `{"river:resumable_step":"first"}`,
		"first_cursor_error":           `{"river:resumable_cursor":{"loop":7}}`,
		"step_snooze":                  `{"river:resumable_step":"first"}`,
	}
	require.Len(t, fixture.Cases, len(want))
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Parallel()

			metadata, err := json.Marshal(testCase.ExpectedMetadata)
			require.NoError(t, err)
			require.JSONEq(t, want[testCase.Name], string(metadata))
		})
	}
}
