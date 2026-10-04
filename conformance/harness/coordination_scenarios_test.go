//go:build riverconformance

package harness_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// verifyUnknownKind checks that a job whose kind has no registered worker is
// fetched and failed with the canonical unknown-kind error rather than being
// skipped. The error is retryable, so a job with attempts left is retried.
func verifyUnknownKind(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var discarded, retryable normalizedJob
		pair.inserter.call(t, "raw_insert_no_notify", map[string]any{
			"kind": "conformance_unregistered", "message": "must fail compatibly",
			"opts": map[string]any{"max_attempts": 1},
		}, &discarded)
		pair.inserter.call(t, "raw_insert_no_notify", map[string]any{
			"kind": "conformance_unregistered", "message": "must be retried",
			"opts": map[string]any{"max_attempts": 5},
		}, &retryable)
		workerID := pair.worker.name + "-unknown-kind"
		pair.worker.call(t, "start", map[string]any{
			"client_id": workerID, "max_workers": 1,
		}, nil)
		var known normalizedJob
		pair.inserter.call(t, "insert", map[string]any{"message": "known kind from " + pair.inserter.name}, &known)
		pair.worker.call(t, "wait", map[string]any{"id": known.ID}, &known)
		require.Equal(t, "completed", known.State)

		pair.worker.call(t, "wait", map[string]any{
			"id": discarded.ID, "states": []string{"discarded"},
		}, &discarded)
		require.Equal(t, 1, discarded.Attempt)
		require.Equal(t, []string{workerID}, discarded.AttemptedBy)
		require.Len(t, discarded.Errors, 1)
		require.Equal(t,
			"job kind is not registered in the client's Workers bundle: conformance_unregistered",
			discarded.Errors[0].Error,
		)

		// The first retry delay (about one second) is inside the scheduler
		// interval, so the failed job is made available again immediately
		// and retried. The second delay (about sixteen seconds) is not, so
		// the job then waits as retryable.
		pair.worker.call(t, "wait", map[string]any{
			"id": retryable.ID, "states": []string{"retryable"},
		}, &retryable)
		require.Equal(t, 2, retryable.Attempt)
		require.Equal(t, []string{workerID, workerID}, retryable.AttemptedBy)
		require.Len(t, retryable.Errors, 2)
		for _, attemptError := range retryable.Errors {
			require.Equal(t, discarded.Errors[0].Error, attemptError.Error)
		}
		require.Nil(t, retryable.FinalizedAt)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

// verifyClaimOrder checks the order in which a client claims available
// jobs, which every implementation writes in its own SQL: Go claims by
// priority, then scheduled_at, then ID. One implementation inserts jobs
// whose ID order, scheduled_at order, and priority order all differ,
// including two with equal priority and scheduled_at, and the other works
// them one at a time once its scheduler makes them all available together.
// Each job sleeps briefly, so the claims' attempted_at times are distinct and
// record the order.
func verifyClaimOrder(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		base := time.Now().UTC().Truncate(time.Millisecond)
		ids := make(map[string]int64)
		// In insertion (ID) order.
		for _, job := range []struct {
			name     string
			priority int
			ago      time.Duration
		}{
			{name: "priority 1, latest", priority: 1, ago: 30 * time.Second},
			{name: "priority 4", priority: 4, ago: time.Minute},
			{name: "priority 1, later", priority: 1, ago: time.Minute},
			{name: "priority 3, earliest", priority: 3, ago: 3 * time.Minute},
			{name: "priority 1, earliest, lower ID", priority: 1, ago: 2 * time.Minute},
			{name: "priority 1, earliest, higher ID", priority: 1, ago: 2 * time.Minute},
		} {
			var inserted normalizedJob
			pair.inserter.call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 5, "message": "claim order " + job.name,
				"opts": map[string]any{
					"priority": job.priority, "scheduled_at": base.Add(-job.ago).Format(time.RFC3339Nano),
				},
			}, &inserted)
			// Like Go, an explicit schedule inserts the job scheduled even
			// when it's due, and the leader's scheduler makes it available.
			require.Equal(t, "scheduled", inserted.State, job.name)
			ids[job.name] = inserted.ID
		}
		expected := []string{
			"priority 1, earliest, lower ID",
			"priority 1, earliest, higher ID",
			"priority 1, later",
			"priority 1, latest",
			"priority 3, earliest",
			"priority 4",
		}

		clientID := pair.worker.name + "-claim-order"
		pair.worker.startWithTuning(t, map[string]any{"client_id": clientID, "max_workers": 1},
			map[string]any{"elect_interval_ms": 20, "scheduler_interval_ms": 20})
		type claim struct {
			at   time.Time
			name string
		}
		claims := make([]claim, 0, len(ids))
		for name, id := range ids {
			var worked normalizedJob
			pair.worker.call(t, "wait", map[string]any{"id": id}, &worked)
			require.Equal(t, "completed", worked.State, name)
			require.Equal(t, []string{clientID}, worked.AttemptedBy, name)
			require.NotNil(t, worked.AttemptedAt, name)
			claims = append(claims, claim{at: parseTime(t, *worked.AttemptedAt), name: name})
		}
		pair.worker.call(t, "stop", map[string]any{}, nil)

		slices.SortFunc(claims, func(a, b claim) int { return a.at.Compare(b.at) })
		actual := make([]string, len(claims))
		for index, claim := range claims {
			if index > 0 {
				require.True(t, claim.at.After(claims[index-1].at), "%s claimed two jobs at the same time", pair.worker.name)
			}
			actual[index] = claim.name
		}
		require.Equal(t, expected, actual, "%s claimed jobs out of order", pair.worker.name)
	}
}
