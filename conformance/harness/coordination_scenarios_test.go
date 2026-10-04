//go:build riverconformance

package harness_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
