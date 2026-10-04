//go:build riverconformance

package harness_test

import (
	"testing"
	"time"
)

func normalizedJobIDs(jobs []normalizedJob) []int64 {
	ids := make([]int64, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}

// waitForListedJobCountWithin polls a job list until it contains exactly
// count jobs or the timeout elapses.
func waitForListedJobCountWithin(t *testing.T, adapter *adapter, params map[string]any, count int, timeout time.Duration) []normalizedJob {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var result struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		adapter.call(t, "list", params, &result)
		if len(result.Jobs) == count {
			return result.Jobs
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not list %d matching jobs", adapter.name, count)
	return nil
}

func mapKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func jobIDs(jobs []normalizedJob) []int64 {
	ids := make([]int64, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}

func waitForLeader(t *testing.T, observer *adapter, previous string) string {
	t.Helper()

	deadline := time.Now().Add(12 * time.Second)
	var observations []string
	for time.Now().Before(deadline) {
		var result struct {
			ElectedAt *string `json:"elected_at"`
			LeaderID  *string `json:"leader_id"`
		}
		observer.call(t, "leader", map[string]any{}, &result)
		leaderID, electedAt := "<nil>", "<nil>"
		if result.LeaderID != nil {
			leaderID = *result.LeaderID
		}
		if result.ElectedAt != nil {
			electedAt = *result.ElectedAt
		}
		observation := leaderID + "@" + electedAt
		if len(observations) == 0 || observations[len(observations)-1] != observation {
			observations = append(observations, observation)
		}
		if result.LeaderID != nil && *result.LeaderID != previous {
			return *result.LeaderID
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("leader did not change from %q; observations=%v; %s adapter stderr: %s", previous, observations, observer.name, observer.stderr.String())
	return ""
}
