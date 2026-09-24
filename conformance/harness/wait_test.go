//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func normalizedJobIDs(jobs []normalizedJob) []int64 {
	ids := make([]int64, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}

func waitForListedJob(t *testing.T, adapter *adapter, params map[string]any) normalizedJob {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		adapter.call(t, "list", params, &result)
		if len(result.Jobs) > 0 {
			return result.Jobs[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s adapter did not list a matching job", adapter.name)
	return normalizedJob{}
}

func waitForListedJobCount(t *testing.T, adapter *adapter, params map[string]any, count int) []normalizedJob {
	t.Helper()

	return waitForListedJobCountWithin(t, adapter, params, count, 5*time.Second)
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

func waitForRuntimeStats(t *testing.T, adapter *adapter, predicate func(runtimeStats) bool) runtimeStats {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var stats runtimeStats
	for time.Now().Before(deadline) {
		adapter.call(t, "runtime_stats", map[string]any{}, &stats)
		if predicate(stats) {
			return stats
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s adapter runtime observations did not converge: %+v", adapter.name, stats)
	return runtimeStats{}
}

func countRuntimeEvent(stats runtimeStats, kind string) int {
	count := 0
	for _, event := range stats.Events {
		if event == kind {
			count++
		}
	}
	return count
}

func requireOrderedSubsequence(t *testing.T, values, expected []string) {
	t.Helper()

	index := 0
	for _, value := range values {
		if value == expected[index] {
			index++
			if index == len(expected) {
				return
			}
		}
	}
	t.Fatalf("expected ordered subsequence %v in %v", expected, values)
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

type leaderTerm struct {
	ElectedAt string
	LeaderID  string
}

func readLeader(t *testing.T, observer *adapter) leaderTerm {
	t.Helper()

	var result struct {
		ElectedAt *string `json:"elected_at"`
		LeaderID  *string `json:"leader_id"`
	}
	observer.call(t, "leader", map[string]any{}, &result)
	if result.ElectedAt == nil || result.LeaderID == nil {
		return leaderTerm{}
	}
	return leaderTerm{ElectedAt: *result.ElectedAt, LeaderID: *result.LeaderID}
}

func waitForLeaderTerm(t *testing.T, observer *adapter, previousElectedAt string) leaderTerm {
	t.Helper()

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		term := readLeader(t, observer)
		if term.ElectedAt != "" && term.ElectedAt != previousElectedAt {
			return term
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("leadership term did not change from %q", previousElectedAt)
	return leaderTerm{}
}

func waitForListener(t *testing.T, observer *adapter) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Count int `json:"count"`
		}
		response := observer.callResponse(t, "listener_count", map[string]any{})
		if response.Error != nil {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		require.NoError(t, json.Unmarshal(response.Result, &result))
		if result.Count > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s adapter did not establish a LISTEN connection", observer.name)
}
