//go:build riverconformance

package harness_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// schedulerOutcome is what a leader's scheduler did to one due job, without
// the values that differ between runs.
type schedulerOutcome struct {
	Attempt           int
	Finalized         bool
	State             string
	UniqueKeyConflict any
}

// verifySchedulerUniqueConflictDiscard checks how a leader's scheduler
// handles due retries of unique jobs, which every implementation does in its
// own SQL. Go prepares the same retryable jobs for each implementation's
// leader: a unique job whose key another live job holds, two unique jobs
// sharing a key with none live, and a job that isn't unique. Like Go's
// scheduler, the leader must discard the conflicting job and the later of
// the two duplicates, marking each with `unique_key_conflict`, and make the
// others available.
func verifySchedulerUniqueConflictDiscard(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const queue = "scheduler_discard"
	uniqueOpts := map[string]any{
		"max_attempts": 3, "queue": queue,
		"unique": map[string]any{"by_args": true, "by_state": []string{"available", "pending", "running", "scheduled"}},
	}
	insertRetryable := func(label string, opts map[string]any) normalizedJob {
		t.Helper()

		var job normalizedJob
		goAdapter.call(t, "insert", map[string]any{"behavior": "error", "message": label, "opts": opts}, &job)
		return waitForJobStateWithin(t, goAdapter, job.ID, []string{"retryable"}, 30*time.Second)
	}

	outcomes := make(map[string]map[string]schedulerOutcome)
	for _, leader := range []*adapter{goAdapter, candidateAdapter} {
		goAdapter.call(t, "reset", map[string]any{}, nil)
		// The retry delay exceeds Go's default scheduler interval, so the
		// retries stay retryable until a scheduler makes them due.
		goAdapter.call(t, "start", map[string]any{
			"client_id": "scheduler-discard-setup", "leader_election_disabled": true, "max_workers": 1,
			"queue": queue, "retry_delay_ms": 5_500,
		}, nil)
		jobs := map[string]normalizedJob{
			"conflict":         insertRetryable("conflict", uniqueOpts),
			"duplicate first":  insertRetryable("duplicate", uniqueOpts),
			"duplicate second": insertRetryable("duplicate", uniqueOpts),
			"not unique":       insertRetryable("not unique", map[string]any{"max_attempts": 3, "queue": queue}),
		}
		goAdapter.call(t, "stop", map[string]any{}, nil)
		require.NotEqual(t, jobs["duplicate first"].ID, jobs["duplicate second"].ID,
			"a retryable job outside its unique states blocked insertion")
		// A live job takes the conflicting job's key. Nothing works its queue.
		var holder normalizedJob
		goAdapter.call(t, "insert", map[string]any{"behavior": "error", "message": "conflict", "opts": uniqueOpts}, &holder)
		require.NotEqual(t, jobs["conflict"].ID, holder.ID)
		require.Equal(t, "available", holder.State)

		latest := time.Time{}
		for _, job := range jobs {
			if scheduledAt := parseTime(t, job.ScheduledAt); scheduledAt.After(latest) {
				latest = scheduledAt
			}
		}
		time.Sleep(time.Until(latest.Add(100 * time.Millisecond)))
		leader.startWithTuning(t, map[string]any{"client_id": "scheduler-discard-leader", "max_workers": 1},
			map[string]any{"elect_interval_ms": 20, "scheduler_interval_ms": 20})
		expectedStates := map[string]string{
			"conflict":         "discarded",
			"duplicate first":  "available",
			"duplicate second": "discarded",
			"not unique":       "available",
		}
		outcomes[leader.name] = make(map[string]schedulerOutcome)
		for label, job := range jobs {
			scheduled := waitForJobStateWithin(t, goAdapter, job.ID, []string{expectedStates[label]}, 30*time.Second)
			outcomes[leader.name][label] = schedulerOutcome{
				Attempt:           scheduled.Attempt,
				Finalized:         scheduled.FinalizedAt != nil,
				State:             scheduled.State,
				UniqueKeyConflict: scheduled.Metadata["unique_key_conflict"],
			}
		}
		leader.call(t, "stop", map[string]any{}, nil)
		var unchanged normalizedJob
		goAdapter.call(t, "get", map[string]any{"id": holder.ID}, &unchanged)
		require.Equal(t, "available", unchanged.State, "%s's scheduler changed the live job holding the key", leader.name)
	}

	reference := outcomes[goAdapter.name]
	require.Equal(t, "scheduler_discarded", reference["conflict"].UniqueKeyConflict)
	require.True(t, reference["conflict"].Finalized)
	require.Equal(t, "scheduler_discarded", reference["duplicate second"].UniqueKeyConflict)
	require.Nil(t, reference["duplicate first"].UniqueKeyConflict)
	require.Equal(t, reference, outcomes[candidateAdapter.name],
		"%s's scheduler and Go's left due retries differently", candidateAdapter.name)
}
