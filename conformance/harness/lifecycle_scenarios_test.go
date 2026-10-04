//go:build riverconformance

package harness_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rescuedErrorText is the attempt error River records for a rescued job.
const rescuedErrorText = "Stuck job rescued by JobRescuer"

// maintenanceWait bounds maintenance-driven waits for implementations that
// run with their default intervals: an election retry, a rescuer run, and a
// scheduler run can each take several seconds.
const maintenanceWait = 45 * time.Second

// startDisposable starts a new process of the implementation behind current,
// suitable for being killed.
func startDisposable(t *testing.T, root, databaseURL, name string, current *adapter) *adapter {
	t.Helper()

	if current.spec.Implementation == referenceSpec().Implementation {
		return startReferenceAdapter(t, root, databaseURL, name)
	}
	return startCandidateAdapter(t, root, databaseURL, name, current.spec, current.spec.RestartCommand)
}

// waitForJobStateWithin polls a job until it reaches one of states or the
// timeout elapses. Unlike the adapter's own wait, the bound is chosen by the
// scenario, for transitions driven by default maintenance intervals.
func waitForJobStateWithin(t *testing.T, observer *adapter, id int64, states []string, timeout time.Duration) normalizedJob {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var job normalizedJob
	for time.Now().Before(deadline) {
		observer.call(t, "get", map[string]any{"id": id}, &job)
		if slices.Contains(states, job.State) {
			return job
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %d did not reach %v within %s; last state %s: %+v", id, states, timeout, job.State, job)
	return job
}

// waitUntilRescuable waits until a running attempt is older than the rescue
// horizon, so the next rescuer run must rescue it.
func waitUntilRescuable(t *testing.T, job normalizedJob, rescueAfter time.Duration) {
	t.Helper()

	require.NotNil(t, job.AttemptedAt)
	time.Sleep(time.Until(parseTime(t, *job.AttemptedAt).Add(rescueAfter + 100*time.Millisecond)))
}

// verifyProcessKillCrossEngineRescue kills a process of one implementation
// while it holds a running attempt and requires the other implementation to
// take over leadership, rescue the abandoned attempt, and complete it.
func verifyProcessKillCrossEngineRescue(t *testing.T, root, databaseURL string, crashingKind, recovery *adapter) {
	t.Helper()

	const rescueAfter = 1_500 * time.Millisecond
	recovery.call(t, "reset", map[string]any{}, nil)
	queue := "process_kill_" + crashingKind.spec.Implementation
	crashingID := crashingKind.spec.Implementation + "-killed-worker"
	crashing := startDisposable(t, root, databaseURL, crashingID, crashingKind)
	crashing.call(t, "start", map[string]any{"client_id": crashingID, "max_workers": 1, "queue": queue}, nil)

	var job normalizedJob
	recovery.call(t, "insert", map[string]any{
		"behavior": "sleep", "duration_ms": 1_000, "message": "rescue after " + crashingKind.spec.Implementation + " dies",
		"opts": map[string]any{"queue": queue},
	}, &job)
	job = waitForJobStateWithin(t, recovery, job.ID, []string{"running"}, 10*time.Second)
	require.Equal(t, []string{crashingID}, job.AttemptedBy)
	crashing.kill(t)
	// The killed process cannot resign. Expiring its lease stands in for the
	// lease running out, which would otherwise take the full TTL.
	recovery.call(t, "fault_expire_leader", map[string]any{}, nil)
	waitUntilRescuable(t, job, rescueAfter)

	recoveryID := recovery.spec.Implementation + "-rescuer"
	recovery.startWithTuning(t, map[string]any{
		"client_id": recoveryID, "job_timeout_ms": rescueAfter.Milliseconds(), "max_workers": 1,
		"queue": queue, "rescue_after_ms": rescueAfter.Milliseconds(),
	}, map[string]any{"elect_interval_ms": 20, "rescuer_interval_ms": 20, "scheduler_interval_ms": 20})
	require.Equal(t, recoveryID, waitForLeader(t, recovery, crashingID))
	job = waitForJobStateWithin(t, recovery, job.ID, []string{"cancelled", "completed", "discarded"}, maintenanceWait)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 2, job.Attempt)
	require.Equal(t, []string{crashingID, recoveryID}, job.AttemptedBy)
	require.Len(t, job.Errors, 1)
	require.Equal(t, rescuedErrorText, job.Errors[0].Error)
	require.EqualValues(t, 1, job.Metadata["river:rescue_count"])
	recovery.call(t, "stop", map[string]any{}, nil)
}
