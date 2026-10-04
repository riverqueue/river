//go:build riverconformance

package harness_test

import (
	"fmt"
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

// verifyClockBoundaries checks scheduling boundaries across implementations:
// a job scheduled in the future is never attempted before its time, and a
// snooze no longer than the scheduler interval leaves the job available
// with a future scheduled_at that the other implementation's fetch honors.
func verifyClockBoundaries(t *testing.T, inserter, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{"client_id": worker.name + "-clock-boundary", "max_workers": 2}, nil)

	scheduledAt := time.Now().Add(time.Second).UTC()
	var scheduled normalizedJob
	inserter.call(t, "insert", map[string]any{
		"message": "scheduled in the future",
		"opts":    map[string]any{"scheduled_at": scheduledAt.Format(time.RFC3339Nano)},
	}, &scheduled)
	require.Equal(t, "scheduled", scheduled.State)
	scheduled = waitForJobStateWithin(t, worker, scheduled.ID, []string{"completed"}, maintenanceWait)
	require.NotNil(t, scheduled.AttemptedAt)
	require.False(t, parseTime(t, *scheduled.AttemptedAt).Before(parseTime(t, scheduled.ScheduledAt)),
		"attempted at %s before scheduled at %s", *scheduled.AttemptedAt, scheduled.ScheduledAt)

	// A two-second snooze is inside both implementations' default
	// five-second scheduler interval, so the snoozed job stays available
	// with a future scheduled_at.
	const snooze = 2 * time.Second
	var snoozed normalizedJob
	inserter.call(t, "insert", map[string]any{
		"behavior": "snooze_once", "duration_ms": snooze.Milliseconds(), "message": "short snooze boundary",
	}, &snoozed)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		inserter.call(t, "get", map[string]any{"id": snoozed.ID}, &snoozed)
		if snoozed.Metadata["snoozes"] != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.EqualValues(t, 1, snoozed.Metadata["snoozes"])
	if snoozed.State != "completed" && snoozed.State != "running" {
		require.Equal(t, "available", snoozed.State, "a snooze within the scheduler interval stays available")
	}
	snoozedUntil := snoozed.ScheduledAt
	snoozed = waitForJobStateWithin(t, worker, snoozed.ID, []string{"completed"}, 15*time.Second)
	require.NotNil(t, snoozed.AttemptedAt)
	require.False(t, parseTime(t, *snoozed.AttemptedAt).Before(parseTime(t, snoozedUntil)),
		"snoozed job attempted at %s before its snooze ended at %s", *snoozed.AttemptedAt, snoozedUntil)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyStuckJobDetection runs a worker that ignores its timeout's
// cancellation in a disposable process and requires the runtime to report
// the job stuck once the timeout and stuck threshold pass. What happens to
// the stuck attempt afterwards is implementation-specific (Go cannot stop a
// goroutine; other runtimes may abort the task), so only the detection is
// asserted. The process is killed afterwards because Go's worker never
// returns.
func verifyStuckJobDetection(t *testing.T, root, databaseURL string, kind *adapter) {
	t.Helper()

	name := kind.spec.Implementation + "-stuck-detection"
	stuck := startDisposable(t, root, databaseURL, name, kind)
	stuck.call(t, "reset", map[string]any{}, nil)
	stuck.call(t, "start", map[string]any{
		"client_id": name, "job_stuck_threshold_ms": 100, "job_timeout_ms": 50, "max_workers": 1,
	}, nil)
	var job normalizedJob
	stuck.call(t, "insert", map[string]any{"behavior": "ignored_cancel", "message": "stuck detection"}, &job)
	job = waitForJobStateWithin(t, stuck, job.ID, []string{"running"}, 10*time.Second)
	stats := waitForRuntimeStats(t, stuck, func(stats runtimeStats) bool { return stats.StuckJobs > 0 })
	require.Equal(t, 1, stats.StuckJobs)
	require.NotNil(t, job.AttemptedAt)
	stuck.kill(t)
}

// verifyPoolPressure runs far more workers than either implementation's
// database pool holds and requires every job to complete exactly once while
// each adapter's connection count stays bounded throughout.
func verifyPoolPressure(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const (
		jobCount       = 600
		maxConnections = 20
	)
	adapters := []*adapter{goAdapter, candidateAdapter}
	goAdapter.call(t, "reset", map[string]any{}, nil)
	for _, current := range adapters {
		current.call(t, "start", map[string]any{"client_id": current.name + "-pool-pressure", "max_workers": 100}, nil)
	}
	for _, inserter := range adapters {
		jobs := make([]map[string]any, jobCount/2)
		for index := range jobs {
			jobs[index] = map[string]any{
				"behavior": "sleep", "duration_ms": 10, "message": fmt.Sprintf("pool pressure %d", index),
				"opts": map[string]any{"tags": []string{"pool_pressure"}},
			}
		}
		inserter.call(t, "insert_many", map[string]any{"jobs": jobs}, nil)
	}
	deadline := time.Now().Add(60 * time.Second)
	peak := make(map[string]int)
	var completed []normalizedJob
	for time.Now().Before(deadline) {
		for _, current := range adapters {
			var connections struct {
				Count int `json:"count"`
			}
			current.call(t, "connection_count", map[string]any{}, &connections)
			peak[current.name] = max(peak[current.name], connections.Count)
			require.LessOrEqual(t, connections.Count, maxConnections, "%s connections grew under pool pressure", current.name)
		}
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		goAdapter.call(t, "list", map[string]any{
			"limit": jobCount, "states": []string{"completed"}, "tags_all": []string{"pool_pressure"},
		}, &listed)
		if completed = listed.Jobs; len(completed) == jobCount {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Len(t, completed, jobCount)
	for _, job := range completed {
		require.Equal(t, 1, job.Attempt)
		require.Empty(t, job.Errors)
	}
	t.Logf("peak connections under pool pressure: %v", peak)
	for _, current := range adapters {
		current.call(t, "stop", map[string]any{}, nil)
	}
}
