//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// verifyLeaderDeathFailover kills the leading process of one implementation
// and requires the other implementation to take over. Both run the same
// run-on-start periodic job with instrumentation, so the enqueued periodic
// jobs and each engine's periodic-enqueuer starts show that exactly one
// engine runs leader-only maintenance in each term.
func verifyLeaderDeathFailover(t *testing.T, root, databaseURL string, leaderKind, follower *adapter) {
	t.Helper()

	periodicFilter := map[string]any{"metadata": map[string]any{"river:periodic_job_id": "conformance-periodic"}}
	follower.call(t, "reset", map[string]any{}, nil)
	leaderID := leaderKind.spec.Implementation + "-dying-leader"
	leader := startDisposable(t, root, databaseURL, leaderID, leaderKind)
	leader.call(t, "start", map[string]any{
		"client_id": leaderID, "instrumented": true, "max_workers": 1, "periodic_run_on_start": true,
	}, nil)
	require.Equal(t, leaderID, waitForLeader(t, follower, ""))
	waitForRuntimeStats(t, leader, func(stats runtimeStats) bool { return stats.PeriodicStarts == 1 })
	waitForListedJobCount(t, follower, periodicFilter, 1)

	followerID := follower.spec.Implementation + "-surviving-follower"
	follower.call(t, "start", map[string]any{
		"client_id": followerID, "instrumented": true, "max_workers": 1, "periodic_run_on_start": true,
	}, nil)
	// Completing a job gives a follower that wrongly started leader-only
	// maintenance time to show it before the checks below.
	var marker normalizedJob
	follower.call(t, "insert", map[string]any{"message": "follower running"}, &marker)
	follower.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
	stats := waitForRuntimeStats(t, follower, func(runtimeStats) bool { return true })
	require.Zero(t, stats.PeriodicStarts, "a follower ran the leader-only periodic enqueuer")
	require.Equal(t, leaderID, readLeader(t, follower).LeaderID)
	waitForListedJobCount(t, follower, periodicFilter, 1)

	leader.kill(t)
	// The dead leader cannot resign; expiring its lease stands in for the
	// TTL running out.
	follower.call(t, "fault_expire_leader", map[string]any{}, nil)
	require.Equal(t, followerID, waitForLeader(t, follower, leaderID))
	waitForRuntimeStats(t, follower, func(stats runtimeStats) bool { return stats.PeriodicStarts == 1 })
	periodic := waitForListedJobCount(t, follower, periodicFilter, 2)
	for _, job := range periodic {
		require.Equal(t, true, job.Metadata["periodic"])
	}
	// The count stays at one periodic job per term after later work.
	follower.call(t, "insert", map[string]any{"message": "after takeover"}, &marker)
	follower.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
	waitForListedJobCount(t, follower, periodicFilter, 2)
	require.Equal(t, followerID, readLeader(t, follower).LeaderID)
	follower.call(t, "stop", map[string]any{}, nil)
}

// verifyRollingDeployment replaces every engine's process one at a time
// while both implementations keep inserting and working jobs, then requires
// every job to complete exactly once. The engines share a protocol revision
// but run as independently restarted processes, which is the version skew
// a rolling deployment of mixed implementations produces.
func verifyRollingDeployment(t *testing.T, root, databaseURL string, pair mixedPair) {
	t.Helper()

	const jobsPerStep = 20
	pair.reference.call(t, "reset", map[string]any{}, nil)
	type deployment struct {
		adapter *adapter
		kind    *adapter
		version int
	}
	deployments := []*deployment{
		{adapter: startDisposable(t, root, databaseURL, "go-rolling-0", pair.reference), kind: pair.reference},
		{adapter: startDisposable(t, root, databaseURL, pair.candidateSpec.Implementation+"-rolling-0", pair.candidate), kind: pair.candidate},
	}
	clientID := func(current *deployment) string {
		return fmt.Sprintf("%s-rolling-%d", current.kind.spec.Implementation, current.version)
	}
	for _, current := range deployments {
		current.adapter.call(t, "start", map[string]any{"client_id": clientID(current), "max_workers": 4}, nil)
	}
	var ids []int64
	insertBatch := func(step string) {
		for index := range jobsPerStep {
			inserter := deployments[index%len(deployments)].adapter
			var job normalizedJob
			inserter.call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 20, "message": fmt.Sprintf("rolling %s %d", step, index),
				"opts": map[string]any{"tags": []string{"rolling_deployment"}},
			}, &job)
			ids = append(ids, job.ID)
		}
	}
	insertBatch("initial")
	for _, current := range deployments {
		// Stop the old process gracefully, insert while it is gone, then
		// bring up a new process of the same implementation.
		current.adapter.call(t, "stop", map[string]any{}, nil)
		insertBatch(fmt.Sprintf("without-%s-%d", current.kind.spec.Implementation, current.version))
		current.version++
		name := fmt.Sprintf("%s-rolling-%d", current.kind.spec.Implementation, current.version)
		current.adapter = startDisposable(t, root, databaseURL, name, current.kind)
		current.adapter.call(t, "start", map[string]any{"client_id": clientID(current), "max_workers": 4}, nil)
		insertBatch(fmt.Sprintf("with-%s-%d", current.kind.spec.Implementation, current.version))
	}

	completed := waitForListedJobCountWithin(t, pair.reference, map[string]any{
		"limit": len(ids), "states": []string{"completed"}, "tags_all": []string{"rolling_deployment"},
	}, len(ids), 30*time.Second)
	require.ElementsMatch(t, ids, jobIDs(completed))
	workers := make(map[string]int)
	for _, job := range completed {
		require.Equal(t, 1, job.Attempt, "job %d ran more than once", job.ID)
		require.Len(t, job.AttemptedBy, 1)
		require.Empty(t, job.Errors)
		workers[job.AttemptedBy[0]]++
	}
	t.Logf("rolling deployment work split: %v", workers)
	for _, current := range deployments {
		require.Positive(t, workers[clientID(current)], "%s did no work after its replacement", clientID(current))
	}
	leader := waitForLeader(t, pair.reference, "")
	require.Contains(t, []string{clientID(deployments[0]), clientID(deployments[1])}, leader,
		"leadership must end with a replacement process")
	for _, current := range deployments {
		current.adapter.call(t, "stop", map[string]any{}, nil)
	}
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

// verifyDefaultRetrySchedule runs failing jobs under an implementation's
// production default retry policy. The first retry delay (about one second)
// is inside the scheduler interval, so the job stays available and is
// retried at its scheduled time; the second (about sixteen seconds) is not,
// so the job waits as retryable. Both delays must fall within the bounds
// generated from River's Go retry policy.
func verifyDefaultRetrySchedule(t *testing.T, repositoryRoot string, worker, observer *adapter) {
	t.Helper()

	var fixture struct {
		RetryCases []struct {
			ErrorCount int   `json:"error_count"`
			MaxDelayNS int64 `json:"max_delay_ns"`
			MinDelayNS int64 `json:"min_delay_ns"`
		} `json:"retry_cases"`
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/fixtures/protocol_values.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &fixture))
	bounds := func(errorCount int) (time.Duration, time.Duration) {
		for _, retryCase := range fixture.RetryCases {
			if retryCase.ErrorCount == errorCount {
				return time.Duration(retryCase.MinDelayNS), time.Duration(retryCase.MaxDelayNS)
			}
		}
		t.Fatalf("no retry bounds for error count %d", errorCount)
		return 0, 0
	}
	// Timestamps come from the worker's clock and the database; allow for
	// the time between recording the error and scheduling the retry.
	const slack = 250 * time.Millisecond

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{"client_id": worker.name + "-default-retry", "max_workers": 1}, nil)
	var job normalizedJob
	observer.call(t, "insert", map[string]any{
		"behavior": "error", "message": "default retry policy", "opts": map[string]any{"max_attempts": 5},
	}, &job)
	job = waitForJobStateWithin(t, observer, job.ID, []string{"retryable"}, 15*time.Second)
	require.Equal(t, 2, job.Attempt)
	require.Len(t, job.Errors, 2)
	firstMin, firstMax := bounds(1)
	require.NotNil(t, job.AttemptedAt)
	firstDelay := parseTime(t, *job.AttemptedAt).Sub(parseTime(t, job.Errors[0].At))
	require.GreaterOrEqual(t, firstDelay, firstMin-slack, "second attempt started before the first retry delay")
	require.Less(t, firstDelay, firstMax+5*time.Second, "second attempt started long after the first retry delay")
	secondMin, secondMax := bounds(2)
	secondDelay := parseTime(t, job.ScheduledAt).Sub(parseTime(t, job.Errors[1].At))
	require.GreaterOrEqual(t, secondDelay, secondMin-slack)
	require.LessOrEqual(t, secondDelay, secondMax+slack)
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
		inserter.call(t, "insert_many_fast", map[string]any{"jobs": jobs}, nil)
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
