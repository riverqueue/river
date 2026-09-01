//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	threeEngineCandidateID = "candidate-three-engine"
	threeEngineGoID        = "go-three-engine"
	threeEnginePeerID      = "peer-three-engine"
)

func TestThreeEngineConformance(t *testing.T) {
	// All three adapters intentionally compete in one externally supplied
	// disposable database, so this test cannot run in parallel.
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	root := repoRoot(t)
	candidateSpec := conformanceCandidateSpec(t, root, false)
	peerSpec := conformancePeerSpec(t, root, false)
	requireThreeImplementations(t, candidateSpec, peerSpec)

	adapters := []*adapter{
		startAdapter(t, root, databaseURL, "go-three-engine", "go", "run", "./internal/cmd/riverconformanceadapter"),
		startAdapterCommand(t, root, databaseURL, candidateSpec.Implementation+"-three-engine", candidateSpec.Command),
		startAdapterCommand(t, root, databaseURL, peerSpec.Implementation+"-three-engine", peerSpec.Command),
	}
	specs := []adapterSpec{
		{ApplicationName: "river-conformance-go", Implementation: "go"},
		candidateSpec,
		peerSpec,
	}
	clientIDs := []string{threeEngineGoID, threeEngineCandidateID, threeEnginePeerID}
	adapterByClientID := map[string]*adapter{}
	adapterByImplementation := map[string]*adapter{}
	specByImplementation := map[string]adapterSpec{}
	for index, current := range adapters {
		adapterByClientID[clientIDs[index]] = current
		adapterByImplementation[specs[index].Implementation] = current
		specByImplementation[specs[index].Implementation] = specs[index]
		var handshake adapterHandshake
		current.call(t, "handshake", map[string]any{}, &handshake)
		require.Equal(t, specs[index].Implementation, handshake.Implementation)
		require.Equal(t, "postgres-full-v1", handshake.Profile)
	}

	goAdapter := adapters[0]
	goAdapter.call(t, "migrate", map[string]any{}, nil)
	goAdapter.call(t, "reset", map[string]any{}, nil)
	for index, current := range adapters {
		current.call(t, "start", map[string]any{
			"client_id": clientIDs[index], "max_workers": 1,
		}, nil)
	}

	jobs := make([]normalizedJob, len(adapters))
	for index, inserter := range adapters {
		inserter.call(t, "insert", map[string]any{
			"behavior": "sleep", "duration_ms": 1_000,
			"message": fmt.Sprintf("three-engine competition %d", index),
		}, &jobs[index])
	}
	workersSeen := make(map[string]bool)
	for _, job := range jobs {
		var running normalizedJob
		goAdapter.call(t, "wait", map[string]any{
			"id": job.ID, "states": []string{"running"},
		}, &running)
		require.Len(t, running.AttemptedBy, 1)
		workersSeen[running.AttemptedBy[0]] = true
	}
	require.ElementsMatch(t, clientIDs, mapKeys(workersSeen))
	for _, job := range jobs {
		var completed normalizedJob
		goAdapter.call(t, "wait", map[string]any{"id": job.ID}, &completed)
		require.Equal(t, "completed", completed.State)
		require.Equal(t, 1, completed.Attempt)
	}

	firstLeader := waitForLeader(t, goAdapter, "")
	firstAdapter := adapterByClientID[firstLeader]
	require.NotNil(t, firstAdapter)
	firstAdapter.call(t, "stop", map[string]any{}, nil)
	secondLeader := waitForLeader(t, goAdapter, firstLeader)
	secondAdapter := adapterByClientID[secondLeader]
	require.NotNil(t, secondAdapter)
	secondAdapter.call(t, "stop", map[string]any{}, nil)
	thirdLeader := waitForLeader(t, goAdapter, secondLeader)
	require.NotEqual(t, firstLeader, thirdLeader)
	require.NotEqual(t, secondLeader, thirdLeader)
	for _, stoppedID := range []string{firstLeader, secondLeader} {
		adapterByClientID[stoppedID].call(t, "start", map[string]any{
			"client_id": stoppedID, "max_workers": 1,
		}, nil)
	}

	for index, target := range adapters {
		waitForListener(t, target)
		var disconnected struct {
			Count int `json:"count"`
		}
		goAdapter.call(t, "fault_disconnect_application", map[string]any{
			"application_name": specs[index].ApplicationName,
		}, &disconnected)
		require.Positive(t, disconnected.Count)
		waitForListener(t, target)
	}

	for index, inserter := range adapters {
		var inserted, completed normalizedJob
		inserter.call(t, "insert", map[string]any{
			"message": fmt.Sprintf("three-engine fault recovery %d", index),
		}, &inserted)
		goAdapter.call(t, "wait", map[string]any{"id": inserted.ID}, &completed)
		require.Equal(t, "completed", completed.State)
		require.Equal(t, 1, completed.Attempt)
	}
	assertThreeEngineConnectionBounds(t, adapters)
	for _, current := range adapters {
		current.call(t, "stop", map[string]any{}, nil)
	}
	verifyThreeEngineDirectedJavaScriptRust(t, goAdapter, adapterByImplementation)
	verifyThreeEngineProcessKillRescueFailover(t, root, databaseURL, goAdapter, specByImplementation)

	scenarios := newScenarioTracker(t, scenarioOwnerThreeEngine)
	scenarios.pass(
		"three_engine_competition",
		"three_engine_cross_engine_process_kill_rescue_failover",
		"three_engine_directed_javascript_rust_work_notification_cancellation",
		"three_engine_fault_recovery",
		"three_engine_leader_failover",
		"three_engine_resource_bound",
	)
}

func verifyThreeEngineDirectedJavaScriptRust(
	t *testing.T,
	goAdapter *adapter,
	adapterByImplementation map[string]*adapter,
) {
	t.Helper()

	for _, direction := range []struct {
		controller string
		worker     string
	}{
		{controller: "javascript", worker: "rust"},
		{controller: "rust", worker: "javascript"},
	} {
		controller := adapterByImplementation[direction.controller]
		worker := adapterByImplementation[direction.worker]
		require.NotNil(t, controller)
		require.NotNil(t, worker)
		goAdapter.call(t, "reset", map[string]any{}, nil)
		workerID := direction.worker + "-directed-worker"
		worker.call(t, "start", map[string]any{
			"client_id": workerID, "fetch_poll_interval_ms": 60_000, "max_workers": 1,
		}, nil)
		waitForListener(t, worker)

		startedAt := time.Now()
		var worked normalizedJob
		controller.call(t, "insert", map[string]any{
			"message": direction.controller + " notification to " + direction.worker,
		}, &worked)
		goAdapter.call(t, "wait", map[string]any{"id": worked.ID}, &worked)
		require.Equal(t, "completed", worked.State)
		require.Equal(t, []string{workerID}, worked.AttemptedBy)
		require.Less(t, time.Since(startedAt), 5*time.Second,
			"%s did not wake %s through the cross-engine notification path",
			direction.controller, direction.worker)

		var cancelled normalizedJob
		controller.call(t, "insert", map[string]any{
			"behavior": "cooperative_cancel",
			"message":  direction.controller + " cancellation to " + direction.worker,
		}, &cancelled)
		goAdapter.call(t, "wait", map[string]any{
			"id": cancelled.ID, "states": []string{"running"},
		}, &cancelled)
		require.Equal(t, []string{workerID}, cancelled.AttemptedBy)
		controller.call(t, "cancel", map[string]any{"id": cancelled.ID}, &cancelled)
		goAdapter.call(t, "wait", map[string]any{"id": cancelled.ID}, &cancelled)
		require.Equal(t, "cancelled", cancelled.State)
		require.Len(t, cancelled.Errors, 1)
		require.Equal(t, "JobCancelError: job cancelled remotely", cancelled.Errors[0].Error)

		worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifyThreeEngineProcessKillRescueFailover(
	t *testing.T,
	root string,
	databaseURL string,
	goAdapter *adapter,
	specByImplementation map[string]adapterSpec,
) {
	t.Helper()

	for _, direction := range []struct {
		crashing string
		recovery string
	}{
		{crashing: "javascript", recovery: "rust"},
		{crashing: "rust", recovery: "javascript"},
	} {
		crashingSpec, ok := specByImplementation[direction.crashing]
		require.True(t, ok)
		recoverySpec, ok := specByImplementation[direction.recovery]
		require.True(t, ok)
		goAdapter.call(t, "reset", map[string]any{}, nil)

		queue := "three_engine_crash_" + direction.crashing
		crashingID := direction.crashing + "-process-kill"
		crashing := startAdapterCommand(t, root, databaseURL,
			direction.crashing+"-process-kill", crashingSpec.RestartCommand)
		crashing.call(t, "start", map[string]any{
			"client_id": crashingID, "elect_interval_ms": 20, "max_workers": 1,
			"queue": queue,
		}, nil)
		require.Equal(t, crashingID, waitForLeader(t, goAdapter, ""))

		var job normalizedJob
		goAdapter.call(t, "insert", map[string]any{
			"behavior": "sleep", "duration_ms": 1_000,
			"message": "process-kill rescue from " + direction.crashing + " to " + direction.recovery,
			"opts":    map[string]any{"queue": queue},
		}, &job)
		goAdapter.call(t, "wait", map[string]any{
			"id": job.ID, "states": []string{"running"},
		}, &job)
		require.Equal(t, []string{crashingID}, job.AttemptedBy)
		crashing.kill(t)
		goAdapter.call(t, "fault_expire_leader", map[string]any{}, nil)

		recoveryID := direction.recovery + "-process-recovery"
		recovery := startAdapterCommand(t, root, databaseURL,
			direction.recovery+"-process-recovery", recoverySpec.RestartCommand)
		recovery.call(t, "start", map[string]any{
			"client_id": recoveryID, "elect_interval_ms": 20, "job_timeout_ms": 1_500,
			"max_workers": 1, "queue": queue, "rescue_after_ms": 1_500,
			"rescuer_interval_ms": 20, "scheduler_interval_ms": 20,
		}, nil)
		require.Equal(t, recoveryID, waitForLeader(t, goAdapter, crashingID))
		goAdapter.call(t, "wait", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "completed", job.State)
		require.Equal(t, 2, job.Attempt)
		require.Equal(t, []string{crashingID, recoveryID}, job.AttemptedBy)
		recovery.call(t, "stop", map[string]any{}, nil)
	}
}

func TestThreeEnginePerformanceGate(t *testing.T) {
	// All three release adapters intentionally share one externally supplied
	// database, so this test cannot run in parallel.
	if os.Getenv("RIVER_CONFORMANCE_THREE_ENGINE_PERFORMANCE") != "1" {
		t.Skip("RIVER_CONFORMANCE_THREE_ENGINE_PERFORMANCE=1 is required")
	}
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	require.NotEmpty(t, databaseURL)
	jobs := 200
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_JOBS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		jobs = parsed
	}
	require.GreaterOrEqual(t, jobs, 20)

	root := repoRoot(t)
	candidateSpec := conformanceCandidateSpec(t, root, true)
	peerSpec := conformancePeerSpec(t, root, true)
	requireThreeImplementations(t, candidateSpec, peerSpec)
	adapters := []*adapter{
		startAdapter(t, root, databaseURL, "go-three-engine-performance", "go", "run", "./internal/cmd/riverconformanceadapter"),
		startAdapterCommand(t, root, databaseURL, candidateSpec.Implementation+"-three-engine-performance", candidateSpec.Command),
		startAdapterCommand(t, root, databaseURL, peerSpec.Implementation+"-three-engine-performance", peerSpec.Command),
	}
	adapters[0].call(t, "migrate", map[string]any{}, nil)
	for _, mode := range []string{"enqueue", "worker", "mixed"} {
		for _, current := range adapters {
			_ = runAdapterBenchmark(t, current, mode, max(20, jobs/10))
		}
		metrics := make([]benchmarkMetrics, len(adapters))
		for index, current := range adapters {
			runs := make([]benchmarkMetrics, 0, 3)
			for range 3 {
				runs = append(runs, runAdapterBenchmark(t, current, mode, jobs))
			}
			metrics[index] = medianMetrics(runs)
		}
		referenceThroughput := min(metrics[0].throughput, metrics[2].throughput)
		referenceP95 := max(metrics[0].p95, metrics[2].p95)
		gate := benchmarkGateForMode(mode, candidateSpec.Implementation)
		t.Logf("%s: Go %.1f jobs/s p95=%s; %s %.1f jobs/s p95=%s; %s %.1f jobs/s p95=%s",
			mode, metrics[0].throughput, metrics[0].p95,
			candidateSpec.Implementation, metrics[1].throughput, metrics[1].p95,
			peerSpec.Implementation, metrics[2].throughput, metrics[2].p95)
		require.GreaterOrEqual(t, metrics[1].throughput, referenceThroughput*gate.throughputRatio,
			"%s must sustain at least %.0f%% of the slower Go/peer reference in %s",
			candidateSpec.Implementation, gate.throughputRatio*100, mode)
		require.LessOrEqual(t, metrics[1].p95,
			referenceP95*time.Duration(gate.p95Numerator)/time.Duration(gate.p95Denominator),
			"%s p95 exceeds the %.2fx slower Go/peer bound in %s", candidateSpec.Implementation,
			float64(gate.p95Numerator)/float64(gate.p95Denominator), mode)
	}
	newScenarioTracker(t, scenarioOwnerThreeEnginePerformance).pass("three_engine_release_performance")
}

func TestThreeEngineSoak(t *testing.T) {
	// All three adapters intentionally share one externally supplied database,
	// so this test cannot run in parallel.
	durationString := os.Getenv("RIVER_CONFORMANCE_THREE_ENGINE_SOAK_DURATION")
	if durationString == "" {
		t.Skip("RIVER_CONFORMANCE_THREE_ENGINE_SOAK_DURATION is required")
	}
	duration, err := time.ParseDuration(durationString)
	require.NoError(t, err)
	require.Positive(t, duration)
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	require.NotEmpty(t, databaseURL)
	root := repoRoot(t)
	candidateSpec := conformanceCandidateSpec(t, root, false)
	peerSpec := conformancePeerSpec(t, root, false)
	requireThreeImplementations(t, candidateSpec, peerSpec)
	adapters := []*adapter{
		startAdapter(t, root, databaseURL, "go-three-engine-soak", "go", "run", "./internal/cmd/riverconformanceadapter"),
		startAdapterCommand(t, root, databaseURL, candidateSpec.Implementation+"-three-engine-soak", candidateSpec.Command),
		startAdapterCommand(t, root, databaseURL, peerSpec.Implementation+"-three-engine-soak", peerSpec.Command),
	}
	clientIDs := []string{threeEngineGoID, threeEngineCandidateID, threeEnginePeerID}
	adapterByClientID := make(map[string]*adapter, len(adapters))
	adapters[0].call(t, "migrate", map[string]any{}, nil)
	adapters[0].call(t, "reset", map[string]any{}, nil)
	for index, current := range adapters {
		adapterByClientID[clientIDs[index]] = current
		current.call(t, "start", map[string]any{
			"client_id": clientIDs[index], "max_workers": 1,
		}, nil)
	}
	warmupJobs := make([]normalizedJob, len(adapters))
	for index, inserter := range adapters {
		inserter.call(t, "insert", map[string]any{
			"behavior": "sleep", "duration_ms": 1_000,
			"message": fmt.Sprintf("three-engine soak competition %d", index),
		}, &warmupJobs[index])
	}
	workersSeen := make(map[string]bool)
	for _, job := range warmupJobs {
		var running normalizedJob
		adapters[0].call(t, "wait", map[string]any{
			"id": job.ID, "states": []string{"running"},
		}, &running)
		require.Len(t, running.AttemptedBy, 1)
		workersSeen[running.AttemptedBy[0]] = true
	}
	for _, job := range warmupJobs {
		var completed normalizedJob
		adapters[0].call(t, "wait", map[string]any{"id": job.ID}, &completed)
		require.Equal(t, "completed", completed.State)
	}
	for index, current := range adapters {
		current.call(t, "stop", map[string]any{}, nil)
		current.call(t, "start", map[string]any{
			"client_id": clientIDs[index], "max_workers": 8,
		}, nil)
	}

	deadline := time.Now().Add(duration)
	jobsCompleted := 0
	batch := 0
	for time.Now().Before(deadline) {
		ids := make([]int64, 0, 30)
		for index := range 30 {
			var job normalizedJob
			adapters[index%len(adapters)].call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 5,
				"message": fmt.Sprintf("three-engine-soak-%d", jobsCompleted+index),
			}, &job)
			ids = append(ids, job.ID)
		}
		for _, id := range ids {
			var job normalizedJob
			adapters[0].call(t, "wait", map[string]any{"id": id}, &job)
			require.Equal(t, "completed", job.State)
			require.Equal(t, 1, job.Attempt)
			require.Len(t, job.AttemptedBy, 1)
			workersSeen[job.AttemptedBy[0]] = true
		}
		jobsCompleted += len(ids)
		batch++
		assertThreeEngineConnectionBounds(t, adapters)
		if batch%10 == 0 {
			leaderID := waitForLeader(t, adapters[0], "")
			leader := adapterByClientID[leaderID]
			require.NotNil(t, leader)
			leader.call(t, "stop", map[string]any{}, nil)
			_ = waitForLeader(t, adapters[0], leaderID)
			leader.call(t, "start", map[string]any{
				"client_id": leaderID, "max_workers": 8,
			}, nil)
		}
	}
	for _, current := range adapters {
		current.call(t, "stop", map[string]any{}, nil)
	}
	require.ElementsMatch(t, clientIDs, mapKeys(workersSeen))
	t.Logf("completed %d three-engine jobs over %s", jobsCompleted, duration)
	newScenarioTracker(t, scenarioOwnerThreeEngineSoak).pass("three_engine_soak")
}

func assertThreeEngineConnectionBounds(t *testing.T, adapters []*adapter) {
	t.Helper()

	total := 0
	for _, current := range adapters {
		var connections struct {
			Count int `json:"count"`
		}
		current.call(t, "connection_count", map[string]any{}, &connections)
		require.LessOrEqual(t, connections.Count, 20,
			"%s database connections grew without bound", current.name)
		total += connections.Count
	}
	require.LessOrEqual(t, total, 60, "three-engine database connections grew without bound")
}

func conformancePeerSpec(t *testing.T, root string, release bool) adapterSpec {
	t.Helper()

	encoded := os.Getenv("RIVER_CONFORMANCE_PEER")
	descriptorPath := os.Getenv("RIVER_CONFORMANCE_PEER_FILE")
	require.False(t, encoded != "" && descriptorPath != "",
		"set only one of RIVER_CONFORMANCE_PEER or RIVER_CONFORMANCE_PEER_FILE")
	var descriptor []byte
	if encoded != "" {
		descriptor = []byte(encoded)
	} else {
		if descriptorPath == "" {
			descriptorPath = "conformance/adapter/candidates/rust.json"
		}
		if !filepath.IsAbs(descriptorPath) {
			descriptorPath = filepath.Join(root, descriptorPath)
		}
		var err error
		//nolint:gosec // The caller explicitly selects a local peer descriptor.
		descriptor, err = os.ReadFile(descriptorPath)
		require.NoError(t, err)
	}
	var spec adapterSpec
	require.NoError(t, json.Unmarshal(descriptor, &spec))
	require.NotEmpty(t, spec.ApplicationName)
	require.NotEmpty(t, spec.Command)
	require.NotEmpty(t, spec.Implementation)
	if release && len(spec.ReleaseCommand) > 0 {
		spec.Command = append([]string(nil), spec.ReleaseCommand...)
	}
	if len(spec.RestartCommand) == 0 {
		spec.RestartCommand = append([]string(nil), spec.Command...)
	}
	return spec
}

func requireThreeImplementations(t *testing.T, candidate, peer adapterSpec) {
	t.Helper()

	implementations := []string{"go", candidate.Implementation, peer.Implementation}
	sort.Strings(implementations)
	require.Equal(t, []string{"go", "javascript", "rust"}, implementations,
		"three-engine tiers require exactly Go, Rust, and JavaScript descriptors")
	require.NotEqual(t, candidate.ApplicationName, peer.ApplicationName)
}
