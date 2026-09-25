//go:build riverconformance

package harness_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// engine is one implementation participating in a multi-engine tier.
type engine struct {
	adapter  *adapter
	clientID string
	spec     adapterSpec
}

// multiEngineSpecs returns the reference and every configured candidate:
// the ordinary candidate descriptor plus one or more peer descriptors. At
// least two distinct candidates are required so the tier cannot degrade into
// a duplicated pairwise test.
func multiEngineSpecs(t *testing.T, root string, release bool) []adapterSpec {
	t.Helper()

	peers := conformancePeerSpecs(t, root, release)
	specs := make([]adapterSpec, 0, 2+len(peers))
	specs = append(specs, referenceSpec(), conformanceCandidateSpec(t, root, release))
	specs = append(specs, peers...)
	implementations := make(map[string]bool, len(specs))
	applicationNames := make(map[string]bool, len(specs))
	for _, spec := range specs {
		require.False(t, implementations[spec.Implementation],
			"multi-engine tiers need distinct implementations; %q appears twice (set RIVER_CONFORMANCE_PEER or RIVER_CONFORMANCE_PEER_FILE)", spec.Implementation)
		require.False(t, applicationNames[spec.ApplicationName],
			"multi-engine tiers need distinct application names; %q appears twice", spec.ApplicationName)
		implementations[spec.Implementation] = true
		applicationNames[spec.ApplicationName] = true
	}
	require.GreaterOrEqual(t, len(specs), 3, "multi-engine tiers need the reference and at least two candidates")
	return specs
}

func startEngines(t *testing.T, root, databaseURL, suffix string, specs []adapterSpec) []engine {
	t.Helper()

	engines := make([]engine, len(specs))
	for index, spec := range specs {
		name := spec.Implementation + "-" + suffix
		var started *adapter
		if index == 0 {
			started = startReferenceAdapter(t, root, databaseURL, name)
		} else {
			started = startCandidateAdapter(t, root, databaseURL, name, spec, spec.Command)
		}
		engines[index] = engine{adapter: started, clientID: name, spec: spec}
	}
	return engines
}

// candidatePairs returns every ordered pair of distinct non-reference engines.
func candidatePairs(engines []engine) [][2]engine {
	var pairs [][2]engine
	for _, first := range engines[1:] {
		for _, second := range engines[1:] {
			if first.spec.Implementation != second.spec.Implementation {
				pairs = append(pairs, [2]engine{first, second})
			}
		}
	}
	return pairs
}

//nolint:paralleltest // Scenarios share one database and adapter processes, so they run sequentially.
func TestMultiEngineConformance(t *testing.T) {
	// Every engine competes in one externally supplied disposable database,
	// so this test cannot run in parallel with other tiers.
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerMultiEngine)
	root := repoRoot(t)
	engines := startEngines(t, root, databaseURL, "multi-engine", multiEngineSpecs(t, root, false))
	reference := engines[0].adapter
	adapters := make([]*adapter, len(engines))
	clientIDs := make([]string, len(engines))
	adapterByClientID := make(map[string]*adapter, len(engines))
	for index, current := range engines {
		adapters[index] = current.adapter
		clientIDs[index] = current.clientID
		adapterByClientID[current.clientID] = current.adapter
		var handshake adapterHandshake
		current.adapter.call(t, "handshake", map[string]any{}, &handshake)
		require.Equal(t, current.spec.Implementation, handshake.Implementation)
		require.Equal(t, profilePostgresFull, handshake.Profile)
	}
	scenarios.attach(adapters...)
	reference.call(t, "migrate", map[string]any{}, nil)
	startAll := func(t *testing.T) {
		t.Helper()

		reference.call(t, "reset", map[string]any{}, nil)
		for _, current := range engines {
			current.adapter.call(t, "start", map[string]any{
				"client_id": current.clientID, "max_workers": 1,
			}, nil)
		}
	}
	stopAll := func(t *testing.T) {
		t.Helper()

		for _, current := range adapters {
			current.call(t, "stop", map[string]any{}, nil)
		}
	}

	t.Run("multi_engine_competition", func(t *testing.T) {
		defer scenarios.record(t)

		startAll(t)
		jobs := make([]normalizedJob, len(adapters))
		for index, inserter := range adapters {
			inserter.call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 1_000,
				"message": fmt.Sprintf("multi-engine competition %d", index),
			}, &jobs[index])
		}
		workersSeen := make(map[string]bool)
		for _, job := range jobs {
			var running normalizedJob
			reference.call(t, "wait", map[string]any{
				"id": job.ID, "states": []string{"running"},
			}, &running)
			require.Len(t, running.AttemptedBy, 1)
			workersSeen[running.AttemptedBy[0]] = true
		}
		require.ElementsMatch(t, clientIDs, mapKeys(workersSeen), "every engine must claim one blocked job")
		for _, job := range jobs {
			var completed normalizedJob
			reference.call(t, "wait", map[string]any{"id": job.ID}, &completed)
			require.Equal(t, "completed", completed.State)
			require.Equal(t, 1, completed.Attempt)
		}
		stopAll(t)
	})
	t.Run("multi_engine_leader_failover", func(t *testing.T) {
		defer scenarios.record(t)

		startAll(t)
		stopped := make([]string, 0, len(engines)-1)
		leader := waitForLeader(t, reference, "")
		for range len(engines) - 1 {
			require.NotContains(t, stopped, leader, "a stopped engine is still the leader")
			adapterByClientID[leader].call(t, "stop", map[string]any{}, nil)
			stopped = append(stopped, leader)
			leader = waitForLeader(t, reference, leader)
		}
		require.NotContains(t, stopped, leader)
		for _, stoppedID := range stopped {
			adapterByClientID[stoppedID].call(t, "start", map[string]any{
				"client_id": stoppedID, "max_workers": 1,
			}, nil)
		}
		for _, current := range adapters {
			require.Equal(t, leader, readLeader(t, current).LeaderID, "%s disagrees about the leader", current.name)
		}
		stopAll(t)
	})
	t.Run("multi_engine_fault_recovery", func(t *testing.T) {
		defer scenarios.record(t)

		startAll(t)
		for _, target := range engines {
			waitForListener(t, target.adapter)
			var disconnected struct {
				Count int `json:"count"`
			}
			reference.call(t, "fault_disconnect_application", map[string]any{
				"application_name": target.adapter.applicationName,
			}, &disconnected)
			require.Positive(t, disconnected.Count)
			waitForListener(t, target.adapter)
		}
		for index, inserter := range adapters {
			var inserted, completed normalizedJob
			inserter.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("multi-engine fault recovery %d", index),
			}, &inserted)
			reference.call(t, "wait", map[string]any{"id": inserted.ID}, &completed)
			require.Equal(t, "completed", completed.State)
			require.Equal(t, 1, completed.Attempt)
		}
		stopAll(t)
	})
	t.Run("multi_engine_resource_bound", func(t *testing.T) {
		defer scenarios.record(t)

		startAll(t)
		assertMultiEngineConnectionBounds(t, adapters)
		stopAll(t)
	})
	t.Run("multi_engine_directed_candidate_work_notification_cancellation", func(t *testing.T) {
		defer scenarios.record(t)

		for _, pair := range candidatePairs(engines) {
			verifyDirectedCandidateWork(t, reference, pair[0], pair[1])
		}
	})
	t.Run("multi_engine_resumable_cursor", func(t *testing.T) {
		defer scenarios.record(t)

		for _, pair := range candidatePairs(engines) {
			if pair[0].spec.Implementation < pair[1].spec.Implementation {
				verifyResumableInteroperability(t, pair[0].adapter, pair[1].adapter)
			}
		}
	})
	t.Run("multi_engine_process_kill_rescue_failover", func(t *testing.T) {
		defer scenarios.record(t)

		for _, pair := range candidatePairs(engines) {
			verifyCrossEngineProcessKillRescue(t, root, databaseURL, reference, pair[0].spec, pair[1].spec)
		}
	})
}

// verifyDirectedCandidateWork has one candidate insert and cancel work that
// another candidate executes, with the reference only observing. The worker
// polls once a minute, so prompt execution proves the candidates exchange
// notifications directly.
func verifyDirectedCandidateWork(t *testing.T, reference *adapter, controller, worker engine) {
	t.Helper()

	reference.call(t, "reset", map[string]any{}, nil)
	workerID := worker.spec.Implementation + "-directed-worker"
	worker.adapter.call(t, "start", map[string]any{
		"client_id": workerID, "fetch_poll_interval_ms": 60_000, "max_workers": 1,
	}, nil)
	waitForListener(t, worker.adapter)

	startedAt := time.Now()
	var worked normalizedJob
	controller.adapter.call(t, "insert", map[string]any{
		"message": controller.spec.Implementation + " notification to " + worker.spec.Implementation,
	}, &worked)
	reference.call(t, "wait", map[string]any{"id": worked.ID}, &worked)
	require.Equal(t, "completed", worked.State)
	require.Equal(t, []string{workerID}, worked.AttemptedBy)
	require.Less(t, time.Since(startedAt), 5*time.Second,
		"%s did not wake %s through the cross-engine notification path",
		controller.spec.Implementation, worker.spec.Implementation)

	var cancelled normalizedJob
	controller.adapter.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel",
		"message":  controller.spec.Implementation + " cancellation to " + worker.spec.Implementation,
	}, &cancelled)
	reference.call(t, "wait", map[string]any{
		"id": cancelled.ID, "states": []string{"running"},
	}, &cancelled)
	require.Equal(t, []string{workerID}, cancelled.AttemptedBy)
	controller.adapter.call(t, "cancel", map[string]any{"id": cancelled.ID}, &cancelled)
	reference.call(t, "wait", map[string]any{"id": cancelled.ID}, &cancelled)
	require.Equal(t, "cancelled", cancelled.State)
	require.Len(t, cancelled.Errors, 1)
	require.Equal(t, "JobCancelError: job cancelled remotely", cancelled.Errors[0].Error)

	worker.adapter.call(t, "stop", map[string]any{}, nil)
}

// verifyCrossEngineProcessKillRescue kills a disposable crashing process that
// leads and holds a running attempt, then requires a process of another
// implementation to take over leadership, rescue the abandoned attempt, and
// complete it.
func verifyCrossEngineProcessKillRescue(t *testing.T, root, databaseURL string, reference *adapter, crashingSpec, recoverySpec adapterSpec) {
	t.Helper()

	reference.call(t, "reset", map[string]any{}, nil)
	queue := "process_kill_" + crashingSpec.Implementation
	crashingID := crashingSpec.Implementation + "-process-kill"
	crashing := startCandidateAdapter(t, root, databaseURL, crashingID, crashingSpec, crashingSpec.RestartCommand)
	crashing.startWithTuning(t, map[string]any{
		"client_id": crashingID, "max_workers": 1, "queue": queue,
	}, map[string]any{"elect_interval_ms": 20})
	require.Equal(t, crashingID, waitForLeader(t, reference, ""))

	var job normalizedJob
	reference.call(t, "insert", map[string]any{
		"behavior": "sleep", "duration_ms": 1_000,
		"message": "process-kill rescue from " + crashingSpec.Implementation + " to " + recoverySpec.Implementation,
		"opts":    map[string]any{"queue": queue},
	}, &job)
	reference.call(t, "wait", map[string]any{
		"id": job.ID, "states": []string{"running"},
	}, &job)
	require.Equal(t, []string{crashingID}, job.AttemptedBy)
	crashing.kill(t)
	reference.call(t, "fault_expire_leader", map[string]any{}, nil)

	recoveryID := recoverySpec.Implementation + "-process-recovery"
	recovery := startCandidateAdapter(t, root, databaseURL, recoveryID, recoverySpec, recoverySpec.RestartCommand)
	recovery.startWithTuning(t, map[string]any{
		"client_id": recoveryID, "job_timeout_ms": 1_500, "max_workers": 1,
		"queue": queue, "rescue_after_ms": 1_500,
	}, map[string]any{
		"elect_interval_ms": 20, "rescuer_interval_ms": 20, "scheduler_interval_ms": 20,
	})
	require.Equal(t, recoveryID, waitForLeader(t, reference, crashingID))
	reference.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 2, job.Attempt)
	require.Equal(t, []string{crashingID, recoveryID}, job.AttemptedBy)
	recovery.call(t, "stop", map[string]any{}, nil)
}

// TestMultiEngineSQLiteConformance runs the SQLite storage and runtime
// cross-language checks between every pair of configured candidates, without
// the reference, against one shared WAL database per pair.
//
//nolint:paralleltest // Scenarios share one database and adapter processes, so they run sequentially.
func TestMultiEngineSQLiteConformance(t *testing.T) {
	scenarios := newScenarioTracker(t, scenarioOwnerMultiEngineSQLite)
	root := repoRoot(t)
	specs := multiEngineSpecs(t, root, false)

	t.Run("multi_engine_sqlite_candidate_pairs", func(t *testing.T) {
		defer scenarios.record(t)

		for _, first := range specs[1:] {
			for _, second := range specs[1:] {
				if first.Implementation >= second.Implementation {
					continue
				}
				require.True(t, first.servesProfile(profileSQLiteRuntime) && second.servesProfile(profileSQLiteRuntime),
					"%s and %s must both declare %s", first.Implementation, second.Implementation, profileSQLiteRuntime)
				databaseURL := filepath.Join(t.TempDir(), first.Implementation+"-"+second.Implementation+".sqlite")
				firstAdapter := startAdapterCommandForProfile(t, root, databaseURL, "sqlite", profileSQLiteRuntime, first.Implementation, first, first.Command)
				secondAdapter := startAdapterCommandForProfile(t, root, databaseURL, "sqlite", profileSQLiteRuntime, second.Implementation, second, second.Command)
				scenarios.attach(firstAdapter, secondAdapter)
				verifySQLiteCandidatePair(t, firstAdapter, secondAdapter)
			}
		}
	})
}

// verifySQLiteCandidatePair runs the reference-independent SQLite checks
// with two candidates in both roles.
func verifySQLiteCandidatePair(t *testing.T, first, second *adapter) {
	t.Helper()

	pair := mixedPair{candidate: second, reference: first}
	first.call(t, "migrate", map[string]any{}, nil)
	verifySQLiteCrossLanguageInsertion(t, first, second)
	verifyBatchInsertion(t, first, second)
	verifyDifferentialJobCRUD(t, first, second)
	verifyDifferentialListCursors(t, first, second, false)
	verifySQLiteTransactions(t, first, second)
	verifySQLiteTimestampEncoding(t, first, second)
	verifySQLiteCrossLanguageWork(t, first, second)
	verifyUnknownKind(t, first, second)
	verifySQLiteCompetingWorkers(t, first, second)
	pair.eachDirection(func(controller, worker *adapter) {
		verifyInsertNotificationWakeup(t, controller, worker)
		verifyPauseResumeNotification(t, controller, worker)
		verifyRemoteCancelNotification(t, controller, worker)
	})
	verifySQLiteLeadershipFailover(t, first, second)
	verifyResumableInteroperability(t, first, second)
}

func TestMultiEnginePerformanceGate(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	// Every release-built engine shares one externally supplied database, so
	// this test cannot run in parallel.
	requireOptIn(t, "RIVER_CONFORMANCE_MULTI_ENGINE_PERFORMANCE")
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerMultiEnginePerformance)
	jobs := performanceJobs(t)
	root := repoRoot(t)
	engines := startEngines(t, root, databaseURL, "multi-engine-performance", multiEngineSpecs(t, root, true))
	engines[0].adapter.call(t, "migrate", map[string]any{}, nil)
	for _, mode := range []string{"enqueue", "worker", "mixed"} {
		for _, current := range engines {
			_ = runAdapterBenchmark(t, current.adapter, mode, max(20, jobs/10))
		}
		// Each candidate is compared with the slowest of the reference and
		// the other candidates, so the gate catches a candidate that is out
		// of line with the group without requiring every runtime to match
		// the fastest one.
		gateModeWithRetries(t, mode, func() []benchmarkMetrics {
			metrics := make([]benchmarkMetrics, len(engines))
			for index, current := range engines {
				metrics[index] = medianBenchmark(t, current.adapter, mode, jobs)
			}
			return metrics
		}, func(metrics []benchmarkMetrics) []string {
			violations := make([]string, 0, 2*(len(engines)-1))
			for index, candidate := range engines[1:] {
				references := make([]benchmarkMetrics, 0, len(metrics)-1)
				for otherIndex, other := range metrics {
					if otherIndex != index+1 {
						references = append(references, other)
					}
				}
				violations = append(violations, benchmarkViolations(mode, candidate.spec, metrics[index+1], slowestMetrics(references))...)
			}
			return violations
		}, func(metrics []benchmarkMetrics) {
			for index, current := range engines {
				t.Logf("%s %s: %.1f jobs/s p95=%s", mode, current.spec.Implementation, metrics[index].throughput, metrics[index].p95)
			}
		})
	}
	scenarios.pass("multi_engine_release_performance")
}

func TestMultiEngineSoak(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	// Every engine shares one externally supplied database, so this test
	// cannot run in parallel.
	duration, err := time.ParseDuration(requireEnv(t, "RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION"))
	require.NoError(t, err)
	require.Positive(t, duration)
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerMultiEngineSoak)
	root := repoRoot(t)
	engines := startEngines(t, root, databaseURL, "multi-engine-soak", multiEngineSpecs(t, root, false))
	adapters := make([]*adapter, len(engines))
	clientIDs := make([]string, len(engines))
	adapterByClientID := make(map[string]*adapter, len(engines))
	for index, current := range engines {
		adapters[index] = current.adapter
		clientIDs[index] = current.clientID
		adapterByClientID[current.clientID] = current.adapter
	}
	reference := adapters[0]
	reference.call(t, "migrate", map[string]any{}, nil)
	reference.call(t, "reset", map[string]any{}, nil)
	for index, current := range adapters {
		current.call(t, "start", map[string]any{"client_id": clientIDs[index], "max_workers": 8}, nil)
	}

	// Checked after the engines are built and started, so the budget
	// accounts for that setup.
	requireSoakBudget(t, "RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION", duration)
	deadline := time.Now().Add(duration)
	jobsCompleted := 0
	batch := 0
	workersSeen := make(map[string]bool)
	for time.Now().Before(deadline) {
		batchSize := 10 * len(adapters)
		ids := make([]int64, 0, batchSize)
		for index := range batchSize {
			var job normalizedJob
			adapters[index%len(adapters)].call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 5,
				"message": fmt.Sprintf("multi-engine-soak-%d", jobsCompleted+index),
			}, &job)
			ids = append(ids, job.ID)
		}
		for _, id := range ids {
			var job normalizedJob
			reference.call(t, "wait", map[string]any{"id": id}, &job)
			require.Equal(t, "completed", job.State)
			require.Equal(t, 1, job.Attempt)
			require.Len(t, job.AttemptedBy, 1)
			workersSeen[job.AttemptedBy[0]] = true
		}
		jobsCompleted += len(ids)
		batch++
		assertMultiEngineConnectionBounds(t, adapters)
		if batch%10 == 0 {
			leaderID := waitForLeader(t, reference, "")
			leader := adapterByClientID[leaderID]
			require.NotNil(t, leader)
			leader.call(t, "stop", map[string]any{}, nil)
			_ = waitForLeader(t, reference, leaderID)
			leader.call(t, "start", map[string]any{"client_id": leaderID, "max_workers": 8}, nil)
		}
	}
	for _, current := range adapters {
		current.call(t, "stop", map[string]any{}, nil)
	}
	require.ElementsMatch(t, clientIDs, mapKeys(workersSeen), "every engine must work soak jobs")
	t.Logf("completed %d multi-engine jobs over %s", jobsCompleted, duration)
	scenarios.pass("multi_engine_soak")
}

func assertMultiEngineConnectionBounds(t *testing.T, adapters []*adapter) {
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
	require.LessOrEqual(t, total, 20*len(adapters), "multi-engine database connections grew without bound")
}

func performanceJobs(t *testing.T) int {
	t.Helper()

	jobs := 200
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_JOBS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		jobs = parsed
	}
	require.GreaterOrEqual(t, jobs, 20)
	return jobs
}
