//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest,tparallel // Scenarios share one database and adapter processes, so they run sequentially.
func TestMixedSQLiteConformance(t *testing.T) {
	t.Parallel()

	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteStorage)
	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance.sqlite")
	goAdapter := startReferenceAdapterForProfile(t, repositoryRoot, databaseURL, "sqlite", "", "go")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateSpec.requireProfile(t, profilePortableStorage)
	candidateAdapter := startAdapterCommandForProfile(
		t, repositoryRoot, databaseURL, "sqlite", "", candidateSpec.Implementation, candidateSpec, candidateSpec.Command,
	)
	scenarios.attach(goAdapter, candidateAdapter)
	pair := mixedPair{candidate: candidateAdapter, candidateSpec: candidateSpec, reference: goAdapter}

	t.Run("sqlite_profile_handshake", func(t *testing.T) {
		defer scenarios.record(t)

		verifyProfileHandshakes(t, repositoryRoot, "conformance/adapter/profiles/sqlite.json", candidateSpec, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_deterministic_retry_unique", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDeterministicControls(t, repositoryRoot, goAdapter, candidateAdapter)
		verifyUniqueKeyGoldens(t, repositoryRoot, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_migration_cross_language", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteMigrations(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_insert_get_unique_cross_language", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteCrossLanguageInsertion(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_batch_atomicity", func(t *testing.T) {
		defer scenarios.record(t)

		verifyBatchInsertion(t, goAdapter, candidateAdapter)
		pair.eachDirection(func(actor, observer *adapter) {
			verifyTransactionalBatchInsertion(t, actor, observer, false)
			verifyTransactionalBatchInsertion(t, actor, observer, true)
		})
	})
	t.Run("sqlite_job_crud", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDifferentialJobCRUD(t, goAdapter, candidateAdapter)
		verifyLargeMetadataRoundTrip(t, goAdapter, candidateAdapter)
		verifyBulkDeleteSafety(t, goAdapter, candidateAdapter)
		verifyDifferentialListCursors(t, goAdapter, candidateAdapter, false)
	})
	t.Run("sqlite_unsafe_int64_job_ids_rpc_list_cursors", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUnsafeInt64JobIDs(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_transactions", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteTransactions(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_timestamp_rounding_ordering", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteTimestampEncoding(t, goAdapter, candidateAdapter)
	})
}

//nolint:paralleltest,tparallel // Scenarios share one database and adapter processes, so they run sequentially.
func TestMixedSQLiteRuntimeConformance(t *testing.T) {
	t.Parallel()

	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteRuntime)
	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance-runtime.sqlite")
	const profileName = "sqlite-runtime-v1"
	goAdapter := startReferenceAdapterForProfile(t, repositoryRoot, databaseURL, "sqlite", profileName, "go")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateSpec.requireProfile(t, profileSQLiteRuntime)
	candidateAdapter := startAdapterCommandForProfile(
		t, repositoryRoot, databaseURL, "sqlite", profileName,
		candidateSpec.Implementation, candidateSpec, candidateSpec.Command,
	)
	scenarios.attach(goAdapter, candidateAdapter)
	pair := mixedPair{candidate: candidateAdapter, candidateSpec: candidateSpec, reference: goAdapter}

	t.Run("sqlite_runtime_profile_handshake", func(t *testing.T) {
		defer scenarios.record(t)

		verifyProfileHandshakes(t, repositoryRoot, "conformance/adapter/profiles/sqlite-runtime.json", candidateSpec, goAdapter, candidateAdapter)
	})
	goAdapter.call(t, "migrate", map[string]any{}, nil)
	t.Run("sqlite_runtime_cross_language_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteCrossLanguageWork(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_external_terminal_completion_race", func(t *testing.T) {
		defer scenarios.record(t)

		verifyExternalTerminalCompletionRace(t, goAdapter, candidateAdapter)
		verifyExternalTerminalCompletionRace(t, candidateAdapter, goAdapter)
	})
	t.Run("sqlite_runtime_unknown_kind_error", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUnknownKind(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_attempted_by_ordering", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteAttemptedByHistory(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_competing_workers", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteCompetingWorkers(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_queue_crud_reconfigure_pause", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteQueues(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_notification_wakeups", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) {
			verifyInsertNotificationWakeup(t, controller, worker)
			verifyPauseResumeNotification(t, controller, worker)
		})
	})
	t.Run("sqlite_runtime_remote_cancellation", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) {
			verifyRemoteCancelNotification(t, controller, worker)
			verifyCooperativeRemoteCancellation(t, controller, worker)
		})
	})
	t.Run("sqlite_runtime_remote_queue_subscription_events", func(t *testing.T) {
		defer scenarios.record(t)

		verifyRemoteQueueSubscriptionEvents(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_transactional_notification", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteTransactionalNotification(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_job_list_cursor_interchange", func(t *testing.T) {
		defer scenarios.record(t)

		verifyJobListCursorInterchange(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_leadership_failover", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteLeadershipFailover(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_periodic_scheduler", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLitePeriodicScheduler(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_extensions_resumable_subscriptions", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifySQLiteAdvancedRuntime(t, current) })
	})
	t.Run("sqlite_runtime_resumable_validation", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyResumableValidation(t, current) })
	})
	t.Run("sqlite_runtime_resumable_cross_engine_cursor", func(t *testing.T) {
		defer scenarios.record(t)

		verifyResumableInteroperability(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_poll_only_recovery", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLitePollOnly(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_runtime_lifecycle_shutdown", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteLifecycle(t, goAdapter, candidateAdapter)
	})
}

// verifyProfileHandshakes checks that the reference and candidate advertise
// exactly the named profile's capabilities and methods.
func verifyProfileHandshakes(t *testing.T, repositoryRoot, profilePath string, candidateSpec adapterSpec, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	var profile adapterProfile
	profileBytes, err := os.ReadFile(filepath.Join(repositoryRoot, profilePath))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(profileBytes, &profile))
	manifest := readManifest(t, repositoryRoot)
	for _, testCase := range []struct {
		adapter        *adapter
		implementation string
	}{
		{adapter: goAdapter, implementation: "go"},
		{adapter: candidateAdapter, implementation: candidateSpec.Implementation},
	} {
		var handshake adapterHandshake
		testCase.adapter.call(t, "handshake", map[string]any{}, &handshake)
		require.Equal(t, testCase.implementation, handshake.Implementation)
		require.Equal(t, manifest.Implementations[testCase.implementation].Version, handshake.ImplementationVersion)
		require.Equal(t, profile.Backend, handshake.Backend)
		require.Equal(t, profile.Name, handshake.Profile)
		require.Equal(t, profile.ProtocolRevision, handshake.ProtocolRevision)
		require.Equal(t, profile.Capabilities, handshake.Capabilities)
		require.Equal(t, profile.Methods, handshake.Methods)
		require.Equal(t, map[string]int{manifest.Migration.Line: manifest.Migration.Latest}, handshake.MigrationLines)
	}
	verifyRequestStrictness(t, goAdapter, candidateAdapter)
	contract, err := sharedAdapterContract()
	require.NoError(t, err)
	for method := range contract.methods {
		if !slices.Contains(profile.Methods, method) {
			for _, current := range []*adapter{goAdapter, candidateAdapter} {
				current.requireUnvalidatedCallError(t, method, map[string]any{}, "method_not_found")
			}
		}
	}
}

func verifySQLiteCompetingWorkers(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "start", map[string]any{
		"client_id": "go-sqlite-competitor", "max_workers": 2,
	}, nil)
	candidateAdapter.call(t, "start", map[string]any{
		"client_id": "candidate-sqlite-competitor", "max_workers": 2,
	}, nil)
	const jobCount = 40
	jobs := make([]map[string]any, jobCount)
	for index := range jobs {
		jobs[index] = map[string]any{
			"behavior": "sleep", "duration_ms": 20,
			"message": fmt.Sprintf("SQLite competing worker %d", index),
			"opts":    map[string]any{"tags": []string{"sqlite_competing_workers"}},
		}
	}
	var inserted struct {
		Count int `json:"count"`
	}
	goAdapter.call(t, "insert_many_fast", map[string]any{"jobs": jobs}, &inserted)
	require.Equal(t, jobCount, inserted.Count)
	worked := waitForListedJobCount(t, candidateAdapter, map[string]any{
		"states": []string{"completed"}, "tags_all": []string{"sqlite_competing_workers"},
	}, jobCount)
	workerIDs := make(map[string]bool)
	for _, job := range worked {
		for _, workerID := range job.AttemptedBy {
			workerIDs[workerID] = true
		}
	}
	require.True(t, workerIDs["go-sqlite-competitor"], "Go worker claimed no jobs")
	require.True(t, workerIDs["candidate-sqlite-competitor"], "Candidate worker claimed no jobs")
	goAdapter.call(t, "stop", map[string]any{}, nil)
	candidateAdapter.call(t, "stop", map[string]any{}, nil)
}

func verifySQLiteAdvancedRuntime(t *testing.T, adapter *adapter) {
	t.Helper()

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-sqlite-advanced-runtime", "instrumented": true,
		"max_workers": 2, "retry_delay_ms": 5,
	}, nil)

	var ordinary normalizedJob
	adapter.call(t, "insert", map[string]any{"message": "SQLite extension order"}, &ordinary)
	adapter.call(t, "wait", map[string]any{"id": ordinary.ID}, &ordinary)
	require.Equal(t, "completed", ordinary.State)

	var resumable normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "resumable", "message": "SQLite resumable",
		"opts": map[string]any{"max_attempts": 2},
	}, &resumable)
	adapter.call(t, "wait", map[string]any{"id": resumable.ID}, &resumable)
	require.Equal(t, "completed", resumable.State)
	require.Len(t, resumable.Errors, 1)
	require.Equal(t, "first", resumable.Metadata["river:resumable_step"])

	adapter.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	_ = waitForRuntimeStats(t, adapter, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "queue_paused")
	})
	adapter.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
	stats := waitForRuntimeStats(t, adapter, func(stats runtimeStats) bool {
		return stats.ResumableFirstRuns == 1 && stats.ResumableSecondRuns == 2 &&
			slices.Contains(stats.Events, "job_completed") &&
			slices.Contains(stats.Events, "job_failed") &&
			slices.Contains(stats.Events, "queue_paused") &&
			slices.Contains(stats.Events, "queue_resumed")
	})
	requireOrderedSubsequence(t, stats.Trace, []string{
		"hook:insert_begin",
		"middleware:insert_before",
		"middleware:insert_after",
	})
	requireOrderedSubsequence(t, stats.Trace, []string{
		"hook:work_begin",
		"hook:work_end",
	})
	requireOrderedSubsequence(t, stats.Trace, []string{
		"middleware:work_before",
		"middleware:work_after",
	})
	adapter.call(t, "stop", map[string]any{}, nil)
}

func verifySQLiteAttemptedByHistory(t *testing.T, inserter, worker *adapter) {
	t.Helper()

	inserter.call(t, "reset", map[string]any{}, nil)
	var job normalizedJob
	inserter.call(t, "insert", map[string]any{
		"behavior": "error", "message": "SQLite attempted_by history",
		"opts": map[string]any{"max_attempts": 200},
	}, &job)
	const attemptCount = 102
	workerIDs := make([]string, attemptCount)
	for attempt := range attemptCount {
		workerIDs[attempt] = fmt.Sprintf("%s-sqlite-history-%03d", worker.name, attempt)
		worker.call(t, "start", map[string]any{
			"client_id": workerIDs[attempt], "max_workers": 1, "retry_delay_ms": 60_000,
		}, nil)
		worker.call(t, "wait", map[string]any{
			"id": job.ID, "states": []string{"retryable"},
		}, &job)
		require.Equal(t, attempt+1, job.Attempt)
		worker.call(t, "stop", map[string]any{}, nil)
		if attempt+1 < attemptCount {
			inserter.call(t, "retry", map[string]any{"id": job.ID}, &job)
			require.Equal(t, "available", job.State)
		}
	}
	for _, observer := range []*adapter{inserter, worker} {
		observer.call(t, "get", map[string]any{"id": job.ID}, &job)
		require.Equal(t, workerIDs[attemptCount-100:], job.AttemptedBy)
	}
}

func verifySQLiteCrossLanguageWork(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var inserted, worked normalizedJob
		pair.inserter.call(t, "insert", map[string]any{
			"message": "SQLite cross-language work " + pair.inserter.name,
		}, &inserted)
		pair.worker.call(t, "work", map[string]any{
			"client_id": pair.worker.name + "-sqlite-worker", "id": inserted.ID,
		}, &worked)
		require.Equal(t, "completed", worked.State)
		require.Equal(t, []string{pair.worker.name + "-sqlite-worker"}, worked.AttemptedBy)
	}
}

func verifySQLiteLeadershipFailover(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	verifyLeadershipRequestLifecycle(t, nil, goAdapter, candidateAdapter)

	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "start", map[string]any{
		"client_id": "go-sqlite-leader", "max_workers": 1,
	}, nil)
	candidateAdapter.call(t, "start", map[string]any{
		"client_id": "candidate-sqlite-leader", "max_workers": 1,
	}, nil)
	first := waitForLeader(t, goAdapter, "")
	var leader, follower *adapter
	var followerID string
	if first == "go-sqlite-leader" {
		leader, follower, followerID = goAdapter, candidateAdapter, "candidate-sqlite-leader"
	} else {
		require.Equal(t, "candidate-sqlite-leader", first)
		leader, follower, followerID = candidateAdapter, goAdapter, "go-sqlite-leader"
	}
	leader.call(t, "stop", map[string]any{}, nil)
	require.Equal(t, followerID, waitForLeader(t, follower, first))
	term := readLeader(t, follower)
	follower.call(t, "request_resign", map[string]any{}, nil)
	_ = waitForLeaderTerm(t, follower, term.ElectedAt)
	follower.call(t, "stop", map[string]any{}, nil)
}

func verifySQLiteLifecycle(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, worker := range []*adapter{goAdapter, candidateAdapter} {
		worker.call(t, "reset", map[string]any{}, nil)
		worker.call(t, "start", map[string]any{
			"client_id": worker.name + "-sqlite-lifecycle", "max_workers": 1,
		}, nil)
		var job normalizedJob
		worker.call(t, "insert", map[string]any{
			"behavior": "sleep", "duration_ms": 150, "message": "graceful SQLite shutdown",
		}, &job)
		worker.call(t, "wait", map[string]any{
			"id": job.ID, "states": []string{"running"},
		}, &job)
		worker.call(t, "stop", map[string]any{}, nil)
		worker.call(t, "get", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "completed", job.State)
	}
}

func verifySQLitePeriodicScheduler(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, worker := range []*adapter{goAdapter, candidateAdapter} {
		worker.call(t, "reset", map[string]any{}, nil)
		worker.startWithTuning(t, map[string]any{
			"client_id": worker.name + "-sqlite-maintenance", "instrumented": true,
			"max_workers": 1, "periodic_run_on_start": true,
		}, map[string]any{"scheduler_interval_ms": 20})
		var scheduled normalizedJob
		worker.call(t, "insert", map[string]any{
			"message": "SQLite scheduled job",
			"opts": map[string]any{
				"scheduled_at": time.Now().Add(150 * time.Millisecond).UTC().Format(time.RFC3339Nano),
				"tags":         []string{"sqlite_scheduler"},
			},
		}, &scheduled)
		worker.call(t, "wait", map[string]any{"id": scheduled.ID}, &scheduled)
		require.Equal(t, "completed", scheduled.State)

		periodic := waitForListedJob(t, worker, map[string]any{})
		deadline := time.Now().Add(10 * time.Second)
		for periodic.Metadata["river:periodic_job_id"] != "conformance-periodic" && time.Now().Before(deadline) {
			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			worker.call(t, "list", map[string]any{}, &listed)
			for _, candidate := range listed.Jobs {
				if candidate.Metadata["river:periodic_job_id"] == "conformance-periodic" {
					periodic = candidate
					break
				}
			}
			if periodic.Metadata["river:periodic_job_id"] != "conformance-periodic" {
				time.Sleep(10 * time.Millisecond)
			}
		}
		require.Equal(t, "conformance-periodic", periodic.Metadata["river:periodic_job_id"])
		worker.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
		require.Equal(t, "completed", periodic.State)
		stats := waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
			return stats.PeriodicStarts == 1
		})
		require.Equal(t, 1, stats.PeriodicStarts)
		worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifySQLitePollOnly(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.worker.call(t, "reset", map[string]any{}, nil)
		pair.worker.call(t, "start", map[string]any{
			"client_id": pair.worker.name + "-sqlite-poll-only", "fetch_poll_interval_ms": 20,
			"max_workers": 1, "poll_only": true,
		}, nil)
		var job normalizedJob
		pair.inserter.call(t, "insert", map[string]any{
			"message": "SQLite poll-only recovery " + pair.inserter.name,
		}, &job)
		pair.worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "completed", job.State)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifySQLiteQueues(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		observer *adapter
		writer   *adapter
	}{
		{observer: candidateAdapter, writer: goAdapter},
		{observer: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		pair.writer.call(t, "start", map[string]any{
			"client_id": pair.writer.name + "-sqlite-queue-crud", "max_workers": 1,
		}, nil)
		pair.writer.call(t, "stop", map[string]any{}, nil)
		var observed, updated, written normalizedQueue
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &written)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observed)
		require.Equal(t, written, observed)
		pair.observer.call(t, "queue_update", map[string]any{
			"metadata": map[string]any{"updated_by": pair.observer.name}, "name": "default",
		}, &updated)
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &observed)
		require.Equal(t, updated, observed)
		var queues struct {
			Queues []normalizedQueue `json:"queues"`
		}
		pair.writer.call(t, "queue_list", map[string]any{}, &queues)
		require.Contains(t, queues.Queues, updated)
	}
	verifyTransactionalJobCRUD(t, goAdapter, candidateAdapter)
	verifyTransactionalQueueOperations(t, goAdapter, candidateAdapter)
	for _, worker := range []*adapter{goAdapter, candidateAdapter} {
		worker.call(t, "reset", map[string]any{}, nil)
		worker.call(t, "start", map[string]any{
			"client_id": worker.name + "-sqlite-dynamic-queue", "instrumented": true,
			"max_workers": 1,
		}, nil)
		worker.call(t, "queue_add", map[string]any{"max_workers": 1, "name": "dynamic"}, nil)
		worker.call(t, "queue_add", map[string]any{"max_workers": 2, "name": "dynamic"}, nil)
		var warmup normalizedJob
		worker.call(t, "insert", map[string]any{
			"message": "activate SQLite dynamic queue",
			"opts":    map[string]any{"queue": "dynamic"},
		}, &warmup)
		worker.call(t, "wait", map[string]any{"id": warmup.ID}, &warmup)
		require.Equal(t, "completed", warmup.State)
		worker.call(t, "queue_pause", map[string]any{"name": "dynamic"}, nil)
		_ = waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
			return slices.Contains(stats.Events, "queue_paused")
		})
		// A default-queue marker inserted after the paused job proves the
		// worker kept fetching while the dynamic queue held its job.
		var job, marker normalizedJob
		worker.call(t, "insert", map[string]any{
			"message": "SQLite dynamic queue", "opts": map[string]any{"queue": "dynamic"},
		}, &job)
		worker.call(t, "insert", map[string]any{"message": "SQLite default queue marker"}, &marker)
		worker.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
		require.Equal(t, "completed", marker.State)
		worker.call(t, "get", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "available", job.State)
		worker.call(t, "queue_resume", map[string]any{"name": "dynamic"}, nil)
		var queue normalizedQueue
		worker.call(t, "queue_get", map[string]any{"name": "dynamic"}, &queue)
		worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "completed", job.State)
		require.NotNil(t, job.AttemptedAt)
		require.False(t, parseTime(t, *job.AttemptedAt).Before(parseTime(t, queue.UpdatedAt)),
			"paused dynamic queue job attempted before it resumed")
		worker.call(t, "queue_remove", map[string]any{"name": "dynamic"}, nil)
		worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifySQLiteTransactionalNotification(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		controller *adapter
		worker     *adapter
	}{
		{controller: candidateAdapter, worker: goAdapter},
		{controller: goAdapter, worker: candidateAdapter},
	} {
		pair.worker.call(t, "reset", map[string]any{}, nil)
		pair.worker.call(t, "start", map[string]any{
			"client_id":              pair.worker.name + "-sqlite-transaction-notification",
			"fetch_poll_interval_ms": 60_000, "max_workers": 2,
		}, nil)
		handle := "sqlite-notification-" + pair.controller.name
		tag := strings.ReplaceAll(handle, "-", "_")
		pair.controller.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted struct {
			Results []normalizedInsertResult `json:"results"`
		}
		pair.controller.call(t, "tx_insert_many", map[string]any{
			"handle": handle,
			"jobs": []map[string]any{
				{"message": handle + " first", "opts": map[string]any{"tags": []string{tag}}},
				{"message": handle + " second", "opts": map[string]any{"tags": []string{tag}}},
			},
		}, &inserted)
		require.Len(t, inserted.Results, 2)
		// The worker polls once a minute, so prompt completion after commit
		// proves the committed outbox notification woke it.
		startedAt := time.Now()
		pair.controller.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		waitForListedJobCount(t, pair.worker, map[string]any{
			"states": []string{"completed"}, "tags_all": []string{tag},
		}, 2)
		require.Less(t, time.Since(startedAt), 5*time.Second)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifySQLiteCrossLanguageInsertion(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		observer *adapter
		writer   *adapter
	}{
		{observer: candidateAdapter, writer: goAdapter},
		{observer: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		params := map[string]any{
			"message": "SQLite insertion from " + pair.writer.name,
			"opts": map[string]any{
				"metadata": map[string]any{"writer": pair.writer.name},
				"tags":     []string{"sqlite_cross_language"},
			},
		}
		var inserted, observed normalizedJob
		pair.writer.call(t, "insert", params, &inserted)
		require.NotNil(t, inserted.Errors)
		require.Empty(t, inserted.Errors)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.NotNil(t, observed.Errors)
		require.Equal(t, inserted, observed)

		uniqueParams := map[string]any{
			"message": "SQLite unique from " + pair.writer.name,
			"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
		}
		pair.writer.call(t, "insert", uniqueParams, &inserted)
		pair.observer.call(t, "insert", uniqueParams, &observed)
		require.Equal(t, inserted, observed)
	}
}

func verifySQLiteMigrations(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	type migrationResult struct {
		Applied  []int `json:"applied"`
		Existing []int `json:"existing"`
		Valid    bool  `json:"valid"`
	}
	expectedLatest := []int{1, 2, 3, 4, 5, 6, 7}
	for initializerIndex, initializer := range []*adapter{goAdapter, candidateAdapter} {
		observer := []*adapter{candidateAdapter, goAdapter}[initializerIndex]
		for version := 1; version <= len(expectedLatest); version++ {
			var result migrationResult
			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)

			initializer.call(t, "migrate", map[string]any{
				"direction": "up", "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)
			require.Equal(t, version == len(expectedLatest), result.Valid)

			observer.call(t, "migrate", map[string]any{
				"direction": "down", "dry_run": true, "target_version": version,
			}, &result)
			require.Empty(t, result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)

			observer.call(t, "migrate", map[string]any{}, &result)
			require.Equal(t, expectedLatest[version:], result.Applied)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			var inserted, observed normalizedJob
			observer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("SQLite historical migration %d", version),
			}, &inserted)
			initializer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
			require.Equal(t, inserted, observed)

			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			observer.call(t, "migrate", map[string]any{
				"direction": "down", "dry_run": true, "target_version": version,
			}, &result)
			require.Empty(t, result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)

			observer.call(t, "migrate", map[string]any{}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			observer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)
		}
	}
	var result migrationResult
	goAdapter.call(t, "migrate", map[string]any{}, &result)
	require.Equal(t, expectedLatest, result.Applied)
	require.Equal(t, expectedLatest, result.Existing)
	require.True(t, result.Valid)
}

func verifySQLiteTimestampEncoding(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	goAdapter.call(t, "reset", map[string]any{}, nil)
	type timestampCase struct {
		expected string
		input    string
		writer   *adapter
	}
	testCases := []timestampCase{
		{expected: "2026-01-02T03:04:05.123Z", input: "2026-01-02T03:04:05.1234Z", writer: goAdapter},
		{expected: "2026-01-02T03:04:05.124Z", input: "2026-01-02T03:04:05.1238Z", writer: candidateAdapter},
	}
	insertedIDs := make([]int64, 0, len(testCases))
	for index, testCase := range testCases {
		var inserted normalizedJob
		testCase.writer.call(t, "insert", map[string]any{
			"message": fmt.Sprintf("SQLite timestamp %d", index),
			"opts": map[string]any{
				"scheduled_at": testCase.input,
				"tags":         []string{"sqlite_timestamps"},
			},
		}, &inserted)
		require.Equal(t, testCase.expected, inserted.ScheduledAt)
		insertedIDs = append(insertedIDs, inserted.ID)
		for _, observer := range []*adapter{goAdapter, candidateAdapter} {
			var observed normalizedJob
			observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
			require.Equal(t, testCase.expected, observed.ScheduledAt)
			var raw struct {
				CreatedAt   string `json:"created_at"`
				ScheduledAt string `json:"scheduled_at"`
			}
			observer.call(t, "raw_job_timestamps", map[string]any{"id": inserted.ID}, &raw)
			require.Equal(t, strings.TrimSuffix(strings.Replace(testCase.expected, "T", " ", 1), "Z"), raw.ScheduledAt)
			_, err := time.Parse("2006-01-02 15:04:05.000", raw.CreatedAt)
			require.NoError(t, err)
		}
	}
	for _, observer := range []*adapter{goAdapter, candidateAdapter} {
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{
			"direction": "asc", "order_by": "scheduled_at", "states": []string{"scheduled"},
			"tags_all": []string{"sqlite_timestamps"},
		}, &listed)
		require.Equal(t, insertedIDs, jobIDs(listed.Jobs))
	}
}

func verifySQLiteTransactions(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		handle := "sqlite-commit-" + pair.actor.name
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted, inTransaction normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job": map[string]any{
				"message": "SQLite transaction commit",
				"opts":    map[string]any{"tags": []string{"sqlite_transaction"}},
			},
		}, &inserted)
		pair.actor.call(t, "tx_get", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, inserted, inTransaction)
		requireJobNotFound(t, pair.observer, inserted.ID)
		pair.actor.call(t, "tx_update", map[string]any{
			"handle": handle, "id": inserted.ID, "output": map[string]any{"committed": true},
		}, &inTransaction)
		pair.actor.call(t, "tx_cancel", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, "cancelled", inTransaction.State)
		pair.actor.call(t, "tx_retry", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, "available", inTransaction.State)
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_list", map[string]any{
			"handle": handle, "ids": []int64{inserted.ID},
		}, &listed)
		require.Equal(t, []normalizedJob{inTransaction}, listed.Jobs)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		var observed normalizedJob
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inTransaction, observed)

		handle = "sqlite-rollback-" + pair.actor.name
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle, "job": map[string]any{"message": "SQLite transaction rollback"},
		}, &inserted)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		requireJobNotFound(t, pair.observer, inserted.ID)

		handle = "sqlite-batch-error-" + pair.actor.name
		tag := strings.ReplaceAll(handle, "-", "_")
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.actor.requireCallError(t, "tx_insert_many", map[string]any{
			"handle": handle,
			"jobs": []map[string]any{
				{"message": "must not partially commit", "opts": map[string]any{"tags": []string{tag}}},
				{"message": "invalid", "opts": map[string]any{"priority": 99}},
			},
		}, "rejected")
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs)
	}
}
