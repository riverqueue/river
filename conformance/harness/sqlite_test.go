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

func TestMixedSQLiteConformance(t *testing.T) {
	t.Parallel()
	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteStorage)

	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance.sqlite")
	goAdapter := startAdapterForBackend(
		t, repositoryRoot, databaseURL, "sqlite", "go", "go", "run", "./internal/cmd/riverconformanceadapter",
	)
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateAdapter := startAdapterCommandForBackend(
		t, repositoryRoot, databaseURL, "sqlite", candidateSpec.Implementation, candidateSpec.Command,
	)

	var profile adapterProfile
	profileBytes, err := os.ReadFile(filepath.Join(
		repositoryRoot, "conformance/adapter/profiles/sqlite.json",
	))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(profileBytes, &profile))
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
		require.Equal(t, profile.Backend, handshake.Backend)
		require.Equal(t, profile.Name, handshake.Profile)
		require.Equal(t, profile.ProtocolRevision, handshake.ProtocolRevision)
		require.Equal(t, profile.Capabilities, handshake.Capabilities)
		require.Equal(t, profile.Methods, handshake.Methods)
		require.Equal(t, map[string]int{"main": 7}, handshake.MigrationLines)
	}
	scenarios.pass("sqlite_profile_handshake")

	verifyDeterministicControls(t, repositoryRoot, goAdapter, candidateAdapter)
	verifyUniqueKeyGoldens(t, repositoryRoot, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_deterministic_retry_unique")
	verifySQLiteMigrations(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_migration_cross_language")
	verifySQLiteCrossLanguageInsertion(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_insert_get_unique_cross_language")
	verifyBatchInsertion(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_batch_atomicity")
	verifyDifferentialCRUD(t, goAdapter, candidateAdapter, false)
	scenarios.pass("sqlite_job_crud")
	verifyUnsafeInt64JobIDs(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_unsafe_int64_job_ids_rpc_list_cursors")
	verifySQLiteTransactions(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_transactions")
	verifySQLiteTimestampEncoding(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_timestamp_rounding_ordering")
}

func TestMixedSQLiteRuntimeConformance(t *testing.T) {
	t.Parallel()
	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteRuntime)

	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance-runtime.sqlite")
	const profileName = "sqlite-runtime-v1"
	goAdapter := startAdapterForProfile(
		t, repositoryRoot, databaseURL, "sqlite", profileName,
		"go", "go", "run", "./internal/cmd/riverconformanceadapter",
	)
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateAdapter := startAdapterCommandForProfile(
		t, repositoryRoot, databaseURL, "sqlite", profileName,
		candidateSpec.Implementation, candidateSpec.Command,
	)

	var profile adapterProfile
	profileBytes, err := os.ReadFile(filepath.Join(
		repositoryRoot, "conformance/adapter/profiles/sqlite-runtime.json",
	))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(profileBytes, &profile))
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
		require.Equal(t, profile.Backend, handshake.Backend)
		require.Equal(t, profile.Name, handshake.Profile)
		require.Equal(t, profile.ProtocolRevision, handshake.ProtocolRevision)
		require.Equal(t, profile.Capabilities, handshake.Capabilities)
		require.Equal(t, profile.Methods, handshake.Methods)
		require.Equal(t, map[string]int{"main": 7}, handshake.MigrationLines)
	}
	scenarios.pass("sqlite_runtime_profile_handshake")

	goAdapter.call(t, "migrate", map[string]any{}, nil)
	verifySQLiteCrossLanguageWork(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_cross_language_work")
	verifyExternalTerminalCompletionRace(t, goAdapter, candidateAdapter)
	verifyExternalTerminalCompletionRace(t, candidateAdapter, goAdapter)
	scenarios.pass("sqlite_runtime_external_terminal_completion_race")
	verifySQLiteUnknownKind(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_unknown_kind_error")
	verifySQLiteAttemptedByHistory(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_attempted_by_ordering")
	verifySQLiteCompetingWorkers(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_competing_workers")
	verifySQLiteQueues(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_queue_crud_reconfigure_pause")
	verifyNotificationWakeups(t, goAdapter, candidateAdapter)
	scenarios.pass(
		"sqlite_runtime_notification_wakeups",
		"sqlite_runtime_remote_cancellation",
	)
	verifyRemoteQueueSubscriptionEvents(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_remote_queue_subscription_events")
	verifySQLiteTransactionalNotification(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_transactional_notification")
	verifySQLiteLeadershipFailover(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_leadership_failover")
	verifySQLitePeriodicScheduler(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_periodic_scheduler")
	for _, adapter := range []*adapter{goAdapter, candidateAdapter} {
		verifySQLiteAdvancedRuntime(t, adapter)
	}
	scenarios.pass("sqlite_runtime_extensions_resumable_subscriptions")
	verifyResumableValidation(t, goAdapter)
	verifyResumableValidation(t, candidateAdapter)
	verifyResumableInteroperability(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_resumable_cross_engine_cursor", "sqlite_runtime_resumable_validation")
	verifySQLitePollOnly(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_poll_only_recovery")
	verifySQLiteLifecycle(t, goAdapter, candidateAdapter)
	scenarios.pass("sqlite_runtime_lifecycle_shutdown")
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

	verifyLeadershipRequestLifecycle(t, goAdapter, candidateAdapter)

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
		worker.call(t, "start", map[string]any{
			"client_id": worker.name + "-sqlite-maintenance", "instrumented": true,
			"max_workers": 1, "periodic_run_on_start": true, "scheduler_interval_ms": 20,
		}, nil)
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
	verifyTransactionalCRUD(t, goAdapter, candidateAdapter)
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
		var job normalizedJob
		worker.call(t, "insert", map[string]any{
			"message": "SQLite dynamic queue", "opts": map[string]any{"queue": "dynamic"},
		}, &job)
		time.Sleep(100 * time.Millisecond)
		worker.call(t, "get", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "available", job.State)
		worker.call(t, "queue_resume", map[string]any{"name": "dynamic"}, nil)
		worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
		require.Equal(t, "completed", job.State)
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
		// Let any incorrectly early outbox notification propagate. Querying the
		// running observer while the other adapter holds SQLite's write lock can
		// starve its one-connection runtime pool; the 60-second poll interval and
		// prompt post-commit completion below prove commit-bound delivery.
		time.Sleep(100 * time.Millisecond)
		startedAt := time.Now()
		pair.controller.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		waitForListedJobCount(t, pair.worker, map[string]any{
			"states": []string{"completed"}, "tags_all": []string{tag},
		}, 2)
		require.Less(t, time.Since(startedAt), 5*time.Second)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifySQLiteUnknownKind(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var unknown normalizedJob
		pair.inserter.call(t, "raw_insert_no_notify", map[string]any{
			"kind": "conformance_unregistered", "message": "must fail compatibly",
			"opts": map[string]any{"max_attempts": 1},
		}, &unknown)
		workerID := pair.worker.name + "-sqlite-unknown-kind"
		pair.worker.call(t, "start", map[string]any{
			"client_id": workerID, "max_workers": 1,
		}, nil)
		pair.worker.call(t, "wait", map[string]any{
			"id": unknown.ID, "states": []string{"discarded"},
		}, &unknown)
		require.Equal(t, "discarded", unknown.State)
		require.Equal(t, 1, unknown.Attempt)
		require.Equal(t, []string{workerID}, unknown.AttemptedBy)
		require.Len(t, unknown.Errors, 1)
		require.Equal(t,
			"job kind is not registered in the client's Workers bundle: conformance_unregistered",
			unknown.Errors[0].Error,
		)
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
		require.Contains(t, pair.observer.callError(t, "get", map[string]any{
			"id": inserted.ID,
		}), "not found")
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
		require.Contains(t, pair.observer.callError(t, "get", map[string]any{
			"id": inserted.ID,
		}), "not found")

		handle = "sqlite-batch-error-" + pair.actor.name
		tag := strings.ReplaceAll(handle, "-", "_")
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		require.NotEmpty(t, pair.actor.callError(t, "tx_insert_many", map[string]any{
			"handle": handle,
			"jobs": []map[string]any{
				{"message": "must not partially commit", "opts": map[string]any{"tags": []string{tag}}},
				{"message": "invalid", "opts": map[string]any{"priority": 99}},
			},
		}))
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs)
	}
}
