//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceApplicationName is the PostgreSQL application_name of the Go
// reference adapter.
const referenceApplicationName = "river-conformance-go"

// TestMixedConformance runs every PostgreSQL scenario between the Go reference
// and the configured candidate. Each registered scenario is its own subtest
// and is credited only by its own assertions.
//
//nolint:paralleltest // Scenarios share one database and adapter processes, so they run sequentially.
func TestMixedConformance(t *testing.T) {
	// The adapters intentionally share one externally supplied disposable
	// database, so this integration test cannot run in parallel with other
	// conformance tiers.
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerMixed)
	repositoryRoot := repoRoot(t)
	observer := newPostgresObserver(t, databaseURL)
	goAdapter := startReferenceAdapter(t, repositoryRoot, databaseURL, "go")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateSpec.requireProfile(t, profilePostgresFull)
	candidateAdapter := startCandidateAdapter(t, repositoryRoot, databaseURL, candidateSpec.Implementation, candidateSpec, candidateSpec.Command)
	scenarios.attach(goAdapter, candidateAdapter)
	pair := mixedPair{candidate: candidateAdapter, candidateSpec: candidateSpec, reference: goAdapter}

	t.Run("adapter_handshake_and_capabilities", func(t *testing.T) {
		defer scenarios.record(t)

		verifyPostgresHandshakes(t, repositoryRoot, candidateSpec, goAdapter, candidateAdapter)
	})
	t.Run("deterministic_retry_clock_rng", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDeterministicControls(t, repositoryRoot, goAdapter, candidateAdapter)
	})
	t.Run("unique_hash_goldens", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUniqueKeyGoldens(t, repositoryRoot, goAdapter, candidateAdapter)
	})
	t.Run("historical_migration_down_up", func(t *testing.T) {
		defer scenarios.record(t)

		verifyHistoricalMigrations(t, readManifest(t, repositoryRoot).Migration.Latest, goAdapter, candidateAdapter)
	})

	// Every following scenario uses the default schema. Scenarios reset River
	// tables themselves, so they do not depend on each other's data.
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	t.Run("reference_migrator_candidate_runtime", func(t *testing.T) {
		defer scenarios.record(t)

		verifyMigratorRuntime(t, goAdapter, candidateAdapter)
	})
	t.Run("candidate_migrator_reference_runtime", func(t *testing.T) {
		defer scenarios.record(t)

		verifyMigratorRuntime(t, candidateAdapter, goAdapter)
	})
	t.Run("reference_insert_candidate_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertThenWork(t, goAdapter, candidateAdapter)
	})
	t.Run("candidate_insert_reference_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertThenWork(t, candidateAdapter, goAdapter)
	})
	t.Run("custom_schema_reference_migrate_candidate_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifyCustomSchema(t, "river_conformance_go_migrated", goAdapter, candidateAdapter)
	})
	t.Run("custom_schema_candidate_migrate_reference_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifyCustomSchema(t, "river_conformance_candidate_migrated", candidateAdapter, goAdapter)
	})
	t.Run("cross_language_unique_conflict", func(t *testing.T) {
		defer scenarios.record(t)

		verifyConcurrentUniqueConflicts(t, observer, goAdapter, candidateAdapter)
	})
	t.Run("typed_batch_insertion", func(t *testing.T) {
		defer scenarios.record(t)

		verifyBatchInsertion(t, goAdapter, candidateAdapter)
		verifyLargeBatchInsertion(t, goAdapter, candidateAdapter)
	})
	t.Run("transactional_batch_insertion", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(actor, observer *adapter) {
			verifyTransactionalBatchInsertion(t, actor, observer, false)
		})
	})
	t.Run("transactional_fast_batch_insertion", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(actor, observer *adapter) {
			verifyTransactionalBatchInsertion(t, actor, observer, true)
		})
	})
	t.Run("fast_insert_both_implementations", func(t *testing.T) {
		defer scenarios.record(t)

		verifyFastInsertion(t, goAdapter, candidateAdapter)
	})
	t.Run("differential_job_crud", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDifferentialJobCRUD(t, goAdapter, candidateAdapter)
	})
	t.Run("bulk_delete_safety", func(t *testing.T) {
		defer scenarios.record(t)

		verifyBulkDeleteSafety(t, goAdapter, candidateAdapter)
	})
	t.Run("differential_job_list_filters_and_cursors", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDifferentialListCursors(t, goAdapter, candidateAdapter, true)
	})
	t.Run("differential_queue_crud", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDifferentialQueueCRUD(t, goAdapter, candidateAdapter)
	})
	t.Run("job_row_round_trip_all_fields", func(t *testing.T) {
		defer scenarios.record(t)

		verifyJobRowRoundTrip(t, goAdapter, candidateAdapter)
	})
	t.Run("unsafe_int64_job_ids_rpc_list_cursors", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUnsafeInt64JobIDs(t, goAdapter, candidateAdapter)
	})
	t.Run("mixed_unknown_kind_error", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUnknownKind(t, goAdapter, candidateAdapter)
	})
	t.Run("transactional_crud_commit_rollback", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionalJobCRUD(t, goAdapter, candidateAdapter)
	})
	t.Run("transactional_queue_operations", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionalQueueOperations(t, goAdapter, candidateAdapter)
	})
	t.Run("transaction_commit_visibility", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionCommitVisibility(t, goAdapter, candidateAdapter)
	})
	t.Run("transaction_rollback_visibility", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionRollbackVisibility(t, goAdapter, candidateAdapter)
	})
	t.Run("transactional_cross_language_cancel", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionalCrossLanguageCancel(t, goAdapter, candidateAdapter)
	})
	t.Run("transaction_abort_rollback_visibility", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionAbortRollback(t, goAdapter, candidateAdapter)
	})
	t.Run("barrier_wait_and_release", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyBarrierWaitAndRelease(t, current) })
	})
	t.Run("single_implementation_worker_outcomes", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyWorkerOutcomes(t, current) })
	})
	t.Run("panic_attempt_trace", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(worker, observer *adapter) { verifyPanicAttemptTrace(t, worker, observer) })
	})
	t.Run("transactional_completion", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyTransactionalCompletion(t, current) })
	})
	t.Run("snooze_once_metadata_transition", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(worker, observer *adapter) { verifySnoozeTransition(t, worker, observer) })
	})
	t.Run("external_terminal_completion_race", func(t *testing.T) {
		defer scenarios.record(t)

		verifyExternalTerminalCompletionRace(t, goAdapter, candidateAdapter)
		verifyExternalTerminalCompletionRace(t, candidateAdapter, goAdapter)
	})
	t.Run("extension_hook_middleware_order", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyExtensionOrder(t, current) })
	})
	t.Run("resumable_retry", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyResumableRetry(t, current) })
	})
	t.Run("dynamic_queue_add_reconfigure_remove", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyDynamicQueues(t, current) })
	})
	t.Run("periodic_run_on_start", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyPeriodicRunOnStart(t, current) })
	})
	t.Run("error_handler_cancel_override", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyErrorHandlerCancel(t, current) })
	})
	t.Run("resumable_validation", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyResumableValidation(t, current) })
	})
	t.Run("resumable_cross_engine_cursor", func(t *testing.T) {
		defer scenarios.record(t)

		verifyResumableInteroperability(t, goAdapter, candidateAdapter)
	})
	t.Run("notification_only_wakeups", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) { verifyInsertNotificationWakeup(t, controller, worker) })
	})
	t.Run("pause_resume_notification", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) { verifyPauseResumeNotification(t, controller, worker) })
	})
	t.Run("remote_cancel_notification", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) { verifyRemoteCancelNotification(t, controller, worker) })
	})
	t.Run("cooperative_remote_cancellation", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(controller, worker *adapter) { verifyCooperativeRemoteCancellation(t, controller, worker) })
	})
	t.Run("remote_queue_subscription_events", func(t *testing.T) {
		defer scenarios.record(t)

		verifyRemoteQueueSubscriptionEvents(t, goAdapter, candidateAdapter)
	})
	t.Run("transactional_insert_notification_commit_only", func(t *testing.T) {
		defer scenarios.record(t)

		verifyTransactionalNotificationWakeups(t, observer, goAdapter, candidateAdapter)
	})
	t.Run("refetched_attempt_cancellation", func(t *testing.T) {
		defer scenarios.record(t)

		verifyRefetchedAttemptCancellation(t, candidateAdapter, goAdapter)
		verifyRefetchedAttemptCancellation(t, goAdapter, candidateAdapter)
	})
	t.Run("timeout_cancellation", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(worker, observer *adapter) { verifyTimeoutCancellation(t, worker, observer) })
	})
	t.Run("completion_batching", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachAdapter(func(current *adapter) { verifyCompletionBatching(t, observer, current) })
	})
	t.Run("mixed_request_resign_terms", func(t *testing.T) {
		defer scenarios.record(t)

		verifyLeadershipRequestLifecycle(t, observer, goAdapter, candidateAdapter)
	})
	t.Run("mixed_leader_failover_both_directions", func(t *testing.T) {
		defer scenarios.record(t)

		verifyGracefulLeaderFailover(t, pair)
	})
	t.Run("listener_backend_disconnect_reconnect", func(t *testing.T) {
		defer scenarios.record(t)

		verifyListenerReconnect(t, pair)
	})
	t.Run("lost_notification_poll_recovery", func(t *testing.T) {
		defer scenarios.record(t)

		pair.eachDirection(func(inserter, worker *adapter) { verifyLostNotificationPollRecovery(t, inserter, worker) })
	})
	t.Run("mixed_skip_locked_competition", func(t *testing.T) {
		defer scenarios.record(t)

		verifySkipLockedCompetition(t, goAdapter, candidateAdapter)
	})
	t.Run("ignored_cancellation_hard_abort", func(t *testing.T) {
		defer scenarios.record(t)

		verifyIgnoredCancellationHardAbort(t, repositoryRoot, databaseURL, pair)
	})
	t.Run("process_kill_restart_and_rescue", func(t *testing.T) {
		defer scenarios.record(t)

		verifyProcessKillRestartAndRescue(t, repositoryRoot, databaseURL, pair)
	})
}

// mixedPair is the reference adapter and one candidate sharing a database.
type mixedPair struct {
	candidate     *adapter
	candidateSpec adapterSpec
	reference     *adapter
}

// eachAdapter runs a single-implementation check against both adapters.
func (pair mixedPair) eachAdapter(check func(current *adapter)) {
	check(pair.reference)
	check(pair.candidate)
}

// eachDirection runs a two-party check with the reference first and then the
// candidate in the first role.
func (pair mixedPair) eachDirection(check func(first, second *adapter)) {
	check(pair.reference, pair.candidate)
	check(pair.candidate, pair.reference)
}

type conformanceManifest struct {
	Capabilities    map[string]string `json:"capabilities"`
	Implementations map[string]struct {
		Version string `json:"version"`
	} `json:"implementations"`
	Migration struct {
		Latest int    `json:"latest"`
		Line   string `json:"line"`
	} `json:"migration"`
	ProtocolRevision int `json:"protocol_revision"`
}

func readManifest(t *testing.T, repositoryRoot string) conformanceManifest {
	t.Helper()

	var manifest conformanceManifest
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/manifest.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &manifest))
	return manifest
}

func verifyPostgresHandshakes(t *testing.T, repositoryRoot string, candidateSpec adapterSpec, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	var goHandshake, candidateHandshake adapterHandshake
	goAdapter.call(t, "handshake", map[string]any{}, &goHandshake)
	candidateAdapter.call(t, "handshake", map[string]any{}, &candidateHandshake)
	manifest := readManifest(t, repositoryRoot)
	expectedCapabilities := make([]string, 0, len(manifest.Capabilities))
	for capability, status := range manifest.Capabilities {
		if status == "complete" {
			expectedCapabilities = append(expectedCapabilities, capability)
		}
	}

	require.Equal(t, "go", goHandshake.Implementation)
	require.Equal(t, candidateSpec.Implementation, candidateHandshake.Implementation)
	require.Equal(t, "postgres", goHandshake.Backend)
	require.Equal(t, goHandshake.Backend, candidateHandshake.Backend)
	require.Equal(t, "postgres-full-v1", goHandshake.Profile)
	require.Equal(t, goHandshake.Profile, candidateHandshake.Profile)
	require.Positive(t, goHandshake.AdapterVersion)
	require.Equal(t, goHandshake.AdapterVersion, candidateHandshake.AdapterVersion)
	require.Equal(t, manifest.Implementations[goHandshake.Implementation].Version,
		goHandshake.ImplementationVersion)
	if candidateSpec.Version != "" {
		require.Equal(t, candidateSpec.Version, candidateHandshake.ImplementationVersion)
	}
	require.Equal(t, manifest.Implementations[candidateHandshake.Implementation].Version,
		candidateHandshake.ImplementationVersion)
	require.Equal(t, manifest.ProtocolRevision, goHandshake.ProtocolRevision)
	require.Equal(t, goHandshake.ProtocolRevision, candidateHandshake.ProtocolRevision)
	require.Equal(t, map[string]int{manifest.Migration.Line: manifest.Migration.Latest}, goHandshake.MigrationLines)
	require.Equal(t, goHandshake.MigrationLines, candidateHandshake.MigrationLines)
	require.ElementsMatch(t, expectedCapabilities, goHandshake.Capabilities)
	require.ElementsMatch(t, goHandshake.Capabilities, candidateHandshake.Capabilities)
	var adapterContract struct {
		AdapterVersion int `json:"adapter_version"`
		Methods        []struct {
			Name string `json:"name"`
		} `json:"methods"`
		ProtocolRevision int `json:"protocol_revision"`
	}
	contractBytes, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/adapter/contract.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contractBytes, &adapterContract))
	expectedMethods := make([]string, len(adapterContract.Methods))
	for index, method := range adapterContract.Methods {
		expectedMethods[index] = method.Name
	}
	require.Equal(t, adapterContract.AdapterVersion, goHandshake.AdapterVersion)
	require.Equal(t, adapterContract.ProtocolRevision, goHandshake.ProtocolRevision)
	require.Equal(t, expectedMethods, goHandshake.Methods)
	require.Equal(t, goHandshake.Methods, candidateHandshake.Methods)
}

// verifyMigratorRuntime rebuilds the default schema with one implementation's
// migrator and then runs the other implementation's worker runtime on it.
func verifyMigratorRuntime(t *testing.T, migrator, runtime *adapter) {
	t.Helper()

	type migrationResult struct {
		Existing []int `json:"existing"`
		Valid    bool  `json:"valid"`
	}
	var result migrationResult
	migrator.call(t, "migrate", map[string]any{"direction": "down", "target_version": -1}, &result)
	require.Empty(t, result.Existing)
	migrator.call(t, "migrate", map[string]any{}, &result)
	require.True(t, result.Valid)
	runtime.call(t, "reset", map[string]any{}, nil)

	clientID := runtime.name + "-runtime-on-" + migrator.name + "-schema"
	var inserted, worked normalizedJob
	runtime.call(t, "insert", map[string]any{"message": "runtime on " + migrator.name + " migrations"}, &inserted)
	runtime.call(t, "work", map[string]any{"client_id": clientID, "id": inserted.ID}, &worked)
	require.Equal(t, "completed", worked.State)
	require.Equal(t, []string{clientID}, worked.AttemptedBy)
}

// verifyInsertThenWork inserts with one implementation and works the job
// with the other, comparing every normalized field in between.
func verifyInsertThenWork(t *testing.T, inserter, worker *adapter) {
	t.Helper()

	inserter.call(t, "reset", map[string]any{}, nil)
	var inserted, observed normalizedJob
	inserter.call(t, "insert", map[string]any{"message": inserter.name + " to " + worker.name}, &inserted)
	require.Equal(t, "available", inserted.State)
	require.Equal(t, "conformance_echo", inserted.Kind)
	require.Equal(t, 0, inserted.Attempt)
	require.Empty(t, inserted.AttemptedBy)
	worker.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
	require.Equal(t, inserted, observed)

	clientID := worker.name + "-conformance-adapter"
	worker.call(t, "work", map[string]any{"client_id": clientID, "id": inserted.ID}, &observed)
	require.Equal(t, "completed", observed.State)
	require.Equal(t, 1, observed.Attempt)
	require.Equal(t, []string{clientID}, observed.AttemptedBy)
	require.NotNil(t, observed.AttemptedAt)
	require.NotNil(t, observed.FinalizedAt)

	var fromInserter normalizedJob
	inserter.call(t, "get", map[string]any{"id": inserted.ID}, &fromInserter)
	require.Equal(t, observed, fromInserter)
}

func verifyTransactionCommitVisibility(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct{ actor, observer *adapter }{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		handle := pair.actor.name + "-commit"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted, observed normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job":    map[string]any{"message": "transaction commit"},
		}, &inserted)
		pair.actor.call(t, "tx_get", map[string]any{"handle": handle, "id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)
		requireJobNotFound(t, pair.observer, inserted.ID)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)
	}
}

func verifyTransactionRollbackVisibility(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct{ actor, observer *adapter }{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		handle := pair.actor.name + "-rollback"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted, observed normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job":    map[string]any{"message": "transaction rollback"},
		}, &inserted)
		pair.actor.call(t, "tx_get", map[string]any{"handle": handle, "id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		requireJobNotFound(t, pair.observer, inserted.ID)
		requireJobNotFound(t, pair.actor, inserted.ID)
	}
}

func verifyTransactionalCrossLanguageCancel(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct{ canceller, inserter *adapter }{
		{canceller: candidateAdapter, inserter: goAdapter},
		{canceller: goAdapter, inserter: candidateAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var cancellable, observed normalizedJob
		pair.inserter.call(t, "insert", map[string]any{"message": "transactional cancellation"}, &cancellable)
		handle := pair.canceller.name + "-cancel"
		pair.canceller.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.canceller.call(t, "tx_cancel", map[string]any{"handle": handle, "id": cancellable.ID}, &observed)
		require.Equal(t, "cancelled", observed.State)
		require.NotNil(t, observed.FinalizedAt)
		pair.inserter.call(t, "get", map[string]any{"id": cancellable.ID}, &observed)
		require.Equal(t, "available", observed.State)
		pair.canceller.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.inserter.call(t, "get", map[string]any{"id": cancellable.ID}, &observed)
		require.Equal(t, "cancelled", observed.State)
		require.NotNil(t, observed.FinalizedAt)
	}
}

// verifyTransactionAbortRollback aborts PostgreSQL transaction state and
// proves the work done before the failure is never visible. PostgreSQL rolls
// an aborted transaction back on COMMIT; drivers disagree about whether that
// COMMIT reports an error, so only visibility is portable.
func verifyTransactionAbortRollback(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	goAdapter.call(t, "reset", map[string]any{}, nil)
	for _, transactionAdapter := range []*adapter{goAdapter, candidateAdapter} {
		handle := transactionAdapter.name + "-failed-transaction"
		transactionAdapter.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var failedTxJob normalizedJob
		transactionAdapter.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job":    map[string]any{"message": "must roll back after SQL failure"},
		}, &failedTxJob)
		require.NotEmpty(t, transactionAdapter.callError(t, "tx_fail", map[string]any{"handle": handle}))
		_ = transactionAdapter.callResponse(t, "tx_commit", map[string]any{"handle": handle})
		requireJobNotFound(t, goAdapter, failedTxJob.ID)
		requireJobNotFound(t, candidateAdapter, failedTxJob.ID)
	}
}

func requireJobNotFound(t *testing.T, observer *adapter, id int64) {
	t.Helper()

	require.Contains(t, observer.callError(t, "get", map[string]any{"id": id}), "not found",
		"%s adapter unexpectedly found job %d", observer.name, id)
}
