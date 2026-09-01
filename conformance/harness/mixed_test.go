//go:build riverconformance

package harness_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type adapter struct {
	command           *exec.Cmd
	expectedExitError bool
	input             io.WriteCloser
	name              string
	nextID            int
	output            *bufio.Scanner
	stderr            lockedBuffer
}

type adapterSpec struct {
	ApplicationName string   `json:"application_name"`
	Command         []string `json:"command"`
	Implementation  string   `json:"implementation"`
	ReleaseCommand  []string `json:"release_command"`
	RestartCommand  []string `json:"restart_command"`
	Version         string   `json:"version"`
}

type adapterHandshake struct {
	AdapterVersion        int            `json:"adapter_version"`
	Backend               string         `json:"backend"`
	Capabilities          []string       `json:"capabilities"`
	Implementation        string         `json:"implementation"`
	ImplementationVersion string         `json:"implementation_version"`
	Methods               []string       `json:"methods"`
	MigrationLines        map[string]int `json:"migration_lines"`
	Profile               string         `json:"profile"`
	ProtocolRevision      int            `json:"protocol_revision"`
}

type adapterProfile struct {
	Backend          string   `json:"backend"`
	Capabilities     []string `json:"capabilities"`
	Methods          []string `json:"methods"`
	Name             string   `json:"name"`
	ProtocolRevision int      `json:"protocol_revision"`
}

type lockedBuffer struct {
	buffer bytes.Buffer
	mu     sync.Mutex
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	return buffer.buffer.String()
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	return buffer.buffer.Write(data)
}

type rpcResponse struct {
	Error  *rpcError       `json:"error"`
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type normalizedJob struct {
	Args         map[string]any           `json:"args"`
	Attempt      int                      `json:"attempt"`
	AttemptedAt  *string                  `json:"attempted_at"`
	AttemptedBy  []string                 `json:"attempted_by"`
	CreatedAt    string                   `json:"created_at"`
	Errors       []normalizedAttemptError `json:"errors"`
	FinalizedAt  *string                  `json:"finalized_at"`
	ID           int64                    `json:"id"`
	Kind         string                   `json:"kind"`
	MaxAttempts  int                      `json:"max_attempts"`
	Metadata     map[string]any           `json:"metadata"`
	Priority     int                      `json:"priority"`
	Queue        string                   `json:"queue"`
	ScheduledAt  string                   `json:"scheduled_at"`
	State        string                   `json:"state"`
	Tags         []string                 `json:"tags"`
	UniqueKey    *string                  `json:"unique_key"`
	UniqueStates []string                 `json:"unique_states"`
}

type normalizedAttemptError struct {
	At      string `json:"at"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error"`
	Trace   string `json:"trace"`
}

type normalizedInsertResult struct {
	Job                      normalizedJob `json:"job"`
	UniqueSkippedAsDuplicate bool          `json:"unique_skipped_as_duplicate"`
}

type normalizedQueue struct {
	CreatedAt string         `json:"created_at"`
	Metadata  map[string]any `json:"metadata"`
	Name      string         `json:"name"`
	PausedAt  *string        `json:"paused_at"`
	UpdatedAt string         `json:"updated_at"`
}

func TestMixedConformance(t *testing.T) {
	// The adapters intentionally share one externally supplied disposable
	// database, so this integration test cannot run in parallel with other
	// conformance tiers.

	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	scenarios := newScenarioTracker(t, scenarioOwnerMixed)
	repositoryRoot := repoRoot(t)
	goAdapter := startAdapter(t, repositoryRoot, databaseURL, "go", "go", "run", "./internal/cmd/riverconformanceadapter")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateAdapter := startAdapterCommand(t, repositoryRoot, databaseURL, candidateSpec.Implementation, candidateSpec.Command)

	var goHandshake, candidateHandshake adapterHandshake
	goAdapter.call(t, "handshake", map[string]any{}, &goHandshake)
	candidateAdapter.call(t, "handshake", map[string]any{}, &candidateHandshake)
	var manifest struct {
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
	manifestBytes, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/manifest.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	expectedCapabilities := make([]string, 0, len(manifest.Capabilities))
	for capability, status := range manifest.Capabilities {
		require.Equal(t, "complete", status, "manifest capability %s", capability)
		expectedCapabilities = append(expectedCapabilities, capability)
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
	verifyDeterministicControls(t, repositoryRoot, goAdapter, candidateAdapter)
	verifyUniqueKeyGoldens(t, repositoryRoot, goAdapter, candidateAdapter)
	verifyHistoricalMigrations(t, manifest.Migration.Latest, goAdapter, candidateAdapter)

	goAdapter.call(t, "migrate", map[string]any{}, nil)
	goAdapter.call(t, "reset", map[string]any{}, nil)

	var goInserted normalizedJob
	goAdapter.call(t, "insert", map[string]any{"message": "reference to candidate"}, &goInserted)
	require.Equal(t, "available", goInserted.State)
	require.Equal(t, "conformance_echo", goInserted.Kind)

	var candidateObserved normalizedJob
	candidateAdapter.call(t, "get", map[string]any{"id": goInserted.ID}, &candidateObserved)
	require.Equal(t, goInserted, candidateObserved)
	candidateAdapter.call(t, "work", map[string]any{
		"client_id": candidateSpec.Implementation + "-conformance-adapter", "id": goInserted.ID,
	}, &candidateObserved)
	require.Equal(t, "completed", candidateObserved.State)
	require.Equal(t, 1, candidateObserved.Attempt)
	require.Equal(t, []string{candidateSpec.Implementation + "-conformance-adapter"}, candidateObserved.AttemptedBy)

	candidateAdapter.call(t, "reset", map[string]any{}, nil)
	candidateAdapter.call(t, "migrate", map[string]any{}, nil)
	var candidateInserted normalizedJob
	candidateAdapter.call(t, "insert", map[string]any{"message": "candidate to reference"}, &candidateInserted)
	require.Equal(t, "available", candidateInserted.State)

	var goObserved normalizedJob
	goAdapter.call(t, "get", map[string]any{"id": candidateInserted.ID}, &goObserved)
	require.Equal(t, candidateInserted, goObserved)
	goAdapter.call(t, "work", map[string]any{
		"client_id": "go-conformance-adapter", "id": candidateInserted.ID,
	}, &goObserved)
	require.Equal(t, "completed", goObserved.State)
	require.Equal(t, 1, goObserved.Attempt)
	require.Equal(t, []string{"go-conformance-adapter"}, goObserved.AttemptedBy)

	verifyCustomSchemas(t, goAdapter, candidateAdapter)
	verifyConcurrentUniqueConflicts(t, goAdapter, candidateAdapter)
	scenarios.pass("cross_language_unique_conflict")
	verifyBatchInsertion(t, goAdapter, candidateAdapter)
	verifyLargeBatchInsertion(t, goAdapter, candidateAdapter)
	scenarios.pass(
		"transactional_batch_insertion",
		"transactional_fast_batch_insertion",
		"typed_batch_insertion",
	)
	verifyDifferentialCRUD(t, goAdapter, candidateAdapter, true)
	verifyJobRowRoundTrip(t, goAdapter, candidateAdapter)
	verifyUnsafeInt64JobIDs(t, goAdapter, candidateAdapter)
	scenarios.pass("unsafe_int64_job_ids_rpc_list_cursors")
	verifyMixedUnknownKind(t, goAdapter, candidateAdapter)
	verifyTransactionalCRUD(t, goAdapter, candidateAdapter)
	verifySingleImplementationRuntime(t, goAdapter)
	verifySingleImplementationRuntime(t, candidateAdapter)
	verifyExternalTerminalCompletionRace(t, goAdapter, candidateAdapter)
	verifyExternalTerminalCompletionRace(t, candidateAdapter, goAdapter)
	scenarios.pass("external_terminal_completion_race")
	verifyAdvancedRuntime(t, goAdapter)
	verifyAdvancedRuntime(t, candidateAdapter)
	scenarios.pass(
		"dynamic_queue_add_reconfigure_remove",
		"error_handler_cancel_override",
		"extension_hook_middleware_order",
		"periodic_run_on_start",
		"resumable_retry",
	)
	verifyNotificationWakeups(t, goAdapter, candidateAdapter)
	scenarios.pass(
		"cooperative_remote_cancellation",
		"notification_only_wakeups",
		"pause_resume_notification",
		"remote_cancel_notification",
	)
	verifyRemoteQueueSubscriptionEvents(t, goAdapter, candidateAdapter)
	scenarios.pass("remote_queue_subscription_events")
	verifyTransactionalNotificationWakeups(t, goAdapter, candidateAdapter)
	scenarios.pass("transactional_insert_notification_commit_only")
	verifyRefetchedAttemptCancellation(t, candidateAdapter, goAdapter)
	verifyRefetchedAttemptCancellation(t, goAdapter, candidateAdapter)
	scenarios.pass("refetched_attempt_cancellation")
	verifyTimeoutCancellation(t, goAdapter)
	verifyTimeoutCancellation(t, candidateAdapter)
	scenarios.pass("timeout_cancellation")
	verifyCompletionBurst(t, goAdapter)
	verifyCompletionBurst(t, candidateAdapter)
	scenarios.pass("completion_batching")

	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "tx_begin", map[string]any{"handle": "go-commit"}, nil)
	var goTxInserted normalizedJob
	goAdapter.call(t, "tx_insert", map[string]any{
		"handle": "go-commit",
		"job":    map[string]any{"message": "transaction commit"},
	}, &goTxInserted)
	var txObserved normalizedJob
	goAdapter.call(t, "tx_get", map[string]any{"handle": "go-commit", "id": goTxInserted.ID}, &txObserved)
	require.Equal(t, goTxInserted, txObserved)
	require.Contains(t, candidateAdapter.callError(t, "get", map[string]any{"id": goTxInserted.ID}), "not found")
	goAdapter.call(t, "tx_commit", map[string]any{"handle": "go-commit"}, nil)
	candidateAdapter.call(t, "get", map[string]any{"id": goTxInserted.ID}, &txObserved)
	require.Equal(t, goTxInserted, txObserved)

	candidateAdapter.call(t, "tx_begin", map[string]any{"handle": "candidate-rollback"}, nil)
	var candidateTxInserted normalizedJob
	candidateAdapter.call(t, "tx_insert", map[string]any{
		"handle": "candidate-rollback",
		"job":    map[string]any{"message": "transaction rollback"},
	}, &candidateTxInserted)
	candidateAdapter.call(t, "tx_get", map[string]any{"handle": "candidate-rollback", "id": candidateTxInserted.ID}, &txObserved)
	require.Equal(t, candidateTxInserted, txObserved)
	candidateAdapter.call(t, "tx_rollback", map[string]any{"handle": "candidate-rollback"}, nil)
	require.Contains(t, goAdapter.callError(t, "get", map[string]any{"id": candidateTxInserted.ID}), "not found")

	var cancellable normalizedJob
	goAdapter.call(t, "insert", map[string]any{"message": "transactional cancellation"}, &cancellable)
	candidateAdapter.call(t, "tx_begin", map[string]any{"handle": "candidate-cancel"}, nil)
	candidateAdapter.call(t, "tx_cancel", map[string]any{"handle": "candidate-cancel", "id": cancellable.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)
	goAdapter.call(t, "get", map[string]any{"id": cancellable.ID}, &txObserved)
	require.Equal(t, "available", txObserved.State)
	candidateAdapter.call(t, "tx_commit", map[string]any{"handle": "candidate-cancel"}, nil)
	goAdapter.call(t, "get", map[string]any{"id": cancellable.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)

	for _, transactionAdapter := range []*adapter{goAdapter, candidateAdapter} {
		handle := transactionAdapter.name + "-failed-transaction"
		transactionAdapter.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var failedTxJob normalizedJob
		transactionAdapter.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job":    map[string]any{"message": "must roll back after SQL failure"},
		}, &failedTxJob)
		require.Contains(t, transactionAdapter.callError(t, "tx_fail", map[string]any{"handle": handle}), "zero")
		// PostgreSQL rolls an aborted transaction back on COMMIT. pgx reports
		// ErrTxCommitRollback while SQLx treats the server's ROLLBACK command tag
		// as a successful close, so only the visibility result is portable.
		_ = transactionAdapter.callResponse(t, "tx_commit", map[string]any{"handle": handle})
		require.Contains(t, goAdapter.callError(t, "get", map[string]any{"id": failedTxJob.ID}), "not found")
	}

	goAdapter.call(t, "reset", map[string]any{}, nil)
	var fastResult struct {
		Count int `json:"count"`
	}
	candidateAdapter.call(t, "insert_many_fast", map[string]any{"jobs": []map[string]any{
		{"message": "candidate fast one", "opts": map[string]any{"tags": []string{"fast-candidate"}}},
		{"message": "candidate fast two", "opts": map[string]any{"tags": []string{"fast-candidate"}}},
		{"message": "candidate fast pending", "opts": map[string]any{"pending": true, "tags": []string{"fast-candidate"}}},
	}}, &fastResult)
	require.Equal(t, 3, fastResult.Count)
	goAdapter.call(t, "insert_many_fast", map[string]any{"jobs": []map[string]any{
		{"message": "go fast one", "opts": map[string]any{"tags": []string{"fast-go"}}},
		{"message": "go fast two", "opts": map[string]any{"tags": []string{"fast-go"}}},
	}}, &fastResult)
	require.Equal(t, 2, fastResult.Count)
	var listed struct {
		Jobs []normalizedJob `json:"jobs"`
	}
	candidateAdapter.call(t, "list", map[string]any{"kinds": []string{"conformance_echo"}}, &listed)
	require.Len(t, listed.Jobs, 5)

	uniqueParams := map[string]any{
		"message": "cross-language unique",
		"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
	}
	var uniqueGo, uniqueCandidate normalizedJob
	goAdapter.call(t, "insert", uniqueParams, &uniqueGo)
	candidateAdapter.call(t, "insert", uniqueParams, &uniqueCandidate)
	require.Equal(t, uniqueGo, uniqueCandidate)

	verifyLeadershipRequestLifecycle(t, goAdapter, candidateAdapter)

	goAdapter.call(t, "start", map[string]any{
		"client_id": "go-mixed-worker", "max_workers": 4,
	}, nil)
	candidateClientID := candidateSpec.Implementation + "-mixed-worker"
	candidateAdapter.call(t, "start", map[string]any{
		"client_id": candidateClientID, "max_workers": 4,
	}, nil)
	firstLeader := waitForLeader(t, goAdapter, "")
	firstTerm := readLeader(t, goAdapter)
	goAdapter.call(t, "request_resign", map[string]any{}, nil)
	secondTerm := waitForLeaderTerm(t, goAdapter, firstTerm.ElectedAt)
	candidateAdapter.call(t, "request_resign", map[string]any{}, nil)
	thirdTerm := waitForLeaderTerm(t, goAdapter, secondTerm.ElectedAt)

	var leaderAdapter, followerAdapter *adapter
	var leaderID, followerID string
	if thirdTerm.LeaderID == "go-mixed-worker" {
		leaderAdapter, leaderID = goAdapter, "go-mixed-worker"
		followerAdapter, followerID = candidateAdapter, candidateClientID
	} else {
		leaderAdapter, leaderID = candidateAdapter, candidateClientID
		followerAdapter, followerID = goAdapter, "go-mixed-worker"
	}
	leaderAdapter.call(t, "stop", map[string]any{}, nil)
	require.Equal(t, followerID, waitForLeader(t, followerAdapter, leaderID))
	leaderAdapter.call(t, "start", map[string]any{
		"client_id": leaderID, "max_workers": 4,
	}, nil)
	followerAdapter.call(t, "stop", map[string]any{}, nil)
	require.Equal(t, leaderID, waitForLeader(t, leaderAdapter, followerID))
	followerAdapter.call(t, "start", map[string]any{
		"client_id": followerID, "max_workers": 4,
	}, nil)
	require.NotEmpty(t, firstLeader)

	waitForListener(t, goAdapter)
	waitForListener(t, candidateAdapter)
	for _, adapter := range []*adapter{goAdapter, candidateAdapter} {
		var disconnected struct {
			Count int `json:"count"`
		}
		adapter.call(t, "fault_disconnect_listeners", map[string]any{}, &disconnected)
		require.GreaterOrEqual(t, disconnected.Count, 1)
		waitForListener(t, adapter)
	}
	// A replacement backend appearing in pg_stat_activity precedes the
	// notifier's resubscription loop becoming ready by a small interval.
	time.Sleep(500 * time.Millisecond)
	var disconnectedApplication struct {
		Count int `json:"count"`
	}
	goAdapter.call(t, "fault_disconnect_application", map[string]any{
		"application_name": candidateSpec.ApplicationName,
	}, &disconnectedApplication)
	require.GreaterOrEqual(t, disconnectedApplication.Count, 1)
	candidateAdapter.call(t, "fault_disconnect_application", map[string]any{
		"application_name": "river-conformance-go",
	}, &disconnectedApplication)
	require.GreaterOrEqual(t, disconnectedApplication.Count, 1)
	waitForListener(t, goAdapter)
	waitForListener(t, candidateAdapter)
	time.Sleep(500 * time.Millisecond)

	var notificationLost normalizedJob
	goAdapter.call(t, "raw_insert_no_notify", map[string]any{"message": "poll recovery"}, &notificationLost)
	candidateAdapter.call(t, "wait", map[string]any{"id": notificationLost.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)

	var competitionIDs []int64
	for i := range 40 {
		var inserted normalizedJob
		adapter := goAdapter
		if i%2 == 1 {
			adapter = candidateAdapter
		}
		adapter.call(t, "insert", map[string]any{"message": fmt.Sprintf("competition %d", i)}, &inserted)
		competitionIDs = append(competitionIDs, inserted.ID)
	}
	workersSeen := make(map[string]bool)
	for _, id := range competitionIDs {
		var worked normalizedJob
		goAdapter.call(t, "wait", map[string]any{"id": id}, &worked)
		require.Equal(t, "completed", worked.State)
		require.Equal(t, 1, worked.Attempt)
		require.Len(t, worked.AttemptedBy, 1)
		workersSeen[worked.AttemptedBy[0]] = true
	}
	require.ElementsMatch(t, []string{"go-mixed-worker", candidateClientID}, mapKeys(workersSeen))

	var pausedJob normalizedJob
	goAdapter.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	time.Sleep(100 * time.Millisecond)
	candidateAdapter.call(t, "insert", map[string]any{"message": "paused cross-language"}, &pausedJob)
	time.Sleep(100 * time.Millisecond)
	candidateAdapter.call(t, "get", map[string]any{"id": pausedJob.ID}, &txObserved)
	require.Equal(t, "available", txObserved.State)
	candidateAdapter.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
	goAdapter.call(t, "wait", map[string]any{"id": pausedJob.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)

	var remoteCancel normalizedJob
	goAdapter.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "remote cancellation",
	}, &remoteCancel)
	goAdapter.call(t, "wait", map[string]any{"id": remoteCancel.ID, "states": []string{"running"}}, &txObserved)
	candidateAdapter.call(t, "cancel", map[string]any{"id": remoteCancel.ID}, &txObserved)
	goAdapter.call(t, "wait", map[string]any{"id": remoteCancel.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)

	goAdapter.call(t, "stop", map[string]any{}, nil)
	candidateAdapter.call(t, "stop", map[string]any{}, nil)

	goAdapter.call(t, "reset", map[string]any{}, nil)
	stuck := startAdapterCommand(t, repositoryRoot, databaseURL, "candidate-stuck", candidateSpec.RestartCommand)
	stuckClientID := candidateSpec.Implementation + "-stuck-worker"
	stuck.call(t, "start", map[string]any{
		"client_id": stuckClientID, "max_workers": 1, "queue": "ignored",
	}, nil)
	var stuckJob normalizedJob
	goAdapter.call(t, "insert", map[string]any{
		"behavior": "ignored_cancel",
		"message":  "ignored cancellation",
		"opts":     map[string]any{"queue": "ignored"},
	}, &stuckJob)
	goAdapter.call(t, "wait", map[string]any{
		"id": stuckJob.ID, "states": []string{"running"},
	}, &txObserved)
	stuck.call(t, "stop", map[string]any{"cancel": true}, nil)
	goAdapter.call(t, "get", map[string]any{"id": stuckJob.ID}, &txObserved)
	require.Equal(t, "available", txObserved.State)
	require.Equal(t, 0, txObserved.Attempt)

	goAdapter.call(t, "reset", map[string]any{}, nil)
	crashing := startAdapterCommand(t, repositoryRoot, databaseURL, "candidate-crashing", candidateSpec.RestartCommand)
	crashingClientID := candidateSpec.Implementation + "-crashing-worker"
	crashing.call(t, "start", map[string]any{
		"client_id": crashingClientID, "max_workers": 1,
	}, nil)
	var crashJob normalizedJob
	goAdapter.call(t, "insert", map[string]any{
		"behavior": "sleep", "duration_ms": 250, "message": "process death rescue",
	}, &crashJob)
	goAdapter.call(t, "wait", map[string]any{
		"id": crashJob.ID, "states": []string{"running"},
	}, &txObserved)
	crashing.kill(t)
	goAdapter.call(t, "fault_expire_leader", map[string]any{}, nil)

	recovery := startAdapterCommand(t, repositoryRoot, databaseURL, "candidate-recovery", candidateSpec.RestartCommand)
	recoveryClientID := candidateSpec.Implementation + "-recovery-worker"
	recovery.call(t, "start", map[string]any{
		"client_id":             recoveryClientID,
		"elect_interval_ms":     20,
		"job_timeout_ms":        500,
		"max_workers":           1,
		"rescue_after_ms":       500,
		"rescuer_interval_ms":   20,
		"scheduler_interval_ms": 20,
	}, nil)
	recovery.call(t, "wait", map[string]any{"id": crashJob.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)
	require.Equal(t, 2, txObserved.Attempt)
	require.Equal(t, []string{crashingClientID, recoveryClientID}, txObserved.AttemptedBy)
	recovery.call(t, "stop", map[string]any{}, nil)

	scenarios.pass(
		"adapter_handshake_and_capabilities",
		"barrier_wait_and_release",
		"bulk_delete_safety",
		"copy_from_both_implementations",
		"custom_schema_reference_migrate_candidate_work",
		"custom_schema_candidate_migrate_reference_work",
		"deterministic_retry_clock_rng",
		"differential_job_crud",
		"differential_job_list_filters_and_cursors",
		"differential_queue_crud",
		"reference_insert_candidate_work",
		"reference_migrator_candidate_runtime",
		"historical_migration_down_up",
		"ignored_cancellation_hard_abort",
		"job_row_round_trip_all_fields",
		"listener_backend_disconnect_reconnect",
		"lost_notification_poll_recovery",
		"mixed_leader_failover_both_directions",
		"mixed_request_resign_terms",
		"mixed_skip_locked_competition",
		"mixed_unknown_kind_error",
		"panic_attempt_trace",
		"process_kill_restart_and_rescue",
		"candidate_insert_reference_work",
		"candidate_migrator_reference_runtime",
		"single_implementation_worker_outcomes",
		"snooze_once_metadata_transition",
		"transaction_abort_rollback_visibility",
		"transaction_commit_visibility",
		"transaction_rollback_visibility",
		"transactional_completion",
		"transactional_cross_language_cancel",
		"transactional_crud_commit_rollback",
		"transactional_queue_operations",
		"unique_hash_goldens",
	)
}

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

func verifyExternalTerminalCompletionRace(t *testing.T, worker, externalizer *adapter) {
	t.Helper()

	externalizer.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-completion-race", "instrumented": true, "max_workers": 1,
	}, nil)

	for index, testCase := range []struct {
		behavior      string
		expectsOutput bool
		externalState string
	}{
		{behavior: "barrier_output", expectsOutput: true, externalState: "completed"},
		{behavior: "barrier_output", expectsOutput: true, externalState: "discarded"},
		{behavior: "barrier_wait", externalState: "completed"},
	} {
		barrierName := fmt.Sprintf("completion-race-%s-%d", testCase.externalState, index)
		worker.call(t, "barrier_create", map[string]any{"name": barrierName}, nil)
		var inserted, running normalizedJob
		externalizer.call(t, "insert", map[string]any{
			"behavior": testCase.behavior, "message": barrierName,
		}, &inserted)
		externalizer.call(t, "wait", map[string]any{
			"id": inserted.ID, "states": []string{"running"},
		}, &running)

		var external normalizedJob
		externalizer.call(t, "raw_finalize", map[string]any{
			"id": inserted.ID,
			"metadata": map[string]any{
				"external": testCase.externalState,
				"shared":   "external",
			},
			"state": testCase.externalState,
		}, &external)
		require.Equal(t, testCase.externalState, external.State)
		require.NotNil(t, external.FinalizedAt)
		if testCase.externalState == "discarded" {
			require.Equal(t, []normalizedAttemptError{{
				At:      "2026-02-03T04:05:06.789Z",
				Attempt: 1,
				Error:   "external discard",
				Trace:   "external trace",
			}}, external.Errors)
		} else {
			require.Empty(t, external.Errors)
		}

		worker.call(t, "barrier_release", map[string]any{"name": barrierName}, nil)
		waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
			return len(stats.Events) == index+1
		})
		var completed normalizedJob
		externalizer.call(t, "get", map[string]any{"id": inserted.ID}, &completed)
		if testCase.expectsOutput {
			require.Equal(t, map[string]any{"race": "worker"}, completed.Metadata["output"])
		} else {
			require.NotContains(t, completed.Metadata, "output")
		}
		require.Equal(t, testCase.externalState, completed.State)
		require.Equal(t, external.FinalizedAt, completed.FinalizedAt)
		require.Equal(t, external.Errors, completed.Errors)
		require.Equal(t, testCase.externalState, completed.Metadata["external"])
		require.Equal(t, "external", completed.Metadata["shared"])
	}

	stats := waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
		return len(stats.Events) == 3
	})
	require.Equal(t, []string{"job_completed", "job_failed", "job_completed"}, stats.Events)
	worker.call(t, "stop", map[string]any{}, nil)
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

func verifyLeadershipRequestLifecycle(t *testing.T, first, second *adapter) {
	t.Helper()

	for _, pair := range []struct {
		leader    *adapter
		requester *adapter
	}{
		{leader: first, requester: second},
		{leader: second, requester: first},
	} {
		pair.leader.call(t, "reset", map[string]any{}, nil)
		pair.leader.call(t, "start", map[string]any{
			"client_id": pair.leader.name + "-resign-lifecycle", "max_workers": 1,
		}, nil)
		initial := waitForLeaderTerm(t, pair.leader, "")

		pair.requester.call(t, "request_resign", map[string]any{}, nil)
		afterDirect := waitForLeaderTerm(t, pair.leader, initial.ElectedAt)

		rollbackHandle := pair.requester.name + "-resign-rollback"
		pair.requester.call(t, "tx_begin", map[string]any{"handle": rollbackHandle}, nil)
		pair.requester.call(t, "request_resign", map[string]any{"handle": rollbackHandle}, nil)
		pair.requester.call(t, "tx_rollback", map[string]any{"handle": rollbackHandle}, nil)
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, afterDirect.ElectedAt, readLeader(t, pair.leader).ElectedAt,
			"rolled-back resignation changed the leadership term")

		commitHandle := pair.requester.name + "-resign-commit"
		pair.requester.call(t, "tx_begin", map[string]any{"handle": commitHandle}, nil)
		pair.requester.call(t, "request_resign", map[string]any{"handle": commitHandle}, nil)
		pair.requester.call(t, "tx_commit", map[string]any{"handle": commitHandle}, nil)
		_ = waitForLeaderTerm(t, pair.leader, afterDirect.ElectedAt)
		pair.leader.call(t, "stop", map[string]any{}, nil)
	}
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

func verifyBatchInsertion(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		require.Contains(t, pair.actor.callError(t, "insert_many", map[string]any{
			"jobs": []map[string]any{},
		}), "no jobs to insert")
		uniqueParams := map[string]any{
			"message": "typed batch duplicate " + pair.actor.name,
			"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
		}
		var existing normalizedJob
		pair.actor.call(t, "insert", uniqueParams, &existing)

		jobs := []map[string]any{
			{
				"message": "typed batch first " + pair.actor.name,
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 0},
					"priority": 2,
					"tags":     []string{"typed_batch_" + pair.actor.name},
				},
			},
			uniqueParams,
			{
				"message": "typed batch third " + pair.actor.name,
				"opts": map[string]any{
					"pending": true,
					"tags":    []string{"typed_batch_" + pair.actor.name},
				},
			},
		}
		var inserted struct {
			Results []normalizedInsertResult `json:"results"`
		}
		pair.actor.call(t, "insert_many", map[string]any{"jobs": jobs}, &inserted)
		require.Len(t, inserted.Results, 3)
		for _, result := range inserted.Results {
			require.NotNil(t, result.Job.Errors)
			require.Empty(t, result.Job.Errors)
		}
		require.False(t, inserted.Results[0].UniqueSkippedAsDuplicate)
		require.Equal(t, existing, inserted.Results[1].Job)
		require.True(t, inserted.Results[1].UniqueSkippedAsDuplicate)
		require.False(t, inserted.Results[2].UniqueSkippedAsDuplicate)
		require.Equal(t, "pending", inserted.Results[2].Job.State)

		var observed normalizedJob
		for _, result := range inserted.Results {
			observed = normalizedJob{}
			pair.observer.call(t, "get", map[string]any{"id": result.Job.ID}, &observed)
			require.Equal(t, result.Job, observed)
		}

		invalidTag := "invalid_batch_" + pair.actor.name
		require.NotEmpty(t, pair.actor.callError(t, "insert_many", map[string]any{"jobs": []map[string]any{
			{"message": "must roll back", "opts": map[string]any{"tags": []string{invalidTag}}},
			{"message": "invalid priority", "opts": map[string]any{"priority": 99}},
		}}))
		var invalidRows struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{invalidTag}}, &invalidRows)
		require.Empty(t, invalidRows.Jobs)

		verifyTransactionalBatchInsertion(t, pair.actor, pair.observer, false)
		verifyTransactionalBatchInsertion(t, pair.actor, pair.observer, true)
	}
}

func verifyLargeBatchInsertion(t *testing.T, adapters ...*adapter) {
	t.Helper()

	const batchSize = 6_000
	for _, actor := range adapters {
		actor.call(t, "reset", map[string]any{}, nil)
		jobs := make([]map[string]any, batchSize)
		for index := range jobs {
			jobs[index] = map[string]any{
				"message": fmt.Sprintf("large ordinary batch %s %d", actor.name, index),
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": index},
				},
			}
		}
		var inserted struct {
			Results []normalizedInsertResult `json:"results"`
		}
		actor.call(t, "insert_many", map[string]any{"jobs": jobs}, &inserted)
		require.Len(t, inserted.Results, batchSize)
		require.Equal(t, float64(0), inserted.Results[0].Job.Metadata["batch_index"])
		require.Equal(t, float64(batchSize-1), inserted.Results[batchSize-1].Job.Metadata["batch_index"])
	}
}

func verifyConcurrentUniqueConflicts(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	allStates := []string{
		"available",
		"cancelled",
		"completed",
		"discarded",
		"pending",
		"retryable",
		"running",
		"scheduled",
	}
	fixedScheduledAt := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	testCases := []struct {
		name string
		opts map[string]any
	}{
		{
			name: "by_args",
			opts: map[string]any{"unique": map[string]any{"by_args": true}},
		},
		{
			name: "by_period",
			opts: map[string]any{
				"scheduled_at": fixedScheduledAt,
				"unique":       map[string]any{"by_period_ms": 60_000},
			},
		},
		{
			name: "by_queue",
			opts: map[string]any{
				"queue":  "unique_queue",
				"unique": map[string]any{"by_queue": true},
			},
		},
		{
			name: "by_state",
			opts: map[string]any{"unique": map[string]any{"by_state": allStates}},
		},
	}

	for _, testCase := range testCases {
		for _, direction := range []struct {
			loser  *adapter
			winner *adapter
		}{
			{loser: candidateAdapter, winner: goAdapter},
			{loser: goAdapter, winner: candidateAdapter},
		} {
			loser, winner := direction.loser, direction.winner
			goAdapter.call(t, "reset", map[string]any{}, nil)
			winnerHandle := fmt.Sprintf("%s-%s-winner", testCase.name, winner.name)
			loserHandle := fmt.Sprintf("%s-%s-loser", testCase.name, loser.name)
			winner.call(t, "tx_begin", map[string]any{"handle": winnerHandle}, nil)
			loser.call(t, "tx_begin", map[string]any{"handle": loserHandle}, nil)

			params := map[string]any{
				"handle": winnerHandle,
				"job": map[string]any{
					"message": fmt.Sprintf("concurrent unique %s", testCase.name),
					"opts":    testCase.opts,
				},
			}
			var winnerJob normalizedJob
			winner.call(t, "tx_insert", params, &winnerJob)

			params["handle"] = loserHandle
			resultCh := make(chan struct {
				job normalizedJob
				err error
			}, 1)
			go func() {
				var job normalizedJob
				err := loser.callWithoutTest("tx_insert", params, &job)
				resultCh <- struct {
					job normalizedJob
					err error
				}{job: job, err: err}
			}()

			var (
				loserResult struct {
					job normalizedJob
					err error
				}
				waitedForWinner bool
			)
			select {
			case loserResult = <-resultCh:
			case <-time.After(100 * time.Millisecond):
				waitedForWinner = true
			}
			winner.call(t, "tx_commit", map[string]any{"handle": winnerHandle}, nil)

			if waitedForWinner {
				select {
				case loserResult = <-resultCh:
				case <-time.After(5 * time.Second):
					loser.call(t, "tx_rollback", map[string]any{"handle": loserHandle}, nil)
					t.Fatalf("%s unique insert remained blocked after %s committed (%s)", loser.name, winner.name, testCase.name)
				}
			}
			if loserResult.err != nil {
				loser.call(t, "tx_rollback", map[string]any{"handle": loserHandle}, nil)
			} else {
				loser.call(t, "tx_commit", map[string]any{"handle": loserHandle}, nil)
			}
			require.NoError(t, loserResult.err)
			require.Truef(t, waitedForWinner,
				"%s unique insert did not wait for %s's uncommitted conflict (%s)",
				loser.name, winner.name, testCase.name)
			require.Equal(t, winnerJob, loserResult.job)

			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			goAdapter.call(t, "list", map[string]any{}, &listed)
			require.Equal(t, []normalizedJob{winnerJob}, listed.Jobs)
		}
	}
}

func verifyHistoricalMigrations(t *testing.T, latest int, adapters ...*adapter) {
	t.Helper()

	type migrationResult struct {
		Applied  []int `json:"applied"`
		Existing []int `json:"existing"`
		Valid    bool  `json:"valid"`
	}
	expectedLatest := make([]int, latest)
	for index := range latest {
		expectedLatest[index] = index + 1
	}
	for initializerIndex, initializer := range adapters {
		upgrader := adapters[(initializerIndex+1)%len(adapters)]
		for version := 1; version <= latest; version++ {
			schema := fmt.Sprintf("river_conformance_history_%s_%d", initializer.name, version)
			var result migrationResult
			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": -1,
			}, &result)
			initializer.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema, "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			require.Equal(t, version == latest, result.Valid)

			upgrader.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema,
			}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			var inserted, observed normalizedJob
			upgrader.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("historical migration %d", version), "schema": schema,
			}, &inserted)
			initializer.call(t, "get", map[string]any{
				"id": inserted.ID, "schema": schema,
			}, &observed)
			require.Equal(t, inserted, observed)

			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			upgrader.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema,
			}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			upgrader.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)
		}
	}
}

func verifyDifferentialCRUD(t *testing.T, goAdapter, candidateAdapter *adapter, includeQueues bool) {
	t.Helper()

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		writerTag := "writer_" + pair.writer.name
		pair.writer.call(t, "reset", map[string]any{}, nil)
		var inserted, observed normalizedJob
		pair.writer.call(t, "insert", map[string]any{
			"message": "differential CRUD",
			"opts": map[string]any{
				"metadata": map[string]any{"writer": pair.writer.name},
				"priority": 3,
				"tags":     []string{"all_jobs", writerTag},
			},
		}, &inserted)
		pair.reader.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)

		listParams := map[string]any{
			"ids": []int64{inserted.ID}, "tags_all": []string{"all_jobs", writerTag},
		}
		var readerList, writerList struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.writer.call(t, "list", listParams, &writerList)
		pair.reader.call(t, "list", listParams, &readerList)
		require.Equal(t, writerList, readerList)
		require.Equal(t, []normalizedJob{inserted}, writerList.Jobs)

		var updated normalizedJob
		pair.reader.call(t, "update", map[string]any{
			"id": inserted.ID, "output": map[string]any{"updated_by": pair.reader.name},
		}, &updated)
		pair.writer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, updated, observed)

		var cancelled normalizedJob
		pair.writer.call(t, "cancel", map[string]any{"id": inserted.ID}, &cancelled)
		pair.reader.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, cancelled, observed)
		require.Equal(t, "cancelled", cancelled.State)

		var retried normalizedJob
		pair.reader.call(t, "retry", map[string]any{"id": inserted.ID}, &retried)
		pair.writer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, retried, observed)
		require.Equal(t, "available", retried.State)

		var deleted normalizedJob
		pair.writer.call(t, "delete", map[string]any{"id": inserted.ID}, &deleted)
		require.Equal(t, retried, deleted)
		require.Contains(t, pair.reader.callError(t, "get", map[string]any{"id": inserted.ID}), "not found")

		bulkIDs := make([]int64, 0, 2)
		for index := range 2 {
			var bulk normalizedJob
			pair.writer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("bulk delete %d", index),
			}, &bulk)
			bulkIDs = append(bulkIDs, bulk.ID)
		}
		var bulkDeleted struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.reader.call(t, "delete_many", map[string]any{"ids": bulkIDs}, &bulkDeleted)
		require.Len(t, bulkDeleted.Jobs, 2)
		for _, id := range bulkIDs {
			require.Contains(t, pair.writer.callError(t, "get", map[string]any{"id": id}), "not found")
		}
		for _, adapter := range []*adapter{pair.writer, pair.reader} {
			require.NotEmpty(t, adapter.callError(t, "delete_many", map[string]any{}))
		}

		paginationIDs := make([]int64, 0, 3)
		for index := range 3 {
			var paginationJob normalizedJob
			pair.writer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("pagination %d", index),
				"opts": map[string]any{
					"metadata":     map[string]any{"pagination_writer": pair.writer.name},
					"priority":     index + 1,
					"scheduled_at": fmt.Sprintf("2099-01-01T00:00:0%dZ", index+1),
					"tags":         []string{"pagination_jobs"},
				},
			}, &paginationJob)
			paginationIDs = append(paginationIDs, paginationJob.ID)
		}
		type jobPage struct {
			Cursor *string         `json:"cursor"`
			Jobs   []normalizedJob `json:"jobs"`
		}
		pageParams := func(after *string) map[string]any {
			params := map[string]any{
				"direction":  "desc",
				"limit":      2,
				"order_by":   "scheduled_at",
				"priorities": []int{1, 2, 3},
				"states":     []string{"scheduled"},
				"tags_all":   []string{"pagination_jobs"},
			}
			if includeQueues {
				params["metadata"] = map[string]any{"pagination_writer": pair.writer.name}
			}
			if after != nil {
				params["after"] = *after
			}
			return params
		}
		var readerPage, writerPage jobPage
		pair.reader.call(t, "list", pageParams(nil), &readerPage)
		pair.writer.call(t, "list", pageParams(nil), &writerPage)
		require.Equal(t, writerPage, readerPage)
		require.Len(t, writerPage.Jobs, 2)
		require.NotNil(t, writerPage.Cursor)
		require.Equal(t, paginationIDs[2], writerPage.Jobs[0].ID)
		require.Equal(t, paginationIDs[1], writerPage.Jobs[1].ID)

		var readerSecondPage, writerSecondPage jobPage
		pair.reader.call(t, "list", pageParams(writerPage.Cursor), &readerSecondPage)
		pair.writer.call(t, "list", pageParams(readerPage.Cursor), &writerSecondPage)
		require.Equal(t, writerSecondPage, readerSecondPage)
		require.Len(t, writerSecondPage.Jobs, 1)
		require.Equal(t, paginationIDs[0], writerSecondPage.Jobs[0].ID)
		var paginationDeleted struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.reader.call(t, "delete_many", map[string]any{"ids": paginationIDs}, &paginationDeleted)
		require.Len(t, paginationDeleted.Jobs, 3)

		if !includeQueues {
			continue
		}
		pair.writer.call(t, "start", map[string]any{
			"client_id": pair.writer.name + "-queue-crud", "max_workers": 1,
		}, nil)
		pair.writer.call(t, "stop", map[string]any{}, nil)
		var readerQueue, updatedQueue, writerQueue normalizedQueue
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &writerQueue)
		pair.reader.call(t, "queue_get", map[string]any{"name": "default"}, &readerQueue)
		require.Equal(t, writerQueue, readerQueue)
		pair.reader.call(t, "queue_update", map[string]any{
			"metadata": map[string]any{"updated_by": pair.reader.name}, "name": "default",
		}, &updatedQueue)
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &writerQueue)
		require.Equal(t, updatedQueue, writerQueue)
		var readerQueues, writerQueues struct {
			Queues []normalizedQueue `json:"queues"`
		}
		pair.reader.call(t, "queue_list", map[string]any{}, &readerQueues)
		pair.writer.call(t, "queue_list", map[string]any{}, &writerQueues)
		require.Equal(t, writerQueues, readerQueues)
	}
}

func verifyUnsafeInt64JobIDs(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const firstUnsafeID int64 = 9_007_199_254_740_993
	type jobPage struct {
		Cursor *string         `json:"cursor"`
		Jobs   []normalizedJob `json:"jobs"`
	}
	for pairIndex, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		ids := []int64{
			firstUnsafeID + int64(pairIndex*10),
			firstUnsafeID + int64(pairIndex*10) + 1,
		}
		for _, id := range ids {
			var inserted struct {
				ID int64 `json:"id"`
			}
			pair.writer.call(t, "raw_insert_exact_json", map[string]any{"id": id}, &inserted)
			require.Equal(t, id, inserted.ID)

			var observed normalizedJob
			pair.reader.call(t, "get", map[string]any{"id": id}, &observed)
			require.Equal(t, id, observed.ID)
		}

		listParams := func(after *string) map[string]any {
			params := map[string]any{
				"direction": "asc",
				"ids":       ids,
				"limit":     1,
				"order_by":  "id",
			}
			if after != nil {
				params["after"] = *after
			}
			return params
		}
		var readerFirst, writerFirst jobPage
		pair.reader.call(t, "list", listParams(nil), &readerFirst)
		pair.writer.call(t, "list", listParams(nil), &writerFirst)
		require.Equal(t, writerFirst, readerFirst)
		require.Equal(t, []int64{ids[0]}, normalizedJobIDs(writerFirst.Jobs))
		require.NotNil(t, writerFirst.Cursor)

		var readerSecond, writerSecond jobPage
		pair.reader.call(t, "list", listParams(writerFirst.Cursor), &readerSecond)
		pair.writer.call(t, "list", listParams(readerFirst.Cursor), &writerSecond)
		require.Equal(t, writerSecond, readerSecond)
		require.Equal(t, []int64{ids[1]}, normalizedJobIDs(writerSecond.Jobs))

		var cancelled, observed normalizedJob
		pair.reader.call(t, "cancel", map[string]any{"id": ids[0]}, &cancelled)
		pair.writer.call(t, "get", map[string]any{"id": ids[0]}, &observed)
		require.Equal(t, cancelled, observed)
		require.Equal(t, ids[0], cancelled.ID)
		require.Equal(t, "cancelled", cancelled.State)
	}
}

func normalizedJobIDs(jobs []normalizedJob) []int64 {
	ids := make([]int64, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}

func verifyJobRowRoundTrip(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		observer *adapter
	}{
		{inserter: goAdapter, observer: candidateAdapter},
		{inserter: candidateAdapter, observer: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var exactInserted struct {
			ID int64 `json:"id"`
		}
		pair.inserter.call(t, "raw_insert_exact_json", map[string]any{}, &exactInserted)
		var exactAtInserter, exactAtObserver struct {
			Decimal  string `json:"decimal"`
			Integer  string `json:"integer"`
			Negative string `json:"negative"`
		}
		pair.inserter.call(t, "raw_job_exact_json", map[string]any{"id": exactInserted.ID}, &exactAtInserter)
		pair.observer.call(t, "raw_job_exact_json", map[string]any{"id": exactInserted.ID}, &exactAtObserver)
		require.Equal(t, exactAtInserter, exactAtObserver)
		require.Equal(t, "0.12345678901234567890123456789", exactAtObserver.Decimal)
		require.Equal(t, "9223372036854775807", exactAtObserver.Integer)
		require.Equal(t, "-9223372036854775808", exactAtObserver.Negative)

		var inserted, observed normalizedJob
		pair.inserter.call(t, "raw_insert_full_row", map[string]any{}, &inserted)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)
		require.Equal(t, map[string]any{
			"nested": map[string]any{"enabled": true},
			"values": []any{float64(1), "two", nil},
		}, observed.Args)
		require.Equal(t, 3, observed.Attempt)
		require.NotNil(t, observed.AttemptedAt)
		require.Equal(t, "2026-01-02T03:04:06.123456Z", *observed.AttemptedAt)
		require.Equal(t, []string{"go-client", "candidate-client"}, observed.AttemptedBy)
		require.Equal(t, "2026-01-02T03:04:05.6789Z", observed.CreatedAt)
		require.Len(t, observed.Errors, 1)
		require.Equal(t, "2026-01-02T03:04:06.123456Z", observed.Errors[0].At)
		require.Equal(t, 3, observed.Errors[0].Attempt)
		require.Equal(t, "worker failed: escaped \"detail\"", observed.Errors[0].Error)
		require.Equal(t, "frame one\nframe two", observed.Errors[0].Trace)
		require.NotNil(t, observed.FinalizedAt)
		require.Equal(t, "2026-01-02T03:04:07.000001Z", *observed.FinalizedAt)
		require.Equal(t, "conformance_full_row", observed.Kind)
		require.Equal(t, 4, observed.MaxAttempts)
		require.Equal(t, map[string]any{
			"output":             map[string]any{"ok": true},
			"river:rescue_count": float64(2),
			"user":               "metadata",
		}, observed.Metadata)
		require.Equal(t, 2, observed.Priority)
		require.Equal(t, "priority_jobs", observed.Queue)
		require.Equal(t, "2026-01-02T03:04:05.999999Z", observed.ScheduledAt)
		require.Equal(t, "discarded", observed.State)
		require.Equal(t, []string{"alpha_tag", "beta_tag"}, observed.Tags)
		require.NotNil(t, observed.UniqueKey)
		require.Equal(t, strings.Repeat("ab", 32), *observed.UniqueKey)
		require.Equal(t, []string{
			"available", "completed", "pending", "retryable", "running", "scheduled",
		}, observed.UniqueStates)
	}
}

func verifyMixedUnknownKind(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	goAdapter.call(t, "reset", map[string]any{}, nil)
	var unknown normalizedJob
	goAdapter.call(t, "raw_insert_no_notify", map[string]any{
		"kind": "conformance_unregistered", "message": "must fail compatibly",
		"opts": map[string]any{"max_attempts": 1},
	}, &unknown)
	goAdapter.call(t, "start", map[string]any{
		"client_id": "go-skip-unknown", "max_workers": 1,
	}, nil)
	candidateAdapter.call(t, "start", map[string]any{
		"client_id": "candidate-skip-unknown", "max_workers": 1,
	}, nil)

	knownIDs := make([]int64, 0, 2)
	for _, inserter := range []*adapter{goAdapter, candidateAdapter} {
		var known normalizedJob
		inserter.call(t, "insert", map[string]any{
			"message": "known kind from " + inserter.name,
		}, &known)
		knownIDs = append(knownIDs, known.ID)
	}
	for _, id := range knownIDs {
		var completed normalizedJob
		candidateAdapter.call(t, "wait", map[string]any{"id": id}, &completed)
		require.Equal(t, "completed", completed.State)
		require.Equal(t, 1, completed.Attempt)
	}
	var observed normalizedJob
	candidateAdapter.call(t, "wait", map[string]any{
		"id": unknown.ID, "states": []string{"discarded"},
	}, &observed)
	require.Equal(t, "discarded", observed.State)
	require.Equal(t, 1, observed.Attempt)
	require.Len(t, observed.Errors, 1)
	require.Equal(t,
		"job kind is not registered in the client's Workers bundle: conformance_unregistered",
		observed.Errors[0].Error,
	)

	goAdapter.call(t, "stop", map[string]any{}, nil)
	candidateAdapter.call(t, "stop", map[string]any{}, nil)
}

func verifyNotificationWakeups(t *testing.T, goAdapter, candidateAdapter *adapter) {
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
			"client_id": pair.worker.name + "-notification-only", "fetch_poll_interval_ms": 60_000,
			"instrumented": true, "max_workers": 1,
		}, nil)

		startedAt := time.Now()
		var inserted normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"message": "cross-language insert notification",
		}, &inserted)
		pair.worker.call(t, "wait", map[string]any{"id": inserted.ID}, &inserted)
		require.Equal(t, "completed", inserted.State)
		require.Less(t, time.Since(startedAt), 5*time.Second)

		pair.controller.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
		var queue normalizedQueue
		waitForQueuePaused(t, pair.worker, "default", true, &queue)
		var paused normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"message": "cross-language queue notification while paused",
		}, &paused)
		time.Sleep(100 * time.Millisecond)
		pair.worker.call(t, "get", map[string]any{"id": paused.ID}, &paused)
		require.Equal(t, "available", paused.State)

		startedAt = time.Now()
		pair.controller.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
		waitForQueuePaused(t, pair.worker, "default", false, &queue)
		pair.worker.call(t, "wait", map[string]any{"id": paused.ID}, &paused)
		require.Equal(t, "completed", paused.State)
		require.Less(t, time.Since(startedAt), 5*time.Second)

		var cancellable normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"behavior": "cooperative_cancel", "message": "cross-language cancel notification",
		}, &cancellable)
		pair.worker.call(t, "wait", map[string]any{
			"id": cancellable.ID, "states": []string{"running"},
		}, &cancellable)
		startedAt = time.Now()
		pair.controller.call(t, "cancel", map[string]any{"id": cancellable.ID}, &cancellable)
		pair.worker.call(t, "wait", map[string]any{"id": cancellable.ID}, &cancellable)
		require.Equal(t, "cancelled", cancellable.State)
		require.Len(t, cancellable.Errors, 1)
		require.Equal(t, "JobCancelError: job cancelled remotely", cancellable.Errors[0].Error)
		require.Less(t, time.Since(startedAt), 5*time.Second)
		stats := waitForRuntimeStats(t, pair.worker, func(stats runtimeStats) bool {
			return slices.Contains(stats.Events, "job_cancelled")
		})
		require.NotContains(t, stats.Events, "job_failed")

		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

func verifyRemoteQueueSubscriptionEvents(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		controller *adapter
		observer   *adapter
	}{
		{controller: candidateAdapter, observer: goAdapter},
		{controller: goAdapter, observer: candidateAdapter},
	} {
		pair.observer.call(t, "reset", map[string]any{}, nil)
		pair.observer.call(t, "start", map[string]any{
			"client_id":              pair.observer.name + "-remote-queue-subscriber",
			"fetch_poll_interval_ms": 60_000,
			"instrumented":           true,
			"max_workers":            1,
		}, nil)

		var warmup normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"message": "activate remote queue subscriber",
		}, &warmup)
		pair.observer.call(t, "wait", map[string]any{"id": warmup.ID}, &warmup)
		require.Equal(t, "completed", warmup.State)

		pair.controller.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
		_ = waitForRuntimeStats(t, pair.observer, func(stats runtimeStats) bool {
			return countRuntimeEvent(stats, "queue_paused") == 1
		})
		pair.controller.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
		time.Sleep(500 * time.Millisecond)
		var stats runtimeStats
		pair.observer.call(t, "runtime_stats", map[string]any{}, &stats)
		require.Equal(t, 1, countRuntimeEvent(stats, "queue_paused"))
		require.Zero(t, countRuntimeEvent(stats, "queue_resumed"))

		pair.controller.call(t, "queue_resume", map[string]any{"name": "*"}, nil)
		_ = waitForRuntimeStats(t, pair.observer, func(stats runtimeStats) bool {
			return countRuntimeEvent(stats, "queue_resumed") == 1
		})
		pair.controller.call(t, "queue_resume", map[string]any{"name": "*"}, nil)
		time.Sleep(500 * time.Millisecond)
		pair.observer.call(t, "runtime_stats", map[string]any{}, &stats)
		require.Equal(t, 1, countRuntimeEvent(stats, "queue_paused"))
		require.Equal(t, 1, countRuntimeEvent(stats, "queue_resumed"))

		pair.observer.call(t, "stop", map[string]any{}, nil)
	}
}

func verifyTransactionalNotificationWakeups(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		controller *adapter
		worker     *adapter
	}{
		{controller: candidateAdapter, worker: goAdapter},
		{controller: goAdapter, worker: candidateAdapter},
	} {
		for _, method := range []string{"tx_insert_many", "tx_insert_many_fast"} {
			for _, commit := range []bool{false, true} {
				pair.worker.call(t, "reset", map[string]any{}, nil)
				pair.worker.call(t, "start", map[string]any{
					"client_id":              pair.worker.name + "-transaction-notification",
					"fetch_poll_interval_ms": 60_000,
					"max_workers":            2,
				}, nil)

				outcome := "rollback"
				if commit {
					outcome = "commit"
				}
				handle := fmt.Sprintf("notification-%s-%s-%s", pair.controller.name, method, outcome)
				tag := strings.ReplaceAll(handle, "-", "_")
				pair.controller.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
				jobs := []map[string]any{
					{"message": handle + " first", "opts": map[string]any{"tags": []string{tag}}},
					{"message": handle + " second", "opts": map[string]any{"tags": []string{tag}}},
				}
				var inserted struct {
					Count   int                      `json:"count"`
					Results []normalizedInsertResult `json:"results"`
				}
				pair.controller.call(t, method, map[string]any{
					"handle": handle, "jobs": jobs,
				}, &inserted)
				if method == "tx_insert_many_fast" {
					require.Equal(t, 2, inserted.Count)
				} else {
					require.Len(t, inserted.Results, 2)
				}

				var listed struct {
					Jobs []normalizedJob `json:"jobs"`
				}
				pair.worker.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
				require.Empty(t, listed.Jobs, "transactional batch became visible before commit")
				time.Sleep(100 * time.Millisecond)

				if commit {
					startedAt := time.Now()
					pair.controller.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
					waitForListedJobCount(t, pair.worker, map[string]any{
						"states": []string{"completed"}, "tags_all": []string{tag},
					}, 2)
					require.Less(t, time.Since(startedAt), 5*time.Second,
						"committed transactional insert did not wake a 60-second polling worker")
				} else {
					pair.controller.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
					for _, result := range inserted.Results {
						require.Contains(t,
							pair.worker.callError(t, "get", map[string]any{"id": result.Job.ID}),
							"not found",
						)
					}
					var probe normalizedJob
					pair.controller.call(t, "raw_insert_no_notify", map[string]any{
						"message": handle + " no-notify probe",
					}, &probe)
					time.Sleep(250 * time.Millisecond)
					pair.controller.call(t, "get", map[string]any{"id": probe.ID}, &probe)
					require.Equal(t, "available", probe.State,
						"rolled-back transactional notification woke the worker")
				}
				pair.worker.call(t, "stop", map[string]any{}, nil)
			}
		}
	}
}

func waitForQueuePaused(t *testing.T, observer *adapter, name string, paused bool, queue *normalizedQueue) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		observer.call(t, "queue_get", map[string]any{"name": name}, queue)
		if (queue.PausedAt != nil) == paused {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not observe queue %s paused=%t", observer.name, name, paused)
}

func verifyTransactionalCRUD(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		pair.actor.call(t, "start", map[string]any{
			"client_id": pair.actor.name + "-transactional-crud", "max_workers": 1,
		}, nil)
		pair.actor.call(t, "stop", map[string]any{}, nil)

		var queueBefore normalizedQueue
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &queueBefore)

		handle := pair.actor.name + "-transactional-crud-commit"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job": map[string]any{
				"message": "transactional CRUD",
				"opts": map[string]any{
					"metadata": map[string]any{"actor": pair.actor.name},
					"tags":     []string{"transactional_crud"},
				},
			},
		}, &inserted)
		require.Contains(t, pair.observer.callError(t, "get", map[string]any{"id": inserted.ID}), "not found")

		var transactionalJob normalizedJob
		pair.actor.call(t, "tx_get", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, inserted, transactionalJob)
		pair.actor.call(t, "tx_update", map[string]any{
			"handle": handle, "id": inserted.ID,
			"output": map[string]any{"updated_by": pair.actor.name},
		}, &transactionalJob)
		var transactionalJobs struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_list", map[string]any{
			"handle": handle, "ids": []int64{inserted.ID},
		}, &transactionalJobs)
		require.Equal(t, []normalizedJob{transactionalJob}, transactionalJobs.Jobs)

		pair.actor.call(t, "tx_cancel", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, "cancelled", transactionalJob.State)
		pair.actor.call(t, "tx_retry", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, "available", transactionalJob.State)

		var queueInTransaction normalizedQueue
		pair.actor.call(t, "tx_queue_update", map[string]any{
			"handle":   handle,
			"metadata": map[string]any{"updated_by": pair.actor.name},
			"name":     "default",
		}, &queueInTransaction)
		pair.actor.call(t, "tx_queue_pause", map[string]any{
			"handle": handle, "name": "default",
		}, nil)
		pair.actor.call(t, "tx_queue_get", map[string]any{
			"handle": handle, "name": "default",
		}, &queueInTransaction)
		require.NotNil(t, queueInTransaction.PausedAt)
		var queuesInTransaction struct {
			Queues []normalizedQueue `json:"queues"`
		}
		pair.actor.call(t, "tx_queue_list", map[string]any{
			"handle": handle,
		}, &queuesInTransaction)
		require.Contains(t, queuesInTransaction.Queues, queueInTransaction)

		var observedQueue normalizedQueue
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueBefore, observedQueue)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)

		var observedJob normalizedJob
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)

		handle = pair.actor.name + "-transactional-crud-rollback"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var deleted normalizedJob
		pair.actor.call(t, "tx_delete", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &deleted)
		require.Equal(t, transactionalJob, deleted)
		pair.actor.call(t, "tx_queue_resume", map[string]any{
			"handle": handle, "name": "default",
		}, nil)
		pair.actor.call(t, "tx_queue_get", map[string]any{
			"handle": handle, "name": "default",
		}, &observedQueue)
		require.Nil(t, observedQueue.PausedAt)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)

		bulkIDs := make([]int64, 0, 2)
		for index := range 2 {
			var bulk normalizedJob
			pair.actor.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("transactional bulk delete %d", index),
			}, &bulk)
			bulkIDs = append(bulkIDs, bulk.ID)
		}
		handle = pair.actor.name + "-transactional-bulk-delete"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var deletedMany struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_delete_many", map[string]any{
			"handle": handle, "ids": bulkIDs,
		}, &deletedMany)
		require.Len(t, deletedMany.Jobs, 2)
		for _, id := range bulkIDs {
			pair.observer.call(t, "get", map[string]any{"id": id}, &observedJob)
			require.Equal(t, id, observedJob.ID)
		}
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		for _, id := range bulkIDs {
			require.Contains(t, pair.observer.callError(t, "get", map[string]any{"id": id}), "not found")
		}
	}
}

func verifyCustomSchemas(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, testCase := range []struct {
		inserter *adapter
		migrator *adapter
		name     string
		observer *adapter
		worker   *adapter
	}{
		{
			inserter: candidateAdapter,
			migrator: goAdapter,
			name:     "river_conformance_go_migrated",
			observer: goAdapter,
			worker:   candidateAdapter,
		},
		{
			inserter: goAdapter,
			migrator: candidateAdapter,
			name:     "river_conformance_candidate_migrated",
			observer: candidateAdapter,
			worker:   goAdapter,
		},
	} {
		testCase.migrator.call(t, "migrate", map[string]any{"schema": testCase.name}, nil)
		testCase.migrator.call(t, "reset", map[string]any{"schema": testCase.name}, nil)

		var inserted, observed, worked normalizedJob
		testCase.inserter.call(t, "insert", map[string]any{
			"message": "custom schema", "schema": testCase.name,
		}, &inserted)
		testCase.observer.call(t, "get", map[string]any{
			"id": inserted.ID, "schema": testCase.name,
		}, &observed)
		require.Equal(t, inserted, observed)
		testCase.worker.call(t, "work", map[string]any{
			"id": inserted.ID, "schema": testCase.name,
		}, &worked)
		require.Equal(t, "completed", worked.State)
		testCase.observer.call(t, "get", map[string]any{
			"id": inserted.ID, "schema": testCase.name,
		}, &observed)
		require.Equal(t, worked, observed)
	}

	boundarySchema := strings.Repeat("s", 46)
	goAdapter.call(t, "migrate", map[string]any{"schema": boundarySchema}, nil)
	for _, current := range []*adapter{goAdapter, candidateAdapter} {
		var inserted normalizedJob
		current.call(t, "insert", map[string]any{
			"message": "maximum portable schema", "schema": boundarySchema,
		}, &inserted)
		require.Positive(t, inserted.ID)
		require.Contains(t, current.callError(t, "insert", map[string]any{
			"message": "schema too long", "schema": strings.Repeat("s", 47),
		}), "46")
		require.NotEmpty(t, current.callError(t, "insert", map[string]any{
			"message": "invalid schema", "schema": "river-invalid",
		}))
	}
}

func verifySingleImplementationRuntime(t *testing.T, adapter *adapter) {
	t.Helper()

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-runtime", "max_workers": 2,
	}, nil)
	adapter.call(t, "barrier_create", map[string]any{"name": "runtime"}, nil)
	var barrierInserted, barrierRunning, barrierWorked normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "barrier_wait", "message": "runtime",
	}, &barrierInserted)
	adapter.call(t, "wait", map[string]any{
		"id": barrierInserted.ID, "states": []string{"running"},
	}, &barrierRunning)
	require.Equal(t, "running", barrierRunning.State)
	adapter.call(t, "barrier_release", map[string]any{"name": "runtime"}, nil)
	adapter.call(t, "wait", map[string]any{"id": barrierInserted.ID}, &barrierWorked)
	require.Equal(t, "completed", barrierWorked.State)

	for _, testCase := range []struct {
		behavior    string
		maxAttempts int
		state       string
	}{
		{behavior: "cancel", state: "cancelled"},
		{behavior: "discard", maxAttempts: 1, state: "discarded"},
		{behavior: "error", maxAttempts: 1, state: "discarded"},
		{behavior: "panic", maxAttempts: 1, state: "discarded"},
	} {
		params := map[string]any{
			"behavior": testCase.behavior,
			"message":  testCase.behavior,
		}
		if testCase.maxAttempts > 0 {
			params["opts"] = map[string]any{"max_attempts": testCase.maxAttempts}
		}
		var inserted, worked normalizedJob
		adapter.call(t, "insert", params, &inserted)
		adapter.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
		require.Equal(t, testCase.state, worked.State, "%s behavior", testCase.behavior)
		if testCase.behavior == "panic" {
			require.NotEmpty(t, worked.Errors)
			require.NotEmpty(t, worked.Errors[len(worked.Errors)-1].Trace)
		}
	}

	var outputInserted, outputWorked normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "output", "message": "runtime output",
	}, &outputInserted)
	adapter.call(t, "wait", map[string]any{"id": outputInserted.ID}, &outputWorked)
	require.Equal(t, "completed", outputWorked.State)
	require.Equal(t, map[string]any{"message": "runtime output"}, outputWorked.Metadata["output"])

	var transactionalInserted, transactionalWorked normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "transactional_complete", "message": "transactional completion",
	}, &transactionalInserted)
	adapter.call(t, "wait", map[string]any{"id": transactionalInserted.ID}, &transactionalWorked)
	require.Equal(t, "completed", transactionalWorked.State)
	require.Equal(t, true, transactionalWorked.Metadata["transactional_completion"])

	var snoozeInserted, snoozeWorked normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "snooze_once", "duration_ms": 5, "message": "snooze",
	}, &snoozeInserted)
	adapter.call(t, "wait", map[string]any{"id": snoozeInserted.ID}, &snoozeWorked)
	require.Equal(t, "completed", snoozeWorked.State)
	require.Equal(t, 1, snoozeWorked.Attempt)

	adapter.call(t, "stop", map[string]any{}, nil)
}

type runtimeStats struct {
	ErrorHandlerCalls   int      `json:"error_handler_calls"`
	Events              []string `json:"events"`
	PeriodicStarts      int      `json:"periodic_starts"`
	ResumableFirstRuns  int      `json:"resumable_first_runs"`
	ResumableSecondRuns int      `json:"resumable_second_runs"`
	Trace               []string `json:"trace"`
}

func verifyAdvancedRuntime(t *testing.T, adapter *adapter) {
	t.Helper()

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-advanced-runtime", "instrumented": true,
		"max_workers": 2, "retry_delay_ms": 5,
	}, nil)

	var ordinary normalizedJob
	adapter.call(t, "insert", map[string]any{"message": "extension order"}, &ordinary)
	adapter.call(t, "wait", map[string]any{"id": ordinary.ID}, &ordinary)
	require.Equal(t, "completed", ordinary.State)

	var resumable normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "resumable", "message": "resumable", "opts": map[string]any{"max_attempts": 2},
	}, &resumable)
	adapter.call(t, "wait", map[string]any{"id": resumable.ID}, &resumable)
	require.Equal(t, "completed", resumable.State)
	require.Len(t, resumable.Errors, 1)
	require.Equal(t, "first", resumable.Metadata["river:resumable_step"])

	adapter.call(t, "queue_add", map[string]any{"max_workers": 1, "name": "dynamic"}, nil)
	adapter.call(t, "queue_add", map[string]any{"max_workers": 2, "name": "dynamic"}, nil)
	var dynamic normalizedJob
	adapter.call(t, "insert", map[string]any{
		"message": "dynamic queue", "opts": map[string]any{"queue": "dynamic"},
	}, &dynamic)
	adapter.call(t, "wait", map[string]any{"id": dynamic.ID}, &dynamic)
	require.Equal(t, "completed", dynamic.State)
	adapter.call(t, "queue_remove", map[string]any{"name": "dynamic"}, nil)

	adapter.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	waitForRuntimeStats(t, adapter, func(stats runtimeStats) bool {
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

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-periodic", "instrumented": true,
		"max_workers": 1, "periodic_run_on_start": true,
	}, nil)
	periodic := waitForListedJob(t, adapter, map[string]any{
		"metadata": map[string]any{"river:periodic_job_id": "conformance-periodic"},
	})
	adapter.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
	require.Equal(t, "completed", periodic.State)
	require.Equal(t, true, periodic.Metadata["periodic"])
	stats = waitForRuntimeStats(t, adapter, func(stats runtimeStats) bool {
		return stats.PeriodicStarts == 1
	})
	require.Equal(t, 1, stats.PeriodicStarts)
	adapter.call(t, "stop", map[string]any{}, nil)

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-error-handler", "error_handler_cancel": true,
		"instrumented": true, "max_workers": 1,
	}, nil)
	var handled normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "error", "message": "error handler cancellation",
		"opts": map[string]any{"max_attempts": 3},
	}, &handled)
	adapter.call(t, "wait", map[string]any{"id": handled.ID}, &handled)
	require.Equal(t, "cancelled", handled.State)
	require.Len(t, handled.Errors, 1)
	stats = waitForRuntimeStats(t, adapter, func(stats runtimeStats) bool {
		return stats.ErrorHandlerCalls == 1 && slices.Contains(stats.Events, "job_cancelled")
	})
	require.Equal(t, 1, stats.ErrorHandlerCalls)
	adapter.call(t, "stop", map[string]any{}, nil)
}

func verifyCompletionBurst(t *testing.T, adapter *adapter) {
	t.Helper()

	const jobCount = 6_000
	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-completion-burst", "max_workers": 1_000,
	}, nil)
	jobs := make([]map[string]any, jobCount)
	for index := range jobs {
		jobs[index] = map[string]any{"message": fmt.Sprintf("completion-burst-%d", index)}
	}
	var inserted struct {
		Count int `json:"count"`
	}
	adapter.call(t, "insert_many_fast", map[string]any{"jobs": jobs}, &inserted)
	require.Equal(t, jobCount, inserted.Count)

	deadline := time.Now().Add(20 * time.Second)
	completedCount := 0
	for time.Now().Before(deadline) {
		var result struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		adapter.call(t, "list", map[string]any{
			"limit": jobCount, "states": []string{"completed"},
		}, &result)
		completedCount = len(result.Jobs)
		if completedCount == jobCount {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, jobCount, completedCount)
	adapter.call(t, "stop", map[string]any{}, nil)
}

func verifyRefetchedAttemptCancellation(t *testing.T, worker, canceller *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-refetched-cancel", "max_workers": 1,
	}, nil)
	var job normalizedJob
	canceller.call(t, "insert", map[string]any{
		"behavior": "snooze_then_cancel", "duration_ms": 1, "message": "refetched cancellation",
	}, &job)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		worker.call(t, "get", map[string]any{"id": job.ID}, &job)
		if job.State == "running" && job.Metadata["snoozes"] != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, "running", job.State)
	require.NotNil(t, job.Metadata["snoozes"])
	canceller.call(t, "cancel", map[string]any{"id": job.ID}, &job)
	worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "cancelled", job.State)
	worker.call(t, "stop", map[string]any{}, nil)
}

func verifyTimeoutCancellation(t *testing.T, adapter *adapter) {
	t.Helper()

	adapter.call(t, "reset", map[string]any{}, nil)
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-timeout", "job_stuck_threshold_ms": 100,
		"job_timeout_ms": 20, "max_workers": 1,
	}, nil)
	var job normalizedJob
	adapter.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "timeout cancellation",
		"opts": map[string]any{"max_attempts": 1},
	}, &job)
	adapter.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "discarded", job.State)
	require.Len(t, job.Errors, 1)
	adapter.call(t, "stop", map[string]any{}, nil)
}

func verifyTransactionalBatchInsertion(t *testing.T, actor, observer *adapter, fast bool) {
	t.Helper()

	method := "tx_insert_many"
	mode := "typed"
	if fast {
		method = "tx_insert_many_fast"
		mode = "fast"
	} else {
		handle := "batch-empty-" + actor.name
		actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		require.Contains(t, actor.callError(t, method, map[string]any{
			"handle": handle,
			"jobs":   []map[string]any{},
		}), "no jobs to insert")
		actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
	}
	for _, commit := range []bool{false, true} {
		actor.call(t, "reset", map[string]any{}, nil)
		outcome := "rollback"
		if commit {
			outcome = "commit"
		}
		handle := fmt.Sprintf("batch-%s-%s-%s", actor.name, mode, outcome)
		tag := strings.ReplaceAll(handle, "-", "_")
		actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		jobs := []map[string]any{
			{
				"message": handle + " first",
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 0},
					"priority": 2,
					"tags":     []string{tag},
				},
			},
			{
				"message": handle + " second",
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 1},
					"priority": 3,
					"tags":     []string{tag},
				},
			},
		}
		var result struct {
			Count   int                      `json:"count"`
			Results []normalizedInsertResult `json:"results"`
		}
		actor.call(t, method, map[string]any{"handle": handle, "jobs": jobs}, &result)
		if fast {
			require.Equal(t, 2, result.Count)
		} else {
			require.Len(t, result.Results, 2)
			require.Equal(t, float64(0), result.Results[0].Job.Metadata["batch_index"])
			require.Equal(t, float64(1), result.Results[1].Job.Metadata["batch_index"])
		}

		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs)
		if commit {
			actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
			observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.Len(t, listed.Jobs, 2)
		} else {
			actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
			observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.Empty(t, listed.Jobs)
		}
	}
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

	deadline := time.Now().Add(5 * time.Second)
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

func verifyDeterministicControls(t *testing.T, repositoryRoot string, adapters ...*adapter) {
	t.Helper()

	var fixture struct {
		RetryCases []struct {
			ErrorCount      int    `json:"error_count"`
			ExpectedDelayNS uint64 `json:"expected_delay_ns"`
			JobID           int64  `json:"job_id"`
			Now             string `json:"now"`
			Seed            uint64 `json:"seed"`
		} `json:"retry_cases"`
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/fixtures/protocol_values.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &fixture))
	for _, testCase := range fixture.RetryCases {
		for _, adapter := range adapters {
			adapter.call(t, "clock_set", map[string]any{"now": testCase.Now}, nil)
			adapter.call(t, "rng_seed", map[string]any{"seed": testCase.Seed}, nil)
			var result struct {
				DelayNS uint64 `json:"delay_ns"`
			}
			adapter.call(t, "retry_delay", map[string]any{
				"error_count": testCase.ErrorCount,
				"job_id":      testCase.JobID,
			}, &result)
			require.Equal(t, testCase.ExpectedDelayNS, result.DelayNS, "%s adapter", adapter.name)
		}
	}
}

func (adapter *adapter) call(t *testing.T, method string, params any, result any) {
	t.Helper()

	adapter.nextID++
	request := map[string]any{
		"id":      adapter.nextID,
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	_, err = adapter.input.Write(append(encoded, '\n'))
	require.NoErrorf(t, err, "%s adapter stderr: %s", adapter.name, adapter.stderr.String())
	require.Truef(t, adapter.output.Scan(), "%s adapter stopped: %s", adapter.name, adapter.stderr.String())

	var response rpcResponse
	require.NoError(t, json.Unmarshal(adapter.output.Bytes(), &response))
	require.Equal(t, adapter.nextID, response.ID)
	if response.Error != nil {
		t.Fatalf("%s adapter %s failed (%d): %s\nstderr: %s", adapter.name, method, response.Error.Code, response.Error.Message, adapter.stderr.String())
	}
	if result != nil {
		require.NoError(t, json.Unmarshal(response.Result, result))
	}
}

// callWithoutTest performs a serialized adapter call without invoking testing.T
// methods, so a deliberately blocking request can run in a helper goroutine.
func (adapter *adapter) callWithoutTest(method string, params any, result any) error {
	adapter.nextID++
	requestID := adapter.nextID
	request := map[string]any{
		"id":      requestID,
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if _, err := adapter.input.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write %s adapter request: %w", adapter.name, err)
	}
	if !adapter.output.Scan() {
		return fmt.Errorf("%s adapter stopped: %s", adapter.name, adapter.stderr.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(adapter.output.Bytes(), &response); err != nil {
		return err
	}
	if response.ID != requestID {
		return fmt.Errorf("%s adapter response ID %d, expected %d", adapter.name, response.ID, requestID)
	}
	if response.Error != nil {
		return fmt.Errorf("%s adapter %s failed (%d): %s", adapter.name, method, response.Error.Code, response.Error.Message)
	}
	if result != nil {
		if err := json.Unmarshal(response.Result, result); err != nil {
			return err
		}
	}
	return nil
}

func (adapter *adapter) kill(t *testing.T) {
	t.Helper()

	adapter.expectedExitError = true
	require.NoError(t, adapter.command.Process.Kill())
}

func (adapter *adapter) callError(t *testing.T, method string, params any) string {
	t.Helper()

	response := adapter.callResponse(t, method, params)
	require.NotNil(t, response.Error, "%s adapter %s unexpectedly succeeded", adapter.name, method)
	return response.Error.Message
}

func (adapter *adapter) callResponse(t *testing.T, method string, params any) rpcResponse {
	t.Helper()

	adapter.nextID++
	request := map[string]any{
		"id":      adapter.nextID,
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	_, err = adapter.input.Write(append(encoded, '\n'))
	require.NoErrorf(t, err, "%s adapter stderr: %s", adapter.name, adapter.stderr.String())
	require.Truef(t, adapter.output.Scan(), "%s adapter stopped: %s", adapter.name, adapter.stderr.String())

	var response rpcResponse
	require.NoError(t, json.Unmarshal(adapter.output.Bytes(), &response))
	require.Equal(t, adapter.nextID, response.ID)
	return response
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
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

func conformanceCandidateSpec(t *testing.T, root string, release bool) adapterSpec {
	t.Helper()

	encoded := os.Getenv("RIVER_CONFORMANCE_CANDIDATE")
	descriptorPath := os.Getenv("RIVER_CONFORMANCE_CANDIDATE_FILE")
	require.False(t, encoded != "" && descriptorPath != "",
		"set only one of RIVER_CONFORMANCE_CANDIDATE or RIVER_CONFORMANCE_CANDIDATE_FILE")

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
		//nolint:gosec // The caller explicitly selects a local candidate descriptor.
		descriptor, err = os.ReadFile(descriptorPath)
		require.NoError(t, err)
	}

	var spec adapterSpec
	require.NoError(t, json.Unmarshal(descriptor, &spec))
	require.NotEmpty(t, spec.ApplicationName)
	require.NotEmpty(t, spec.Command)
	require.NotEmpty(t, spec.Implementation)
	if release && len(spec.ReleaseCommand) > 0 {
		spec.Command = slices.Clone(spec.ReleaseCommand)
	}
	if len(spec.RestartCommand) == 0 {
		spec.RestartCommand = slices.Clone(spec.Command)
	}
	return spec
}

func startAdapterCommand(t *testing.T, root, databaseURL, name string, command []string) *adapter {
	t.Helper()

	return startAdapterCommandForBackend(t, root, databaseURL, "postgres", name, command)
}

func startAdapterCommandForBackend(
	t *testing.T,
	root, databaseURL, databaseKind, name string,
	command []string,
) *adapter {
	t.Helper()

	require.NotEmpty(t, command)
	return startAdapterForProfile(t, root, databaseURL, databaseKind, "", name, command[0], command[1:]...)
}

func startAdapterCommandForProfile(
	t *testing.T,
	root, databaseURL, databaseKind, profile, name string,
	command []string,
) *adapter {
	t.Helper()

	require.NotEmpty(t, command)
	return startAdapterForProfile(
		t, root, databaseURL, databaseKind, profile, name, command[0], command[1:]...,
	)
}

func startAdapter(t *testing.T, root, databaseURL, name, executable string, args ...string) *adapter {
	t.Helper()

	return startAdapterForBackend(t, root, databaseURL, "postgres", name, executable, args...)
}

func startAdapterForBackend(
	t *testing.T,
	root, databaseURL, databaseKind, name, executable string,
	args ...string,
) *adapter {
	t.Helper()
	return startAdapterForProfile(t, root, databaseURL, databaseKind, "", name, executable, args...)
}

func startAdapterForProfile(
	t *testing.T,
	root, databaseURL, databaseKind, profile, name, executable string,
	args ...string,
) *adapter {
	t.Helper()

	command := exec.Command(executable, args...)
	command.Dir = root
	command.Env = append(
		os.Environ(),
		"RIVER_CONFORMANCE_DATABASE_KIND="+databaseKind,
		"RIVER_CONFORMANCE_DATABASE_URL="+databaseURL,
	)
	if profile != "" {
		command.Env = append(command.Env, "RIVER_CONFORMANCE_PROFILE="+profile)
	}
	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, err := command.StdoutPipe()
	require.NoError(t, err)
	adapter := &adapter{
		command: command,
		input:   input,
		name:    name,
		output:  bufio.NewScanner(output),
	}
	adapter.output.Buffer(make([]byte, 64*1024), 4*1024*1024)
	command.Stderr = &adapter.stderr
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if err := adapter.input.Close(); err != nil && !adapter.expectedExitError {
			t.Errorf("%s adapter stdin close: %v", name, err)
		}
		if err := adapter.command.Wait(); err != nil && !adapter.expectedExitError {
			t.Errorf("%s adapter exit: %v\nstderr: %s", name, err, adapter.stderr.String())
		}
	})
	return adapter
}

func Example_protocolRequest() {
	fmt.Println(`{"id":1,"jsonrpc":"2.0","method":"handshake","params":{}}`)
	// Output: {"id":1,"jsonrpc":"2.0","method":"handshake","params":{}}
}
