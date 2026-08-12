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
	"strings"
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
	stderr            bytes.Buffer
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

type normalizedQueue struct {
	CreatedAt string         `json:"created_at"`
	Metadata  map[string]any `json:"metadata"`
	Name      string         `json:"name"`
	PausedAt  *string        `json:"paused_at"`
	UpdatedAt string         `json:"updated_at"`
}

func TestMixedGoRustConformance(t *testing.T) {
	// The adapters intentionally share one externally supplied disposable
	// database, so this integration test cannot run in parallel with other
	// conformance tiers.

	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	repositoryRoot := repoRoot(t)
	goAdapter := startAdapter(t, repositoryRoot, databaseURL, "go", "go", "run", "./internal/cmd/riverconformanceadapter")
	rustAdapter := startAdapter(t, repositoryRoot, databaseURL, "rust", "cargo", "run", "--quiet", "--manifest-path", "rust/Cargo.toml", "-p", "riverqueue-conformance")

	var goHandshake, rustHandshake struct {
		AdapterVersion        int            `json:"adapter_version"`
		Capabilities          []string       `json:"capabilities"`
		Implementation        string         `json:"implementation"`
		ImplementationVersion string         `json:"implementation_version"`
		MigrationLines        map[string]int `json:"migration_lines"`
		ProtocolRevision      int            `json:"protocol_revision"`
	}
	goAdapter.call(t, "handshake", map[string]any{}, &goHandshake)
	rustAdapter.call(t, "handshake", map[string]any{}, &rustHandshake)
	var manifest struct {
		Capabilities map[string]string `json:"capabilities"`
		Go           struct {
			Version string `json:"version"`
		} `json:"go"`
		Migration struct {
			Latest int    `json:"latest"`
			Line   string `json:"line"`
		} `json:"migration"`
		ProtocolRevision int `json:"protocol_revision"`
		Rust             struct {
			Version string `json:"version"`
		} `json:"rust"`
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
	require.Equal(t, "rust", rustHandshake.Implementation)
	require.Positive(t, goHandshake.AdapterVersion)
	require.Equal(t, goHandshake.AdapterVersion, rustHandshake.AdapterVersion)
	require.Equal(t, manifest.Go.Version, goHandshake.ImplementationVersion)
	require.Equal(t, manifest.Rust.Version, rustHandshake.ImplementationVersion)
	require.Equal(t, manifest.ProtocolRevision, goHandshake.ProtocolRevision)
	require.Equal(t, goHandshake.ProtocolRevision, rustHandshake.ProtocolRevision)
	require.Equal(t, map[string]int{manifest.Migration.Line: manifest.Migration.Latest}, goHandshake.MigrationLines)
	require.Equal(t, goHandshake.MigrationLines, rustHandshake.MigrationLines)
	require.ElementsMatch(t, expectedCapabilities, goHandshake.Capabilities)
	require.ElementsMatch(t, goHandshake.Capabilities, rustHandshake.Capabilities)
	verifyDeterministicControls(t, repositoryRoot, goAdapter, rustAdapter)

	goAdapter.call(t, "migrate", map[string]any{}, nil)
	goAdapter.call(t, "reset", map[string]any{}, nil)

	var goInserted normalizedJob
	goAdapter.call(t, "insert", map[string]any{"message": "Go to Rust"}, &goInserted)
	require.Equal(t, "available", goInserted.State)
	require.Equal(t, "conformance_echo", goInserted.Kind)

	var rustObserved normalizedJob
	rustAdapter.call(t, "get", map[string]any{"id": goInserted.ID}, &rustObserved)
	require.Equal(t, goInserted, rustObserved)
	rustAdapter.call(t, "work", map[string]any{"id": goInserted.ID}, &rustObserved)
	require.Equal(t, "completed", rustObserved.State)
	require.Equal(t, 1, rustObserved.Attempt)
	require.Equal(t, []string{"rust-conformance-adapter"}, rustObserved.AttemptedBy)

	rustAdapter.call(t, "reset", map[string]any{}, nil)
	rustAdapter.call(t, "migrate", map[string]any{}, nil)
	var rustInserted normalizedJob
	rustAdapter.call(t, "insert", map[string]any{"message": "Rust to Go"}, &rustInserted)
	require.Equal(t, "available", rustInserted.State)

	var goObserved normalizedJob
	goAdapter.call(t, "get", map[string]any{"id": rustInserted.ID}, &goObserved)
	require.Equal(t, rustInserted, goObserved)
	goAdapter.call(t, "work", map[string]any{"id": rustInserted.ID}, &goObserved)
	require.Equal(t, "completed", goObserved.State)
	require.Equal(t, 1, goObserved.Attempt)
	require.Equal(t, []string{"go-conformance-adapter"}, goObserved.AttemptedBy)

	verifyCustomSchemas(t, goAdapter, rustAdapter)
	verifyDifferentialCRUD(t, goAdapter, rustAdapter)
	verifyJobRowRoundTrip(t, goAdapter, rustAdapter)
	verifyMixedUnknownKind(t, goAdapter, rustAdapter)
	verifyTransactionalCRUD(t, goAdapter, rustAdapter)
	verifySingleImplementationRuntime(t, goAdapter)
	verifySingleImplementationRuntime(t, rustAdapter)

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
	require.Contains(t, rustAdapter.callError(t, "get", map[string]any{"id": goTxInserted.ID}), "not found")
	goAdapter.call(t, "tx_commit", map[string]any{"handle": "go-commit"}, nil)
	rustAdapter.call(t, "get", map[string]any{"id": goTxInserted.ID}, &txObserved)
	require.Equal(t, goTxInserted, txObserved)

	rustAdapter.call(t, "tx_begin", map[string]any{"handle": "rust-rollback"}, nil)
	var rustTxInserted normalizedJob
	rustAdapter.call(t, "tx_insert", map[string]any{
		"handle": "rust-rollback",
		"job":    map[string]any{"message": "transaction rollback"},
	}, &rustTxInserted)
	rustAdapter.call(t, "tx_get", map[string]any{"handle": "rust-rollback", "id": rustTxInserted.ID}, &txObserved)
	require.Equal(t, rustTxInserted, txObserved)
	rustAdapter.call(t, "tx_rollback", map[string]any{"handle": "rust-rollback"}, nil)
	require.Contains(t, goAdapter.callError(t, "get", map[string]any{"id": rustTxInserted.ID}), "not found")

	var cancellable normalizedJob
	goAdapter.call(t, "insert", map[string]any{"message": "transactional cancellation"}, &cancellable)
	rustAdapter.call(t, "tx_begin", map[string]any{"handle": "rust-cancel"}, nil)
	rustAdapter.call(t, "tx_cancel", map[string]any{"handle": "rust-cancel", "id": cancellable.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)
	goAdapter.call(t, "get", map[string]any{"id": cancellable.ID}, &txObserved)
	require.Equal(t, "available", txObserved.State)
	rustAdapter.call(t, "tx_commit", map[string]any{"handle": "rust-cancel"}, nil)
	goAdapter.call(t, "get", map[string]any{"id": cancellable.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)

	for _, transactionAdapter := range []*adapter{goAdapter, rustAdapter} {
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
	rustAdapter.call(t, "insert_many_fast", map[string]any{"jobs": []map[string]any{
		{"message": "rust fast one", "opts": map[string]any{"tags": []string{"fast-rust"}}},
		{"message": "rust fast two", "opts": map[string]any{"tags": []string{"fast-rust"}}},
		{"message": "rust fast pending", "opts": map[string]any{"pending": true, "tags": []string{"fast-rust"}}},
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
	rustAdapter.call(t, "list", map[string]any{"kinds": []string{"conformance_echo"}}, &listed)
	require.Len(t, listed.Jobs, 5)

	uniqueParams := map[string]any{
		"message": "cross-language unique",
		"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
	}
	var uniqueGo, uniqueRust normalizedJob
	goAdapter.call(t, "insert", uniqueParams, &uniqueGo)
	rustAdapter.call(t, "insert", uniqueParams, &uniqueRust)
	require.Equal(t, uniqueGo, uniqueRust)

	goAdapter.call(t, "start", map[string]any{
		"client_id": "go-mixed-worker", "max_workers": 4,
	}, nil)
	rustAdapter.call(t, "start", map[string]any{
		"client_id": "rust-mixed-worker", "max_workers": 4,
	}, nil)
	firstLeader := waitForLeader(t, goAdapter, "")
	firstTerm := readLeader(t, goAdapter)
	goAdapter.call(t, "request_resign", map[string]any{}, nil)
	secondTerm := waitForLeaderTerm(t, goAdapter, firstTerm.ElectedAt)
	rustAdapter.call(t, "request_resign", map[string]any{}, nil)
	thirdTerm := waitForLeaderTerm(t, goAdapter, secondTerm.ElectedAt)

	var leaderAdapter, followerAdapter *adapter
	var leaderID, followerID string
	if thirdTerm.LeaderID == "go-mixed-worker" {
		leaderAdapter, leaderID = goAdapter, "go-mixed-worker"
		followerAdapter, followerID = rustAdapter, "rust-mixed-worker"
	} else {
		leaderAdapter, leaderID = rustAdapter, "rust-mixed-worker"
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
	waitForListener(t, rustAdapter)
	for _, adapter := range []*adapter{goAdapter, rustAdapter} {
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
		"application_name": "river-conformance-rust",
	}, &disconnectedApplication)
	require.GreaterOrEqual(t, disconnectedApplication.Count, 1)
	rustAdapter.call(t, "fault_disconnect_application", map[string]any{
		"application_name": "river-conformance-go",
	}, &disconnectedApplication)
	require.GreaterOrEqual(t, disconnectedApplication.Count, 1)
	waitForListener(t, goAdapter)
	waitForListener(t, rustAdapter)
	time.Sleep(500 * time.Millisecond)

	var notificationLost normalizedJob
	goAdapter.call(t, "raw_insert_no_notify", map[string]any{"message": "poll recovery"}, &notificationLost)
	rustAdapter.call(t, "wait", map[string]any{"id": notificationLost.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)

	var competitionIDs []int64
	for i := range 40 {
		var inserted normalizedJob
		adapter := goAdapter
		if i%2 == 1 {
			adapter = rustAdapter
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
	require.ElementsMatch(t, []string{"go-mixed-worker", "rust-mixed-worker"}, mapKeys(workersSeen))

	var pausedJob normalizedJob
	goAdapter.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	time.Sleep(100 * time.Millisecond)
	rustAdapter.call(t, "insert", map[string]any{"message": "paused cross-language"}, &pausedJob)
	time.Sleep(100 * time.Millisecond)
	rustAdapter.call(t, "get", map[string]any{"id": pausedJob.ID}, &txObserved)
	require.Equal(t, "available", txObserved.State)
	rustAdapter.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
	goAdapter.call(t, "wait", map[string]any{"id": pausedJob.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)

	var remoteCancel normalizedJob
	goAdapter.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "remote cancellation",
	}, &remoteCancel)
	goAdapter.call(t, "wait", map[string]any{"id": remoteCancel.ID, "states": []string{"running"}}, &txObserved)
	rustAdapter.call(t, "cancel", map[string]any{"id": remoteCancel.ID}, &txObserved)
	goAdapter.call(t, "wait", map[string]any{"id": remoteCancel.ID}, &txObserved)
	require.Equal(t, "cancelled", txObserved.State)

	goAdapter.call(t, "stop", map[string]any{}, nil)
	rustAdapter.call(t, "stop", map[string]any{}, nil)

	goAdapter.call(t, "reset", map[string]any{}, nil)
	rustBinary := filepath.Join(repositoryRoot, "rust", "target", "debug", "riverqueue-conformance")
	stuck := startAdapter(t, repositoryRoot, databaseURL, "rust-stuck", rustBinary)
	stuck.call(t, "start", map[string]any{
		"client_id": "rust-stuck-worker", "max_workers": 1, "queue": "ignored",
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
	crashing := startAdapter(t, repositoryRoot, databaseURL, "rust-crashing", rustBinary)
	crashing.call(t, "start", map[string]any{
		"client_id": "rust-crashing-worker", "max_workers": 1,
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

	recovery := startAdapter(t, repositoryRoot, databaseURL, "rust-recovery", rustBinary)
	recovery.call(t, "start", map[string]any{
		"client_id":             "rust-recovery-worker",
		"elect_interval_ms":     20,
		"max_workers":           1,
		"rescue_after_ms":       500,
		"rescuer_interval_ms":   20,
		"scheduler_interval_ms": 20,
	}, nil)
	recovery.call(t, "wait", map[string]any{"id": crashJob.ID}, &txObserved)
	require.Equal(t, "completed", txObserved.State)
	require.Equal(t, 2, txObserved.Attempt)
	require.Equal(t, []string{"rust-crashing-worker", "rust-recovery-worker"}, txObserved.AttemptedBy)
	recovery.call(t, "stop", map[string]any{}, nil)
}

func verifyDifferentialCRUD(t *testing.T, goAdapter, rustAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: rustAdapter, writer: goAdapter},
		{reader: goAdapter, writer: rustAdapter},
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
				"metadata":   map[string]any{"pagination_writer": pair.writer.name},
				"order_by":   "scheduled_at",
				"priorities": []int{1, 2, 3},
				"states":     []string{"scheduled"},
				"tags_all":   []string{"pagination_jobs"},
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

func verifyJobRowRoundTrip(t *testing.T, goAdapter, rustAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		observer *adapter
	}{
		{inserter: goAdapter, observer: rustAdapter},
		{inserter: rustAdapter, observer: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
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
		require.Equal(t, []string{"go-client", "rust-client"}, observed.AttemptedBy)
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

func verifyMixedUnknownKind(t *testing.T, goAdapter, rustAdapter *adapter) {
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
	rustAdapter.call(t, "start", map[string]any{
		"client_id": "rust-skip-unknown", "max_workers": 1,
	}, nil)

	knownIDs := make([]int64, 0, 2)
	for _, inserter := range []*adapter{goAdapter, rustAdapter} {
		var known normalizedJob
		inserter.call(t, "insert", map[string]any{
			"message": "known kind from " + inserter.name,
		}, &known)
		knownIDs = append(knownIDs, known.ID)
	}
	for _, id := range knownIDs {
		var completed normalizedJob
		rustAdapter.call(t, "wait", map[string]any{"id": id}, &completed)
		require.Equal(t, "completed", completed.State)
		require.Equal(t, 1, completed.Attempt)
	}
	var observed normalizedJob
	rustAdapter.call(t, "wait", map[string]any{
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
	rustAdapter.call(t, "stop", map[string]any{}, nil)
}

func verifyTransactionalCRUD(t *testing.T, goAdapter, rustAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: rustAdapter},
		{actor: rustAdapter, observer: goAdapter},
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

func verifyCustomSchemas(t *testing.T, goAdapter, rustAdapter *adapter) {
	t.Helper()

	for _, testCase := range []struct {
		inserter *adapter
		migrator *adapter
		name     string
		observer *adapter
		worker   *adapter
	}{
		{
			inserter: rustAdapter,
			migrator: goAdapter,
			name:     "river_conformance_go_migrated",
			observer: goAdapter,
			worker:   rustAdapter,
		},
		{
			inserter: goAdapter,
			migrator: rustAdapter,
			name:     "river_conformance_rust_migrated",
			observer: rustAdapter,
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

func startAdapter(t *testing.T, root, databaseURL, name, executable string, args ...string) *adapter {
	t.Helper()

	command := exec.Command(executable, args...)
	command.Dir = root
	command.Env = append(os.Environ(), "RIVER_CONFORMANCE_DATABASE_URL="+databaseURL)
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
